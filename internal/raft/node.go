// Package raft is a pure, deterministic Raft state machine (spec §5.4), in the
// style of etcd/raft but written from Figure 2 of the Raft paper.
//
// A Node performs no I/O, starts no goroutines and reads no clocks. The driver
// (internal/partition, or the rafttest harness) feeds it inputs — Tick(),
// Step(msg), Propose*/ReadIndex/TransferLeadership — and collects its outputs
// with Ready(), performs the I/O, and calls Advance().
//
// One Raft entry is one Kafka RecordBatch; the entry's Raft index is the
// batch's last offset, so indices are not dense (spec §5.4.2, see Entry).
//
// # The Ready/Advance contract
//
// Between a call to Ready() and the matching Advance() the driver must not
// call any other Node method (Step, Tick, Propose, ...); doing so panics. For
// each Ready the driver must, in this order:
//
//  1. If rd.Snapshot != nil: install it durably (LogStore.ApplySnapshot) and
//     restore the application state from it.
//  2. If rd.HardState != nil: persist it. When rd.MustSync is true (Term or
//     Vote changed) it MUST be durable before any message in rd.Messages is
//     sent. A Commit-only change may be persisted lazily or skipped.
//  3. If rd.TruncateFrom != 0: LogStore.TruncateSuffix(rd.TruncateFrom).
//  4. LogStore.Append(rd.Entries).
//  5. Synchronous durability (default): make 3–4 durable before sending
//     rd.Messages. With Config.AsyncDurability the messages may be sent once
//     steps 1–2 are durable; the Node withholds every acknowledgement that
//     depends on not-yet-durable entries until ReportDurable covers them, and
//     the leader counts itself toward commit only up to its durable index.
//  6. Send rd.Messages (never blocking; dropping is fine).
//  7. Apply rd.CommittedEntries in order.
//  8. Serve reads for rd.ReadStates once applied >= ReadState.Index.
//  9. Call Advance(rd).
package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
)

// StateType is the role of a node.
type StateType uint8

// Node roles.
const (
	Follower StateType = iota
	PreCandidate
	Candidate
	Leader
)

func (s StateType) String() string {
	switch s {
	case Follower:
		return "FOLLOWER"
	case PreCandidate:
		return "PRECANDIDATE"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	}
	return fmt.Sprintf("StateType(%d)", uint8(s))
}

// Errors returned by Node methods.
var (
	// ErrNotLeader is matched (errors.Is) by every *NotLeaderError.
	ErrNotLeader = errors.New("raft: not leader")
	// ErrProposalDropped is returned while a leadership transfer is in progress.
	ErrProposalDropped = errors.New("raft: proposal dropped (leadership transfer in progress)")
	// ErrUnknownPeer is returned by Step for messages from non-members.
	ErrUnknownPeer = errors.New("raft: message from unknown peer")
	// ErrWrongGroup / ErrWrongRecipient: misrouted messages.
	ErrWrongGroup     = errors.New("raft: message for another group")
	ErrWrongRecipient = errors.New("raft: message for another node")
	// ErrInvalidEntries is returned for a MsgApp whose entries violate the
	// contiguity/term invariants (corruption or a buggy peer); it is dropped.
	ErrInvalidEntries = errors.New("raft: invalid entries in MsgApp")
)

// NotLeaderError is returned for operations that require leadership.
// Leader is the current leader hint (None if unknown), for Kafka's
// NOT_LEADER_OR_FOLLOWER + metadata refresh.
type NotLeaderError struct {
	Leader NodeID
	Term   uint64
}

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("raft: not leader (leader hint %d, term %d)", e.Leader, e.Term)
}

// Is makes errors.Is(err, ErrNotLeader) true.
func (e *NotLeaderError) Is(target error) bool { return target == ErrNotLeader }

// Logger receives debug lines; nil disables logging.
type Logger func(format string, args ...any)

// Config configures a Node. Zero values take the documented defaults.
type Config struct {
	ID    NodeID
	Group uint64   // stamped on every outgoing message
	Peers []NodeID // all voters, including ID (static membership)
	Store LogStore
	// HardState is the state loaded from the HardStateStore at startup.
	HardState HardState
	// Applied is the last index the application has applied (restart). The
	// Node returns committed entries after max(Applied, snapshot index).
	Applied uint64

	ElectionTick  int // default 10; randomized timeout in [ElectionTick, 2*ElectionTick)
	HeartbeatTick int // default 1; must be < ElectionTick
	PreVote       bool
	CheckQuorum   bool

	MaxSizePerMsg            uint64 // entry bytes per MsgApp, default 1 MiB
	MaxInflightMsgs          int    // pipelined MsgApps per follower, default 64
	MaxInflightBytes         uint64 // pipelined bytes per follower, default 4 MiB (0 → default)
	MaxCommittedSizePerReady uint64 // default 16 MiB

	// AsyncDurability: Advance only means "written"; the driver reports
	// durability later with ReportDurable (spec §5.3.2 flusher).
	AsyncDurability bool

	// UnsafeLeaseReads enables lease-based ReadIndex (see readindex.go). Unsafe
	// under clock-rate skew and process pauses. Requires CheckQuorum.
	UnsafeLeaseReads bool
	LeaseMarginTicks int // default 2

	// NoopData, if set, produces the payload of the leader's election no-op
	// entry (e.g. a Kafka control batch). Default nil data.
	NoopData func(index, term uint64) []byte

	// Seed seeds the election-timeout RNG (mixed with ID). Production drivers
	// should pass a random seed; tests pass a fixed one for determinism.
	Seed uint64

	Logger Logger

	// DebugChecks enables extra (costlier) invariant assertions that panic on
	// violation, e.g. "prevLogIndex is a batch end" on both leader and
	// follower (spec §13.1 batch-as-entry off-by-ones). Tests enable it.
	DebugChecks bool

	knobs testKnobs // zero = safe; set only by negative-control tests
}

// testKnobs deliberately break individual safety rules. They exist so that
// negative-control tests can prove the harness's invariant checkers detect
// the corresponding bugs (spec §13.5: "a checker that can't fail"). They can
// only be set from this package's tests (export_test.go).
type testKnobs struct {
	commitOldTerms      bool // drop the Figure 8 guard in maybeCommit
	readIndexNoQuorum   bool // confirm ReadIndex without a heartbeat round
	voteIgnoreLog       bool // grant votes without the election restriction
	heartbeatFullCommit bool // leader sends its full commit index in heartbeats
	truncateOnAppend    bool // follower truncates at the first entry even if it matches
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("raft: config: ID must be non-zero")
	}
	if c.Store == nil {
		return errors.New("raft: config: Store is required")
	}
	if !slices.Contains(c.Peers, c.ID) {
		return fmt.Errorf("raft: config: ID %d not in Peers %v", c.ID, c.Peers)
	}
	seen := map[NodeID]bool{}
	for _, p := range c.Peers {
		if p == None || seen[p] {
			return fmt.Errorf("raft: config: invalid or duplicate peer %d", p)
		}
		seen[p] = true
	}
	if c.ElectionTick == 0 {
		c.ElectionTick = 10
	}
	if c.HeartbeatTick == 0 {
		c.HeartbeatTick = 1
	}
	if c.HeartbeatTick >= c.ElectionTick {
		return fmt.Errorf("raft: config: HeartbeatTick %d must be < ElectionTick %d", c.HeartbeatTick, c.ElectionTick)
	}
	if c.MaxSizePerMsg == 0 {
		c.MaxSizePerMsg = 1 << 20
	}
	if c.MaxInflightMsgs <= 0 {
		c.MaxInflightMsgs = 64
	}
	if c.MaxInflightBytes == 0 {
		c.MaxInflightBytes = 4 << 20
	}
	if c.MaxCommittedSizePerReady == 0 {
		c.MaxCommittedSizePerReady = 16 << 20
	}
	if c.LeaseMarginTicks == 0 {
		c.LeaseMarginTicks = 2
	}
	if c.UnsafeLeaseReads && !c.CheckQuorum {
		return errors.New("raft: config: UnsafeLeaseReads requires CheckQuorum")
	}
	return nil
}

// SoftState is volatile state that the driver may want to observe (it is not
// persisted): e.g. fail pending produce waiters when leadership is lost.
type SoftState struct {
	Lead  NodeID
	State StateType
}

// Ready bundles everything the driver must do for one iteration. See the
// package documentation for the contract.
type Ready struct {
	SoftState *SoftState // non-nil if changed
	HardState *HardState // non-nil if changed
	// MustSync is true when HardState changed Term or Vote: it must be durable
	// before Messages are sent.
	MustSync bool
	Snapshot *Snapshot // InstallSnapshot to apply first
	// TruncateFrom, if non-zero: drop stable batches with FirstIndex >= this
	// before appending Entries (never <= the commit index).
	TruncateFrom     uint64
	Entries          []Entry     // append to stable storage
	CommittedEntries []Entry     // apply, in order
	Messages         []Message   // send after persisting (see contract)
	ReadStates       []ReadState // confirmed ReadIndex requests
}

// Status is a snapshot of a node's state for introspection (admin API,
// tests). Progress is only populated on the leader.
type Status struct {
	ID             NodeID
	State          StateType
	Term           uint64
	Vote           NodeID
	Lead           NodeID
	Commit         uint64
	Applied        uint64
	FirstIndex     uint64
	LastIndex      uint64
	LastTerm       uint64
	Durable        uint64
	LeadTransferee NodeID
	Progress       map[NodeID]Progress
}

type campaignType uint8

const (
	campaignPreElection campaignType = iota
	campaignElection
	campaignTransfer
)

// Node is a single Raft replica's state machine. It is not safe for
// concurrent use: the driver goroutine owns it.
type Node struct {
	cfg   Config
	id    NodeID
	peers []NodeID // sorted, includes id

	state StateType
	term  uint64
	vote  NodeID
	lead  NodeID

	log   *raftLog
	prs   map[NodeID]*Progress // leader only; excludes self
	votes map[NodeID]bool

	ticks                     uint64 // local tick counter (debug + lease)
	electionElapsed           int
	heartbeatElapsed          int
	randomizedElectionTimeout int

	leadTransferee  NodeID
	transferElapsed int

	// durable: every entry <= durable in the current log is durable.
	durable  uint64
	heldAcks []Message // async durability: success acks waiting for durability

	ro         readOnly
	readStates []ReadState

	msgs   []Message
	prevHS HardState
	prevSS SoftState

	inReady bool
	ready   Ready

	rand *rand.Rand
}

// NewNode creates a Node from its persisted state (Config.Store and
// Config.HardState). A fresh replica starts as a follower in term 0.
func NewNode(cfg Config) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	n := &Node{
		cfg:   cfg,
		id:    cfg.ID,
		peers: slices.Sorted(slices.Values(cfg.Peers)),
		log:   newRaftLog(cfg.Store),
		rand:  rand.New(rand.NewPCG(cfg.Seed, uint64(cfg.ID)*0x9E3779B97F4A7C15)),
	}
	snap, err := cfg.Store.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("raft: load snapshot: %w", err)
	}
	last := n.log.lastIndex()
	n.log.committed = snap.Index
	n.log.applied = max(snap.Index, cfg.Applied)
	if n.log.applied > last {
		return nil, fmt.Errorf("raft: applied index %d beyond last index %d", n.log.applied, last)
	}
	// Commit is a hint; it is never ahead of the durable log when emitted by
	// a Node, but clamp anyway (storage may have lost data it fsynced).
	commit := max(cfg.HardState.Commit, n.log.applied)
	if commit > last {
		n.logf("WARN persisted commit %d beyond last index %d; clamping", commit, last)
		commit = last
	}
	n.log.commitTo(commit)
	n.term = cfg.HardState.Term
	n.vote = cfg.HardState.Vote
	n.prevHS = cfg.HardState
	n.durable = last
	n.ro.reset()
	n.becomeFollower(n.term, None)
	n.prevSS = n.softState()
	n.logf("START term=%d vote=%d commit=%d applied=%d first=%d last=(%d,%d) peers=%v",
		n.term, n.vote, n.log.committed, n.log.applied, n.log.firstIndex(), last, n.log.lastTerm(), n.peers)
	return n, nil
}

// ID returns the node's id.
func (n *Node) ID() NodeID { return n.id }

func (n *Node) logf(format string, args ...any) {
	if n.cfg.Logger != nil {
		n.cfg.Logger("T%d N%d "+format, append([]any{n.ticks, n.id}, args...)...)
	}
}

func (n *Node) assertNotInReady(op string) {
	if n.inReady {
		panic("raft: " + op + " called between Ready and Advance")
	}
}

func (n *Node) quorum() int { return len(n.peers)/2 + 1 }

func (n *Node) isPeer(id NodeID) bool {
	_, ok := slices.BinarySearch(n.peers, id)
	return ok
}

func (n *Node) softState() SoftState { return SoftState{Lead: n.lead, State: n.state} }

func (n *Node) hardState() HardState {
	return HardState{Term: n.term, Vote: n.vote, Commit: min(n.log.committed, n.durable)}
}

// quorumValue returns the largest value v such that a quorum of vals is >= v.
func quorumValue(vals []uint64) uint64 {
	s := slices.Clone(vals)
	slices.Sort(s)
	return s[len(s)-(len(s)/2+1)]
}

func (n *Node) committedInCurrentTerm() bool {
	t, err := n.log.term(n.log.committed)
	return err == nil && t == n.term
}

// ---------------------------------------------------------------------------
// State transitions.

func (n *Node) transition(to StateType) {
	if n.state != to {
		n.logf("%v→%v term=%d last=(%d,%d) commit=%d", n.state, to, n.term, n.log.lastIndex(), n.log.lastTerm(), n.log.committed)
	}
	n.state = to
}

func (n *Node) reset(term uint64) {
	if n.term != term {
		n.term = term
		n.vote = None
		n.heldAcks = nil // acks addressed to a leader of an older term are useless
	}
	n.lead = None
	n.electionElapsed = 0
	n.heartbeatElapsed = 0
	n.randomizedElectionTimeout = n.cfg.ElectionTick + n.rand.IntN(n.cfg.ElectionTick)
	n.leadTransferee = None
	n.transferElapsed = 0
	n.votes = map[NodeID]bool{}
	n.prs = nil
	n.ro.reset()
}

func (n *Node) becomeFollower(term uint64, lead NodeID) {
	n.reset(term)
	n.lead = lead
	n.transition(Follower)
}

func (n *Node) becomePreCandidate() {
	// Pre-vote does not change term or vote; it only asks.
	n.votes = map[NodeID]bool{}
	n.lead = None
	n.transition(PreCandidate)
}

func (n *Node) becomeCandidate() {
	n.reset(n.term + 1)
	n.vote = n.id
	n.transition(Candidate)
}

func (n *Node) becomeLeader() {
	n.reset(n.term)
	n.lead = n.id
	n.transition(Leader)
	last := n.log.lastIndex()
	n.prs = make(map[NodeID]*Progress, len(n.peers)-1)
	for _, p := range n.peers {
		if p != n.id {
			n.prs[p] = newProgress(last+1, n.cfg.MaxInflightMsgs, n.cfg.MaxInflightBytes)
		}
	}
	// Append a no-op of the new term so that entries of previous terms get
	// committed (Figure 8 rule) and ReadIndex can start serving (§5.13).
	noop := Entry{Term: n.term, FirstIndex: last + 1, LastIndex: last + 1}
	if n.cfg.NoopData != nil {
		noop.Data = n.cfg.NoopData(noop.LastIndex, n.term)
	}
	n.log.appendLeader(noop)
	n.bcastAppend(false)
}

func (n *Node) hup(t campaignType) {
	if n.state == Leader {
		return
	}
	if t == campaignPreElection && !n.cfg.PreVote {
		t = campaignElection
	}
	n.campaign(t)
}

func (n *Node) campaign(t campaignType) {
	var term uint64
	var mt MsgType
	if t == campaignPreElection {
		n.becomePreCandidate()
		mt, term = MsgPreVote, n.term+1
	} else {
		n.becomeCandidate()
		mt, term = MsgVote, n.term
	}
	if n.poll(n.id, true) {
		n.voteWon(t)
		return
	}
	lastIdx, lastTerm := n.log.lastIndex(), n.log.lastTerm()
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		n.send(Message{Type: mt, To: p, Term: term, LastLogIndex: lastIdx, LastLogTerm: lastTerm, Force: t == campaignTransfer})
	}
}

func (n *Node) voteWon(t campaignType) {
	if t == campaignPreElection {
		n.campaign(campaignElection)
	} else {
		n.becomeLeader()
	}
}

// poll records a vote and reports whether the election is won. If it is
// lost (a quorum rejected), the node reverts to follower.
func (n *Node) poll(id NodeID, granted bool) bool {
	if _, ok := n.votes[id]; !ok {
		n.votes[id] = granted
	}
	var gr, rj int
	for _, v := range n.votes {
		if v {
			gr++
		} else {
			rj++
		}
	}
	if gr >= n.quorum() {
		return true
	}
	if rj >= n.quorum() {
		n.logf("lost election term=%d (%d rejections)", n.term, rj)
		n.becomeFollower(n.term, None)
	}
	return false
}

// ---------------------------------------------------------------------------
// Inputs.

// Tick advances the logical clock by one tick.
func (n *Node) Tick() {
	n.assertNotInReady("Tick")
	n.ticks++
	if n.state == Leader {
		n.tickLeader()
	} else {
		n.electionElapsed++
		if n.electionElapsed >= n.randomizedElectionTimeout {
			n.electionElapsed = 0
			n.hup(campaignPreElection)
		}
	}
}

func (n *Node) tickLeader() {
	n.heartbeatElapsed++
	n.electionElapsed++
	if n.leadTransferee != None {
		n.transferElapsed++
		if n.transferElapsed >= n.cfg.ElectionTick {
			n.logf("transfer to %d timed out", n.leadTransferee)
			n.leadTransferee = None
			n.transferElapsed = 0
		}
	}
	if n.electionElapsed >= n.cfg.ElectionTick {
		n.electionElapsed = 0
		if n.cfg.CheckQuorum {
			active := 1
			for _, pr := range n.prs {
				if pr.RecentActive {
					active++
				}
				pr.RecentActive = false
			}
			if active < n.quorum() {
				n.logf("check-quorum failed (%d/%d active): stepping down", active, len(n.peers))
				n.becomeFollower(n.term, None)
				return
			}
		}
	}
	for _, id := range n.peers {
		pr := n.prs[id]
		switch {
		case pr == nil:
		case pr.State == StateSnapshot:
			pr.snapshotElapsed++
			if pr.snapshotElapsed >= n.cfg.ElectionTick {
				n.logf("snapshot to %d timed out; probing", id)
				pr.becomeProbe()
				n.sendAppend(id, true)
			}
		case pr.State == StateReplicate && pr.inflights.count > 0:
			// Pipelined MsgApps may have been lost; without new proposals
			// nothing would ever be resent. Fall back to probing if the
			// follower made no progress for half an election timeout.
			pr.stallTicks++
			if pr.stallTicks >= max(2, n.cfg.ElectionTick/2) {
				n.logf("replication to %d stalled at match=%d next=%d; probing", id, pr.Match, pr.Next)
				pr.becomeProbe()
				n.sendAppend(id, true)
			}
		}
	}
	if n.heartbeatElapsed >= n.cfg.HeartbeatTick {
		n.heartbeatElapsed = 0
		n.bcastHeartbeat()
	}
}

// Campaign makes the node start an election now (pre-vote first if enabled).
func (n *Node) Campaign() {
	n.assertNotInReady("Campaign")
	n.hup(campaignPreElection)
}

// Propose appends data as a single-offset entry. It is shorthand for
// ProposeBatch(1, data, nil).
func (n *Node) Propose(data []byte) (Entry, error) { return n.ProposeBatch(1, data, nil) }

// ProposeBatch appends an entry covering span offsets. The leader assigns
// [FirstIndex, LastIndex] and Term; stamp, if non-nil, is called with the
// assigned entry before it is appended and may replace e.Data (e.g. to write
// baseOffset and partitionLeaderEpoch into the RecordBatch header). It must
// not change the indices or term. Returns *NotLeaderError on a non-leader.
func (n *Node) ProposeBatch(span uint64, data []byte, stamp func(e *Entry)) (Entry, error) {
	n.assertNotInReady("Propose")
	if n.state != Leader {
		return Entry{}, &NotLeaderError{Leader: n.lead, Term: n.term}
	}
	if n.leadTransferee != None {
		return Entry{}, ErrProposalDropped
	}
	if span == 0 {
		return Entry{}, errors.New("raft: proposal span must be >= 1")
	}
	last := n.log.lastIndex()
	e := Entry{Term: n.term, FirstIndex: last + 1, LastIndex: last + span, Data: data}
	if stamp != nil {
		want := e
		stamp(&e)
		if e.Term != want.Term || e.FirstIndex != want.FirstIndex || e.LastIndex != want.LastIndex {
			return Entry{}, errors.New("raft: stamp must not change entry indices or term")
		}
	}
	n.log.appendLeader(e)
	n.bcastAppend(false)
	return e, nil
}

// ReadIndex requests a linearizable read point (spec §5.13). The result
// arrives later as a Ready.ReadStates entry carrying ctx; ctx must be unique
// among this node's outstanding requests. On a follower the request is
// forwarded to the leader. Requests are dropped silently on leadership change
// or message loss, so the driver needs a timeout.
func (n *Node) ReadIndex(ctx uint64) error {
	n.assertNotInReady("ReadIndex")
	if n.state == Leader {
		n.readIndex(None, ctx)
		return nil
	}
	if n.lead == None {
		return &NotLeaderError{Term: n.term}
	}
	n.send(Message{Type: MsgReadIndex, To: n.lead, Context: ctx})
	return nil
}

// TransferLeadership asks the leader to hand leadership to `to`: it catches
// `to` up, then sends MsgTimeoutNow so `to` campaigns immediately. Proposals
// are rejected while the transfer is pending; it aborts after ElectionTick
// ticks.
func (n *Node) TransferLeadership(to NodeID) error {
	n.assertNotInReady("TransferLeadership")
	if n.state != Leader {
		return &NotLeaderError{Leader: n.lead, Term: n.term}
	}
	if to == n.id {
		return nil
	}
	if !n.isPeer(to) {
		return fmt.Errorf("raft: transfer target %d is not a peer", to)
	}
	if n.leadTransferee == to {
		return nil
	}
	n.logf("transfer leadership to %d", to)
	n.leadTransferee = to
	n.transferElapsed = 0
	if pr := n.prs[to]; pr.Match == n.log.lastIndex() {
		n.sendTimeoutNow(to)
	} else {
		n.sendAppendLoop(to)
	}
	return nil
}

func (n *Node) sendTimeoutNow(to NodeID) {
	n.ro.leaseAllowed = false
	n.send(Message{Type: MsgTimeoutNow, To: to})
}

// ReportDurable tells the node that all entries up to (index, term) are
// durable (AsyncDurability only). Report the last (index, term) written
// before the fsync started. Stale reports (entry since truncated) are ignored.
func (n *Node) ReportDurable(index, term uint64) {
	n.assertNotInReady("ReportDurable")
	n.handleDurable(index, term)
}

func (n *Node) handleDurable(index, term uint64) {
	if !n.cfg.AsyncDurability || index <= n.durable {
		return
	}
	if index > n.log.stableLast() || !n.log.matchTerm(index, term) {
		return
	}
	if index < n.log.lastIndex() {
		// Durability is tracked at batch boundaries only: index must be the
		// LastIndex of an entry (the last one written before the fsync).
		if bs, err := n.log.batchStart(index + 1); err != nil || bs != index+1 {
			n.logf("WARN ReportDurable(%d) is not a batch end; ignored", index)
			return
		}
	}
	n.durable = index
	n.releaseAcks()
	if n.state == Leader && n.maybeCommit() {
		n.bcastAppend(true)
	}
}

func (n *Node) releaseAcks() {
	k := 0
	for _, m := range n.heldAcks {
		if m.MatchIndex <= n.durable {
			n.msgs = append(n.msgs, m)
		} else {
			n.heldAcks[k] = m
			k++
		}
	}
	n.heldAcks = n.heldAcks[:k]
}

// Step processes a message from a peer (or a local message: MsgHup, MsgProp,
// MsgDurable, MsgReadIndex with From == None).
func (n *Node) Step(m Message) error {
	n.assertNotInReady("Step")
	return n.step(m)
}

func (n *Node) step(m Message) error {
	switch m.Type {
	case MsgHup:
		n.hup(campaignPreElection)
		return nil
	case MsgProp:
		for _, e := range m.Entries {
			span := uint64(1)
			if e.LastIndex > e.FirstIndex {
				span = e.LastIndex - e.FirstIndex + 1
			}
			if _, err := n.ProposeBatch(span, e.Data, nil); err != nil {
				return err
			}
		}
		return nil
	case MsgDurable:
		n.handleDurable(m.Index, m.LogTerm)
		return nil
	case MsgReadIndex:
		if m.From == None || m.From == n.id {
			return n.ReadIndex(m.Context)
		}
	}
	if m.Type >= numMsgTypes {
		return fmt.Errorf("%w: type %d", ErrBadMessage, m.Type)
	}
	if m.Group != n.cfg.Group {
		return ErrWrongGroup
	}
	if m.To != n.id {
		return ErrWrongRecipient
	}
	if m.From == n.id || !n.isPeer(m.From) {
		return ErrUnknownPeer
	}

	switch {
	case m.Term > n.term:
		if m.Type == MsgVote || m.Type == MsgPreVote {
			// Leader stickiness: a node that heard from a live leader within
			// the minimum election timeout ignores (pre-)votes, unless the
			// vote comes from a leadership transfer. Pre-vote always applies
			// this rule (thesis §9.6); real votes only with check-quorum.
			inLease := n.lead != None && n.electionElapsed < n.cfg.ElectionTick
			if n.cfg.UnsafeLeaseReads && n.ticks < uint64(n.cfg.ElectionTick) {
				// A freshly (re)started voter may have acked a lease-holding
				// leader just before it crashed: honour that lease blindly for
				// one election timeout after startup.
				inLease = true
			}
			if inLease && !m.Force && (n.cfg.CheckQuorum || m.Type == MsgPreVote) {
				n.logf("ignore %v from %d term=%d: in leader lease (lead=%d elapsed=%d)", m.Type, m.From, m.Term, n.lead, n.electionElapsed)
				return nil
			}
		}
		switch {
		case m.Type == MsgPreVote:
			// Never adopt the term of a pre-vote request.
		case m.Type == MsgPreVoteResp && m.Granted:
			// A granted pre-vote carries the future term we asked for.
		default:
			n.logf("term %d→%d on %v from %d", n.term, m.Term, m.Type, m.From)
			if m.Type == MsgApp || m.Type == MsgHeartbeat || m.Type == MsgSnap {
				n.becomeFollower(m.Term, m.From)
			} else {
				// Learning a newer term from a vote request or a response does
				// not reset the election timer (Students' Guide: only a valid
				// leader message, a granted vote or our own campaign does), so
				// a node with a stale log cannot keep postponing elections.
				elapsed := n.electionElapsed
				if n.state == Leader {
					elapsed = 0
				}
				n.becomeFollower(m.Term, None)
				n.electionElapsed = elapsed
			}
		}
	case m.Term < n.term:
		switch m.Type {
		case MsgApp, MsgHeartbeat, MsgSnap:
			// Tell the stale leader about the newer term so it steps down.
			n.send(Message{Type: MsgAppResp, To: m.From})
		case MsgPreVote:
			n.send(Message{Type: MsgPreVoteResp, To: m.From, Granted: false})
		case MsgVote:
			n.send(Message{Type: MsgVoteResp, To: m.From, Granted: false})
		}
		// Stale responses (term != current term) are dropped (§13.1).
		return nil
	}

	switch m.Type {
	case MsgPreVote, MsgVote:
		n.handleVote(m)
		return nil
	}
	switch n.state {
	case Leader:
		n.stepLeader(m)
	case Candidate, PreCandidate:
		n.stepCandidate(m)
	default:
		return n.stepFollower(m)
	}
	return nil
}

func (n *Node) handleVote(m Message) {
	respType := MsgVoteResp
	if m.Type == MsgPreVote {
		respType = MsgPreVoteResp
	}
	canVote := n.vote == m.From ||
		(n.vote == None && n.lead == None) ||
		(m.Type == MsgPreVote && m.Term > n.term)
	if canVote && (n.log.isUpToDate(m.LastLogIndex, m.LastLogTerm) || n.cfg.knobs.voteIgnoreLog) {
		n.logf("grant %v to %d term=%d (cand last=(%d,%d) mine=(%d,%d))", m.Type, m.From, m.Term,
			m.LastLogIndex, m.LastLogTerm, n.log.lastIndex(), n.log.lastTerm())
		// Responses to (pre-)votes carry the request's term (for a pre-vote,
		// the future term), so the candidate does not drop them as stale.
		n.send(Message{Type: respType, To: m.From, Term: m.Term, Granted: true})
		if m.Type == MsgVote {
			n.electionElapsed = 0
			n.vote = m.From // persisted (MustSync) before this response leaves
		}
		return
	}
	n.logf("reject %v from %d term=%d (vote=%d lead=%d cand last=(%d,%d) mine=(%d,%d))", m.Type, m.From, m.Term,
		n.vote, n.lead, m.LastLogIndex, m.LastLogTerm, n.log.lastIndex(), n.log.lastTerm())
	n.send(Message{Type: respType, To: m.From, Granted: false})
}

func (n *Node) stepCandidate(m Message) {
	switch m.Type {
	case MsgApp, MsgHeartbeat, MsgSnap:
		// A leader exists in our term (for a pre-candidate: in the term we
		// did not advance yet).
		n.becomeFollower(m.Term, m.From)
		_ = n.stepFollower(m)
	case MsgVoteResp:
		if n.state == Candidate && n.poll(m.From, m.Granted) {
			n.voteWon(campaignElection)
		}
	case MsgPreVoteResp:
		if n.state != PreCandidate {
			return
		}
		// Grants must be for exactly the term we are pre-campaigning for.
		if m.Granted && m.Term != n.term+1 {
			return
		}
		if n.poll(m.From, m.Granted) {
			n.voteWon(campaignPreElection)
		}
	}
}

func (n *Node) stepFollower(m Message) error {
	switch m.Type {
	case MsgApp:
		n.electionElapsed = 0
		n.lead = m.From
		return n.handleAppendEntries(m)
	case MsgHeartbeat:
		n.electionElapsed = 0
		n.lead = m.From
		n.handleHeartbeat(m)
	case MsgSnap:
		n.electionElapsed = 0
		n.lead = m.From
		n.handleSnapshot(m)
	case MsgTimeoutNow:
		n.logf("TimeoutNow from %d: campaigning term=%d", m.From, n.term+1)
		n.campaign(campaignTransfer)
	case MsgReadIndexResp:
		n.readStates = append(n.readStates, ReadState{Index: m.Index, Context: m.Context})
	}
	return nil
}

// ---------------------------------------------------------------------------
// Follower: AppendEntries / heartbeat / snapshot (spec §5.4.6).

func validEntries(m *Message) bool {
	next := m.PrevLogIndex + 1
	lastTerm := m.PrevLogTerm
	if m.PrevLogTerm > m.Term {
		return false
	}
	for _, e := range m.Entries {
		if e.FirstIndex != next || e.LastIndex < e.FirstIndex || e.LastIndex == ^uint64(0) ||
			e.Term < lastTerm || e.Term > m.Term {
			return false
		}
		next = e.LastIndex + 1
		lastTerm = e.Term
	}
	return true
}

func (n *Node) handleAppendEntries(m Message) error {
	if !validEntries(&m) {
		n.logf("drop invalid MsgApp from %d prev=(%d,%d)", m.From, m.PrevLogIndex, m.PrevLogTerm)
		return ErrInvalidEntries
	}
	// 1. Everything <= our commit index already matches the leader.
	if m.PrevLogIndex < n.log.committed {
		n.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: n.log.committed})
		return nil
	}
	// 2. Log matching check on (prevLogIndex, prevLogTerm).
	if !n.log.matchTerm(m.PrevLogIndex, m.PrevLogTerm) {
		ci, ct := n.log.conflictHint(m.PrevLogIndex)
		n.logf("reject MsgApp from %d prev=(%d,%d) last=(%d,%d) hint=(%d,t%d)", m.From, m.PrevLogIndex, m.PrevLogTerm,
			n.log.lastIndex(), n.log.lastTerm(), ci, ct)
		n.send(Message{Type: MsgAppResp, To: m.From, Success: false, PrevLogIndex: m.PrevLogIndex,
			ConflictIndex: ci, ConflictTerm: ct})
		return nil
	}
	if n.cfg.DebugChecks {
		n.assertBatchEnd(m.PrevLogIndex, "matched prevLogIndex")
	}
	// 3. Append, truncating only at the first real conflict: a delayed or
	// duplicated MsgApp must never erase entries we already have.
	lastNew := m.PrevLogIndex
	for i, e := range m.Entries {
		if n.log.matchTerm(e.LastIndex, e.Term) && !(n.cfg.knobs.truncateOnAppend && i == 0 && e.FirstIndex > n.log.committed) {
			lastNew = e.LastIndex
			continue
		}
		if e.FirstIndex <= n.log.committed {
			panic(fmt.Sprintf("raft: N%d leader %d tried to overwrite committed entry %v (committed=%d)", n.id, m.From, e, n.log.committed))
		}
		if n.state == Leader {
			panic("raft: leader truncating its own log")
		}
		if e.FirstIndex <= n.log.lastIndex() {
			n.logf("conflict at %v (have t%d): truncating from %d", e, n.termOrZero(e.LastIndex), e.FirstIndex)
		}
		n.log.truncateAndAppend(m.Entries[i:])
		// Entries from e.FirstIndex on are new and not durable, whatever was
		// durable at those offsets before (the commit hint relies on this).
		n.durable = min(n.durable, e.FirstIndex-1)
		lastNew = m.Entries[len(m.Entries)-1].LastIndex
		break
	}
	// 4. Commit = min(leaderCommit, index of the last NEW entry) — not
	// min(leaderCommit, lastIndex), which could commit entries that did not
	// come from this leader.
	if m.Commit > n.log.committed {
		n.log.commitTo(min(m.Commit, lastNew))
	}
	// 5. Ack (withheld until durable under AsyncDurability).
	n.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: lastNew})
	return nil
}

// assertBatchEnd panics unless i is 0, the snapshot index, or the LastIndex
// of an entry in this node's log.
func (n *Node) assertBatchEnd(i uint64, what string) {
	if i == 0 || i == n.log.firstIndex()-1 || i == n.log.lastIndex() {
		return
	}
	if i < n.log.firstIndex() {
		return // compacted: cannot check
	}
	if bs, err := n.log.batchStart(i + 1); err != nil || bs != i+1 {
		panic(fmt.Sprintf("raft: N%d %s %d is not a batch end (batch of %d starts at %d, err=%v)", n.id, what, i, i+1, bs, err))
	}
}

func (n *Node) termOrZero(i uint64) uint64 {
	t, _ := n.log.term(i)
	return t
}

func (n *Node) handleHeartbeat(m Message) {
	// The leader caps m.Commit at our acknowledged Match, which is a prefix of
	// our log that matches the leader's (we only ack durable, matching
	// entries and never truncate them within a term), so this is safe. Clamp
	// to our last index defensively.
	n.log.commitTo(min(m.Commit, n.log.lastIndex()))
	n.send(Message{Type: MsgHeartbeatResp, To: m.From, Context: m.Context})
}

func (n *Node) handleSnapshot(m Message) {
	s := m.Snapshot
	if s == nil {
		return
	}
	switch {
	case s.Index <= n.log.committed:
		// Ignore snapshots at or below our commit (§13.1).
		n.send(Message{Type: MsgSnapResp, To: m.From, Success: true, MatchIndex: n.log.committed})
	case n.log.matchTerm(s.Index, s.Term):
		n.logf("snapshot (%d,%d) matches log: fast-forward commit", s.Index, s.Term)
		n.log.commitTo(s.Index)
		n.send(Message{Type: MsgSnapResp, To: m.From, Success: true, MatchIndex: s.Index})
	default:
		n.logf("restore snapshot (%d,%d) from %d (last=(%d,%d) commit=%d)", s.Index, s.Term, m.From,
			n.log.lastIndex(), n.log.lastTerm(), n.log.committed)
		// Until Advance installs the snapshot only the old committed prefix
		// counts as durable (keeps HardState.Commit safe in any write order).
		n.durable = min(n.durable, n.log.committed)
		n.log.restore(*s)
		n.send(Message{Type: MsgSnapResp, To: m.From, Success: true, MatchIndex: s.Index})
	}
}

// ---------------------------------------------------------------------------
// Leader: replication (spec §5.4.7) and commit (§5.4.8).

func (n *Node) stepLeader(m Message) {
	pr := n.prs[m.From]
	if pr == nil {
		return
	}
	switch m.Type {
	case MsgAppResp:
		pr.RecentActive = true
		n.handleAppendResp(m, pr)
	case MsgHeartbeatResp:
		pr.RecentActive = true
		pr.ProbeSent = false
		if pr.State == StateReplicate && pr.inflights.full() {
			pr.inflights.freeFirst()
		}
		if m.Context > pr.AckedHeartbeat && m.Context <= n.ro.hbSeq {
			pr.AckedHeartbeat = m.Context
			n.advanceReads()
		}
		switch pr.State {
		case StateProbe:
			// Resume probing (also lets a caught-up prober return to replicate).
			n.sendAppend(m.From, true)
		case StateReplicate:
			n.sendAppendLoop(m.From)
		}
	case MsgSnapResp:
		pr.RecentActive = true
		if pr.State != StateSnapshot {
			n.handleAppendResp(Message{Type: MsgAppResp, From: m.From, Success: true, MatchIndex: m.MatchIndex}, pr)
			return
		}
		if m.Success && m.MatchIndex <= n.log.lastIndex() {
			pr.maybeUpdate(m.MatchIndex)
		}
		n.logf("snapshot ack from %d match=%d", m.From, pr.Match)
		pr.becomeProbe()
		if m.From == n.leadTransferee && pr.Match == n.log.lastIndex() {
			// Same check as handleAppendResp: the transferee is caught up.
			n.logf("transferee %d caught up at %d (snapshot): TimeoutNow", m.From, pr.Match)
			n.sendTimeoutNow(m.From)
		}
		n.sendAppendLoop(m.From)
	case MsgReadIndex:
		n.readIndex(m.From, m.Context)
	}
}

func (n *Node) handleAppendResp(m Message, pr *Progress) {
	if !m.Success {
		// Drop stale/reordered rejections.
		switch pr.State {
		case StateReplicate:
			if m.PrevLogIndex <= pr.Match {
				return
			}
		case StateProbe:
			if m.PrevLogIndex != pr.Next-1 {
				return
			}
		default:
			return
		}
		next := n.backupNext(m, pr)
		n.logf("MsgApp rejected by %d prev=%d hint=(%d,t%d): next %d→%d", m.From, m.PrevLogIndex, m.ConflictIndex, m.ConflictTerm, pr.Next, next)
		pr.becomeProbe()
		pr.Next = next
		n.sendAppend(m.From, true)
		return
	}
	if m.MatchIndex > n.log.lastIndex() {
		n.logf("WARN ack from %d beyond last index (%d > %d)", m.From, m.MatchIndex, n.log.lastIndex())
		return
	}
	pr.ProbeSent = false
	if pr.maybeUpdate(m.MatchIndex) {
		switch pr.State {
		case StateProbe:
			pr.becomeReplicate()
		case StateSnapshot:
			if pr.Match >= pr.PendingSnapshot {
				pr.becomeReplicate()
			}
		case StateReplicate:
			pr.inflights.freeLE(pr.Match)
		}
		if n.maybeCommit() {
			n.bcastAppend(true)
		}
		if m.From == n.leadTransferee && pr.Match == n.log.lastIndex() {
			n.logf("transferee %d caught up at %d: TimeoutNow", m.From, pr.Match)
			n.sendTimeoutNow(m.From)
		}
	}
	n.sendAppendLoop(m.From)
}

// backupNext computes the new Next after a rejection using the fast-backup
// hints, normalized to a batch boundary of the leader's log.
func (n *Node) backupNext(m Message, pr *Progress) uint64 {
	next := m.ConflictIndex
	if m.ConflictTerm != 0 {
		if idx, ok := n.log.lastIndexOfTerm(m.ConflictTerm); ok {
			next = idx + 1
		}
	}
	// Always move strictly backwards (the new prev must be < the rejected
	// prev), never below what is known to match.
	next = min(next, m.PrevLogIndex)
	next = max(next, pr.Match+1, 1)
	if next >= n.log.firstIndex() && next <= n.log.lastIndex() {
		if bs, err := n.log.batchStart(next); err == nil {
			next = bs
		}
	}
	return next
}

// maybeCommit advances the commit index to the highest index durable on a
// quorum (the leader counts only its own durable index) AND of the current
// term. Older-term entries are only committed indirectly (Figure 8).
func (n *Node) maybeCommit() bool {
	matches := make([]uint64, 0, len(n.peers))
	matches = append(matches, n.durable)
	for _, pr := range n.prs {
		matches = append(matches, pr.Match)
	}
	mci := quorumValue(matches)
	if mci <= n.log.committed {
		return false
	}
	if t, err := n.log.term(mci); err != nil || (t != n.term && !n.cfg.knobs.commitOldTerms) {
		return false
	}
	n.log.commitTo(mci)
	n.releaseWaitingReads()
	return true
}

// sendAppend sends at most one MsgApp to `to`. It returns whether a message
// was sent.
func (n *Node) sendAppend(to NodeID, sendIfEmpty bool) bool {
	pr := n.prs[to]
	if pr.isPaused() {
		return false
	}
	last := n.log.lastIndex()
	if pr.Next > last+1 {
		pr.Next = last + 1
	}
	prev := pr.Next - 1
	prevTerm, errT := n.log.term(prev)
	var ents []Entry
	var errE error
	if pr.Next <= last {
		ents, errE = n.log.entries(pr.Next, last+1, n.cfg.MaxSizePerMsg)
	}
	if errT != nil || errE != nil {
		if errors.Is(errT, ErrCompacted) || errors.Is(errE, ErrCompacted) {
			return n.sendSnapshot(to, pr)
		}
		panic(fmt.Sprintf("raft: N%d sendAppend to %d next=%d: term err %v, entries err %v", n.id, to, pr.Next, errT, errE))
	}
	if len(ents) == 0 && !sendIfEmpty {
		return false
	}
	var bytes uint64
	for _, e := range ents {
		bytes += e.size()
	}
	if n.cfg.DebugChecks {
		n.assertBatchEnd(prev, "leader prevLogIndex")
	}
	n.send(Message{Type: MsgApp, To: to, PrevLogIndex: prev, PrevLogTerm: prevTerm, Entries: ents, Commit: n.log.committed})
	lastSent := prev
	if len(ents) > 0 {
		lastSent = ents[len(ents)-1].LastIndex
	}
	pr.sentEntries(lastSent, bytes, len(ents) > 0)
	return true
}

// sendAppendLoop fills the follower's pipeline.
func (n *Node) sendAppendLoop(to NodeID) {
	for n.sendAppend(to, false) {
		if n.prs[to].State != StateReplicate {
			return
		}
	}
}

func (n *Node) sendSnapshot(to NodeID, pr *Progress) bool {
	snap, err := n.cfg.Store.Snapshot()
	if err != nil || snap.IsEmpty() {
		n.logf("snapshot for %d unavailable: %v", to, err)
		return false
	}
	n.logf("send snapshot (%d,%d) to %d (next=%d)", snap.Index, snap.Term, to, pr.Next)
	s := cloneSnapshot(snap)
	n.send(Message{Type: MsgSnap, To: to, Snapshot: &s})
	pr.becomeSnapshot(snap.Index)
	return true
}

func (n *Node) bcastAppend(sendIfEmpty bool) {
	for _, p := range n.peers {
		if p != n.id {
			n.sendAppend(p, sendIfEmpty)
		}
	}
}

func (n *Node) bcastHeartbeat() {
	n.ro.hbSeq++
	n.ro.needBcast = false
	if n.cfg.UnsafeLeaseReads {
		n.ro.hbSent = append(n.ro.hbSent, hbRecord{seq: n.ro.hbSeq, tick: n.ticks})
	}
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		pr := n.prs[p]
		commit := min(pr.Match, n.log.committed)
		if n.cfg.knobs.heartbeatFullCommit {
			commit = n.log.committed
		}
		n.send(Message{Type: MsgHeartbeat, To: p, Commit: commit, Context: n.ro.hbSeq})
	}
}

// send queues an outgoing message, filling in From/Group/Term.
func (n *Node) send(m Message) {
	m.From = n.id
	m.Group = n.cfg.Group
	if m.Term == 0 {
		m.Term = n.term
	}
	if n.cfg.AsyncDurability && m.Success && (m.Type == MsgAppResp || m.Type == MsgSnapResp) && m.MatchIndex > n.durable {
		n.heldAcks = append(n.heldAcks, m)
		return
	}
	n.msgs = append(n.msgs, m)
}

// ---------------------------------------------------------------------------
// Outputs.

func (n *Node) flush() {
	n.flushReadBroadcast()
}

// HasReady reports whether Ready would return anything.
func (n *Node) HasReady() bool {
	n.assertNotInReady("HasReady")
	n.flush()
	l := n.log
	return len(n.msgs) > 0 || len(l.ents) > 0 || l.snap != nil || l.truncateFrom != 0 ||
		l.hasNextCommittedEnts() || len(n.readStates) > 0 ||
		n.softState() != n.prevSS || n.hardState() != n.prevHS
}

// Ready returns the pending outputs. The driver must process it and call
// Advance before calling any other method.
func (n *Node) Ready() Ready {
	n.assertNotInReady("Ready")
	n.flush()
	l := n.log
	rd := Ready{
		Snapshot:         l.snap,
		TruncateFrom:     l.truncateFrom,
		Entries:          slices.Clip(l.ents),
		CommittedEntries: l.nextCommittedEnts(n.cfg.MaxCommittedSizePerReady),
		Messages:         n.msgs,
		ReadStates:       n.readStates,
	}
	if ss := n.softState(); ss != n.prevSS {
		rd.SoftState = &ss
	}
	if hs := n.hardState(); hs != n.prevHS {
		rd.HardState = &hs
		rd.MustSync = hs.Term != n.prevHS.Term || hs.Vote != n.prevHS.Vote
	}
	n.msgs = nil
	n.readStates = nil
	n.inReady = true
	n.ready = rd
	return rd
}

// Advance acknowledges that the driver has processed the last Ready.
func (n *Node) Advance(Ready) {
	if !n.inReady {
		panic("raft: Advance called without Ready")
	}
	rd := n.ready
	n.ready = Ready{}
	n.inReady = false
	l := n.log
	if rd.SoftState != nil {
		n.prevSS = *rd.SoftState
	}
	if rd.HardState != nil {
		n.prevHS = *rd.HardState
	}
	if rd.Snapshot != nil {
		l.snap = nil
		l.applied = max(l.applied, rd.Snapshot.Index)
		n.durable = rd.Snapshot.Index
	}
	if rd.TruncateFrom != 0 {
		l.truncateFrom = 0
		n.durable = min(n.durable, rd.TruncateFrom-1)
	}
	if len(rd.Entries) > 0 {
		l.ents = l.ents[len(rd.Entries):]
		if len(l.ents) == 0 {
			l.ents = nil
		}
	}
	if k := len(rd.CommittedEntries); k > 0 {
		l.applied = rd.CommittedEntries[k-1].LastIndex
	}
	if !n.cfg.AsyncDurability {
		n.durable = l.lastIndex()
	} else {
		n.releaseAcks()
	}
	if n.state == Leader && n.maybeCommit() {
		n.bcastAppend(true)
	}
}

// BasicStatus is Status without the (allocating) Progress map.
func (n *Node) BasicStatus() Status {
	return Status{
		ID: n.id, State: n.state, Term: n.term, Vote: n.vote, Lead: n.lead,
		Commit: n.log.committed, Applied: n.log.applied,
		FirstIndex: n.log.firstIndex(), LastIndex: n.log.lastIndex(), LastTerm: n.log.lastTerm(),
		Durable: n.durable, LeadTransferee: n.leadTransferee,
	}
}

// Status returns a snapshot of the node's state.
func (n *Node) Status() Status {
	s := Status{
		ID: n.id, State: n.state, Term: n.term, Vote: n.vote, Lead: n.lead,
		Commit: n.log.committed, Applied: n.log.applied,
		FirstIndex: n.log.firstIndex(), LastIndex: n.log.lastIndex(), LastTerm: n.log.lastTerm(),
		Durable: n.durable, LeadTransferee: n.leadTransferee,
	}
	if n.state == Leader {
		s.Progress = make(map[NodeID]Progress, len(n.prs))
		for id, pr := range n.prs {
			cp := *pr
			cp.inflights = nil
			s.Progress[id] = cp
		}
	}
	return s
}

// LogTerm returns the term of the batch containing offset i in the node's
// current log view (stable + unstable). For tests and debugging.
func (n *Node) LogTerm(i uint64) (uint64, error) { return n.log.term(i) }

// Peers returns the voter set.
func (n *Node) Peers() []NodeID { return slices.Clone(n.peers) }
