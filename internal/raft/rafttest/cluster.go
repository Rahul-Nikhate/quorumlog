// Package rafttest is a deterministic in-process cluster harness for
// internal/raft: N nodes on the in-memory transport, a fake clock driven by
// ticks, one seeded RNG, a driver loop that honours the Ready contract, and
// continuous invariant checking (spec §7.2):
//
//   - election safety: at most one leader per term;
//   - leader append-only (asserted inside raft) and leader completeness: a new
//     leader's log contains the highest globally committed entry;
//   - log matching: two logs holding an entry with the same (index, term)
//     have identical prefixes up to it (incremental cumulative hashes);
//   - state machine safety: every node applies the same entry at every index,
//     in order, with contiguous offsets; snapshot state matches;
//   - commit index monotonic per incarnation, applied <= commit; a node's
//     term never decreases and it never changes its vote within a term;
//   - ReadIndex: every ReadState index is >= the highest index committed
//     anywhere when the read was issued (linearizable read point).
//
// Every check runs after every single input (Step/Tick/flush) and every
// Ready, so a violation is caught at the step that causes it.
package rafttest

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft/transport"
	"github.com/Rahul-Nikhate/quorumlog/internal/raftlog"
)

// diskByTest lets forSeeds run the same test body on several disk kinds
// without threading the kind through every test: New consults it when
// Options.Disk is left at DiskMem.
var diskByTest sync.Map // *testing.T → DiskKind

// Options configures a Cluster.
type Options struct {
	N           int
	Seed        uint64
	PreVote     bool
	CheckQuorum bool
	// Async enables raft.Config.AsyncDurability: entries become durable only
	// after a simulated fsync completes (random delay); a crash loses every
	// write not yet fsynced (truncations included).
	Async            bool
	UnsafeLeaseReads bool

	ElectionTick  int    // default 10
	HeartbeatTick int    // default 1
	UnitsPerTick  uint64 // network time units per tick, default 5

	// Link is the default network link (default: delay 1–2 units, reliable).
	Link *transport.LinkConfig
	// MaxSpan > 1 makes proposals random multi-offset batches of 1..MaxSpan
	// offsets (exercises the gapped Raft index space of spec §5.4.2).
	MaxSpan uint64
	// TickSkipRate is the probability that a node misses a tick (pauses,
	// clock-rate jitter). Must be 0 for lease-read tests.
	TickSkipRate float64

	MaxSizePerMsg   uint64
	MaxInflightMsgs int

	// Logf receives debug output; default: stderr if RAFT_DEBUG is set.
	Logf func(format string, args ...any)

	// ConfigHook may adjust every node's raft.Config before NewNode.
	ConfigHook func(*raft.Config)
	// PanicOnViolation makes invariant violations (and raft panics inside the
	// harness) panic with a *Violation instead of failing the test, so that
	// negative-control tests can assert that a bug IS detected.
	PanicOnViolation bool

	// Disk selects the LogStore implementation (default DiskMem; see
	// DiskKind). Tests run through forSeeds also get log-backed variants.
	Disk DiskKind
}

// Violation is the panic value used with Options.PanicOnViolation.
type Violation struct{ Msg string }

func (v *Violation) Error() string { return v.Msg }

// Cluster is the harness. It is not safe for concurrent use; all methods run
// on the test goroutine.
type Cluster struct {
	t    testing.TB
	opts Options
	rng  *rand.Rand
	Net  *transport.Network
	ids  []raft.NodeID
	peer map[raft.NodeID]*peer
	now  uint64 // network units

	// Global invariant state.
	leaders     map[uint64]raft.NodeID   // term → leader
	commitTerm  map[uint64]uint64        // committed index → term
	applied     map[uint64]appliedRecord // index → entry applied there
	smAt        map[uint64]uint64        // index → state machine hash after applying it
	prefixHash  map[[2]uint64]uint64     // (index, term) → cumulative log hash
	maxCommit   uint64                   // highest index committed anywhere
	maxCommitT  uint64                   // its term
	dataIndex   map[uint64]uint64        // proposal id → first index it was applied at
	reads       []ReadResult             // confirmed ReadIndex results
	propSeq     uint64
	readSeq     uint64
	Elections   int // number of times some node became leader
	tickSkip    map[raft.NodeID]float64
	incarnation uint64
}

type appliedRecord struct {
	term, first uint64
	data        uint64 // hash
}

// ReadResult is a confirmed ReadIndex request.
type ReadResult struct {
	Node   raft.NodeID
	Ctx    uint64
	Index  uint64
	MinIdx uint64 // global max commit when issued
}

type pendingRead struct {
	minIdx uint64
	done   bool
}

type prefixEntry struct {
	first, last, term, hash uint64
}

type peer struct {
	id   raft.NodeID
	node *raft.Node // nil while crashed
	disk Disk
	hs   raft.HardStateStore
	ep   *transport.Endpoint

	inbox []raft.Message

	// Application state machine: a rolling hash of applied entries.
	applied uint64
	smHash  uint64

	// Cumulative hashes of the stable log, parallel to its entries.
	plog     []prefixEntry
	baseIdx  uint64 // log start - 1 (snapshot index)
	baseHash uint64

	lastCommit  uint64
	leaderTerm  uint64 // last term in which we saw this node as leader
	persistedHS raft.HardState
	reads       map[uint64]pendingRead

	// Async durability: one simulated fsync in flight at a time.
	dirty        bool // async: written since the last fsync started
	flushPending bool
	flushAt      uint64
	flushIdx     uint64
	flushTerm    uint64
}

// New builds and starts an N-node cluster.
func New(t testing.TB, o Options) *Cluster {
	t.Helper()
	if o.N == 0 {
		o.N = 3
	}
	if o.ElectionTick == 0 {
		o.ElectionTick = 10
	}
	if o.HeartbeatTick == 0 {
		o.HeartbeatTick = 1
	}
	if o.UnitsPerTick == 0 {
		o.UnitsPerTick = 5
	}
	if o.MaxSpan == 0 {
		o.MaxSpan = 1
	}
	if o.Disk == DiskMem {
		if k, ok := diskByTest.Load(t); ok {
			o.Disk = k.(DiskKind)
		}
	}
	if o.Logf == nil && os.Getenv("RAFT_DEBUG") != "" {
		o.Logf = func(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...) }
	}
	c := &Cluster{
		t:          t,
		opts:       o,
		rng:        rand.New(rand.NewPCG(o.Seed, 0xC0FFEE)),
		Net:        transport.NewNetwork(o.Seed),
		peer:       map[raft.NodeID]*peer{},
		leaders:    map[uint64]raft.NodeID{},
		commitTerm: map[uint64]uint64{},
		applied:    map[uint64]appliedRecord{},
		smAt:       map[uint64]uint64{},
		prefixHash: map[[2]uint64]uint64{},
		dataIndex:  map[uint64]uint64{},
	}
	link := transport.LinkConfig{MinDelay: 1, MaxDelay: 2}
	if o.Link != nil {
		link = *o.Link
	}
	c.Net.SetDefaultLink(link)
	base := ""
	switch o.Disk {
	case DiskOS:
		base = t.TempDir()
	case DiskFaultFS:
		base = "/rafttest"
	}
	for i := 1; i <= o.N; i++ {
		id := raft.NodeID(i)
		c.ids = append(c.ids, id)
		p := &peer{id: id}
		if o.Disk == DiskMem {
			p.disk = NewMemDisk()
		} else {
			d, err := NewLogDisk(o.Disk, diskDir(base, id), o.Seed*31+uint64(i))
			if err != nil {
				t.Fatalf("N%d: open log disk: %v", id, err)
			}
			p.disk = d
		}
		t.Cleanup(p.disk.Close)
		c.peer[id] = p
		p.ep = c.Net.Register(id, func(m raft.Message) { p.inbox = append(p.inbox, m) })
	}
	for _, id := range c.ids {
		c.start(c.peer[id])
	}
	return c
}

// IDs returns the node ids.
func (c *Cluster) IDs() []raft.NodeID { return slices.Clone(c.ids) }

// Node returns the live raft.Node for id (nil if crashed). Tests may call
// its input methods directly (between harness steps).
func (c *Cluster) Node(id raft.NodeID) *raft.Node { return c.peer[id].node }

// Store returns the current (possibly unsynced) store of id.
func (c *Cluster) Store(id raft.NodeID) Store { return c.peer[id].disk.Store() }

// DiskKind returns the storage the cluster runs on.
func (c *Cluster) DiskKind() DiskKind { return c.opts.Disk }

// Applied returns the applied index of id.
func (c *Cluster) Applied(id raft.NodeID) uint64 { return c.peer[id].applied }

// Now returns the harness time in ticks.
func (c *Cluster) Now() uint64 { return c.now / c.opts.UnitsPerTick }

func (c *Cluster) logger(id raft.NodeID) raft.Logger {
	if c.opts.Logf == nil {
		return nil
	}
	return func(format string, args ...any) {
		c.opts.Logf("U%07d "+format, append([]any{c.now}, args...)...)
	}
}

func (c *Cluster) start(p *peer) {
	p.hs = p.disk.HardState()
	hs, err := p.hs.Load()
	if err != nil {
		c.fatalf("N%d load hard state: %v", p.id, err)
	}
	store := p.disk.Store()
	snap, _ := store.Snapshot()
	p.applied, p.smHash = decodeSM(snap.Data)
	if snap.Index != p.applied {
		c.fatalf("N%d snapshot index %d != state machine index %d", p.id, snap.Index, p.applied)
	}
	if snap.Index > 0 {
		if want, ok := c.smAt[snap.Index]; ok && want != p.smHash {
			c.fatalf("N%d restart: snapshot state at %d differs from global state", p.id, snap.Index)
		}
	}
	c.rebuildPrefix(p)
	c.incarnation++
	cfg := raft.Config{
		ID:               p.id,
		Peers:            c.ids,
		Store:            store,
		HardState:        hs,
		Applied:          snap.Index,
		ElectionTick:     c.opts.ElectionTick,
		HeartbeatTick:    c.opts.HeartbeatTick,
		PreVote:          c.opts.PreVote,
		CheckQuorum:      c.opts.CheckQuorum,
		AsyncDurability:  c.opts.Async,
		UnsafeLeaseReads: c.opts.UnsafeLeaseReads,
		MaxSizePerMsg:    c.opts.MaxSizePerMsg,
		MaxInflightMsgs:  c.opts.MaxInflightMsgs,
		Seed:             c.opts.Seed*1000003 + c.incarnation,
		Logger:           c.logger(p.id),
		DebugChecks:      true,
	}
	if p.disk.LogBacked() {
		cfg.NoopData = raftlog.NoopBatch // the election no-op is a control batch on disk
	}
	if c.opts.ConfigHook != nil {
		c.opts.ConfigHook(&cfg)
	}
	n, err := raft.NewNode(cfg)
	if err != nil {
		c.fatalf("N%d NewNode: %v", p.id, err)
	}
	p.node = n
	p.persistedHS = hs
	p.lastCommit = 0
	p.leaderTerm = 0
	p.inbox = nil
	p.reads = map[uint64]pendingRead{}
	p.flushPending = false
	p.dirty = false
	c.checkNode(p)
}

// ---------------------------------------------------------------------------
// Driving.

// Step advances the cluster by one network time unit: deliver due messages,
// tick every node at tick boundaries, complete due fsyncs, and run every
// node's Ready loop.
func (c *Cluster) Step() {
	c.now++
	c.Net.Advance(1)
	for _, id := range c.ids {
		p := c.peer[id]
		if p.node == nil {
			p.inbox = nil
			continue
		}
		msgs := p.inbox
		p.inbox = nil
		for _, m := range msgs {
			_ = p.node.Step(m) // errors are misrouted/invalid messages: dropped
			c.checkNode(p)
		}
	}
	if c.now%c.opts.UnitsPerTick == 0 {
		for _, id := range c.ids {
			p := c.peer[id]
			if p.node == nil {
				continue
			}
			skip := c.opts.TickSkipRate
			if r, ok := c.tickSkip[id]; ok {
				skip = r
			}
			if skip > 0 && c.rng.Float64() < skip {
				continue
			}
			p.node.Tick()
			c.checkNode(p)
		}
	}
	for _, id := range c.ids {
		p := c.peer[id]
		if p.node != nil && p.flushPending && p.flushAt <= c.now {
			p.flushPending = false
			p.disk.CompleteFlush()
			p.node.ReportDurable(p.flushIdx, p.flushTerm)
			c.checkNode(p)
		}
	}
	for _, id := range c.ids {
		c.processReady(c.peer[id])
	}
}

// RunTicks runs the cluster for n ticks.
func (c *Cluster) RunTicks(n int) {
	for range uint64(n) * c.opts.UnitsPerTick {
		c.Step()
	}
}

// RunUntil runs until cond holds (checked every tick) or maxTicks elapse.
func (c *Cluster) RunUntil(maxTicks int, cond func() bool) bool {
	for range maxTicks {
		if cond() {
			return true
		}
		c.RunTicks(1)
	}
	return cond()
}

func (c *Cluster) processReady(p *peer) {
	for iter := 0; p.node != nil && p.node.HasReady(); iter++ {
		if iter > 64 {
			c.fatalf("N%d Ready loop does not converge", p.id)
		}
		rd := p.node.Ready()
		store := p.disk.Store()
		if rd.Snapshot != nil {
			s := *rd.Snapshot
			if s.Index <= p.applied {
				c.fatalf("N%d asked to install snapshot %d at or below applied %d", p.id, s.Index, p.applied)
			}
			if err := store.ApplySnapshot(s); err != nil {
				c.fatalf("N%d ApplySnapshot: %v", p.id, err)
			}
			idx, h := decodeSM(s.Data)
			if idx != s.Index {
				c.fatalf("N%d snapshot data index %d != snapshot index %d", p.id, idx, s.Index)
			}
			if want, ok := c.smAt[s.Index]; !ok || want != h {
				c.fatalf("N%d installed snapshot (%d,%d) whose state differs from the global state machine (known=%v)", p.id, s.Index, s.Term, ok)
			}
			p.applied, p.smHash = idx, h
			c.resetPrefixToSnapshot(p, s)
			// Snapshot installation is synchronous; an fsync in flight that
			// predates it must not resurrect the old log.
			p.disk.SyncAll()
			p.flushPending = false
		}
		if rd.HardState != nil {
			hs := *rd.HardState
			prev := p.persistedHS
			// The emitted commit must already be durable and never inside the
			// region this Ready truncates, so restoring it after any crash is safe.
			durableLast := store.LastIndex()
			if c.opts.Async {
				durableLast = p.disk.DurableLastIndex()
			}
			if hs.Commit > durableLast || (rd.TruncateFrom != 0 && hs.Commit >= rd.TruncateFrom) {
				c.fatalf("N%d HardState.Commit %d not covered by the durable log (durable last %d, truncate %d)",
					p.id, hs.Commit, durableLast, rd.TruncateFrom)
			}
			if hs.Term < prev.Term {
				c.fatalf("N%d term regressed %d → %d", p.id, prev.Term, hs.Term)
			}
			if hs.Term == prev.Term && prev.Vote != raft.None && hs.Vote != prev.Vote {
				c.fatalf("N%d changed vote in term %d: %d → %d", p.id, hs.Term, prev.Vote, hs.Vote)
			}
			if err := p.hs.Save(hs); err != nil {
				c.fatalf("N%d save hard state: %v", p.id, err)
			}
			p.persistedHS = hs
		}
		if rd.TruncateFrom != 0 {
			if rd.TruncateFrom <= p.applied {
				c.fatalf("N%d truncating from %d at or below applied %d", p.id, rd.TruncateFrom, p.applied)
			}
			if err := store.TruncateSuffix(rd.TruncateFrom); err != nil {
				c.fatalf("N%d TruncateSuffix(%d): %v", p.id, rd.TruncateFrom, err)
			}
			c.truncatePrefix(p, rd.TruncateFrom)
			p.dirty = true
		}
		if len(rd.Entries) > 0 {
			if err := store.Append(rd.Entries); err != nil {
				c.fatalf("N%d Append: %v", p.id, err)
			}
			c.appendPrefix(p, rd.Entries)
			p.dirty = true
		}
		if rd.TruncateFrom != 0 || len(rd.Entries) > 0 {
			p.disk.Written(c.opts.Async)
		}
		if c.opts.Async && !p.flushPending && p.dirty {
			p.dirty = false
			// Start a simulated fsync covering everything written so far.
			last := store.LastIndex()
			term, _ := store.Term(last)
			p.flushPending = true
			p.disk.BeginFlush()
			p.flushIdx, p.flushTerm = last, term
			p.flushAt = c.now + 1 + c.rng.Uint64N(3*c.opts.UnitsPerTick)
		}
		for _, m := range rd.Messages {
			// Persist-before-send (Figure 2 / §13.1): every term a message
			// carries, and every granted vote, must already be durable.
			future := m.Type == raft.MsgPreVote || (m.Type == raft.MsgPreVoteResp && m.Granted)
			if !future && m.Term > p.persistedHS.Term {
				c.fatalf("N%d sends %v with term %d before persisting it (persisted term %d)", p.id, m, m.Term, p.persistedHS.Term)
			}
			// (A later term persisted in the same Ready also suffices: the node
			// can never vote in the older term again.)
			if m.Type == raft.MsgVoteResp && m.Granted && (p.persistedHS.Term < m.Term ||
				(p.persistedHS.Term == m.Term && p.persistedHS.Vote != m.To)) {
				c.fatalf("N%d grants vote %v before persisting it (persisted %+v)", p.id, m, p.persistedHS)
			}
		}
		if len(rd.Messages) > 0 {
			p.ep.Send(rd.Messages)
		}
		for _, e := range rd.CommittedEntries {
			c.apply(p, e)
		}
		for _, rs := range rd.ReadStates {
			c.readConfirmed(p, rs)
		}
		p.node.Advance(rd)
		c.checkNode(p)
	}
}

// ---------------------------------------------------------------------------
// Faults.

// Crash stops a node. With Async, writes not yet fsynced are lost.
func (c *Cluster) Crash(id raft.NodeID) {
	p := c.peer[id]
	if p.node == nil {
		return
	}
	c.logf("CRASH N%d", id)
	p.node = nil
	p.inbox = nil
	p.flushPending = false
	p.disk.Crash(c.opts.Async)
	c.Net.Crash(id)
}

// Restart restarts a crashed node from its durable store and hard state.
func (c *Cluster) Restart(id raft.NodeID) {
	p := c.peer[id]
	if p.node != nil {
		return
	}
	c.logf("RESTART N%d", id)
	c.Net.Restart(id)
	c.start(p)
}

// SetTickSkip makes id miss ticks with probability p (a slow clock), which
// models clock-rate skew between nodes.
func (c *Cluster) SetTickSkip(id raft.NodeID, p float64) {
	if c.tickSkip == nil {
		c.tickSkip = map[raft.NodeID]float64{}
	}
	c.tickSkip[id] = p
}

// Alive reports whether id is running.
func (c *Cluster) Alive(id raft.NodeID) bool { return c.peer[id].node != nil }

// Disconnect isolates id from the network.
func (c *Cluster) Disconnect(id raft.NodeID) { c.logf("DISCONNECT N%d", id); c.Net.Disconnect(id) }

// Connect reconnects id.
func (c *Cluster) Connect(id raft.NodeID) { c.logf("CONNECT N%d", id); c.Net.Connect(id) }

// Partition splits the cluster into groups.
func (c *Cluster) Partition(groups ...[]raft.NodeID) {
	c.logf("PARTITION %v", groups)
	c.Net.Partition(groups...)
}

// Heal removes partitions and reconnects everyone.
func (c *Cluster) Heal() { c.logf("HEAL"); c.Net.Heal() }

// SetLink changes the default link configuration.
func (c *Cluster) SetLink(l transport.LinkConfig) { c.Net.SetDefaultLink(l) }

// Compact compacts id's log up to its applied index, recording the state
// machine in the snapshot (the log-compaction hook).
func (c *Cluster) Compact(id raft.NodeID) bool {
	p := c.peer[id]
	if p.node == nil || p.applied <= p.baseIdx {
		return false
	}
	store := p.disk.Store()
	if err := store.Compact(p.applied, c.ids, encodeSM(p.applied, p.smHash)); err != nil {
		c.fatalf("N%d Compact(%d): %v", id, p.applied, err)
	}
	// Drop compacted prefix hashes, remembering the new base.
	k := 0
	for k < len(p.plog) && p.plog[k].last <= p.applied {
		k++
	}
	p.baseIdx, p.baseHash = p.plog[k-1].last, p.plog[k-1].hash
	p.plog = slices.Clone(p.plog[k:])
	c.logf("COMPACT N%d to %d", id, p.applied)
	return true
}

func (c *Cluster) logf(format string, args ...any) {
	if c.opts.Logf != nil {
		c.opts.Logf("U%07d "+format, append([]any{c.now}, args...)...)
	}
}

// ---------------------------------------------------------------------------
// Client operations.

// Propose proposes a fresh payload on id. It returns the proposal id (used by
// WaitCommitted) and the entry, or ok=false if id is not a live leader.
func (c *Cluster) Propose(id raft.NodeID) (pid uint64, e raft.Entry, ok bool) {
	p := c.peer[id]
	if p.node == nil {
		return 0, raft.Entry{}, false
	}
	c.propSeq++
	pid = c.propSeq
	noise := make([]byte, c.rng.IntN(24))
	for i := range noise {
		noise[i] = byte(c.rng.Uint32())
	}
	span := uint64(1)
	if c.opts.MaxSpan > 1 {
		span = 1 + c.rng.Uint64N(c.opts.MaxSpan)
	}
	e, err := p.node.ProposeBatch(span, makePayload(p.disk.LogBacked(), pid, span, noise), c.stamp(p))
	if err != nil {
		return pid, raft.Entry{}, false
	}
	c.checkNode(p)
	return pid, e, true
}

// proposeID re-proposes an existing proposal id (client retry).
func (c *Cluster) proposeID(id raft.NodeID, pid uint64) bool {
	p := c.peer[id]
	if p.node == nil {
		return false
	}
	span := uint64(1)
	if c.opts.MaxSpan > 1 {
		span = 1 + c.rng.Uint64N(c.opts.MaxSpan)
	}
	if _, err := p.node.ProposeBatch(span, makePayload(p.disk.LogBacked(), pid, span, nil), c.stamp(p)); err != nil {
		return false
	}
	c.checkNode(p)
	return true
}

// stamp returns the ProposeBatch stamp function for p: on a log-backed disk
// the leader writes baseOffset and partitionLeaderEpoch into the batch header.
func (c *Cluster) stamp(p *peer) func(*raft.Entry) {
	if p.disk.LogBacked() {
		return stampBatch
	}
	return nil
}

// CommittedIndex returns the index at which proposal pid was first applied.
func (c *Cluster) CommittedIndex(pid uint64) (uint64, bool) {
	i, ok := c.dataIndex[pid]
	return i, ok
}

// NCommitted returns how many live nodes have applied index i.
func (c *Cluster) NCommitted(i uint64) int {
	n := 0
	for _, id := range c.ids {
		if p := c.peer[id]; p.node != nil && p.applied >= i {
			n++
		}
	}
	return n
}

// One submits a new proposal and waits until it is applied on at least
// `expected` live nodes, like 6.5840's one(). With retry, it re-proposes on
// whichever node is leader if the first attempt does not commit. Returns the
// index, or fails the test after maxTicks.
func (c *Cluster) One(expected int, retry bool, maxTicks int) uint64 {
	c.t.Helper()
	c.propSeq++
	pid := c.propSeq
	start := c.Now()
	attempt := 0
	for c.Now()-start < uint64(maxTicks) {
		// Prefer the leader with the highest term; rotate on retries so a
		// stale (partitioned) leader cannot absorb every attempt.
		proposed := false
		ls := c.Leaders(nil)
		slices.SortStableFunc(ls, func(a, b raft.NodeID) int { return cmp.Compare(c.Term(b), c.Term(a)) })
		if len(ls) > 0 {
			id := ls[attempt%len(ls)]
			proposed = c.proposeID(id, pid)
		}
		attempt++
		if proposed {
			deadline := c.Now() + uint64(4*c.opts.ElectionTick)
			for c.Now() < deadline {
				if idx, ok := c.dataIndex[pid]; ok && c.NCommitted(idx) >= expected {
					return idx
				}
				c.RunTicks(1)
			}
			if !retry {
				c.fatalf("One(pid=%d): not committed on %d nodes", pid, expected)
			}
		} else {
			c.RunTicks(1)
		}
	}
	c.fatalf("One(pid=%d, expected=%d): no agreement within %d ticks", pid, expected, maxTicks)
	return 0
}

// WaitCommitted runs until proposal pid is applied on `expected` live nodes.
func (c *Cluster) WaitCommitted(pid uint64, expected, maxTicks int) (uint64, bool) {
	ok := c.RunUntil(maxTicks, func() bool {
		idx, ok := c.dataIndex[pid]
		return ok && c.NCommitted(idx) >= expected
	})
	return c.dataIndex[pid], ok
}

// ReadIndex issues a ReadIndex request on id and returns its context.
func (c *Cluster) ReadIndex(id raft.NodeID) (uint64, error) {
	p := c.peer[id]
	if p.node == nil {
		return 0, fmt.Errorf("N%d crashed", id)
	}
	c.readSeq++
	ctx := c.readSeq
	if err := p.node.ReadIndex(ctx); err != nil {
		return 0, err
	}
	p.reads[ctx] = pendingRead{minIdx: c.maxCommit}
	c.checkNode(p)
	return ctx, nil
}

// Reads returns all confirmed reads.
func (c *Cluster) Reads() []ReadResult { return slices.Clone(c.reads) }

// ReadDone reports whether the read with ctx was confirmed (on any node).
func (c *Cluster) ReadDone(ctx uint64) (ReadResult, bool) {
	for _, r := range c.reads {
		if r.Ctx == ctx {
			return r, true
		}
	}
	return ReadResult{}, false
}

// ---------------------------------------------------------------------------
// Queries.

func (c *Cluster) isLeader(id raft.NodeID) bool {
	p := c.peer[id]
	return p.node != nil && p.node.BasicStatus().State == raft.Leader
}

// Status returns id's status (zero if crashed).
func (c *Cluster) Status(id raft.NodeID) raft.Status {
	if p := c.peer[id]; p.node != nil {
		return p.node.Status()
	}
	return raft.Status{}
}

// Leaders returns the live nodes that currently believe they are leader,
// restricted to `among` if non-nil.
func (c *Cluster) Leaders(among []raft.NodeID) []raft.NodeID {
	if among == nil {
		among = c.ids
	}
	var out []raft.NodeID
	for _, id := range among {
		if c.isLeader(id) {
			out = append(out, id)
		}
	}
	return out
}

// Leader returns the live leader with the highest term (None if none).
func (c *Cluster) Leader() raft.NodeID {
	var best raft.NodeID
	var bestTerm uint64
	for _, id := range c.ids {
		if c.isLeader(id) {
			if t := c.peer[id].node.BasicStatus().Term; t >= bestTerm {
				best, bestTerm = id, t
			}
		}
	}
	return best
}

// StableLeader runs until exactly one node among `among` is leader and every
// live node of `among` agrees on its term and leader, and returns it.
func (c *Cluster) StableLeader(among []raft.NodeID, maxTicks int) (raft.NodeID, bool) {
	if among == nil {
		among = c.ids
	}
	var lead raft.NodeID
	ok := c.RunUntil(maxTicks, func() bool {
		ls := c.Leaders(among)
		if len(ls) != 1 {
			return false
		}
		lead = ls[0]
		lt := c.peer[lead].node.BasicStatus().Term
		for _, id := range among {
			p := c.peer[id]
			if p.node == nil {
				continue
			}
			st := p.node.BasicStatus()
			if st.Term != lt || st.Lead != lead {
				return false
			}
		}
		return true
	})
	return lead, ok
}

// Term returns id's current term.
func (c *Cluster) Term(id raft.NodeID) uint64 {
	if p := c.peer[id]; p.node != nil {
		return p.node.BasicStatus().Term
	}
	return 0
}

// Summary describes what happened in the run (for test logs).
func (c *Cluster) Summary() string {
	st := c.Net.Stats()
	snaps := 0
	for _, id := range c.ids {
		if s, _ := c.peer[id].disk.Store().Snapshot(); s.Index > 0 {
			snaps++
		}
	}
	return fmt.Sprintf("ticks=%d elections=%d terms=%d maxCommit=%d appliedIdx=%d reads=%d msgs sent=%d delivered=%d dropped=%d dup=%d snapSent=%d nodesWithSnapshot=%d",
		c.Now(), c.Elections, len(c.leaders), c.maxCommit, len(c.applied), len(c.reads), st.Sent, st.Delivered, st.Dropped, st.Duplicated,
		c.Net.SentByType(raft.MsgSnap), snaps)
}

// MaxCommit returns the highest index committed anywhere.
func (c *Cluster) MaxCommit() uint64 { return c.maxCommit }

// ---------------------------------------------------------------------------
// Invariants.

func (c *Cluster) fatalf(format string, args ...any) {
	c.t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, format, args...)
	fmt.Fprintf(&b, "\n  seed=%d now=%d ticks (rerun with RAFT_SEED=%d [RAFT_DEBUG=1])\n", c.opts.Seed, c.Now(), c.opts.Seed)
	for _, id := range c.ids {
		p := c.peer[id]
		if p.node == nil {
			fmt.Fprintf(&b, "  N%d: crashed applied=%d\n", id, p.applied)
			continue
		}
		st := p.node.Status()
		fmt.Fprintf(&b, "  N%d: %v term=%d vote=%d lead=%d commit=%d applied=%d first=%d last=(%d,%d) durable=%d",
			id, st.State, st.Term, st.Vote, st.Lead, st.Commit, p.applied, st.FirstIndex, st.LastIndex, st.LastTerm, st.Durable)
		if st.Progress != nil {
			fmt.Fprintf(&b, " prs=%v", st.Progress)
		}
		b.WriteString("\n    tail:")
		ents := p.disk.Store().AllEntries()
		for _, e := range ents[max(0, len(ents)-8):] {
			fmt.Fprintf(&b, " %v", e)
		}
		b.WriteString("\n")
	}
	if c.opts.PanicOnViolation {
		panic(&Violation{Msg: b.String()})
	}
	c.t.Fatal(b.String())
}

// checkNode runs the per-node invariants after every input.
func (c *Cluster) checkNode(p *peer) {
	c.t.Helper()
	st := p.node.BasicStatus()
	if st.State == raft.Leader {
		if l, ok := c.leaders[st.Term]; ok && l != p.id {
			c.fatalf("ELECTION SAFETY: two leaders in term %d: N%d and N%d", st.Term, l, p.id)
		}
		c.leaders[st.Term] = p.id
		if p.leaderTerm != st.Term {
			p.leaderTerm = st.Term
			c.Elections++
			// Leader completeness: the new leader has the highest committed entry.
			if c.maxCommit > 0 && c.maxCommit+1 >= st.FirstIndex {
				if t, err := p.node.LogTerm(c.maxCommit); err != nil || t != c.maxCommitT {
					c.fatalf("LEADER COMPLETENESS: N%d leader of term %d lacks committed (%d,%d) (has t%d err=%v)",
						p.id, st.Term, c.maxCommit, c.maxCommitT, t, err)
				}
			}
		}
	}
	if st.Commit < p.lastCommit {
		c.fatalf("N%d commit index regressed %d → %d", p.id, p.lastCommit, st.Commit)
	}
	if st.Applied > st.Commit {
		c.fatalf("N%d applied %d > commit %d", p.id, st.Applied, st.Commit)
	}
	if st.Commit > p.lastCommit {
		p.lastCommit = st.Commit
		if st.Commit+1 >= st.FirstIndex {
			t, err := p.node.LogTerm(st.Commit)
			if err != nil {
				c.fatalf("N%d cannot read term of its commit index %d: %v", p.id, st.Commit, err)
			}
			if prev, ok := c.commitTerm[st.Commit]; ok && prev != t {
				c.fatalf("STATE MACHINE SAFETY: index %d committed with term %d on N%d but %d elsewhere", st.Commit, t, p.id, prev)
			}
			c.commitTerm[st.Commit] = t
			if st.Commit > c.maxCommit {
				c.maxCommit, c.maxCommitT = st.Commit, t
			}
		}
	}
}

func hashBytes(parts ...any) uint64 {
	h := fnv.New64a()
	var buf [8]byte
	for _, p := range parts {
		switch v := p.(type) {
		case uint64:
			binary.BigEndian.PutUint64(buf[:], v)
			h.Write(buf[:])
		case []byte:
			binary.BigEndian.PutUint64(buf[:], uint64(len(v)))
			h.Write(buf[:])
			h.Write(v)
		}
	}
	return h.Sum64()
}

func (c *Cluster) apply(p *peer, e raft.Entry) {
	c.t.Helper()
	if e.FirstIndex != p.applied+1 {
		c.fatalf("N%d applied %v out of order (applied=%d)", p.id, e, p.applied)
	}
	dh := hashBytes(e.Data)
	rec := appliedRecord{term: e.Term, first: e.FirstIndex, data: dh}
	if prev, ok := c.applied[e.LastIndex]; ok && prev != rec {
		c.fatalf("STATE MACHINE SAFETY: N%d applied %v at %d, others applied first=%d term=%d", p.id, e, e.LastIndex, prev.first, prev.term)
	}
	c.applied[e.LastIndex] = rec
	p.smHash = hashBytes(p.smHash, e.Term, e.FirstIndex, e.LastIndex, e.Data)
	p.applied = e.LastIndex
	if prev, ok := c.smAt[e.LastIndex]; ok && prev != p.smHash {
		c.fatalf("STATE MACHINE SAFETY: N%d state after %d differs", p.id, e.LastIndex)
	}
	c.smAt[e.LastIndex] = p.smHash
	if id := payloadID(p.disk.LogBacked(), e.Data); id != 0 {
		if _, ok := c.dataIndex[id]; !ok {
			c.dataIndex[id] = e.LastIndex
		}
	}
}

func (c *Cluster) readConfirmed(p *peer, rs raft.ReadState) {
	c.t.Helper()
	pr, ok := p.reads[rs.Context]
	if !ok {
		// Not issued by this incarnation (a response to a pre-crash request):
		// a real driver ignores it too.
		return
	}
	if pr.done {
		// Duplicated by the network: still must satisfy the bound.
		if rs.Index < pr.minIdx {
			c.fatalf("LINEARIZABILITY: N%d duplicate read ctx=%d index %d < %d", p.id, rs.Context, rs.Index, pr.minIdx)
		}
		return
	}
	pr.done = true
	p.reads[rs.Context] = pr
	if rs.Index < pr.minIdx {
		c.fatalf("LINEARIZABILITY: N%d read ctx=%d got read index %d < %d committed before the read was issued", p.id, rs.Context, rs.Index, pr.minIdx)
	}
	c.reads = append(c.reads, ReadResult{Node: p.id, Ctx: rs.Context, Index: rs.Index, MinIdx: pr.minIdx})
}

// Log matching, incrementally: every (index, term) ever stored on any node
// maps to one cumulative hash of the log prefix ending there.

func (c *Cluster) recordPrefix(p *peer, last, term, h uint64) {
	k := [2]uint64{last, term}
	if prev, ok := c.prefixHash[k]; ok && prev != h {
		c.fatalf("LOG MATCHING: N%d has entry (%d,t%d) with a prefix different from another node's", p.id, last, term)
	}
	c.prefixHash[k] = h
}

func (c *Cluster) tailHash(p *peer) (uint64, uint64) {
	if n := len(p.plog); n > 0 {
		return p.plog[n-1].last, p.plog[n-1].hash
	}
	return p.baseIdx, p.baseHash
}

func (c *Cluster) appendPrefix(p *peer, ents []raft.Entry) {
	_, h := c.tailHash(p)
	for _, e := range ents {
		h = hashBytes(h, e.Term, e.FirstIndex, e.LastIndex, e.Data)
		p.plog = append(p.plog, prefixEntry{first: e.FirstIndex, last: e.LastIndex, term: e.Term, hash: h})
		c.recordPrefix(p, e.LastIndex, e.Term, h)
	}
}

func (c *Cluster) truncatePrefix(p *peer, from uint64) {
	k := len(p.plog)
	for k > 0 && p.plog[k-1].first >= from {
		k--
	}
	p.plog = p.plog[:k]
}

func (c *Cluster) resetPrefixToSnapshot(p *peer, s raft.Snapshot) {
	h, ok := c.prefixHash[[2]uint64{s.Index, s.Term}]
	if !ok {
		c.fatalf("N%d snapshot (%d,%d) refers to an entry no node ever stored", p.id, s.Index, s.Term)
	}
	p.plog = nil
	p.baseIdx, p.baseHash = s.Index, h
}

func (c *Cluster) rebuildPrefix(p *peer) {
	store := p.disk.Store()
	snap, _ := store.Snapshot()
	p.plog = nil
	p.baseIdx, p.baseHash = 0, 0
	if snap.Index > 0 {
		c.resetPrefixToSnapshot(p, snap)
	}
	c.appendPrefix(p, store.AllEntries())
}

// State machine snapshot encoding: [u64 applied][u64 hash].
func encodeSM(applied, h uint64) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b, applied)
	binary.BigEndian.PutUint64(b[8:], h)
	return b
}

func decodeSM(b []byte) (uint64, uint64) {
	if len(b) != 16 {
		return 0, 0
	}
	return binary.BigEndian.Uint64(b), binary.BigEndian.Uint64(b[8:])
}
