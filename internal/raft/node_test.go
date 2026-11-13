package raft

import (
	"errors"
	"slices"
	"testing"
)

// tnet is a minimal synchronous driver for unit tests: it honours the Ready
// contract against MemoryStores and delivers messages in FIFO order.
type tnet struct {
	t      *testing.T
	ids    []NodeID
	nodes  map[NodeID]*Node
	stores map[NodeID]*MemoryStore
	hs     map[NodeID]HardState
	queue  []Message
	// drop, if set, filters messages (return true to drop).
	drop func(m Message) bool
	// sent records every message sent.
	sent []Message
	// applied records committed entries per node.
	applied map[NodeID][]Entry
}

func newTestNet(t *testing.T, n int, mutate func(*Config)) *tnet {
	t.Helper()
	tn := &tnet{t: t, nodes: map[NodeID]*Node{}, stores: map[NodeID]*MemoryStore{},
		hs: map[NodeID]HardState{}, applied: map[NodeID][]Entry{}}
	for i := 1; i <= n; i++ {
		tn.ids = append(tn.ids, NodeID(i))
	}
	for _, id := range tn.ids {
		tn.stores[id] = NewMemoryStore()
		tn.start(id, mutate)
	}
	return tn
}

func (tn *tnet) start(id NodeID, mutate func(*Config)) {
	cfg := Config{ID: id, Peers: tn.ids, Store: tn.stores[id], HardState: tn.hs[id], Seed: uint64(id)}
	if mutate != nil {
		mutate(&cfg)
	}
	n, err := NewNode(cfg)
	if err != nil {
		tn.t.Fatal(err)
	}
	tn.nodes[id] = n
}

// process runs one Ready for id (if any) and queues its messages.
func (tn *tnet) process(id NodeID) bool {
	n := tn.nodes[id]
	if !n.HasReady() {
		return false
	}
	rd := n.Ready()
	st := tn.stores[id]
	if rd.Snapshot != nil {
		must(tn.t, st.ApplySnapshot(*rd.Snapshot))
	}
	if rd.HardState != nil {
		tn.hs[id] = *rd.HardState
	}
	if rd.TruncateFrom != 0 {
		must(tn.t, st.TruncateSuffix(rd.TruncateFrom))
	}
	if len(rd.Entries) > 0 {
		must(tn.t, st.Append(rd.Entries))
	}
	for _, m := range rd.Messages {
		tn.sent = append(tn.sent, m)
		if tn.drop == nil || !tn.drop(m) {
			tn.queue = append(tn.queue, m)
		}
	}
	tn.applied[id] = append(tn.applied[id], rd.CommittedEntries...)
	n.Advance(rd)
	return true
}

func (tn *tnet) processAll() {
	for _, id := range tn.ids {
		for tn.process(id) {
		}
	}
}

// settle delivers messages until the network is quiet.
func (tn *tnet) settle() {
	for range 10000 {
		tn.processAll()
		if len(tn.queue) == 0 {
			return
		}
		q := tn.queue
		tn.queue = nil
		for _, m := range q {
			if n := tn.nodes[m.To]; n != nil {
				_ = n.Step(m)
			}
		}
	}
	tn.t.Fatal("network did not settle")
}

func (tn *tnet) tickAll(k int) {
	for range k {
		for _, id := range tn.ids {
			tn.nodes[id].Tick()
		}
		tn.settle()
	}
}

func (tn *tnet) leader() NodeID {
	for _, id := range tn.ids {
		if tn.nodes[id].state == Leader {
			return id
		}
	}
	return None
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSingleNodeElectsItselfAndCommits(t *testing.T) {
	tn := newTestNet(t, 1, nil)
	tn.nodes[1].Campaign()
	tn.settle()
	if tn.leader() != 1 {
		t.Fatalf("single node is not leader: %v", tn.nodes[1].state)
	}
	e, err := tn.nodes[1].Propose([]byte("a"))
	must(t, err)
	tn.settle()
	if got := tn.nodes[1].log.committed; got != e.LastIndex {
		t.Fatalf("commit=%d want %d", got, e.LastIndex)
	}
	if a := tn.applied[1]; len(a) != 2 || string(a[1].Data) != "a" || a[0].Data != nil {
		t.Fatalf("applied %v, want [no-op, a]", a)
	}
}

func TestElectionAndReplication(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	tn.nodes[1].Campaign()
	tn.settle()
	if tn.leader() != 1 {
		t.Fatal("node 1 should be leader")
	}
	for range 5 {
		_, err := tn.nodes[1].Propose([]byte("x"))
		must(t, err)
	}
	tn.settle()
	tn.tickAll(1) // heartbeat carries the final commit index to followers
	for _, id := range tn.ids {
		if c := tn.nodes[id].log.committed; c != 6 {
			t.Fatalf("N%d commit=%d want 6", id, c)
		}
		if len(tn.applied[id]) != 6 {
			t.Fatalf("N%d applied %d entries", id, len(tn.applied[id]))
		}
	}
}

// Votes must be persisted before the response is sent: the vote and the
// response appear in the same Ready with MustSync set.
func TestVotePersistedBeforeResponse(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	n := tn.nodes[2]
	must(t, n.Step(Message{Type: MsgVote, From: 1, To: 2, Term: 1}))
	rd := n.Ready()
	if rd.HardState == nil || rd.HardState.Vote != 1 || rd.HardState.Term != 1 || !rd.MustSync {
		t.Fatalf("hard state %+v mustSync=%v; want vote=1 term=1 mustSync", rd.HardState, rd.MustSync)
	}
	if len(rd.Messages) != 1 || rd.Messages[0].Type != MsgVoteResp || !rd.Messages[0].Granted {
		t.Fatalf("messages %v", rd.Messages)
	}
	n.Advance(rd)
	// Same term, another candidate: rejected.
	must(t, n.Step(Message{Type: MsgVote, From: 3, To: 2, Term: 1}))
	rd = n.Ready()
	if len(rd.Messages) != 1 || rd.Messages[0].Granted {
		t.Fatalf("second vote in the same term granted: %v", rd.Messages)
	}
	n.Advance(rd)
}

func TestElectionRestriction(t *testing.T) {
	st := NewMemoryStore()
	must(t, st.Append([]Entry{{Term: 1, FirstIndex: 1, LastIndex: 1}, {Term: 3, FirstIndex: 2, LastIndex: 5}}))
	l := newRaftLog(st)
	cases := []struct {
		idx, term uint64
		ok        bool
	}{
		{5, 3, true}, {6, 3, true}, {4, 3, false}, {1, 4, true}, {100, 2, false}, {5, 2, false},
	}
	for _, c := range cases {
		if got := l.isUpToDate(c.idx, c.term); got != c.ok {
			t.Errorf("isUpToDate(%d,%d)=%v want %v", c.idx, c.term, got, c.ok)
		}
	}
}

// Figure 8: a leader must not commit an entry of an older term just because
// it is on a majority; it commits once an entry of its own term is.
func TestFigure8CommitRule(t *testing.T) {
	st := NewMemoryStore()
	must(t, st.Append([]Entry{{Term: 1, FirstIndex: 1, LastIndex: 1}, {Term: 2, FirstIndex: 2, LastIndex: 2}}))
	n, err := NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3, 4, 5}, Store: st, HardState: HardState{Term: 2, Commit: 1}})
	must(t, err)
	n.becomeCandidate() // term 3
	n.becomeLeader()    // appends no-op (3,t3)
	rd := n.Ready()
	must(t, st.Append(rd.Entries))
	n.Advance(rd)
	if n.durable != 3 {
		t.Fatalf("leader durable=%d", n.durable)
	}
	// Two followers ack index 2 (the term-2 entry): {1,2,3} hold index 2.
	for _, f := range []NodeID{2, 3} {
		must(t, n.Step(Message{Type: MsgAppResp, From: f, To: 1, Term: 3, Success: true, MatchIndex: 2}))
	}
	if n.log.committed != 1 {
		t.Fatalf("committed %d: old-term entry committed by counting replicas", n.log.committed)
	}
	// Once the current-term entry is on a majority, everything commits.
	for _, f := range []NodeID{2, 3} {
		must(t, n.Step(Message{Type: MsgAppResp, From: f, To: 1, Term: 3, Success: true, MatchIndex: 3}))
	}
	if n.log.committed != 3 {
		t.Fatalf("committed %d, want 3", n.log.committed)
	}
}

// The leader counts itself only up to its durable index.
func TestLeaderSelfCountsOnlyDurable(t *testing.T) {
	st := NewMemoryStore()
	n, err := NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3}, Store: st, AsyncDurability: true})
	must(t, err)
	n.becomeCandidate()
	n.becomeLeader()
	rd := n.Ready()
	must(t, st.Append(rd.Entries))
	n.Advance(rd)
	// One follower acks the no-op; the leader has not fsynced it yet.
	must(t, n.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, Success: true, MatchIndex: 1}))
	if n.log.committed != 0 {
		t.Fatalf("committed before the leader's own fsync")
	}
	n.ReportDurable(1, 1)
	if n.log.committed != 1 {
		t.Fatalf("not committed after fsync: %d", n.log.committed)
	}
}

// Async durability: a follower withholds its ack until the entries are durable.
func TestAsyncFollowerWithholdsAck(t *testing.T) {
	st := NewMemoryStore()
	n, err := NewNode(Config{ID: 2, Peers: []NodeID{1, 2, 3}, Store: st, AsyncDurability: true})
	must(t, err)
	must(t, n.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 1, Entries: []Entry{{Term: 1, FirstIndex: 1, LastIndex: 3}}}))
	rd := n.Ready()
	for _, m := range rd.Messages {
		if m.Type == MsgAppResp {
			t.Fatalf("ack sent before durability: %v", m)
		}
	}
	must(t, st.Append(rd.Entries))
	n.Advance(rd)
	if n.HasReady() {
		if rd := n.Ready(); len(rd.Messages) > 0 {
			t.Fatalf("ack sent after Advance without ReportDurable: %v", rd.Messages)
		} else {
			n.Advance(rd)
		}
	}
	n.ReportDurable(3, 0) // wrong term: ignored
	if n.durable != 0 {
		t.Fatalf("durable=%d after a stale report", n.durable)
	}
	n.ReportDurable(3, 1)
	rd = n.Ready()
	if len(rd.Messages) != 1 || rd.Messages[0].Type != MsgAppResp || rd.Messages[0].MatchIndex != 3 {
		t.Fatalf("expected the held ack, got %v", rd.Messages)
	}
	n.Advance(rd)
}

func TestStaleResponsesIgnored(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	tn.nodes[1].Campaign()
	tn.settle()
	n := tn.nodes[1]
	match := n.prs[2].Match
	// A response from an older term must not move Match.
	_ = n.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: n.term - 1, Success: true, MatchIndex: 99})
	if n.prs[2].Match != match {
		t.Fatalf("stale-term response changed Match %d → %d", match, n.prs[2].Match)
	}
	// A reordered (older) ack in the current term must not move it back.
	_ = n.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: n.term, Success: true, MatchIndex: 0})
	if n.prs[2].Match != match {
		t.Fatalf("reordered ack moved Match back")
	}
	// A higher term in any response deposes the leader.
	_ = n.Step(Message{Type: MsgHeartbeatResp, From: 3, To: 1, Term: n.term + 5})
	if n.state != Follower || n.term != tn.hs[1].Term+5 {
		t.Fatalf("leader did not step down on a higher-term response: %v t%d", n.state, n.term)
	}
}

// Students' guide: a rejected vote request (even at a higher term) does not
// reset the election timer.
func TestElectionTimerNotResetByRejectedVote(t *testing.T) {
	st := NewMemoryStore()
	must(t, st.Append([]Entry{{Term: 5, FirstIndex: 1, LastIndex: 10}}))
	n, err := NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3}, Store: st, HardState: HardState{Term: 5}})
	must(t, err)
	for range 7 {
		n.Tick()
	}
	must(t, n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 6, LastLogIndex: 3, LastLogTerm: 1}))
	if n.term != 6 || n.vote != None {
		t.Fatalf("term=%d vote=%d", n.term, n.vote)
	}
	if n.electionElapsed != 7 {
		t.Fatalf("electionElapsed reset to %d by a rejected vote", n.electionElapsed)
	}
	// A granted vote does reset it.
	must(t, n.Step(Message{Type: MsgVote, From: 3, To: 1, Term: 7, LastLogIndex: 10, LastLogTerm: 5}))
	if n.vote != 3 || n.electionElapsed != 0 {
		t.Fatalf("vote=%d elapsed=%d after a granted vote", n.vote, n.electionElapsed)
	}
}

// Pre-vote neither bumps the receiver's term nor the pre-candidate's.
func TestPreVoteDoesNotChangeTerms(t *testing.T) {
	tn := newTestNet(t, 3, func(c *Config) { c.PreVote = true })
	n := tn.nodes[1]
	n.Campaign()
	if n.state != PreCandidate || n.term != 0 {
		t.Fatalf("after Campaign: %v t%d", n.state, n.term)
	}
	tn.processAll()
	for _, m := range tn.queue {
		if m.Type != MsgPreVote || m.Term != 1 {
			t.Fatalf("unexpected %v", m)
		}
		_ = tn.nodes[m.To].Step(m)
		if tn.nodes[m.To].term != 0 {
			t.Fatalf("pre-vote bumped N%d's term", m.To)
		}
	}
	tn.queue = nil
	tn.settle()
	if n.state != Leader || n.term != 1 {
		t.Fatalf("pre-vote then vote should elect N1 in term 1: %v t%d", n.state, n.term)
	}
}

// With check-quorum, a follower that recently heard from the leader ignores
// a disruptive higher-term vote request (leader stickiness), unless forced
// by a leadership transfer.
func TestLeaderStickiness(t *testing.T) {
	tn := newTestNet(t, 3, func(c *Config) { c.CheckQuorum = true })
	tn.nodes[1].Campaign()
	tn.settle()
	f := tn.nodes[2]
	term := f.term
	must(t, f.Step(Message{Type: MsgVote, From: 3, To: 2, Term: term + 1, LastLogIndex: 100, LastLogTerm: 100}))
	if f.term != term {
		t.Fatalf("in-lease follower adopted a disruptive term")
	}
	must(t, f.Step(Message{Type: MsgVote, From: 3, To: 2, Term: term + 1, LastLogIndex: 100, LastLogTerm: 100, Force: true}))
	if f.term != term+1 || f.vote != 3 {
		t.Fatalf("forced (transfer) vote not honoured: t%d vote=%d", f.term, f.vote)
	}
}

func TestCheckQuorumStepsDown(t *testing.T) {
	tn := newTestNet(t, 3, func(c *Config) { c.CheckQuorum = true })
	tn.nodes[1].Campaign()
	tn.settle()
	tn.drop = func(Message) bool { return true }
	n := tn.nodes[1]
	for i := 0; i < 2*n.cfg.ElectionTick; i++ {
		n.Tick()
		tn.processAll()
	}
	if n.state == Leader {
		t.Fatal("leader without quorum contact did not step down")
	}
}

// A delayed duplicate AppendEntries must not truncate newer entries.
func TestDelayedAppendDoesNotTruncate(t *testing.T) {
	st := NewMemoryStore()
	n, err := NewNode(Config{ID: 2, Peers: []NodeID{1, 2, 3}, Store: st})
	must(t, err)
	step := func(m Message) Ready {
		must(t, n.Step(m))
		rd := n.Ready()
		if rd.TruncateFrom != 0 {
			must(t, st.TruncateSuffix(rd.TruncateFrom))
		}
		must(t, st.Append(rd.Entries))
		n.Advance(rd)
		return rd
	}
	ents := []Entry{{Term: 1, FirstIndex: 1, LastIndex: 1}, {Term: 1, FirstIndex: 2, LastIndex: 4}, {Term: 1, FirstIndex: 5, LastIndex: 5}}
	step(Message{Type: MsgApp, From: 1, To: 2, Term: 1, Entries: ents})
	rd := step(Message{Type: MsgApp, From: 1, To: 2, Term: 1, PrevLogIndex: 1, PrevLogTerm: 1, Entries: ents[1:2]})
	if rd.TruncateFrom != 0 || len(rd.Entries) != 0 || st.LastIndex() != 5 {
		t.Fatalf("delayed MsgApp truncated the log: truncate=%d entries=%v last=%d", rd.TruncateFrom, rd.Entries, st.LastIndex())
	}
	if m := rd.Messages[0]; !m.Success || m.MatchIndex != 4 {
		t.Fatalf("ack %v, want success match=4", m)
	}
}

// commit = min(leaderCommit, index of last NEW entry), not min with lastIndex.
func TestFollowerCommitUsesLastNewEntry(t *testing.T) {
	st := NewMemoryStore()
	// Stale tail from an old leader of term 1 beyond index 2.
	must(t, st.Append([]Entry{{Term: 1, FirstIndex: 1, LastIndex: 1}, {Term: 1, FirstIndex: 2, LastIndex: 2}, {Term: 1, FirstIndex: 3, LastIndex: 5}}))
	n, err := NewNode(Config{ID: 2, Peers: []NodeID{1, 2, 3}, Store: st, HardState: HardState{Term: 1}})
	must(t, err)
	must(t, n.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 2, PrevLogIndex: 2, PrevLogTerm: 1, Commit: 5}))
	if n.log.committed != 2 {
		t.Fatalf("committed %d, want 2 (entries 3..5 did not come from this leader)", n.log.committed)
	}
}

// Fast backup: a follower with a long divergent suffix is repaired in a few
// round trips, and every MsgApp's prevLogIndex is a batch end on the leader
// even though leader and follower have different batch boundaries.
func TestFastBackupWithBatches(t *testing.T) {
	leaderStore := NewMemoryStore()
	followerStore := NewMemoryStore()
	var le, fe []Entry
	next := uint64(1)
	add := func(dst *[]Entry, term, span uint64, at *uint64) {
		*dst = append(*dst, Entry{Term: term, FirstIndex: *at, LastIndex: *at + span - 1, Data: []byte{byte(term)}})
		*at += span
	}
	for range 10 { // common prefix
		add(&le, 1, 3, &next)
	}
	fe = slices.Clone(le)
	ln, fn := next, next
	for range 50 { // divergent suffixes with different batch sizes
		add(&le, 3, 2, &ln)
		add(&fe, 2, 5, &fn)
	}
	must(t, leaderStore.Append(le))
	must(t, followerStore.Append(fe))
	leader, err := NewNode(Config{ID: 1, Peers: []NodeID{1, 2}, Store: leaderStore, HardState: HardState{Term: 3}})
	must(t, err)
	follower, err := NewNode(Config{ID: 2, Peers: []NodeID{1, 2}, Store: followerStore, HardState: HardState{Term: 3}})
	must(t, err)
	leader.becomeCandidate()
	leader.becomeLeader()
	stores := map[NodeID]*MemoryStore{1: leaderStore, 2: followerStore}
	nodes := map[NodeID]*Node{1: leader, 2: follower}
	rejections := 0
	for range 200 {
		var q []Message
		for _, id := range []NodeID{1, 2} {
			n := nodes[id]
			for n.HasReady() {
				rd := n.Ready()
				if rd.TruncateFrom != 0 {
					must(t, stores[id].TruncateSuffix(rd.TruncateFrom))
				}
				must(t, stores[id].Append(rd.Entries))
				q = append(q, rd.Messages...)
				n.Advance(rd)
			}
		}
		if len(q) == 0 {
			break
		}
		for _, m := range q {
			if m.Type == MsgApp && m.PrevLogIndex > 0 {
				if _, ok := endsBatch(leaderStore, leader, m.PrevLogIndex); !ok {
					t.Fatalf("prevLogIndex %d is not a batch end on the leader", m.PrevLogIndex)
				}
			}
			if m.Type == MsgAppResp && !m.Success {
				rejections++
			}
			must(t, nodes[m.To].Step(m))
		}
	}
	if rejections == 0 {
		t.Fatal("scenario did not exercise backup")
	}
	if rejections > 3 {
		t.Fatalf("%d rejections to repair a 50-batch divergence (fast backup not effective)", rejections)
	}
	a, b := leaderStore.AllEntries(), followerStore.AllEntries()
	if !slices.EqualFunc(a, b, func(x, y Entry) bool {
		return x.Term == y.Term && x.FirstIndex == y.FirstIndex && x.LastIndex == y.LastIndex && string(x.Data) == string(y.Data)
	}) {
		t.Fatalf("logs differ after repair:\nleader   %v\nfollower %v", a[len(a)-3:], b[max(0, len(b)-3):])
	}
}

func endsBatch(st *MemoryStore, n *Node, i uint64) (uint64, bool) {
	for _, e := range st.AllEntries() {
		if e.LastIndex == i {
			return e.Term, true
		}
	}
	for _, e := range n.log.ents {
		if e.LastIndex == i {
			return e.Term, true
		}
	}
	return 0, false
}

func TestProposeNotLeaderAndStamp(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	tn.nodes[1].Campaign()
	tn.settle()
	_, err := tn.nodes[2].Propose([]byte("x"))
	var nle *NotLeaderError
	if !errors.Is(err, ErrNotLeader) || !errors.As(err, &nle) || nle.Leader != 1 {
		t.Fatalf("got %v, want NotLeaderError{Leader:1}", err)
	}
	e, err := tn.nodes[1].ProposeBatch(4, nil, func(e *Entry) {
		e.Data = []byte{byte(e.FirstIndex), byte(e.LastIndex), byte(e.Term)}
	})
	must(t, err)
	if e.FirstIndex != 2 || e.LastIndex != 5 || string(e.Data) != string([]byte{2, 5, 1}) {
		t.Fatalf("stamped entry %v data=%v", e, e.Data)
	}
	if _, err := tn.nodes[1].ProposeBatch(1, nil, func(e *Entry) { e.LastIndex++ }); err == nil {
		t.Fatal("stamp changing indices accepted")
	}
	if err := tn.nodes[1].Step(Message{Type: MsgProp, Entries: []Entry{{Data: []byte("p")}}}); err != nil {
		t.Fatal(err)
	}
	if err := tn.nodes[2].Step(Message{Type: MsgProp, Entries: []Entry{{Data: []byte("p")}}}); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("MsgProp on follower: %v", err)
	}
}

func TestReadyContractEnforced(t *testing.T) {
	tn := newTestNet(t, 1, nil)
	n := tn.nodes[1]
	n.Campaign()
	_ = n.Ready()
	defer func() {
		if recover() == nil {
			t.Fatal("Step between Ready and Advance did not panic")
		}
	}()
	n.Tick()
}

func TestReadIndexWaitsForCurrentTermCommit(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	n := tn.nodes[1]
	n.becomeCandidate()
	n.becomeLeader() // no-op not yet committed
	must(t, n.ReadIndex(42))
	rd := n.Ready()
	if len(rd.ReadStates) != 0 {
		t.Fatalf("read confirmed before the leader committed in its term: %v", rd.ReadStates)
	}
	for _, m := range rd.Messages {
		if m.Type == MsgHeartbeat {
			t.Fatalf("heartbeat round started for a read that must wait")
		}
	}
	must(t, tn.stores[1].Append(rd.Entries))
	tn.queue = append(tn.queue, rd.Messages...)
	n.Advance(rd)
	var got []ReadState
	for range 100 {
		for _, id := range tn.ids {
			nd := tn.nodes[id]
			for nd.HasReady() {
				rd := nd.Ready()
				must(t, tn.stores[id].Append(rd.Entries))
				if id == 1 {
					got = append(got, rd.ReadStates...)
				}
				tn.queue = append(tn.queue, rd.Messages...)
				nd.Advance(rd)
			}
		}
		q := tn.queue
		tn.queue = nil
		for _, m := range q {
			_ = tn.nodes[m.To].Step(m)
		}
	}
	if len(got) != 1 || got[0].Context != 42 || got[0].Index != 1 {
		t.Fatalf("read states %v, want [{1 42}]", got)
	}
}

func TestReadIndexFollowerForwards(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	tn.nodes[1].Campaign()
	tn.settle()
	var got []ReadState
	must(t, tn.nodes[2].ReadIndex(7))
	for range 50 {
		for _, id := range tn.ids {
			nd := tn.nodes[id]
			for nd.HasReady() {
				rd := nd.Ready()
				must(t, tn.stores[id].Append(rd.Entries))
				if id == 2 {
					got = append(got, rd.ReadStates...)
				}
				tn.queue = append(tn.queue, rd.Messages...)
				nd.Advance(rd)
			}
		}
		q := tn.queue
		tn.queue = nil
		for _, m := range q {
			_ = tn.nodes[m.To].Step(m)
		}
	}
	if len(got) != 1 || got[0].Context != 7 || got[0].Index != tn.nodes[1].log.committed {
		t.Fatalf("follower read states %v (leader commit %d)", got, tn.nodes[1].log.committed)
	}
	if err := tn.nodes[2].Step(Message{Type: MsgReadIndex, Context: 8}); err != nil {
		t.Fatal(err)
	}
}

func TestRestartClampsCommitAndKeepsVote(t *testing.T) {
	st := NewMemoryStore()
	must(t, st.Append([]Entry{{Term: 2, FirstIndex: 1, LastIndex: 3}}))
	n, err := NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3}, Store: st, HardState: HardState{Term: 4, Vote: 2, Commit: 10}})
	must(t, err)
	if n.term != 4 || n.vote != 2 || n.log.committed != 3 || n.state != Follower {
		t.Fatalf("restart state t%d vote=%d commit=%d %v", n.term, n.vote, n.log.committed, n.state)
	}
	rd := n.Ready()
	if len(rd.CommittedEntries) != 1 {
		t.Fatalf("committed entries on restart: %v", rd.CommittedEntries)
	}
	n.Advance(rd)
}

func TestSnapshotInstallAndIgnoreOld(t *testing.T) {
	st := NewMemoryStore()
	n, err := NewNode(Config{ID: 2, Peers: []NodeID{1, 2, 3}, Store: st})
	must(t, err)
	snap := Snapshot{Index: 10, Term: 2, Peers: []NodeID{1, 2, 3}, Data: []byte("state")}
	must(t, n.Step(Message{Type: MsgSnap, From: 1, To: 2, Term: 2, Snapshot: &snap}))
	rd := n.Ready()
	if rd.Snapshot == nil || rd.Snapshot.Index != 10 || len(rd.CommittedEntries) != 0 {
		t.Fatalf("ready %+v", rd)
	}
	if m := rd.Messages[0]; m.Type != MsgSnapResp || !m.Success || m.MatchIndex != 10 {
		t.Fatalf("resp %v", m)
	}
	must(t, st.ApplySnapshot(*rd.Snapshot))
	n.Advance(rd)
	if n.log.applied != 10 || n.log.committed != 10 || n.log.firstIndex() != 11 {
		t.Fatalf("after install applied=%d commit=%d first=%d", n.log.applied, n.log.committed, n.log.firstIndex())
	}
	old := Snapshot{Index: 5, Term: 1}
	must(t, n.Step(Message{Type: MsgSnap, From: 1, To: 2, Term: 2, Snapshot: &old}))
	rd = n.Ready()
	if rd.Snapshot != nil {
		t.Fatal("snapshot below commit was installed")
	}
	n.Advance(rd)
	// The follower continues from the snapshot.
	must(t, n.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 2, PrevLogIndex: 10, PrevLogTerm: 2,
		Entries: []Entry{{Term: 2, FirstIndex: 11, LastIndex: 12}}, Commit: 12}))
	rd = n.Ready()
	must(t, st.Append(rd.Entries))
	if len(rd.CommittedEntries) != 1 || rd.CommittedEntries[0].LastIndex != 12 {
		t.Fatalf("committed after snapshot %v", rd.CommittedEntries)
	}
	n.Advance(rd)
}

// The leader switches a follower to snapshot state when it needs compacted
// entries, and resumes replication after the snapshot is acknowledged.
func TestLeaderSendsSnapshotWhenCompacted(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	tn.drop = func(m Message) bool { return m.To == 3 || m.From == 3 }
	tn.nodes[1].Campaign()
	tn.settle()
	for range 10 {
		_, err := tn.nodes[1].Propose([]byte("x"))
		must(t, err)
		tn.settle()
	}
	l := tn.nodes[1]
	must(t, tn.stores[1].Compact(l.log.applied, tn.ids, []byte("sm")))
	tn.drop = nil
	tn.tickAll(3)
	if c := tn.nodes[3].log.committed; c != l.log.committed {
		t.Fatalf("follower 3 commit=%d, leader %d", c, l.log.committed)
	}
	if s, _ := tn.stores[3].Snapshot(); s.Index == 0 || string(s.Data) != "sm" {
		t.Fatalf("follower 3 did not install the snapshot: %+v", s)
	}
	found := false
	for _, m := range tn.sent {
		found = found || m.Type == MsgSnap
	}
	if !found {
		t.Fatal("no MsgSnap sent")
	}
}

func TestLeadershipTransferUnit(t *testing.T) {
	tn := newTestNet(t, 3, func(c *Config) { c.CheckQuorum = true; c.PreVote = true })
	tn.nodes[1].Campaign()
	tn.settle()
	must(t, tn.nodes[1].TransferLeadership(3))
	tn.settle()
	if tn.leader() != 3 || tn.nodes[3].term != 2 {
		t.Fatalf("leader=%d term=%d after transfer", tn.leader(), tn.nodes[3].term)
	}
	if err := tn.nodes[1].TransferLeadership(2); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("transfer on follower: %v", err)
	}
	if err := tn.nodes[3].TransferLeadership(9); err == nil {
		t.Fatal("transfer to non-peer accepted")
	}
}

func TestConfigValidation(t *testing.T) {
	bad := []Config{
		{ID: 0, Peers: []NodeID{1}, Store: NewMemoryStore()},
		{ID: 1, Peers: []NodeID{2}, Store: NewMemoryStore()},
		{ID: 1, Peers: []NodeID{1, 1}, Store: NewMemoryStore()},
		{ID: 1, Peers: []NodeID{1}},
		{ID: 1, Peers: []NodeID{1}, Store: NewMemoryStore(), ElectionTick: 2, HeartbeatTick: 2},
		{ID: 1, Peers: []NodeID{1}, Store: NewMemoryStore(), UnsafeLeaseReads: true},
	}
	for i, c := range bad {
		if _, err := NewNode(c); err == nil {
			t.Errorf("config %d accepted", i)
		}
	}
}

func TestStepRejectsMisroutedMessages(t *testing.T) {
	tn := newTestNet(t, 3, func(c *Config) { c.Group = 7 })
	n := tn.nodes[1]
	if err := n.Step(Message{Type: MsgApp, Group: 8, From: 2, To: 1, Term: 1}); !errors.Is(err, ErrWrongGroup) {
		t.Fatalf("wrong group: %v", err)
	}
	if err := n.Step(Message{Type: MsgApp, Group: 7, From: 2, To: 3, Term: 1}); !errors.Is(err, ErrWrongRecipient) {
		t.Fatalf("wrong recipient: %v", err)
	}
	if err := n.Step(Message{Type: MsgApp, Group: 7, From: 9, To: 1, Term: 1}); !errors.Is(err, ErrUnknownPeer) {
		t.Fatalf("unknown peer: %v", err)
	}
	bad := Message{Type: MsgApp, Group: 7, From: 2, To: 1, Term: 1, Entries: []Entry{{Term: 1, FirstIndex: 3, LastIndex: 3}}}
	if err := n.Step(bad); !errors.Is(err, ErrInvalidEntries) {
		t.Fatalf("non-contiguous entries: %v", err)
	}
}

// A transfer whose target catches up through InstallSnapshot (the leader's
// log was compacted up to its last index) must complete as soon as the
// snapshot is acknowledged: the MsgSnapResp path has to check the transferee
// like the MsgAppResp path does. Otherwise the transfer stalls until it times
// out after an election timeout.
func TestLeadershipTransferAfterSnapshot(t *testing.T) {
	tn := newTestNet(t, 3, func(c *Config) { c.CheckQuorum = true; c.PreVote = true })
	tn.drop = func(m Message) bool { return m.To == 3 || m.From == 3 }
	tn.nodes[1].Campaign()
	tn.settle()
	for range 5 {
		_, err := tn.nodes[1].Propose([]byte("x"))
		must(t, err)
		tn.settle()
	}
	l := tn.nodes[1]
	must(t, tn.stores[1].Compact(l.log.applied, tn.ids, []byte("sm")))
	must(t, l.TransferLeadership(3))
	tn.settle()
	tn.drop = nil
	tn.tickAll(1) // heartbeat → probe → snapshot → ack → TimeoutNow
	if tn.leader() != 3 {
		t.Fatalf("leader=%d after transfer via snapshot (transferee=%d)", tn.leader(), l.leadTransferee)
	}
}
