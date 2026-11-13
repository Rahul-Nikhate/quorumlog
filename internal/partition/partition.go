// Package partition hosts replicated partitions: one Raft group per
// partition replica, whose Raft log IS the partition's segmented log
// (internal/raftlog), driven by one goroutine per replica (spec §5.4.9).
//
// # Driver (single writer)
//
// Partition.run is the only goroutine that touches the raft.Node, the log
// writer and the producer state. It multiplexes the tick, inbound Raft
// messages (from the transport, via Manager.Handle → Deliver), produce
// proposals, ReadIndex requests, control requests and "durable up to X"
// notifications from the log's group-commit flusher; after a bounded drain of
// queued inputs it processes Ready/Advance until the node is quiescent.
//
// The node runs with raft.Config.AsyncDurability: entries are written to the
// page cache and messages sent right away, the flusher fsyncs in the
// background, and the driver reports durability with ReportDurable. The
// node withholds follower acks above the durable index and counts the leader
// toward commit only up to its durable index (thesis §10.2.1), so "committed"
// always means "durable on a majority" under policy always.
//
// # Offsets and the high watermark
//
// Log offset == Raft index (logs start at offset 1; index 0 is Raft's
// empty sentinel). A batch's Raft index is its last offset. HighWatermark =
// applied index + 1 = the first offset not known committed on this replica;
// Fetch only ever returns batches entirely below it, and it never exceeds
// commit+1. On restart the HW resumes from the persisted commit hint.
// Produce/Fetch/Status use log offsets; the message API in messages.go
// (ProduceMessages/ReadMessages, used by the cluster and HTTP layers) uses
// 0-based API offsets = log offset - 1.
//
// # Produce
//
// Produce(acks=all) completes when the batch's entry is applied (committed);
// acks=leader when the leader's log has it durably (per the fsync policy).
// Waiters are failed with an error matching both ErrOutcomeUnknown and
// ErrNotLeader when leadership changes (the entry may still be committed by
// the next leader). Idempotent producers are checked and deduplicated before
// proposing (ProducerState); a string producer id maps to the numeric id in
// the batch header (ProducerIDFor).
package partition

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Rahul-Nikhate/quorumlog/internal/log"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
	"github.com/Rahul-Nikhate/quorumlog/internal/raftlog"
)

type produceResult struct {
	res ProduceResult
	err error
}

type proposal struct {
	batch log.Batch
	acks  Acks
	done  chan produceResult // capacity 1
}

func (pr *proposal) reply(res ProduceResult, err error) { pr.done <- produceResult{res, err} }

type waiter struct {
	res  ProduceResult
	term uint64 // term of the entry the waiter needs committed
	pr   *proposal
}

// waiterQueue is ordered by LastOffset. New proposals get increasing offsets
// (spec §5.5) and are appended; a duplicate of a batch still in flight waits
// for that older offset and is inserted before any newer waiters.
type waiterQueue struct{ q []*waiter }

func (w *waiterQueue) push(x *waiter) {
	i := len(w.q)
	for i > 0 && w.q[i-1].res.LastOffset > x.res.LastOffset {
		i--
	}
	w.q = slices.Insert(w.q, i, x)
}
func (w *waiterQueue) len() int { return len(w.q) }
func (w *waiterQueue) front() *waiter {
	if len(w.q) == 0 {
		return nil
	}
	return w.q[0]
}
func (w *waiterQueue) pop() *waiter {
	x := w.q[0]
	w.q[0] = nil
	w.q = w.q[1:]
	if len(w.q) == 0 {
		w.q = nil
	}
	return x
}
func (w *waiterQueue) failAll(err error) {
	for _, x := range w.q {
		x.pr.reply(x.res, err)
	}
	w.q = nil
}

type readReq struct {
	done     chan readResult // capacity 1
	deadline time.Time
	term     uint64
	index    uint64
}

type readResult struct {
	index uint64
	err   error
}

type ctrlReq struct {
	fn   func() error
	done chan error // capacity 1
}

type proposedEntry struct{ first, term uint64 }

// Partition is one replica of a replicated partition.
type Partition struct {
	cfg     Config
	store   *raftlog.Store
	lg      *log.Log
	hsStore raft.HardStateStore
	node    *raft.Node // driver goroutine only

	inbox chan raft.Message
	propc chan *proposal
	readc chan *readReq
	ctrlc chan *ctrlReq
	durc  chan struct{} // capacity 1: "the durable offset advanced"
	stopc chan struct{}
	halt  atomic.Bool // set by Halt: stop without further disk writes
	donec chan struct{}
	stop  sync.Once

	// Driver-goroutine state.
	ps          *ProducerState // speculative: the whole log incl. proposals
	committedPS *ProducerState // as of p.applied
	proposed    []proposedEntry
	needRebuild bool
	acks1       waiterQueue
	acksAll     waiterQueue
	waiterTerm  uint64
	reads       map[uint64]*readReq
	pending     []*readReq // confirmed, waiting for applied >= index
	readSeq     uint64
	applied     uint64
	lastHS      raft.HardState
	savedHS     raft.HardState
	savedHSAt   time.Time
	tickErr     error

	hw      atomic.Int64
	hwN     *hwNotifier
	dropped atomic.Uint64

	stMu sync.Mutex
	st   Status
}

// Open opens (or creates) the replica in cfg.Dir — log recovery, snapshot
// reconciliation, producer-state rebuild — and starts its driver goroutine.
func Open(cfg Config) (*Partition, error) {
	if err := cfg.withDefaults(); err != nil {
		return nil, err
	}
	p := &Partition{
		cfg:   cfg,
		inbox: make(chan raft.Message, cfg.InboxSize),
		propc: make(chan *proposal, cfg.ProposalQueue),
		readc: make(chan *readReq, 256),
		ctrlc: make(chan *ctrlReq, 16),
		durc:  make(chan struct{}, 1),
		stopc: make(chan struct{}),
		donec: make(chan struct{}),
		reads: map[uint64]*readReq{},
		hwN:   newHWNotifier(),
	}
	lc := cfg.Log
	lc.OnDurable = func(int64) {
		select {
		case p.durc <- struct{}{}:
		default:
		}
	}
	store, err := raftlog.Open(raftlog.Config{Dir: cfg.Dir, FS: cfg.FS, Log: lc})
	if err != nil {
		return nil, err
	}
	if err := p.init(store); err != nil {
		_ = store.Close()
		return nil, err
	}
	go p.run()
	return p, nil
}

func (p *Partition) init(store *raftlog.Store) error {
	cfg := p.cfg
	p.store, p.lg = store, store.Log()
	p.hsStore = raftlog.NewHardStateStore(cfg.FS, cfg.Dir)
	hs, err := p.hsStore.Load()
	if err != nil {
		return err
	}
	snap, err := store.Snapshot()
	if err != nil {
		return err
	}
	base, err := DecodeProducerState(snap.Data)
	if err != nil {
		return err
	}
	// The persisted commit is a committed index (raft never emits one above
	// the durable log), so everything up to it can be applied right away.
	applied := max(snap.Index, min(hs.Commit, store.LastIndex()))
	if err := replay(store, base, snap.Index+1, applied+1); err != nil {
		return err
	}
	p.committedPS = base
	p.ps = base.Clone()
	if err := replay(store, p.ps, applied+1, store.LastIndex()+1); err != nil {
		return err
	}
	p.applied = applied
	p.hw.Store(int64(applied) + 1)
	p.lastHS, p.savedHS, p.savedHSAt = hs, hs, time.Now()
	seed := cfg.Seed
	if seed == 0 {
		seed = rand.Uint64()
	}
	rc := raft.Config{
		ID:              cfg.ID,
		Group:           cfg.Group,
		Peers:           cfg.Peers,
		Store:           store,
		HardState:       hs,
		Applied:         applied,
		ElectionTick:    cfg.ElectionTick,
		HeartbeatTick:   cfg.HeartbeatTick,
		PreVote:         !cfg.DisablePreVote,
		CheckQuorum:     !cfg.DisableCheckQuorum,
		AsyncDurability: true,
		NoopData:        noopBatch,
		Seed:            seed,
		Logger:          cfg.RaftLogger,
	}
	if cfg.RaftConfigHook != nil {
		cfg.RaftConfigHook(&rc)
	}
	n, err := raft.NewNode(rc)
	if err != nil {
		return err
	}
	p.node = n
	p.publish()
	return nil
}

// replay feeds the batches of [lo, hi) into ps.
func replay(store *raftlog.Store, ps *ProducerState, lo, hi uint64) error {
	for lo < hi {
		ents, err := store.Entries(lo, hi, 4<<20)
		if err != nil {
			return fmt.Errorf("partition: replay producer state [%d,%d): %w", lo, hi, err)
		}
		for _, e := range ents {
			ps.Append(log.Batch(e.Data))
		}
		lo = ents[len(ents)-1].LastIndex + 1
	}
	return nil
}

// ---------------------------------------------------------------------------
// The driver.

func (p *Partition) run() {
	defer close(p.donec)
	t := time.NewTicker(p.cfg.TickInterval)
	defer t.Stop()
	for {
		select {
		case <-p.stopc:
			if p.halt.Load() {
				p.failOutstanding(ErrClosed)
				p.markStopped(ErrClosed)
				return
			}
			p.shutdown(ErrClosed)
			return
		case <-t.C:
			p.node.Tick()
			p.onTick()
		case m := <-p.inbox:
			_ = p.node.Step(m) // misrouted/invalid messages are dropped
		case pr := <-p.propc:
			p.handlePropose(pr)
		case r := <-p.readc:
			p.handleRead(r)
		case c := <-p.ctrlc:
			err := c.fn()
			p.publish() // callers may read Status right after
			c.done <- err
		case <-p.durc:
		}
		p.drain(256)
		if err := p.processReady(); err != nil {
			p.cfg.Logger.Error("partition failed (fail-stop)", "tp", p.cfg.TopicPartition.String(), "err", err)
			p.shutdown(fmt.Errorf("%w: %w", ErrStorage, err))
			return
		}
	}
}

// drain handles up to n more queued inputs without blocking, so one Ready
// covers many of them (batching).
func (p *Partition) drain(n int) {
	for range n {
		select {
		case m := <-p.inbox:
			_ = p.node.Step(m)
		case pr := <-p.propc:
			p.handlePropose(pr)
		case r := <-p.readc:
			p.handleRead(r)
		default:
			return
		}
	}
}

func (p *Partition) processReady() error {
	if err := p.tickErr; err != nil {
		return err
	}
	for {
		p.reportDurable()
		if !p.node.HasReady() {
			break
		}
		rd := p.node.Ready()
		if err := p.handleReady(rd); err != nil {
			return err // no Advance: the node is abandoned (fail-stop)
		}
		p.node.Advance(rd)
	}
	p.checkLeadership()
	p.completeAcks1()
	p.publish()
	return nil
}

// reportDurable forwards the log's durable watermark to the node. The
// DurableOffset is read on the driver goroutine, which is the only writer, so
// the entry at that offset is the one that was fsynced (a truncation lowers
// the watermark before rewriting), and it is always a batch end.
func (p *Partition) reportDurable() {
	d := p.lg.DurableOffset()
	if d <= 0 {
		return
	}
	st := p.node.BasicStatus()
	if uint64(d) <= st.Durable || uint64(d) > st.LastIndex {
		return
	}
	term, err := p.store.Term(uint64(d))
	if err != nil {
		return
	}
	p.node.ReportDurable(uint64(d), term)
}

// handleReady performs one Ready in the contract's order (package raft doc).
func (p *Partition) handleReady(rd raft.Ready) error {
	// 1. Snapshot (InstallSnapshot): durable, then restore producer state.
	if rd.Snapshot != nil {
		s := *rd.Snapshot
		if err := p.store.ApplySnapshot(s); err != nil {
			return err
		}
		ps, err := DecodeProducerState(s.Data)
		if err != nil {
			return err
		}
		p.committedPS, p.ps = ps, ps.Clone()
		p.proposed, p.needRebuild = nil, false
		p.applied = s.Index
		p.hw.Store(int64(s.Index) + 1)
		p.hwN.Broadcast()
	}
	// 2. HardState: term/vote changes are fsynced before any message leaves;
	//    commit-only changes are persisted at most every HardStateSyncInterval.
	if rd.HardState != nil {
		p.lastHS = *rd.HardState
		if rd.MustSync || time.Since(p.savedHSAt) >= p.cfg.HardStateSyncInterval {
			if err := p.saveHardState(); err != nil {
				return err
			}
		}
	}
	// 3. Truncate a conflicting (never committed) suffix.
	if rd.TruncateFrom != 0 {
		if err := p.store.TruncateSuffix(rd.TruncateFrom); err != nil {
			return err
		}
		p.needRebuild = true
	}
	// 4. Append (page cache; the flusher fsyncs per the policy and reports).
	if len(rd.Entries) > 0 {
		if err := p.store.Append(rd.Entries); err != nil {
			return err
		}
		for _, e := range rd.Entries {
			if len(p.proposed) > 0 && p.proposed[0] == (proposedEntry{e.FirstIndex, e.Term}) {
				p.proposed = p.proposed[1:] // already in ps (recorded at propose time)
				continue
			}
			p.ps.Append(log.Batch(e.Data))
		}
	}
	if len(p.proposed) > 0 {
		// A proposal of this iteration vanished from the log (we stepped down
		// and a new leader's entries replaced it in memory): ps is wrong.
		p.proposed, p.needRebuild = nil, true
	}
	if p.needRebuild {
		ps := p.committedPS.Clone()
		if err := replay(p.store, ps, p.applied+1, p.store.LastIndex()+1); err != nil {
			return err
		}
		p.ps, p.needRebuild = ps, false
	}
	// 5–6. Send (AsyncDurability: entries need not be durable yet; the node
	//      withholds acks that depend on them).
	if len(rd.Messages) > 0 {
		p.cfg.Transport.Send(rd.Messages)
	}
	// 7. Apply committed entries in order: HW, committed producer state,
	//    acks=all waiters.
	if len(rd.CommittedEntries) > 0 {
		for _, e := range rd.CommittedEntries {
			if e.LastIndex <= p.applied {
				continue
			}
			if len(e.Data) >= log.BatchHeaderSize {
				p.committedPS.Append(log.Batch(e.Data))
			}
			p.applied = e.LastIndex
			p.completeAll(e)
		}
		p.hw.Store(int64(p.applied) + 1)
		p.hwN.Broadcast()
		p.resolveReads()
	}
	// 8. Confirmed ReadIndex requests.
	for _, rs := range rd.ReadStates {
		r, ok := p.reads[rs.Context]
		if !ok {
			continue
		}
		delete(p.reads, rs.Context)
		r.index = rs.Index
		p.pending = append(p.pending, r)
	}
	if len(rd.ReadStates) > 0 {
		p.resolveReads()
	}
	return nil
}

func (p *Partition) saveHardState() error {
	if err := p.hsStore.Save(p.lastHS); err != nil {
		return err
	}
	p.savedHS, p.savedHSAt = p.lastHS, time.Now()
	return nil
}

// completeAll completes acks=all waiters covered by the newly applied entry.
func (p *Partition) completeAll(e raft.Entry) {
	for w := p.acksAll.front(); w != nil && uint64(w.res.LastOffset) <= e.LastIndex; w = p.acksAll.front() {
		p.acksAll.pop()
		if uint64(w.res.LastOffset) == e.LastIndex && w.term == e.Term {
			w.pr.reply(w.res, nil)
			continue
		}
		// Cannot happen while we stay leader of waiterTerm (a leader never
		// truncates its log); be conservative if it does.
		p.cfg.Logger.Error("produce waiter's entry was replaced", "tp", p.cfg.TopicPartition.String(),
			"last", w.res.LastOffset, "term", w.term, "committed", e.String())
		w.pr.reply(w.res, unknown(&NotLeaderError{Term: e.Term}))
	}
}

// completeAcks1 completes acks=1 waiters whose batch is durable locally.
func (p *Partition) completeAcks1() {
	d := p.lg.DurableOffset()
	for w := p.acks1.front(); w != nil && w.res.LastOffset <= d; w = p.acks1.front() {
		p.acks1.pop()
		w.pr.reply(w.res, nil)
	}
}

// checkLeadership fails produce waiters (outcome unknown) and reads (no side
// effects) once this replica is no longer the leader of the term in which
// they were registered.
func (p *Partition) checkLeadership() {
	st := p.node.BasicStatus()
	leader := st.State == raft.Leader
	if p.acks1.len()+p.acksAll.len() > 0 && (!leader || st.Term != p.waiterTerm) {
		err := unknown(&NotLeaderError{Leader: st.Lead, Term: st.Term})
		p.acks1.failAll(err)
		p.acksAll.failAll(err)
	}
	// Unconfirmed ReadIndex requests die with the term (raft drops them).
	// Confirmed ones (p.pending) stay valid: their index was confirmed by a
	// quorum while we led, so serving this replica's state once it has
	// applied that index is still linearizable.
	if len(p.reads) > 0 {
		err := &NotLeaderError{Leader: st.Lead, Term: st.Term}
		for ctx, r := range p.reads {
			if !leader || r.term != st.Term {
				r.done <- readResult{err: err}
				delete(p.reads, ctx)
			}
		}
	}
}

func (p *Partition) resolveReads() {
	k := 0
	for _, r := range p.pending {
		if r.index <= p.applied {
			r.done <- readResult{index: r.index}
			continue
		}
		p.pending[k] = r
		k++
	}
	clear(p.pending[k:])
	p.pending = p.pending[:k]
}

func (p *Partition) onTick() {
	now := time.Now()
	for ctx, r := range p.reads {
		if now.After(r.deadline) {
			r.done <- readResult{err: ErrTimeout}
			delete(p.reads, ctx)
		}
	}
	k := 0
	for _, r := range p.pending {
		if now.After(r.deadline) {
			r.done <- readResult{err: ErrTimeout}
			continue
		}
		p.pending[k] = r
		k++
	}
	clear(p.pending[k:])
	p.pending = p.pending[:k]
	if p.lastHS != p.savedHS && now.Sub(p.savedHSAt) >= p.cfg.HardStateSyncInterval {
		if err := p.saveHardState(); err != nil && p.tickErr == nil {
			p.tickErr = err
		}
	}
}

// noopBatch is the election no-op: a ControlLeaderChange control batch
// stamped with (index, term) and the broker clock as its timestamp.
func noopBatch(index, term uint64) []byte {
	opts := log.DefaultBatchOptions()
	opts.BaseOffset = int64(index)
	opts.PartitionLeaderEpoch = int32(min(term, math.MaxInt32))
	opts.BaseTimestamp = time.Now().UnixMilli()
	b, err := log.NewControlBatch(opts, log.ControlLeaderChange, nil)
	if err != nil {
		panic(err) // cannot happen: fixed-size control record
	}
	return b
}

func stampEntry(e *raft.Entry) { log.Batch(e.Data).Stamp(int64(e.FirstIndex), e.Term) }

func (p *Partition) handlePropose(pr *proposal) {
	st := p.node.BasicStatus()
	if st.State != raft.Leader {
		pr.reply(ProduceResult{}, &NotLeaderError{Leader: st.Lead, Term: st.Term})
		return
	}
	b := pr.batch
	dup, err := p.ps.Check(b)
	if err != nil {
		pr.reply(ProduceResult{}, err)
		return
	}
	if dup != nil {
		p.await(pr, ProduceResult{BaseOffset: dup.BaseOffset, LastOffset: dup.LastOffset, Duplicate: true}, dup.Term, st.Term)
		return
	}
	if st.Term > math.MaxInt32 {
		pr.reply(ProduceResult{}, fmt.Errorf("%w: term %d exceeds partitionLeaderEpoch", ErrStorage, st.Term))
		return
	}
	e, err := p.node.ProposeBatch(uint64(b.LastOffsetDelta())+1, b, stampEntry)
	if err != nil {
		pr.reply(ProduceResult{}, err)
		return
	}
	p.ps.Append(log.Batch(e.Data))
	p.proposed = append(p.proposed, proposedEntry{e.FirstIndex, e.Term})
	p.await(pr, ProduceResult{BaseOffset: int64(e.FirstIndex), LastOffset: int64(e.LastIndex)}, e.Term, st.Term)
}

// await completes pr now or queues it until its acks condition holds.
func (p *Partition) await(pr *proposal, res ProduceResult, entryTerm, leaderTerm uint64) {
	switch pr.acks {
	case AcksLeader:
		if res.LastOffset <= p.lg.DurableOffset() {
			pr.reply(res, nil)
			return
		}
		p.acks1.push(&waiter{res: res, term: entryTerm, pr: pr})
	default:
		if uint64(res.LastOffset) <= p.applied {
			pr.reply(res, nil)
			return
		}
		p.acksAll.push(&waiter{res: res, term: entryTerm, pr: pr})
	}
	p.waiterTerm = leaderTerm
}

func (p *Partition) handleRead(r *readReq) {
	st := p.node.BasicStatus()
	if st.State != raft.Leader {
		r.done <- readResult{err: &NotLeaderError{Leader: st.Lead, Term: st.Term}}
		return
	}
	p.readSeq++
	if err := p.node.ReadIndex(p.readSeq); err != nil {
		r.done <- readResult{err: err}
		return
	}
	r.term = st.Term
	p.reads[p.readSeq] = r
}

// shutdown fails everything outstanding with cause, persists the final
// commit hint (on a clean close) and closes the log.
func (p *Partition) shutdown(cause error) {
	p.failOutstanding(cause)
	if errors.Is(cause, ErrClosed) && p.lastHS != p.savedHS {
		_ = p.saveHardState()
	}
	_ = p.store.Close()
	p.markStopped(cause)
}

func (p *Partition) markStopped(cause error) {
	p.stMu.Lock()
	p.st.Err = cause
	p.stMu.Unlock()
	p.hwN.Broadcast()
}

// failOutstanding answers every waiter and queued request with cause.
func (p *Partition) failOutstanding(cause error) {
	err := unknown(cause)
	p.acks1.failAll(err)
	p.acksAll.failAll(err)
	for ctx, r := range p.reads {
		r.done <- readResult{err: cause}
		delete(p.reads, ctx)
	}
	for _, r := range p.pending {
		r.done <- readResult{err: cause}
	}
	p.pending = nil
	for {
		select {
		case pr := <-p.propc:
			pr.reply(ProduceResult{}, cause) // never proposed: definite
			continue
		case r := <-p.readc:
			r.done <- readResult{err: cause}
			continue
		case c := <-p.ctrlc:
			c.done <- cause
			continue
		default:
		}
		break
	}
}

func (p *Partition) publish() {
	st := p.node.BasicStatus()
	p.stMu.Lock()
	prev := p.st
	p.st = Status{
		TopicPartition: p.cfg.TopicPartition,
		Group:          p.cfg.Group,
		ID:             p.cfg.ID,
		Role:           st.State,
		Term:           st.Term,
		Leader:         st.Lead,
		Commit:         st.Commit,
		Applied:        p.applied,
		HighWatermark:  p.hw.Load(),
		LogStartOffset: p.lg.LogStartOffset(),
		LogEndOffset:   p.lg.LogEndOffset(),
		DurableOffset:  p.lg.DurableOffset(),
		Producers:      p.ps.Len(),
	}
	cur := p.st
	p.stMu.Unlock()
	if p.cfg.OnStateChange != nil && (cur.Role != prev.Role || cur.Term != prev.Term || cur.Leader != prev.Leader) {
		p.cfg.OnStateChange(cur)
	}
}

// ctrl runs fn on the driver goroutine (between Readys).
func (p *Partition) ctrl(ctx context.Context, fn func() error) error {
	c := &ctrlReq{fn: fn, done: make(chan error, 1)}
	select {
	case p.ctrlc <- c:
	case <-ctx.Done():
		return ctx.Err()
	case <-p.donec:
		return p.closedErr()
	}
	select {
	case err := <-c.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-p.donec:
		select {
		case err := <-c.done:
			return err
		default:
			return p.closedErr()
		}
	}
}

func (p *Partition) closedErr() error {
	p.stMu.Lock()
	defer p.stMu.Unlock()
	if p.st.Err != nil {
		return p.st.Err
	}
	return ErrClosed
}
