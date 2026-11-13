package partition_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/Rahul-Nikhate/quorumlog/internal/fs"
	"github.com/Rahul-Nikhate/quorumlog/internal/log"
	"github.com/Rahul-Nikhate/quorumlog/internal/partition"
	pt "github.com/Rahul-Nikhate/quorumlog/internal/partition/partitiontest"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft/transport"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

const wait = 10 * time.Second

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	t.Cleanup(cancel)
	return ctx
}

// batch builds a batch of values (non-idempotent).
func batch(t testing.TB, vals ...string) log.Batch {
	t.Helper()
	return idem(t, -1, -1, -1, vals...)
}

func idem(t testing.TB, pid int64, epoch int16, seq int32, vals ...string) log.Batch {
	t.Helper()
	opts := log.DefaultBatchOptions()
	opts.BaseTimestamp = time.Now().UnixMilli()
	if pid >= 0 {
		opts.ProducerID, opts.ProducerEpoch, opts.BaseSequence = pid, epoch, seq
	}
	recs := make([]log.Record, len(vals))
	for i, v := range vals {
		recs[i] = log.Record{Value: []byte(v)}
	}
	b, err := log.NewBatch(opts, recs...)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func values(recs []pt.Record) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = string(r.Value)
	}
	return out
}

func mustLeader(t *testing.T, c *pt.Cluster, tp partition.TopicPartition, among []raft.NodeID) raft.NodeID {
	t.Helper()
	id, err := c.WaitLeader(tp, among, wait)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// waitHW waits until every live replica of tp (or `among`) has HW >= hw.
func waitHW(t *testing.T, c *pt.Cluster, tp partition.TopicPartition, hw int64, among []raft.NodeID) {
	t.Helper()
	if among == nil {
		among = c.IDs()
	}
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range among {
			if p := c.Partition(id, tp); p != nil && p.HighWatermark() < hw {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("HW %d not reached: %+v", hw, c.Status(tp))
}

func TestSingleNodeProduceFetch(t *testing.T) {
	c := pt.New(t, pt.Options{N: 1, Seed: 1})
	tp := c.TPs()[0]
	id := mustLeader(t, c, tp, nil)
	p := c.Partition(id, tp)
	ctx := ctxT(t)

	// The leader's election no-op is a control batch at offset 1, so the first
	// data batch lands at offset 2.
	waitHW(t, c, tp, 2, nil)
	r1, err := p.Produce(ctx, batch(t, "a", "b", "c"), partition.AcksAll)
	if err != nil || r1.BaseOffset != 2 || r1.LastOffset != 4 {
		t.Fatalf("produce: %+v %v", r1, err)
	}
	if hw := p.HighWatermark(); hw != 5 {
		t.Fatalf("HW after acks=all = %d, want 5", hw)
	}
	r2, err := p.Produce(ctx, batch(t, "d"), partition.AcksLeader)
	if err != nil || r2.BaseOffset != 5 {
		t.Fatalf("acks=leader: %+v %v", r2, err)
	}
	r3, err := p.Produce(ctx, batch(t, "e"), partition.AcksLeader)
	if err != nil || r3.BaseOffset != 6 {
		t.Fatalf("acks=leader: %+v %v", r3, err)
	}
	waitHW(t, c, tp, 7, nil)
	recs, hw, err := pt.ReadAll(ctx, p, partition.ConsistencyReadIndex)
	if err != nil || hw != 7 || !slices.Equal(values(recs), []string{"a", "b", "c", "d", "e"}) || recs[0].Offset != 2 {
		t.Fatalf("read: %v hw=%d err=%v", values(recs), hw, err)
	}
	// Fetch beyond the HW is out of range; at the HW it long-polls.
	if _, err := p.Fetch(ctx, partition.FetchRequest{Offset: 8}); !errors.Is(err, partition.ErrOffsetOutOfRange) {
		t.Fatalf("fetch beyond HW: %v", err)
	}
	if _, err := p.Fetch(ctx, partition.FetchRequest{Offset: 0}); !errors.Is(err, partition.ErrOffsetOutOfRange) {
		t.Fatalf("fetch below log start: %v", err)
	}
	start := time.Now()
	res, err := p.Fetch(ctx, partition.FetchRequest{Offset: 7, MaxWait: 50 * time.Millisecond})
	if err != nil || res.Region.Len != 0 || time.Since(start) < 40*time.Millisecond {
		t.Fatalf("empty long poll: %+v %v after %v", res, err, time.Since(start))
	}
	res.Region.Release()
	got := make(chan []pt.Record, 1)
	go func() {
		res, err := p.Fetch(ctx, partition.FetchRequest{Offset: 7, MaxWait: 5 * time.Second})
		if err != nil {
			got <- nil
			return
		}
		data, _ := res.Region.Bytes()
		res.Region.Release()
		recs, _, _ := pt.DecodeRecords(data)
		got <- recs
	}()
	time.Sleep(30 * time.Millisecond)
	if _, err := p.Produce(ctx, batch(t, "f"), partition.AcksAll); err != nil {
		t.Fatal(err)
	}
	select {
	case recs := <-got:
		if !slices.Equal(values(recs), []string{"f"}) {
			t.Fatalf("long poll woke with %v", values(recs))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("long poll not woken by the HW advance")
	}
	// Invalid batches.
	bad := batch(t, "x")
	bad[len(bad)-1] ^= 1
	if _, err := p.Produce(ctx, bad, partition.AcksAll); !errors.Is(err, partition.ErrInvalidBatch) {
		t.Fatalf("corrupt batch: %v", err)
	}
	ctl, _ := log.NewControlBatch(log.DefaultBatchOptions(), log.ControlCommit, nil)
	if _, err := p.Produce(ctx, ctl, partition.AcksAll); !errors.Is(err, partition.ErrInvalidBatch) {
		t.Fatalf("control batch: %v", err)
	}
}

func TestRestartKeepsDataHWAndProducerState(t *testing.T) {
	for _, mode := range []fs.CrashMode{fs.CrashProcess, fs.CrashDropUnsynced, fs.CrashTearUnsynced} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			c := pt.New(t, pt.Options{N: 1, Seed: 2})
			tp := c.TPs()[0]
			ctx := ctxT(t)
			mustLeader(t, c, tp, nil)
			var want []string
			for i := range 20 {
				v := fmt.Sprintf("v%d", i)
				if _, err := c.Produce(ctx, tp, idem(t, 42, 0, int32(i), v), partition.AcksAll); err != nil {
					t.Fatal(err)
				}
				want = append(want, v)
			}
			hw := c.Partition(1, tp).HighWatermark()
			c.Crash(1, mode)
			if err := c.Restart(1); err != nil {
				t.Fatal(err)
			}
			p := c.Partition(1, tp)
			if p.HighWatermark() > hw {
				t.Fatalf("HW after restart %d > %d", p.HighWatermark(), hw)
			}
			mustLeader(t, c, tp, nil)
			// Acked (acks=all, fsync=always) data survived the crash.
			recs, _, err := pt.ReadAll(ctx, p, partition.ConsistencyReadIndex)
			if err != nil || !slices.Equal(values(recs), want) {
				t.Fatalf("after restart: %v %v", values(recs), err)
			}
			// The producer state was rebuilt from the log: a retry of the last
			// batch is a duplicate with its original offset; the next sequence
			// is accepted; a gap is rejected.
			res, err := c.Produce(ctx, tp, idem(t, 42, 0, 19, "v19"), partition.AcksAll)
			if err != nil || !res.Duplicate || res.BaseOffset != recs[19].Offset {
				t.Fatalf("retry after restart: %+v %v (want dup at %d)", res, err, recs[19].Offset)
			}
			if _, err := c.Produce(ctx, tp, idem(t, 42, 0, 21, "gap"), partition.AcksAll); !errors.Is(err, partition.ErrOutOfOrderSequence) {
				t.Fatalf("gap after restart: %v", err)
			}
			if _, err := c.Produce(ctx, tp, idem(t, 42, 0, 20, "v20"), partition.AcksAll); err != nil {
				t.Fatalf("next seq after restart: %v", err)
			}
		})
	}
}

func TestReplicationAndNotLeader(t *testing.T) {
	c := pt.New(t, pt.Options{N: 3, Seed: 3})
	tp := c.TPs()[0]
	ctx := ctxT(t)
	l := mustLeader(t, c, tp, nil)
	var f raft.NodeID
	for _, id := range c.IDs() {
		if id != l {
			f = id
			break
		}
	}
	_, err := c.Partition(f, tp).Produce(ctx, batch(t, "x"), partition.AcksAll)
	var nl *partition.NotLeaderError
	if !errors.As(err, &nl) || nl.Leader != l || errors.Is(err, partition.ErrOutcomeUnknown) {
		t.Fatalf("follower produce: %v (want definite NotLeader hint %d)", err, l)
	}
	if _, err := c.Partition(f, tp).Fetch(ctx, partition.FetchRequest{Offset: 1}); !errors.Is(err, partition.ErrNotLeader) {
		t.Fatalf("follower fetch (leader consistency): %v", err)
	}
	var last partition.ProduceResult
	for i := range 50 {
		last, err = c.Produce(ctx, tp, batch(t, fmt.Sprintf("m%d", i), "pad"), partition.AcksAll)
		if err != nil {
			t.Fatal(err)
		}
	}
	waitHW(t, c, tp, last.LastOffset+1, nil)
	var first []pt.Record
	for _, id := range c.IDs() {
		recs, hw, err := pt.ReadAll(ctx, c.Partition(id, tp), partition.ConsistencyLocal)
		if err != nil || hw < last.LastOffset+1 || len(recs) != 100 {
			t.Fatalf("N%d local read: %d recs hw=%d %v", id, len(recs), hw, err)
		}
		if first == nil {
			first = recs
		} else if !slices.EqualFunc(first, recs, func(a, b pt.Record) bool { return a.Offset == b.Offset && bytes.Equal(a.Value, b.Value) }) {
			t.Fatalf("replica N%d differs", id)
		}
	}
}

// Leadership loss fails in-flight acks=all waiters with an outcome-unknown
// NotLeader error (they may still commit), and the HW never passes commit+1.
func TestWaitersFailOnLeadershipLoss(t *testing.T) {
	c := pt.New(t, pt.Options{N: 3, Seed: 4})
	tp := c.TPs()[0]
	ctx := ctxT(t)
	l := mustLeader(t, c, tp, nil)
	if _, err := c.Produce(ctx, tp, batch(t, "before"), partition.AcksAll); err != nil {
		t.Fatal(err)
	}
	c.Net.Disconnect(l)
	errc := make(chan error, 5)
	for i := range 5 {
		go func() {
			_, err := c.Partition(l, tp).Produce(ctx, batch(t, fmt.Sprintf("lost%d", i)), partition.AcksAll)
			errc <- err
		}()
	}
	for range 5 {
		select {
		case err := <-errc:
			if !errors.Is(err, partition.ErrOutcomeUnknown) || !errors.Is(err, partition.ErrNotLeader) {
				t.Fatalf("isolated leader's waiter: %v", err)
			}
		case <-time.After(wait):
			t.Fatal("waiters not failed after leadership loss (check-quorum)")
		}
	}
	for id, st := range c.Status(tp) {
		if uint64(st.HighWatermark) > st.Commit+1 {
			t.Fatalf("N%d HW %d > commit %d + 1", id, st.HighWatermark, st.Commit)
		}
	}
	c.Net.Connect(l)
	mustLeader(t, c, tp, nil)
	// None of the lost writes was committed by the majority (they never left
	// the isolated leader), and the old leader truncated them.
	res, err := c.Produce(ctx, tp, batch(t, "after"), partition.AcksAll)
	if err != nil {
		t.Fatal(err)
	}
	waitHW(t, c, tp, res.LastOffset+1, nil)
	recs, _, err := pt.ReadAll(ctx, c.Partition(l, tp), partition.ConsistencyLocal)
	if err != nil || !slices.Equal(values(recs), []string{"before", "after"}) {
		t.Fatalf("old leader's log: %v %v", values(recs), err)
	}
}

func TestLeadershipTransfer(t *testing.T) {
	c := pt.New(t, pt.Options{N: 3, Seed: 5})
	tp := c.TPs()[0]
	ctx := ctxT(t)
	l := mustLeader(t, c, tp, nil)
	res, err := c.Produce(ctx, tp, batch(t, "x"), partition.AcksAll)
	if err != nil {
		t.Fatal(err)
	}
	waitHW(t, c, tp, res.LastOffset+1, nil)
	target := c.IDs()[0]
	if target == l {
		target = c.IDs()[1]
	}
	if err := c.Partition(l, tp).TransferLeadership(ctx, target); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if id, _ := c.Leader(tp); id == target {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if id := mustLeader(t, c, tp, nil); id != target {
		t.Fatalf("leader after transfer = %d, want %d", id, target)
	}
	if _, err := c.Produce(ctx, tp, batch(t, "y"), partition.AcksAll); err != nil {
		t.Fatal(err)
	}
	if err := c.Partition(target, tp).TransferLeadership(ctx, 99); err == nil {
		t.Fatal("transfer to a non-peer accepted")
	}
}

// Idempotent retries across a leader failover are deduplicated: exactly one
// copy of every value, at the offset the producer was finally told.
func TestIdempotentRetryAcrossFailover(t *testing.T) {
	c := pt.New(t, pt.Options{N: 3, Seed: 6})
	tp := c.TPs()[0]
	ctx := ctxT(t)
	mustLeader(t, c, tp, nil)
	const pid = 9
	got := map[string]int64{}
	for seq := range 30 {
		v := fmt.Sprintf("s%d", seq)
		b := idem(t, pid, 0, int32(seq), v)
		if seq == 10 || seq == 20 {
			l, _ := c.Leader(tp)
			c.Crash(l, fs.CrashDropUnsynced)
			go func() { time.Sleep(200 * time.Millisecond); _ = c.Restart(l) }()
		}
		for {
			res, err := c.Produce(ctx, tp, b, partition.AcksAll)
			if err == nil {
				got[v] = res.BaseOffset
				break
			}
			if !errors.Is(err, partition.ErrOutcomeUnknown) && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("seq %d: %v", seq, err)
			}
			if ctx.Err() != nil {
				t.Fatal(err)
			}
		}
		// Also send a duplicate right away (in-flight retry window).
		if res, err := c.Produce(ctx, tp, b, partition.AcksAll); err != nil || !res.Duplicate || res.BaseOffset != got[v] {
			t.Fatalf("immediate duplicate of seq %d: %+v %v", seq, res, err)
		}
	}
	l := mustLeader(t, c, tp, nil)
	recs, _, err := pt.ReadAll(ctx, c.Partition(l, tp), partition.ConsistencyReadIndex)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int64{}
	for _, r := range recs {
		if _, dup := seen[string(r.Value)]; dup {
			t.Fatalf("value %s duplicated", r.Value)
		}
		seen[string(r.Value)] = r.Offset
	}
	for v, off := range got {
		if seen[v] != off {
			t.Fatalf("value %s acked at %d, found at %d", v, off, seen[v])
		}
	}
	if len(seen) != 30 {
		t.Fatalf("%d values in the log, want 30", len(seen))
	}
}

// A follower that falls behind the leader's compacted log start gets
// InstallSnapshot (metadata + producer state) and continues.
func TestSnapshotInstallToLaggingFollower(t *testing.T) {
	c := pt.New(t, pt.Options{N: 3, Seed: 7, SegmentBytes: 2048})
	tp := c.TPs()[0]
	ctx := ctxT(t)
	l := mustLeader(t, c, tp, nil)
	var lag raft.NodeID
	for _, id := range c.IDs() {
		if id != l {
			lag = id
		}
	}
	c.Net.Disconnect(lag)
	var last partition.ProduceResult
	for i := range 100 {
		var err error
		last, err = c.Produce(ctx, tp, idem(t, 5, 0, int32(2*i), fmt.Sprintf("v%d", i), "padpadpadpad"), partition.AcksAll)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range c.IDs() {
		if id == lag {
			continue
		}
		p := c.Partition(id, tp)
		waitHW(t, c, tp, last.LastOffset+1, []raft.NodeID{id})
		if err := p.Compact(ctx, uint64(last.LastOffset)); err != nil {
			t.Fatalf("N%d compact: %v", id, err)
		}
		if st := p.Status(); st.LogStartOffset != last.LastOffset+1 {
			t.Fatalf("N%d log start after compact = %d", id, st.LogStartOffset)
		}
	}
	c.Net.Connect(lag)
	waitHW(t, c, tp, last.LastOffset+1, nil)
	p := c.Partition(lag, tp)
	if st := p.Status(); st.LogStartOffset != last.LastOffset+1 {
		t.Fatalf("lagging follower log start = %d, want %d (snapshot installed)", st.LogStartOffset, last.LastOffset+1)
	}
	ps, err := p.ProducerSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ep, m, ok := ps.Lookup(5); !ok || ep != 0 || m.FirstSeq != 198 || m.LastOffset != last.LastOffset {
		t.Fatalf("follower producer state from snapshot: %v %+v %v", ep, m, ok)
	}
	// After a failover to the restored follower, dedup still works.
	if err := c.Partition(mustLeader(t, c, tp, nil), tp).TransferLeadership(ctx, lag); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(wait)
	for id, _ := c.Leader(tp); id != lag && time.Now().Before(deadline); id, _ = c.Leader(tp) {
		time.Sleep(5 * time.Millisecond)
	}
	res, err := p.Produce(ctx, idem(t, 5, 0, 198, "v99", "padpadpadpad"), partition.AcksAll)
	if err != nil || !res.Duplicate || res.LastOffset != last.LastOffset {
		t.Fatalf("dup on snapshot-restored leader: %+v %v\n%+v", res, err, c.Status(tp))
	}
}

// Many partitions per broker share one transport; messages are routed by
// Raft group.
func TestManyPartitions(t *testing.T) {
	c := pt.New(t, pt.Options{N: 3, Partitions: 8, Seed: 8})
	ctx := ctxT(t)
	var wg sync.WaitGroup
	errs := make(chan error, len(c.TPs()))
	for _, tp := range c.TPs() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 10 {
				if _, err := c.Produce(ctx, tp, batch(t, fmt.Sprintf("%v-%d", tp, i)), partition.AcksAll); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for _, tp := range c.TPs() {
		l := mustLeader(t, c, tp, nil)
		recs, _, err := pt.ReadAll(ctx, c.Partition(l, tp), partition.ConsistencyReadIndex)
		if err != nil || len(recs) != 10 || string(recs[0].Value) != fmt.Sprintf("%v-0", tp) {
			t.Fatalf("%v: %v %v", tp, values(recs), err)
		}
	}
	b := c.Broker(1)
	before := b.Mgr.UnroutableMessages()
	b.Mgr.Handle(raft.Message{Type: raft.MsgHeartbeat, Group: 999, From: 2, To: 1, Term: 1})
	if b.Mgr.UnroutableMessages() != before+1 {
		t.Fatal("message for an unknown group not counted")
	}
}

// A follower whose uncommitted suffix is truncated rebuilds its producer
// state: after becoming leader it accepts the producer's real next sequence.
func TestTruncationRebuildsProducerState(t *testing.T) {
	c := pt.New(t, pt.Options{N: 3, Seed: 9})
	tp := c.TPs()[0]
	ctx := ctxT(t)
	l := mustLeader(t, c, tp, nil)
	if _, err := c.Produce(ctx, tp, idem(t, 3, 0, 0, "a"), partition.AcksAll); err != nil {
		t.Fatal(err)
	}
	waitHW(t, c, tp, c.Partition(l, tp).HighWatermark(), nil)
	// The isolated leader appends seq 1 locally (never committed).
	c.Net.Disconnect(l)
	sctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	_, err := c.Partition(l, tp).Produce(sctx, idem(t, 3, 0, 1, "uncommitted"), partition.AcksAll)
	cancel()
	if !errors.Is(err, partition.ErrOutcomeUnknown) {
		t.Fatalf("isolated produce: %v", err)
	}
	var rest []raft.NodeID
	for _, id := range c.IDs() {
		if id != l {
			rest = append(rest, id)
		}
	}
	l2 := mustLeader(t, c, tp, rest)
	// The new leader never saw seq 1: the producer's retry of seq 1 is new.
	if _, err := c.Partition(l2, tp).Produce(ctx, idem(t, 3, 0, 1, "b"), partition.AcksAll); err != nil {
		t.Fatal(err)
	}
	c.Net.Connect(l)
	waitHW(t, c, tp, c.Partition(l2, tp).HighWatermark(), nil)
	// Old leader truncated "uncommitted" and replayed: its state has seq 1 = "b".
	if err := c.Partition(l2, tp).TransferLeadership(ctx, l); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(wait)
	for id, _ := c.Leader(tp); id != l && time.Now().Before(deadline); id, _ = c.Leader(tp) {
		time.Sleep(5 * time.Millisecond)
	}
	p := c.Partition(l, tp)
	res, err := p.Produce(ctx, idem(t, 3, 0, 1, "b"), partition.AcksAll)
	if err != nil || !res.Duplicate {
		t.Fatalf("retry of seq 1 on the old leader: %+v %v (want duplicate)", res, err)
	}
	if _, err := p.Produce(ctx, idem(t, 3, 0, 2, "c"), partition.AcksAll); err != nil {
		t.Fatalf("seq 2: %v", err)
	}
	recs, _, err := pt.ReadAll(ctx, p, partition.ConsistencyReadIndex)
	if err != nil || !slices.Equal(values(recs), []string{"a", "b", "c"}) {
		t.Fatalf("log: %v %v", values(recs), err)
	}
}

// Fetch regions pin their segment: compaction deletes it only after Release.
func TestFileRegionPinsSegment(t *testing.T) {
	c := pt.New(t, pt.Options{N: 1, Seed: 10, SegmentBytes: 1024})
	tp := c.TPs()[0]
	ctx := ctxT(t)
	mustLeader(t, c, tp, nil)
	var last partition.ProduceResult
	for i := range 40 {
		var err error
		if last, err = c.Produce(ctx, tp, batch(t, fmt.Sprintf("v%02d-%s", i, bytes.Repeat([]byte{'x'}, 60))), partition.AcksAll); err != nil {
			t.Fatal(err)
		}
	}
	p := c.Partition(1, tp)
	res, err := p.Fetch(ctx, partition.FetchRequest{Offset: 2, MaxBytes: 1 << 20})
	if err != nil || res.Region.Len == 0 {
		t.Fatalf("fetch: %v", err)
	}
	if err := p.Compact(ctx, uint64(last.LastOffset)); err != nil {
		t.Fatal(err)
	}
	data, err := res.Region.Bytes() // the deleted segment is still readable
	if err != nil {
		t.Fatalf("read pinned region after compaction: %v", err)
	}
	recs, _, err := pt.DecodeRecords(data)
	if err != nil || len(recs) == 0 || !bytes.HasPrefix(recs[0].Value, []byte("v00")) {
		t.Fatalf("pinned data: %v %v", len(recs), err)
	}
	res.Region.Release()
	res.Region.Release() // idempotent
	if _, err := p.Fetch(ctx, partition.FetchRequest{Offset: 2}); !errors.Is(err, partition.ErrOffsetOutOfRange) {
		t.Fatalf("fetch below the new log start: %v", err)
	}
}

// Closing a partition fails outstanding requests and leaks nothing.
func TestCloseFailsOutstanding(t *testing.T) {
	net := transport.NewNetwork(1)
	m := partition.NewManager(1)
	ep := net.Register(1, m.Handle)
	m.SetTransport(ep)
	// Peers 1,2,3 but only 1 exists: never elects, so produce/fetch wait.
	p, err := m.Create(partition.Config{TopicPartition: partition.TopicPartition{Topic: "x"}, Group: 1, Peers: []raft.NodeID{1, 2, 3},
		Dir: "/x", FS: fs.NewFaultFS(1), TickInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Create(partition.Config{TopicPartition: partition.TopicPartition{Topic: "x"}, Group: 2, Peers: []raft.NodeID{1},
		Dir: "/y", FS: fs.NewFaultFS(1)}); !errors.Is(err, partition.ErrExists) {
		t.Fatalf("duplicate tp: %v", err)
	}
	ctx := ctxT(t)
	errc := make(chan error, 2)
	go func() {
		_, err := p.Fetch(ctx, partition.FetchRequest{Offset: 1, Consistency: partition.ConsistencyLocal, MaxWait: time.Hour})
		errc <- err
	}()
	go func() {
		_, err := p.Fetch(ctx, partition.FetchRequest{Offset: 1, Consistency: partition.ConsistencyReadIndex})
		errc <- err
	}()
	time.Sleep(20 * time.Millisecond)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := <-errc; err == nil {
			t.Fatal("outstanding fetch not failed on close")
		}
	}
	if _, err := p.Produce(ctx, batch(t, "z"), partition.AcksAll); !errors.Is(err, partition.ErrClosed) {
		t.Fatalf("produce after close: %v", err)
	}
	if st := p.Status(); !errors.Is(st.Err, partition.ErrClosed) {
		t.Fatalf("status after close: %v", st.Err)
	}
}

// A storage failure is fail-stop: requests fail with ErrStorage, the
// partition stops, and its acked data is intact after reopening.
func TestStorageFailureIsFailStop(t *testing.T) {
	c := pt.New(t, pt.Options{N: 1, Seed: 11})
	tp := c.TPs()[0]
	ctx := ctxT(t)
	mustLeader(t, c, tp, nil)
	if _, err := c.Produce(ctx, tp, batch(t, "ok"), partition.AcksAll); err != nil {
		t.Fatal(err)
	}
	p := c.Partition(1, tp)
	c.Broker(1).FS.FailNextSyncs(1000)
	_, err := p.Produce(ctx, batch(t, "doomed"), partition.AcksAll)
	if err == nil {
		t.Fatal("produce succeeded with failing fsync")
	}
	select {
	case <-p.Done():
	case <-time.After(wait):
		t.Fatal("partition did not stop after the fsync failure")
	}
	if st := p.Status(); !errors.Is(st.Err, partition.ErrStorage) {
		t.Fatalf("status err = %v", st.Err)
	}
	c.Broker(1).FS.FailNextSyncs(0)
	c.Crash(1, fs.CrashDropUnsynced)
	if err := c.Restart(1); err != nil {
		t.Fatal(err)
	}
	mustLeader(t, c, tp, nil)
	recs, _, err := pt.ReadAll(ctx, c.Partition(1, tp), partition.ConsistencyReadIndex)
	if err != nil || len(recs) < 1 || string(recs[0].Value) != "ok" {
		t.Fatalf("after reopen: %v %v", values(recs), err)
	}
}

func TestOSDirCluster(t *testing.T) {
	c := pt.New(t, pt.Options{N: 3, Seed: 12, OSDir: pt.TempDir(t)})
	tp := c.TPs()[0]
	ctx := ctxT(t)
	mustLeader(t, c, tp, nil)
	for i := range 20 {
		if _, err := c.Produce(ctx, tp, batch(t, fmt.Sprint(i)), partition.AcksAll); err != nil {
			t.Fatal(err)
		}
	}
	l, _ := c.Leader(tp)
	c.Crash(l, fs.CrashProcess)
	if err := c.Restart(l); err != nil {
		t.Fatal(err)
	}
	l = mustLeader(t, c, tp, nil)
	recs, _, err := pt.ReadAll(ctx, c.Partition(l, tp), partition.ConsistencyReadIndex)
	if err != nil || len(recs) != 20 {
		t.Fatalf("%v %v", len(recs), err)
	}
}
