package partition_test

import (
	"context"
	"errors"

	"slices"
	"testing"
	"time"

	"github.com/Rahul-Nikhate/quorumlog/internal/fs"
	"github.com/Rahul-Nikhate/quorumlog/internal/partition"
	pt "github.com/Rahul-Nikhate/quorumlog/internal/partition/partitiontest"
)

func msgs(vals ...string) []partition.Message {
	out := make([]partition.Message, len(vals))
	for i, v := range vals {
		out[i] = partition.Message{Key: []byte("k" + v), Value: []byte(v)}
	}
	return out
}

func msgValues(ms []partition.Message) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = string(m.Value)
	}
	return out
}

func msgOffsets(ms []partition.Message) []int64 {
	out := make([]int64, len(ms))
	for i, m := range ms {
		out[i] = m.Offset
	}
	return out
}

// The message API uses 0-based API offsets (log offset - 1); the election
// no-op occupies API offset 0 of a fresh partition and is never returned.
func TestMessageAPIOffsetsAndReads(t *testing.T) {
	c := pt.New(t, pt.Options{N: 1, Seed: 11})
	tp := c.TPs()[0]
	id := mustLeader(t, c, tp, nil)
	p := c.Partition(id, tp)
	ctx := ctxT(t)
	waitHW(t, c, tp, 2, nil) // no-op committed

	res, err := p.ProduceMessages(ctx, partition.ProduceRequest{Messages: msgs("a", "b", "c"), Acks: partition.AcksAll})
	if err != nil || res.BaseOffset != 1 || res.LastOffset != 3 || res.Duplicate {
		t.Fatalf("produce: %+v %v", res, err)
	}
	if hw := p.HighWatermark(); partition.APIOffset(hw) != 4 {
		t.Fatalf("API HW = %d, want 4", partition.APIOffset(hw))
	}
	rr, err := p.ReadMessages(ctx, partition.ReadRequest{Offset: 0})
	if err != nil || !slices.Equal(msgValues(rr.Messages), []string{"a", "b", "c"}) ||
		!slices.Equal(msgOffsets(rr.Messages), []int64{1, 2, 3}) || rr.NextOffset != 4 || rr.HighWatermark != 4 {
		t.Fatalf("read all: %+v %v", rr, err)
	}
	if string(rr.Messages[0].Key) != "ka" || rr.Messages[0].Timestamp.IsZero() {
		t.Fatalf("key/timestamp: %+v", rr.Messages[0])
	}
	// Max cuts inside a batch; NextOffset resumes exactly there.
	rr, err = p.ReadMessages(ctx, partition.ReadRequest{Offset: 0, Max: 2})
	if err != nil || !slices.Equal(msgValues(rr.Messages), []string{"a", "b"}) || rr.NextOffset != 3 {
		t.Fatalf("read max=2: %+v %v", rr, err)
	}
	// An offset in the middle of a batch skips the earlier records.
	rr, err = p.ReadMessages(ctx, partition.ReadRequest{Offset: 3})
	if err != nil || !slices.Equal(msgValues(rr.Messages), []string{"c"}) || rr.NextOffset != 4 {
		t.Fatalf("read mid-batch: %+v %v", rr, err)
	}
	// At the HW: an empty long poll times out with the same next offset.
	start := time.Now()
	rr, err = p.ReadMessages(ctx, partition.ReadRequest{Offset: 4, Wait: 50 * time.Millisecond})
	if err != nil || len(rr.Messages) != 0 || rr.NextOffset != 4 || time.Since(start) < 40*time.Millisecond {
		t.Fatalf("empty long poll: %+v %v after %v", rr, err, time.Since(start))
	}
	// Above the HW: out of range. Negative: out of range.
	if _, err := p.ReadMessages(ctx, partition.ReadRequest{Offset: 5}); !errors.Is(err, partition.ErrOffsetOutOfRange) {
		t.Fatalf("read above HW: %v", err)
	}
	if _, err := p.ReadMessages(ctx, partition.ReadRequest{Offset: -1}); !errors.Is(err, partition.ErrOffsetOutOfRange) {
		t.Fatalf("negative offset: %v", err)
	}
	// A long poll is woken by a commit.
	got := make(chan partition.ReadResult, 1)
	go func() {
		rr, _ := p.ReadMessages(ctx, partition.ReadRequest{Offset: 4, Wait: 5 * time.Second})
		got <- rr
	}()
	time.Sleep(30 * time.Millisecond)
	if _, err := p.ProduceMessages(ctx, partition.ProduceRequest{Messages: msgs("d"), Acks: partition.AcksLeader}); err != nil {
		t.Fatal(err)
	}
	select {
	case rr := <-got:
		if !slices.Equal(msgValues(rr.Messages), []string{"d"}) || rr.Messages[0].Offset != 4 {
			t.Fatalf("long poll woke with %+v", rr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("long poll not woken")
	}
	// Cancelling the caller's context ends a long poll promptly.
	cctx, cancel := context.WithCancel(ctx)
	errc := make(chan error, 1)
	go func() {
		_, err := p.ReadMessages(cctx, partition.ReadRequest{Offset: 5, Wait: 10 * time.Second})
		errc <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled long poll: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled long poll did not return")
	}
	if _, err := p.ProduceMessages(ctx, partition.ProduceRequest{Acks: partition.AcksAll}); !errors.Is(err, partition.ErrInvalidBatch) {
		t.Fatalf("empty produce: %v", err)
	}
}

// String producer ids with per-message sequences: retries return the
// original offsets; gaps and unknown producers starting above 0 are
// OUT_OF_ORDER_SEQUENCE.
func TestMessageAPIIdempotentProducer(t *testing.T) {
	c := pt.New(t, pt.Options{N: 3, Seed: 12})
	tp := c.TPs()[0]
	l := mustLeader(t, c, tp, nil)
	p := c.Partition(l, tp)
	ctx := ctxT(t)

	req := partition.ProduceRequest{Messages: msgs("x", "y"), Acks: partition.AcksAll, ProducerID: "p-1", Sequence: 0}
	first, err := p.ProduceMessages(ctx, req)
	if err != nil || first.Duplicate {
		t.Fatalf("first: %+v %v", first, err)
	}
	again, err := p.ProduceMessages(ctx, req)
	if err != nil || !again.Duplicate || again.BaseOffset != first.BaseOffset || again.LastOffset != first.LastOffset {
		t.Fatalf("retry: %+v %v (first %+v)", again, err, first)
	}
	for _, seq := range []int32{1, 3, -1} {
		_, err := p.ProduceMessages(ctx, partition.ProduceRequest{Messages: msgs("z"), Acks: partition.AcksAll, ProducerID: "p-1", Sequence: seq})
		if !errors.Is(err, partition.ErrOutOfOrderSequence) {
			t.Fatalf("seq %d: %v", seq, err)
		}
	}
	if _, err := p.ProduceMessages(ctx, partition.ProduceRequest{Messages: msgs("z"), Acks: partition.AcksAll, ProducerID: "p-1", Sequence: 2}); err != nil {
		t.Fatalf("next seq: %v", err)
	}
	if _, err := p.ProduceMessages(ctx, partition.ProduceRequest{Messages: msgs("w"), Acks: partition.AcksAll, ProducerID: "p-2", Sequence: 7}); !errors.Is(err, partition.ErrOutOfOrderSequence) {
		t.Fatalf("new producer at 7: %v", err)
	}
	// Followers learn producer state from the log: after a failover the new
	// leader still deduplicates.
	waitHW(t, c, tp, p.HighWatermark(), nil)
	c.Crash(l, 0)
	l2 := mustLeader(t, c, tp, nil)
	if l2 == l {
		t.Fatal("same leader after crash")
	}
	dup, err := c.Partition(l2, tp).ProduceMessages(ctx, req)
	if err != nil || !dup.Duplicate || dup.BaseOffset != first.BaseOffset {
		t.Fatalf("retry on new leader: %+v %v", dup, err)
	}
	rr, err := c.Partition(l2, tp).ReadMessages(ctx, partition.ReadRequest{Offset: 0})
	if err != nil || !slices.Equal(msgValues(rr.Messages), []string{"x", "y", "z"}) {
		t.Fatalf("log after failover: %+v %v", rr, err)
	}
	// Followers refuse linearizable reads.
	for _, id := range c.IDs() {
		if q := c.Partition(id, tp); q != nil && id != l2 {
			if _, err := q.ReadMessages(ctx, partition.ReadRequest{Offset: 0}); !errors.Is(err, partition.ErrNotLeader) {
				t.Fatalf("follower N%d read: %v", id, err)
			}
		}
	}
	if partition.ProducerIDFor("p-1") == partition.ProducerIDFor("p-2") || partition.ProducerIDFor("p-1") < 0 {
		t.Fatal("bad producer id hash")
	}
}

// A retry of a batch that is still in flight (not committed yet) waits for
// the original entry. When another producer's batch was proposed in between,
// the retry's waiter must still be matched against its own entry (regression:
// it was queued behind the newer waiter and failed as "outcome unknown"
// although its entry committed).
func TestDuplicateOfInFlightBatchWaitsForIt(t *testing.T) {
	c := pt.New(t, pt.Options{N: 1, Seed: 13})
	tp := c.TPs()[0]
	id := mustLeader(t, c, tp, nil)
	p := c.Partition(id, tp)
	ctx := ctxT(t)
	waitHW(t, c, tp, 2, nil) // no-op committed
	// Slow fsyncs keep the next proposals uncommitted for a while.
	c.Broker(id).FS.SetFaults(fs.Faults{SyncLatency: 300 * time.Millisecond})

	type result struct {
		res partition.ProduceResult
		err error
	}
	produce := func(req partition.ProduceRequest) chan result {
		ch := make(chan result, 1)
		go func() {
			res, err := p.ProduceMessages(ctx, req)
			ch <- result{res, err}
		}()
		time.Sleep(30 * time.Millisecond) // proposed before the next one
		return ch
	}
	req := partition.ProduceRequest{Messages: msgs("a"), Acks: partition.AcksAll, ProducerID: "p-1", Sequence: 0}
	first := produce(req)
	other := produce(partition.ProduceRequest{Messages: msgs("b"), Acks: partition.AcksAll})
	retry := produce(req)
	f, o, r := <-first, <-other, <-retry
	c.Broker(id).FS.SetFaults(fs.Faults{}) // fast cleanup
	if f.err != nil || o.err != nil || f.res.Duplicate {
		t.Fatalf("first %+v %v, other %+v %v", f.res, f.err, o.res, o.err)
	}
	if r.err != nil || !r.res.Duplicate || r.res.BaseOffset != f.res.BaseOffset || r.res.LastOffset != f.res.LastOffset {
		t.Fatalf("retry of the in-flight batch: %+v %v (first %+v)", r.res, r.err, f.res)
	}
}

// A read is bounded in bytes as well as in messages, so a client asking for
// many large messages cannot make the server buffer them all (regression: a
// read of max=100 messages of ~512 KiB each held ~50 MiB in memory). At least
// one message is always returned, so a large message still makes progress.
func TestReadMessagesIsBoundedInBytes(t *testing.T) {
	c := pt.New(t, pt.Options{N: 1, Seed: 14, SegmentBytes: 64 << 20})
	tp := c.TPs()[0]
	id := mustLeader(t, c, tp, nil)
	p := c.Partition(id, tp)
	ctx := ctxT(t)
	big := make([]byte, 512<<10)
	for range 8 {
		if _, err := p.ProduceMessages(ctx, partition.ProduceRequest{Messages: []partition.Message{{Value: big}}, Acks: partition.AcksAll}); err != nil {
			t.Fatal(err)
		}
	}
	size := func(ms []partition.Message) (n int) {
		for _, m := range ms {
			n += len(m.Key) + len(m.Value)
		}
		return n
	}
	rr, err := p.ReadMessages(ctx, partition.ReadRequest{Offset: 0, Max: 100})
	if err != nil {
		t.Fatal(err)
	}
	if n := size(rr.Messages); len(rr.Messages) == 0 || n > 2<<20 {
		t.Fatalf("default read returned %d messages, %d bytes; want 1+ messages and at most ~1 MiB", len(rr.Messages), n)
	}
	// One message larger than the bound is still returned.
	rr, err = p.ReadMessages(ctx, partition.ReadRequest{Offset: 0, Max: 100, MaxBytes: 1})
	if err != nil || len(rr.Messages) != 1 {
		t.Fatalf("MaxBytes=1: %d messages, %v", len(rr.Messages), err)
	}
	// Following next_offset reads everything.
	var got int
	for off := int64(0); off < rr.HighWatermark; {
		r, err := p.ReadMessages(ctx, partition.ReadRequest{Offset: off, Max: 100})
		if err != nil {
			t.Fatal(err)
		}
		got += len(r.Messages)
		off = r.NextOffset
	}
	if got != 8 {
		t.Fatalf("read %d messages by following next_offset, want 8", got)
	}
}
