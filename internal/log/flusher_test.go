package log

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Rahul-Nikhate/quorumlog/internal/fs"
)

func faultLog(t *testing.T, p SyncPolicy, mod func(*Config)) (*Log, *fs.FaultFS) {
	t.Helper()
	ff := fs.NewFaultFS(11)
	cfg := Config{Dir: "/p", FS: ff, SegmentBytes: 1 << 20, Sync: p}
	if mod != nil {
		mod(&cfg)
	}
	return mustOpen(t, cfg), ff
}

// TestGroupCommit: many concurrent producers each append and wait for
// durability; with a slow fsync, one fsync covers many batches.
func TestGroupCommit(t *testing.T) {
	l, ff := faultLog(t, SyncPolicy{Mode: SyncAlways}, nil)
	defer l.Close()
	ff.SetFaults(fs.Faults{SyncLatency: 5 * time.Millisecond})
	const producers = 64
	const perProducer = 5
	before := l.Stats().Syncs
	var wg sync.WaitGroup
	errc := make(chan error, producers)
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				res, err := l.Append(mkBatch(t, 1, 100, 0, 'g'), 1)
				if err != nil {
					errc <- err
					return
				}
				if err := l.WaitDurable(context.Background(), res.LastOffset); err != nil {
					errc <- err
					return
				}
				if d := l.DurableOffset(); d < res.LastOffset {
					errc <- errors.New("WaitDurable returned before the durable offset covered the batch")
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}
	syncs := l.Stats().Syncs - before
	batches := int64(producers * perProducer)
	t.Logf("group commit: %d batches acknowledged with %d fsync rounds", batches, syncs)
	if syncs <= 0 || syncs > batches/4 {
		t.Fatalf("group commit ineffective: %d fsyncs for %d batches", syncs, batches)
	}
}

// TestSyncAlwaysAcksOnlyAfterFsync: the durable offset never runs ahead of
// what a crash preserves.
func TestSyncAlwaysAcksOnlyAfterFsync(t *testing.T) {
	l, ff := faultLog(t, SyncPolicy{Mode: SyncAlways}, nil)
	ff.SetFaults(fs.Faults{SyncLatency: time.Millisecond})
	for i := 0; i < 50; i++ {
		res, err := l.Append(mkBatch(t, 2, 50, 0, 'a'), 1)
		if err != nil {
			t.Fatal(err)
		}
		if i%10 == 9 {
			if err := l.WaitDurable(context.Background(), res.LastOffset); err != nil {
				t.Fatal(err)
			}
		}
	}
	durable := l.DurableOffset()
	ff2 := ff.Crash(fs.CrashDropUnsynced)
	abandon(l)
	l2 := mustOpen(t, Config{Dir: "/p", FS: ff2, SegmentBytes: 1 << 20})
	defer l2.Close()
	if l2.LogEndOffset()-1 < durable {
		t.Fatalf("durable offset %d but only %d survived", durable, l2.LogEndOffset()-1)
	}
}

func TestSyncIntervalCoalesces(t *testing.T) {
	const interval = 40 * time.Millisecond
	l, _ := faultLog(t, SyncPolicy{Mode: SyncInterval, Interval: interval}, nil)
	defer l.Close()
	before := l.Stats().Syncs
	start := time.Now()
	var maxWait time.Duration
	for time.Since(start) < 300*time.Millisecond {
		res, err := l.Append(mkBatch(t, 1, 10, 0, 'i'), 1)
		if err != nil {
			t.Fatal(err)
		}
		if res.LastOffset%25 == 0 {
			w := time.Now()
			if err := l.WaitDurable(context.Background(), res.LastOffset); err != nil {
				t.Fatal(err)
			}
			maxWait = max(maxWait, time.Since(w))
		}
		time.Sleep(500 * time.Microsecond)
	}
	elapsed := time.Since(start)
	syncs := l.Stats().Syncs - before
	t.Logf("interval=%v: %d fsyncs over %v for %d batches, max wait %v", interval, syncs, elapsed, l.LogEndOffset(), maxWait)
	if limit := int64(elapsed/interval) + 3; syncs > limit {
		t.Fatalf("%d fsyncs in %v exceeds one per interval (limit %d)", syncs, elapsed, limit)
	}
	if maxWait > interval+200*time.Millisecond {
		t.Fatalf("durability wait %v far exceeds the interval", maxWait)
	}
}

func TestSyncNeverAcksOnWrite(t *testing.T) {
	var notified atomic.Int64
	notified.Store(-1)
	l, ff := faultLog(t, SyncPolicy{Mode: SyncNever}, func(c *Config) {
		c.OnDurable = func(off int64) { notified.Store(off) }
	})
	defer l.Close()
	syncsBefore := ff.Stats().Syncs
	var last int64
	for i := 0; i < 20; i++ {
		res, err := l.Append(mkBatch(t, 3, 10, 0, 'n'), 1)
		if err != nil {
			t.Fatal(err)
		}
		last = res.LastOffset
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := l.WaitDurable(ctx, last); err != nil {
			t.Fatal(err)
		}
		cancel()
	}
	if got := ff.Stats().Syncs - syncsBefore; got != 0 {
		t.Fatalf("policy never issued %d fsyncs on the append path", got)
	}
	if l.DurableOffset() != last || notified.Load() != last {
		t.Fatalf("durable %d, notified %d, want %d", l.DurableOffset(), notified.Load(), last)
	}
}

func TestOnDurableCallback(t *testing.T) {
	var mu sync.Mutex
	var seen []int64
	l, _ := faultLog(t, SyncPolicy{Mode: SyncAlways}, func(c *Config) {
		c.OnDurable = func(off int64) { mu.Lock(); seen = append(seen, off); mu.Unlock() }
	})
	var last int64
	for i := 0; i < 30; i++ {
		res, _ := l.Append(mkBatch(t, 1, 10, 0, 'c'), 1)
		last = res.LastOffset
	}
	if err := l.WaitDurable(context.Background(), last); err != nil {
		t.Fatal(err)
	}
	l.Close()
	mu.Lock()
	defer mu.Unlock()
	if len(seen) == 0 || seen[len(seen)-1] < last {
		t.Fatalf("callbacks %v do not reach %d", seen, last)
	}
	for i := 1; i < len(seen); i++ {
		if seen[i] <= seen[i-1] {
			t.Fatalf("callback offsets not increasing: %v", seen)
		}
	}
}

// TestFsyncFailureIsSticky models fsyncgate: after one failed fsync the log
// never claims durability again, refuses writes, and does not write a
// clean-shutdown record.
func TestFsyncFailureIsSticky(t *testing.T) {
	for _, p := range []SyncPolicy{{Mode: SyncAlways}, {Mode: SyncInterval, Interval: time.Millisecond}} {
		t.Run(p.String(), func(t *testing.T) {
			l, ff := faultLog(t, p, nil)
			ff.SetFaults(fs.Faults{DropDirtyOnSyncError: true})
			res, err := l.Append(mkBatch(t, 1, 10, 0, 'a'), 1)
			if err != nil {
				t.Fatal(err)
			}
			if err := l.WaitDurable(context.Background(), res.LastOffset); err != nil {
				t.Fatal(err)
			}
			durable := l.DurableOffset()
			ff.FailNextSyncs(1)
			res, err = l.Append(mkBatch(t, 1, 10, 0, 'b'), 1)
			if err != nil {
				t.Fatal(err)
			}
			if err := l.WaitDurable(context.Background(), res.LastOffset); !errors.Is(err, ErrLogFailed) || !errors.Is(err, fs.ErrInjected) {
				t.Fatalf("WaitDurable after fsync failure = %v", err)
			}
			if l.DurableOffset() != durable {
				t.Fatalf("durable offset advanced past a failed fsync: %d", l.DurableOffset())
			}
			if _, err := l.Append(mkBatch(t, 1, 10, 0, 'c'), 1); !errors.Is(err, ErrLogFailed) {
				t.Fatalf("append after failure = %v", err)
			}
			if err := l.Sync(); !errors.Is(err, ErrLogFailed) {
				t.Fatalf("Sync after failure = %v", err)
			}
			if err := l.WaitDurable(context.Background(), 0); !errors.Is(err, ErrLogFailed) {
				t.Fatalf("WaitDurable after failure = %v", err)
			}
			l.Close()
			if ck, err := readCheckpoint(ff, "/p"); err != nil || ck.clean {
				t.Fatalf("failed log wrote a clean-shutdown record: %+v %v", ck, err)
			}
			// Recovery sees only what was really synced.
			ff2 := ff.Crash(fs.CrashDropUnsynced)
			l2 := mustOpen(t, Config{Dir: "/p", FS: ff2})
			defer l2.Close()
			if l2.LogEndOffset()-1 != durable {
				t.Fatalf("recovered LEO-1 %d, durable was %d", l2.LogEndOffset()-1, durable)
			}
		})
	}
}

func TestRollFsyncFailureIsSticky(t *testing.T) {
	l, ff := faultLog(t, SyncPolicy{Mode: SyncAlways}, func(c *Config) { c.SegmentBytes = 500 })
	defer l.Close()
	ff.SetFaults(fs.Faults{SyncErrorRate: 1, Match: func(p string) bool { return strings.HasSuffix(p, ".log") }})
	var err error
	for i := 0; i < 20 && err == nil; i++ {
		_, err = l.Append(mkBatch(t, 1, 100, 0, 'r'), 1)
	}
	if !errors.Is(err, ErrLogFailed) {
		t.Fatalf("append across a failing roll fsync = %v", err)
	}
}

func TestWaitDurableTruncated(t *testing.T) {
	l, ff := faultLog(t, SyncPolicy{Mode: SyncAlways}, nil)
	defer l.Close()
	if _, err := l.Append(mkBatch(t, 1, 10, 0, 'a'), 1); err != nil {
		t.Fatal(err)
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	ff.SetFaults(fs.Faults{SyncLatency: 50 * time.Millisecond})
	res, _ := l.Append(mkBatch(t, 4, 10, 0, 'b'), 1)
	errc := make(chan error, 1)
	go func() { errc <- l.WaitDurable(context.Background(), res.LastOffset) }()
	time.Sleep(5 * time.Millisecond)
	if _, err := l.TruncateSuffix(res.BaseOffset); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; !errors.Is(err, ErrTruncated) {
		t.Fatalf("waiter on truncated offset got %v", err)
	}
	// A new batch at the same offset becomes durable normally.
	res2, _ := l.Append(mkBatch(t, 1, 10, 0, 'c'), 2)
	if err := l.WaitDurable(context.Background(), res2.LastOffset); err != nil {
		t.Fatal(err)
	}
}

func TestWaitDurableCancelAndClose(t *testing.T) {
	l, _ := faultLog(t, SyncPolicy{Mode: SyncAlways}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := l.WaitDurable(ctx, 1000); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitDurable with expired ctx = %v", err)
	}
	if n := len(l.fl.waiters); n != 0 {
		t.Fatalf("%d waiters leaked after cancellation", n)
	}
	errc := make(chan error, 1)
	go func() { errc <- l.WaitDurable(context.Background(), 1000) }()
	time.Sleep(5 * time.Millisecond)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; !errors.Is(err, ErrClosed) {
		t.Fatalf("waiter after Close = %v", err)
	}
	if err := l.WaitDurable(context.Background(), 2000); !errors.Is(err, ErrClosed) {
		t.Fatalf("WaitDurable after Close = %v", err)
	}
}

// TestCloseCompletesSatisfiedWaiters: data appended before Close is fsynced by
// Close, so its waiters succeed rather than failing with ErrClosed.
func TestCloseCompletesSatisfiedWaiters(t *testing.T) {
	l, ff := faultLog(t, SyncPolicy{Mode: SyncInterval, Interval: time.Hour}, nil)
	if _, err := l.Append(mkBatch(t, 1, 1, 0, 'x'), 1); err != nil {
		t.Fatal(err)
	}
	_ = l.Sync() // first sync happens immediately; the next waits an hour
	ff.SetFaults(fs.Faults{})
	res, _ := l.Append(mkBatch(t, 1, 1, 0, 'y'), 1)
	errc := make(chan error, 1)
	go func() { errc <- l.WaitDurable(context.Background(), res.LastOffset) }()
	time.Sleep(5 * time.Millisecond)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("waiter for data synced by Close got %v", err)
	}
}
