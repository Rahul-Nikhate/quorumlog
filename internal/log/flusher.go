package log

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// SyncMode selects when appended data is fsynced (spec §5.3.1).
type SyncMode int

const (
	// SyncAlways fsyncs before acknowledging, with group commit: one
	// background fsync covers every batch written while the previous fsync
	// was in flight. This is the mode the correctness claims are made in.
	SyncAlways SyncMode = iota
	// SyncInterval fsyncs at most once per Interval (a write after an idle
	// period is synced immediately); the loss window is about one interval.
	SyncInterval
	// SyncNever never fsyncs on the append path; data is acknowledged once it
	// is written to the page cache. Segment rolls do not fsync either; only
	// Sync, Close and Open (after recovery) do.
	SyncNever
)

// SyncPolicy is a SyncMode plus the interval for SyncInterval.
type SyncPolicy struct {
	Mode     SyncMode
	Interval time.Duration
}

// ParseSyncPolicy parses "always", "never", or "interval=<duration>" where the
// duration is a Go duration ("10ms") or a bare number of milliseconds ("10").
func ParseSyncPolicy(s string) (SyncPolicy, error) {
	switch {
	case s == "always":
		return SyncPolicy{Mode: SyncAlways}, nil
	case s == "never":
		return SyncPolicy{Mode: SyncNever}, nil
	case strings.HasPrefix(s, "interval="):
		v := strings.TrimPrefix(s, "interval=")
		d, err := time.ParseDuration(v)
		if err != nil {
			ms, perr := strconv.ParseInt(v, 10, 64)
			if perr != nil {
				return SyncPolicy{}, fmt.Errorf("log: bad sync interval %q", v)
			}
			d = time.Duration(ms) * time.Millisecond
		}
		if d <= 0 {
			return SyncPolicy{}, fmt.Errorf("log: sync interval must be positive, got %q", v)
		}
		return SyncPolicy{Mode: SyncInterval, Interval: d}, nil
	}
	return SyncPolicy{}, fmt.Errorf("log: unknown sync policy %q (want always, never or interval=<ms>)", s)
}

// String renders the policy in ParseSyncPolicy syntax.
func (p SyncPolicy) String() string {
	switch p.Mode {
	case SyncAlways:
		return "always"
	case SyncNever:
		return "never"
	case SyncInterval:
		return "interval=" + p.Interval.String()
	}
	return fmt.Sprintf("SyncMode(%d)", int(p.Mode))
}

// flusher tracks the written and durable watermarks of one Log and, for
// SyncAlways/SyncInterval, runs the background fsync goroutine (spec §5.3.2).
//
// Writers report written offsets (under Log.mu); the goroutine calls
// Log.syncOnce, which fsyncs every dirty segment without holding Log.mu, so N
// batches written during one fsync are covered by the next single fsync
// (group commit). Waiters register the offset they need and are completed when
// the durable watermark passes it, failed with ErrTruncated if a truncation
// removes it first, or failed with the sticky error after an fsync failure.
//
// Lock order: Log.mu before flusher.mu.
type flusher struct {
	l      *Log
	policy SyncPolicy

	kick   chan struct{} // capacity 1: "there is unsynced data"
	stopCh chan struct{}
	doneCh chan struct{}
	syncs  atomic.Int64

	mu         sync.Mutex
	writtenOff int64
	durable    int64
	gen        uint64 // bumped by every truncation; stale fsync results are discarded
	err        error
	closed     bool
	waiters    []*waiter
}

type waiter struct {
	off int64
	ch  chan error // capacity 1
}

func newFlusher(l *Log, p SyncPolicy, durable int64) *flusher {
	return &flusher{
		l:          l,
		policy:     p,
		kick:       make(chan struct{}, 1),
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
		writtenOff: durable,
		durable:    durable,
	}
}

func (f *flusher) start() {
	if f.policy.Mode == SyncNever {
		close(f.doneCh)
		return
	}
	go f.run()
}

func (f *flusher) run() {
	defer close(f.doneCh)
	var last time.Time
	for {
		select {
		case <-f.kick:
		case <-f.stopCh:
			return
		}
		if f.policy.Mode == SyncInterval && !last.IsZero() {
			if wait := f.policy.Interval - time.Since(last); wait > 0 {
				t := time.NewTimer(wait)
				select {
				case <-t.C:
				case <-f.stopCh:
					t.Stop()
					return
				}
			}
		}
		if !f.pending() {
			continue
		}
		target, gen, err := f.l.syncOnce()
		last = time.Now()
		if err != nil {
			// syncOnce recorded the sticky failure and failed all waiters.
			return
		}
		f.l.notifyDurable(f.complete(target, gen))
		if f.pending() {
			f.signal()
		}
	}
}

func (f *flusher) signal() {
	select {
	case f.kick <- struct{}{}:
	default:
	}
}

func (f *flusher) pending() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writtenOff > f.durable && f.err == nil
}

func (f *flusher) generation() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gen
}

// written records that offsets up to off are in the page cache. Under
// SyncNever that is also the acknowledgement point, so it returns the new
// durable offset to report (or -1).
func (f *flusher) written(off int64) int64 {
	f.mu.Lock()
	if off > f.writtenOff {
		f.writtenOff = off
	}
	if f.policy.Mode != SyncNever {
		f.mu.Unlock()
		f.signal()
		return -1
	}
	f.advanceLocked(off)
	f.mu.Unlock()
	return off
}

// complete records a finished fsync that covered target, unless a truncation
// happened since it started (gen changed). It returns the new durable offset
// to report, or -1 if it did not advance.
func (f *flusher) complete(target int64, gen uint64) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if gen != f.gen || target <= f.durable || f.err != nil {
		return -1
	}
	f.advanceLocked(target)
	return target
}

func (f *flusher) advanceLocked(off int64) {
	if off <= f.durable {
		return
	}
	f.durable = off
	kept := f.waiters[:0]
	for _, w := range f.waiters {
		if w.off <= off {
			w.ch <- nil
		} else {
			kept = append(kept, w)
		}
	}
	clear(f.waiters[len(kept):])
	f.waiters = kept
}

// beginTruncate is called (under Log.mu) before a truncation touches any
// file: it invalidates fsyncs already in flight, whose result may or may not
// include the data about to be removed.
func (f *flusher) beginTruncate() {
	f.mu.Lock()
	f.gen++
	f.mu.Unlock()
}

// truncated resets both watermarks to newLEO-1 after a truncation (which made
// the remaining prefix durable, or under SyncNever needs no fsync) and fails
// waiters for removed offsets.
func (f *flusher) truncated(newLEO int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gen++
	f.writtenOff, f.durable = newLEO-1, min(f.durable, newLEO-1)
	if f.err == nil {
		f.durable = newLEO - 1
	}
	kept := f.waiters[:0]
	for _, w := range f.waiters {
		switch {
		case w.off >= newLEO:
			w.ch <- ErrTruncated
		case w.off <= f.durable:
			w.ch <- nil
		default:
			kept = append(kept, w)
		}
	}
	clear(f.waiters[len(kept):])
	f.waiters = kept
}

// fail makes err sticky and fails every waiter.
func (f *flusher) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err == nil {
		f.err = err
	}
	for _, w := range f.waiters {
		w.ch <- f.err
	}
	f.waiters = nil
}

func (f *flusher) durableOffset() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.durable
}

// stop terminates the background goroutine and waits for it.
func (f *flusher) stop() {
	select {
	case <-f.stopCh:
	default:
		close(f.stopCh)
	}
	<-f.doneCh
}

// closeWaiters fails all remaining waiters with ErrClosed; later waits fail
// immediately.
func (f *flusher) closeWaiters() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	for _, w := range f.waiters {
		w.ch <- ErrClosed
	}
	f.waiters = nil
}

func (f *flusher) wait(ctx context.Context, off int64) error {
	f.mu.Lock()
	switch {
	case f.err != nil:
		err := f.err
		f.mu.Unlock()
		return err
	case off <= f.durable:
		f.mu.Unlock()
		return nil
	case f.closed:
		f.mu.Unlock()
		return ErrClosed
	}
	w := &waiter{off: off, ch: make(chan error, 1)}
	f.waiters = append(f.waiters, w)
	f.mu.Unlock()
	select {
	case err := <-w.ch:
		return err
	case <-ctx.Done():
		f.mu.Lock()
		for i, x := range f.waiters {
			if x == w {
				f.waiters = append(f.waiters[:i], f.waiters[i+1:]...)
				f.mu.Unlock()
				return ctx.Err()
			}
		}
		f.mu.Unlock()
		return <-w.ch // completed concurrently with cancellation
	}
}
