package log

import (
	"math"
	"strings"
	"sync"
	"testing"
	"time"
)

func segmentFiles(t *testing.T, e testEnv) (live, deleted int) {
	t.Helper()
	ents, err := e.fsys.ReadDir(e.dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, en := range ents {
		switch {
		case strings.HasSuffix(en.Name(), logSuffix):
			live++
		case strings.HasSuffix(en.Name(), deletedSuffix):
			deleted++
		}
	}
	return live, deleted
}

func TestRetentionBySize(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		l := mustOpen(t, smallConfig(e))
		defer l.Close()
		want := appendN(t, l, 200, 1)
		st := l.Stats()
		limit := st.Bytes / 3
		n, err := l.ApplyRetention(RetentionPolicy{MaxBytes: limit}, math.MaxInt64)
		if err != nil || n == 0 {
			t.Fatalf("ApplyRetention = %d, %v", n, err)
		}
		after := l.Stats()
		if after.Bytes < limit {
			t.Fatalf("size retention went below the limit: %d < %d", after.Bytes, limit)
		}
		// Deleting one more segment would have gone below the limit.
		if first := l.list()[0]; after.Segments > 1 && after.Bytes-first.size.Load() >= limit {
			t.Fatalf("size retention stopped early: %d bytes, first segment %d, limit %d", after.Bytes, first.size.Load(), limit)
		}
		start := l.LogStartOffset()
		if start != l.list()[0].base {
			t.Fatalf("log start %d != first segment base %d", start, l.list()[0].base)
		}
		var kept []Batch
		for _, b := range want {
			if b.BaseOffset() >= start {
				kept = append(kept, b)
			}
		}
		equalBatches(t, checkInvariants(t, l), kept)
		// StartTerm = term of the last deleted batch.
		if got, err := l.TermAt(start - 1); err != nil || got != 1 {
			t.Fatalf("TermAt(start-1) = %d, %v", got, err)
		}
		if live, del := segmentFiles(t, e); live != after.Segments || del != 0 {
			t.Fatalf("files: %d live, %d .deleted; %d segments", live, del, after.Segments)
		}
	})
}

func TestRetentionByTime(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		cfg := smallConfig(e)
		now := time.UnixMilli(10_000_000)
		var mu sync.Mutex
		cfg.Now = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
		l := mustOpen(t, cfg)
		defer l.Close()
		// Old data (ts ~ 1,000,000 ms) then new data (ts ~ 9,999,000 ms).
		for i := 0; i < 60; i++ {
			l.Append(mkBatch(t, 2, 100, 1_000_000+int64(i), 'o'), 1)
		}
		if err := l.Roll(); err != nil {
			t.Fatal(err)
		}
		newStart := l.LogEndOffset()
		for i := 0; i < 60; i++ {
			l.Append(mkBatch(t, 2, 100, 9_999_000+int64(i), 'n'), 1)
		}
		n, err := l.ApplyRetention(RetentionPolicy{MaxAge: time.Hour}, math.MaxInt64)
		if err != nil || n == 0 {
			t.Fatalf("ApplyRetention = %d, %v", n, err)
		}
		if l.LogStartOffset() != newStart {
			t.Fatalf("log start %d, want %d (only old segments deleted)", l.LogStartOffset(), newStart)
		}
		if n2, _ := l.ApplyRetention(RetentionPolicy{MaxAge: time.Hour}, math.MaxInt64); n2 != 0 {
			t.Fatalf("second pass deleted %d young segments", n2)
		}
	})
}

func TestRetentionRespectsHighWatermarkAndActiveSegment(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		l := mustOpen(t, smallConfig(e))
		defer l.Close()
		appendN(t, l, 100, 1)
		segs := l.list()
		hw := segs[3].base // only segments 0..2 are fully committed
		n, err := l.ApplyRetention(RetentionPolicy{MaxBytes: 1}, hw)
		if err != nil || n != 3 {
			t.Fatalf("deleted %d segments (err %v), want 3", n, err)
		}
		n, _ = l.ApplyRetention(RetentionPolicy{MaxBytes: 1}, math.MaxInt64)
		if l.Stats().Segments != 1 {
			t.Fatalf("active segment deleted or retention incomplete: %d segments left after %d", l.Stats().Segments, n)
		}
	})
}

// TestRetentionWaitsForReaders: a FileRegion pins its segment; the files are
// renamed to *.deleted and only unlinked after Release.
func TestRetentionWaitsForReaders(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		l := mustOpen(t, smallConfig(e))
		defer l.Close()
		want := appendN(t, l, 60, 1)
		r, err := l.Read(0, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := l.ApplyRetention(RetentionPolicy{MaxBytes: 1}, math.MaxInt64); err != nil {
			t.Fatal(err)
		}
		if _, del := segmentFiles(t, e); del != 3 {
			t.Fatalf("pinned segment: %d .deleted files, want 3 (log + 2 indexes)", del)
		}
		data, err := r.Bytes()
		if err != nil {
			t.Fatalf("pinned region unreadable after retention: %v", err)
		}
		bs, _ := SplitBatches(data, math.MaxInt32)
		equalBatches(t, bs, want[:len(bs)])
		r.Release()
		if _, del := segmentFiles(t, e); del != 0 {
			t.Fatalf("%d .deleted files remain after Release", del)
		}
		if _, err := l.Read(0, 100); err == nil {
			t.Fatal("deleted offset still readable")
		}
	})
}

func TestRetentionSurvivesReopen(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		cfg := smallConfig(e)
		l := mustOpen(t, cfg)
		appendN(t, l, 100, 2)
		if _, err := l.ApplyRetention(RetentionPolicy{MaxBytes: 5000}, math.MaxInt64); err != nil {
			t.Fatal(err)
		}
		start, term := l.LogStartOffset(), l.StartTerm()
		bs := readAllBatches(t, l)
		abandon(l)
		l = mustOpen(t, cfg)
		defer l.Close()
		if l.LogStartOffset() != start || l.StartTerm() != term {
			t.Fatalf("after reopen start %d term %d, want %d %d", l.LogStartOffset(), l.StartTerm(), start, term)
		}
		equalBatches(t, checkInvariants(t, l), bs)
	})
}
