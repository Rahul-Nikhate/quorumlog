package log

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Rahul-Nikhate/quorumlog/internal/fs"
)

// abandon simulates a process crash on any FS: it stops the flusher and closes
// all files without sealing segments or writing the clean-shutdown record.
func abandon(l *Log) {
	l.fl.stop()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	l.closed = true
	l.closedR.Store(true)
	segs := l.list()
	empty := []*segment{}
	l.segs.Store(&empty)
	for _, s := range segs {
		s.release()
	}
	l.fl.closeWaiters()
}

func appendN(t testing.TB, l *Log, n int, term uint64) []Batch {
	t.Helper()
	var out []Batch
	for i := 0; i < n; i++ {
		b := mkBatch(t, 1+i%4, 20+(i*37)%300, int64(1000+i), byte('a'+i%26))
		res, err := l.Append(b, term)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if res.BaseOffset != b.BaseOffset() || res.LastOffset != b.LastOffset() {
			t.Fatalf("append result %+v vs batch [%d,%d]", res, b.BaseOffset(), b.LastOffset())
		}
		out = append(out, bytes.Clone(b))
	}
	return out
}

func equalBatches(t testing.TB, got, want []Batch) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d batches, want %d", len(got), len(want))
	}
	for i := range got {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("batch %d differs (got base %d, want base %d)", i, got[i].BaseOffset(), want[i].BaseOffset())
		}
	}
}

func TestAppendAssignsOffsetsAndReads(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		l := mustOpen(t, smallConfig(e))
		defer l.Close()
		want := appendN(t, l, 120, 1)
		var next int64
		for i, b := range want {
			if b.BaseOffset() != next {
				t.Fatalf("batch %d base %d, want %d", i, b.BaseOffset(), next)
			}
			next = b.LastOffset() + 1
		}
		if l.LogEndOffset() != next {
			t.Fatalf("LEO %d, want %d", l.LogEndOffset(), next)
		}
		if st := l.Stats(); st.Segments < 5 {
			t.Fatalf("expected several segments, got %d", st.Segments)
		}
		equalBatches(t, checkInvariants(t, l), want)
		// Read from every offset returns the batch containing it first.
		for _, b := range want {
			for off := b.BaseOffset(); off <= b.LastOffset(); off++ {
				r, err := l.Read(off, 1)
				if err != nil {
					t.Fatalf("Read(%d): %v", off, err)
				}
				data, err := r.Bytes()
				r.Release()
				if err != nil || !bytes.Equal(data, b) {
					t.Fatalf("Read(%d) returned %d bytes, want batch at %d", off, len(data), b.BaseOffset())
				}
				if r.StartOffset != b.BaseOffset() {
					t.Fatalf("StartOffset %d", r.StartOffset)
				}
			}
		}
	})
}

func TestReadMaxBytesAlignment(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		cfg := smallConfig(e)
		cfg.SegmentBytes = 1 << 20
		l := mustOpen(t, cfg)
		defer l.Close()
		want := appendN(t, l, 60, 1)
		for _, maxBytes := range []int{0, 1, 100, 500, 1000, 3000, 1 << 20} {
			for i := 0; i < len(want); i += 7 {
				r, err := l.Read(want[i].BaseOffset(), maxBytes)
				if err != nil {
					t.Fatal(err)
				}
				data, _ := r.Bytes()
				r.Release()
				bs, err := SplitBatches(data, math.MaxInt32)
				if err != nil {
					t.Fatalf("region not whole batches: %v", err)
				}
				if len(bs) == 0 {
					t.Fatal("empty region for a readable offset")
				}
				if len(bs) > 1 && len(data) > maxBytes {
					t.Fatalf("maxBytes %d: got %d bytes in %d batches", maxBytes, len(data), len(bs))
				}
				// Maximal: the next batch would not have fit.
				j := i + len(bs)
				if j < len(want) && len(data)+len(want[j]) <= maxBytes {
					t.Fatalf("maxBytes %d: region of %d bytes stopped early before a %d-byte batch", maxBytes, len(data), len(want[j]))
				}
				equalBatches(t, bs, want[i:j])
			}
		}
	})
}

func TestReadUpToHighWatermark(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		l := mustOpen(t, smallConfig(e))
		defer l.Close()
		want := appendN(t, l, 40, 1)
		hw := want[25].BaseOffset() // batches [0,25) committed
		total := 0
		var got []Batch
		for off := int64(0); ; {
			r, err := l.ReadUpTo(off, 700, hw)
			if err != nil {
				t.Fatal(err)
			}
			if r.Len == 0 {
				if r.File != nil {
					t.Fatal("empty region with a file")
				}
				r.Release()
				break
			}
			data, _ := r.Bytes()
			r.Release()
			bs, _ := SplitBatches(data, math.MaxInt32)
			got = append(got, bs...)
			total += len(data)
			off = bs[len(bs)-1].LastOffset() + 1
		}
		equalBatches(t, got, want[:25])
		// Offset at/after the HW yields an empty region, not an error.
		r, err := l.ReadUpTo(hw, 1000, hw)
		if err != nil || r.Len != 0 {
			t.Fatalf("ReadUpTo(hw) = %+v, %v", r, err)
		}
		// HW inside a batch excludes that batch.
		r, err = l.ReadUpTo(want[25].BaseOffset(), 1<<20, want[25].LastOffset())
		if err != nil || r.Len != 0 {
			t.Fatalf("partially committed batch served: len %d, %v", r.Len, err)
		}
	})
}

func TestReadOffsetBounds(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		l := mustOpen(t, smallConfig(e))
		defer l.Close()
		r, err := l.Read(0, 100)
		if err != nil || r.Len != 0 {
			t.Fatalf("read of empty log: %+v %v", r, err)
		}
		appendN(t, l, 5, 1)
		leo := l.LogEndOffset()
		if r, err := l.Read(leo, 100); err != nil || r.Len != 0 {
			t.Fatalf("Read(LEO) = %+v, %v", r, err)
		}
		if _, err := l.Read(leo+1, 100); !errors.Is(err, ErrOffsetOutOfRange) {
			t.Fatalf("Read(LEO+1) err = %v", err)
		}
		if _, err := l.Read(-1, 100); !errors.Is(err, ErrOffsetOutOfRange) {
			t.Fatalf("Read(-1) err = %v", err)
		}
		if _, err := l.BatchInfoAt(leo); !errors.Is(err, ErrOffsetOutOfRange) {
			t.Fatalf("BatchInfoAt(LEO) err = %v", err)
		}
	})
}

func TestReopenCleanIsTrusted(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		cfg := smallConfig(e)
		l := mustOpen(t, cfg)
		want := appendN(t, l, 80, 3)
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		if err := l.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
		if _, err := l.Append(mkBatch(t, 1, 1, 0, 'x'), 3); !errors.Is(err, ErrClosed) {
			t.Fatalf("append after close: %v", err)
		}
		if _, err := l.Read(0, 10); !errors.Is(err, ErrClosed) {
			t.Fatalf("read after close: %v", err)
		}
		ck, err := readCheckpoint(e.fsys, e.dir)
		if err != nil || !ck.clean || ck.recoveryPoint != want[len(want)-1].LastOffset()+1 {
			t.Fatalf("checkpoint after close = %+v, %v", ck, err)
		}
		var warnings bytes.Buffer
		cfg.Logger = newTestLogger(&warnings)
		l2 := mustOpen(t, cfg)
		defer l2.Close()
		if warnings.Len() != 0 {
			t.Fatalf("clean reopen logged warnings (rebuild/scan?): %s", warnings.String())
		}
		equalBatches(t, checkInvariants(t, l2), want)
		if last, term := l2.LastOffsetAndTerm(); last != want[len(want)-1].LastOffset() || term != 3 {
			t.Fatalf("LastOffsetAndTerm = %d, %d", last, term)
		}
		// The clean flag was cleared durably on open.
		if ck, _ := readCheckpoint(e.fsys, e.dir); ck.clean {
			t.Fatal("clean flag still set while open")
		}
		// Appends continue after reopen, into the reopened active segment.
		more := appendN(t, l2, 10, 4)
		equalBatches(t, checkInvariants(t, l2), append(want, more...))
	})
}

func TestReopenAfterProcessCrashRecovers(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		cfg := smallConfig(e)
		l := mustOpen(t, cfg)
		want := appendN(t, l, 50, 1)
		abandon(l)
		l2 := mustOpen(t, cfg)
		defer l2.Close()
		equalBatches(t, checkInvariants(t, l2), want)
	})
}

func lastLogFile(t *testing.T, e testEnv) string {
	t.Helper()
	ents, err := e.fsys.ReadDir(e.dir)
	if err != nil {
		t.Fatal(err)
	}
	var last string
	for _, en := range ents {
		if strings.HasSuffix(en.Name(), logSuffix) {
			last = en.Name()
		}
	}
	return filepath.Join(e.dir, last)
}

func modifyFile(t *testing.T, fsys fs.FS, path string, fn func(f fs.File, size int64)) {
	t.Helper()
	f, err := fsys.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	st, _ := f.Stat()
	fn(f, st.Size())
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	f.Close()
}

func TestTornTailIsTruncated(t *testing.T) {
	cases := []struct {
		name string
		mod  func(f fs.File, size int64)
		lost int // batches lost from the tail
	}{
		{"partial last batch", func(f fs.File, size int64) { f.Truncate(size - 10) }, 1},
		{"partial header", func(f fs.File, size int64) { f.WriteAt([]byte{0, 0, 0}, size) }, 0},
		{"garbage tail", func(f fs.File, size int64) { f.WriteAt(bytes.Repeat([]byte{0xab}, 100), size) }, 0},
		{"zeroed tail", func(f fs.File, size int64) { f.WriteAt(make([]byte, 4096), size) }, 0},
		{"bit flip in last batch", func(f fs.File, size int64) {
			b := []byte{0}
			f.ReadAt(b, size-5)
			f.WriteAt([]byte{b[0] ^ 0x01}, size-5)
		}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			forEachEnv(t, func(t *testing.T, e testEnv) {
				cfg := smallConfig(e)
				cfg.SegmentBytes = 1 << 20 // keep everything in one segment
				l := mustOpen(t, cfg)
				// Everything appended after Open is above the recovery point
				// (an explicit Sync does not move it), so a crash may tear it.
				want := appendN(t, l, 1, 1)
				if err := l.Sync(); err != nil {
					t.Fatal(err)
				}
				want = append(want, appendN(t, l, 20, 1)...)
				abandon(l)
				modifyFile(t, e.fsys, lastLogFile(t, e), tc.mod)
				l2 := mustOpen(t, cfg)
				defer l2.Close()
				got := checkInvariants(t, l2)
				equalBatches(t, got, want[:len(want)-tc.lost])
				// The log is writable at the recovered end.
				res, err := l2.Append(mkBatch(t, 2, 5, 0, 'n'), 1)
				if err != nil || res.BaseOffset != got[len(got)-1].LastOffset()+1 {
					t.Fatalf("append after recovery: %+v %v", res, err)
				}
			})
		})
	}
}

func TestCorruptionBelowRecoveryPointIsFatal(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		cfg := smallConfig(e)
		cfg.SegmentBytes = 1 << 20
		l := mustOpen(t, cfg)
		appendN(t, l, 20, 1)
		abandon(l)
		// Reopen: recovery makes the 20 batches durable and moves the recovery
		// point to the end. Then crash again and flip a byte in batch 3.
		l = mustOpen(t, cfg)
		appendN(t, l, 5, 1)
		abandon(l)
		modifyFile(t, e.fsys, lastLogFile(t, e), func(f fs.File, size int64) {
			f.WriteAt([]byte{0xff}, 3*BatchHeaderSize+5)
		})
		if _, err := Open(cfg); !errors.Is(err, ErrCorruptLog) {
			t.Fatalf("Open with corruption below the recovery point: %v, want ErrCorruptLog", err)
		}
	})
}

func TestCorruptClosedSegmentIsFatal(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		cfg := smallConfig(e)
		cfg.Sync = SyncPolicy{Mode: SyncAlways}
		l := mustOpen(t, cfg)
		appendN(t, l, 40, 1)
		l.Close()
		first := segmentPath(e.dir, 0, logSuffix)
		modifyFile(t, e.fsys, first, func(f fs.File, size int64) { f.WriteAt([]byte{0xee, 0xee}, size/2) })
		// Remove its index so the trusted segment is re-scanned.
		if err := e.fsys.Remove(segmentPath(e.dir, 0, indexSuffix)); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(cfg); !errors.Is(err, ErrCorruptLog) {
			t.Fatalf("Open = %v, want ErrCorruptLog", err)
		}
	})
}

func TestIndexRebuild(t *testing.T) {
	type mod struct {
		name string
		fn   func(t *testing.T, e testEnv, base int64)
	}
	mods := []mod{
		{"missing", func(t *testing.T, e testEnv, base int64) {
			e.fsys.Remove(segmentPath(e.dir, base, indexSuffix))
			e.fsys.Remove(segmentPath(e.dir, base, timeIndexSuffix))
		}},
		{"garbage", func(t *testing.T, e testEnv, base int64) {
			for _, suf := range []string{indexSuffix, timeIndexSuffix} {
				modifyFile(t, e.fsys, segmentPath(e.dir, base, suf), func(f fs.File, size int64) {
					f.WriteAt(bytes.Repeat([]byte{0x5a}, int(max(size, 24))), 0)
				})
			}
		}},
		{"odd size", func(t *testing.T, e testEnv, base int64) {
			modifyFile(t, e.fsys, segmentPath(e.dir, base, indexSuffix), func(f fs.File, size int64) { f.Truncate(size - 3) })
		}},
		{"truncated to empty", func(t *testing.T, e testEnv, base int64) {
			modifyFile(t, e.fsys, segmentPath(e.dir, base, indexSuffix), func(f fs.File, size int64) { f.Truncate(0) })
		}},
		{"stale entry", func(t *testing.T, e testEnv, base int64) {
			// Shift every position by one batch-header: structurally valid
			// (increasing) but pointing at non-boundaries.
			modifyFile(t, e.fsys, segmentPath(e.dir, base, indexSuffix), func(f fs.File, size int64) {
				f.WriteAt([]byte{0, 0, 0, 0, 0, 0, 0, 1}, 0)
			})
		}},
		{"time index unsorted", func(t *testing.T, e testEnv, base int64) {
			modifyFile(t, e.fsys, segmentPath(e.dir, base, timeIndexSuffix), func(f fs.File, size int64) {
				f.WriteAt(bytes.Repeat([]byte{0x7f}, 12), 0)
				f.WriteAt(make([]byte, 12), 12)
			})
		}},
	}
	for _, m := range mods {
		t.Run(m.name, func(t *testing.T) {
			forEachEnv(t, func(t *testing.T, e testEnv) {
				cfg := smallConfig(e)
				l := mustOpen(t, cfg)
				want := appendN(t, l, 70, 2)
				segs := l.list()
				bases := []int64{segs[0].base, segs[len(segs)/2].base, segs[len(segs)-1].base}
				if err := l.Close(); err != nil {
					t.Fatal(err)
				}
				for _, b := range bases {
					m.fn(t, e, b)
				}
				var logs bytes.Buffer
				cfg.Logger = newTestLogger(&logs)
				l2 := mustOpen(t, cfg)
				defer l2.Close()
				if !strings.Contains(logs.String(), "rebuilding segment indexes") {
					t.Fatalf("expected an index rebuild, logs: %s", logs.String())
				}
				equalBatches(t, checkInvariants(t, l2), want)
				for _, b := range want {
					info, err := l2.BatchInfoAt(b.LastOffset())
					if err != nil || info.BaseOffset != b.BaseOffset() {
						t.Fatalf("BatchInfoAt(%d) = %+v, %v", b.LastOffset(), info, err)
					}
				}
				if _, ok, _ := l2.OffsetForTimestamp(want[30].MaxTimestamp()); !ok {
					t.Fatal("time index lookup failed after rebuild")
				}
			})
		})
	}
}

func TestIndexLookupsMatchBruteForce(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		cfg := smallConfig(e)
		for _, interval := range []int{1, 100, 1000, 1 << 20} {
			cfg.IndexIntervalBytes = interval
			cfg.Dir = filepath.Join(e.dir, fmt.Sprintf("interval-%d", interval))
			l := mustOpen(t, cfg)
			want := appendN(t, l, 60, 1)
			for _, b := range want {
				for off := b.BaseOffset(); off <= b.LastOffset(); off++ {
					info, err := l.BatchInfoAt(off)
					if err != nil || info.BaseOffset != b.BaseOffset() || info.LastOffset != b.LastOffset() {
						t.Fatalf("interval %d BatchInfoAt(%d) = %+v, %v; want [%d,%d]",
							interval, off, info, err, b.BaseOffset(), b.LastOffset())
					}
				}
			}
			l.Close()
		}
	})
}

func TestOffsetIndexUnit(t *testing.T) {
	ff := fs.NewFaultFS(1)
	f, _ := ff.Create("/idx")
	ix := &offsetIndex{}
	if err := ix.init(f, offsetEntrySize, 10*offsetEntrySize, 0, true); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := ix.lookup(5); ok {
		t.Fatal("lookup in empty index")
	}
	for i := uint32(0); i < 8; i++ {
		if err := ix.add(i*10, i*100); err != nil {
			t.Fatal(err)
		}
	}
	if ix.full() {
		t.Fatal("index with 2 free slots should not report full")
	}
	ix.add(80, 800)
	if !ix.full() {
		t.Fatal("index with 1 free slot should report full (the slot is reserved)")
	}
	ix.add(90, 900)
	if err := ix.add(100, 1000); !errors.Is(err, errIndexFull) {
		t.Fatalf("add past capacity: %v", err)
	}
	tests := []struct {
		rel          uint32
		wantRel, pos uint32
	}{{0, 0, 0}, {9, 0, 0}, {10, 10, 100}, {11, 10, 100}, {89, 80, 800}, {90, 90, 900}, {1 << 31, 90, 900}}
	for _, tc := range tests {
		r, p, ok := ix.lookup(tc.rel)
		if !ok || r != tc.wantRel || p != tc.pos {
			t.Errorf("lookup(%d) = %d,%d,%v want %d,%d", tc.rel, r, p, ok, tc.wantRel, tc.pos)
		}
	}
	if r, p, _ := ix.lookupPos(250); r != 20 || p != 200 {
		t.Errorf("lookupPos(250) = %d,%d", r, p)
	}
	ix.truncateAtPos(300)
	if ix.entries() != 3 {
		t.Fatalf("entries after truncateAtPos(300) = %d", ix.entries())
	}
	if err := ix.seal(true); err != nil {
		t.Fatal(err)
	}
	if st, _ := f.Stat(); st.Size() != 3*offsetEntrySize {
		t.Fatalf("sealed size %d", st.Size())
	}
	if err := ix.validate(1000); err != nil {
		t.Fatal(err)
	}
	if err := ix.validate(200); err == nil {
		t.Fatal("validate accepted a position beyond the log")
	}
	if err := ix.unseal(); err != nil {
		t.Fatal(err)
	}
	if err := ix.add(30, 300); err != nil {
		t.Fatal(err)
	}
	if r, _, _ := ix.last(); r != 30 {
		t.Fatalf("last after unseal+add = %d", r)
	}
}

func TestTimeIndexAndOffsetForTimestamp(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		cfg := smallConfig(e)
		l := mustOpen(t, cfg)
		defer l.Close()
		var want []Batch
		// Timestamps rise with occasional dips (producer clock skew).
		for i := 0; i < 80; i++ {
			ts := int64(10_000 + i*10)
			if i%9 == 0 {
				ts -= 500
			}
			b := mkBatch(t, 2, 50, ts, 'q')
			if _, err := l.Append(b, 1); err != nil {
				t.Fatal(err)
			}
			want = append(want, bytes.Clone(b))
		}
		brute := func(ts int64) (int64, bool) {
			for _, b := range want {
				if b.MaxTimestamp() >= ts {
					return b.BaseOffset(), true
				}
			}
			return 0, false
		}
		for ts := int64(9_000); ts < 11_000; ts += 7 {
			got, ok, err := l.OffsetForTimestamp(ts)
			w, wok := brute(ts)
			if err != nil || ok != wok || (ok && got != w) {
				t.Fatalf("OffsetForTimestamp(%d) = %d,%v,%v want %d,%v", ts, got, ok, err, w, wok)
			}
		}
	})
}

func TestTruncateSuffix(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		cfg := smallConfig(e)
		l := mustOpen(t, cfg)
		want := appendN(t, l, 100, 1)
		segsBefore := l.Stats().Segments

		// No-op at/after LEO.
		if leo, err := l.TruncateSuffix(l.LogEndOffset()); err != nil || leo != l.LogEndOffset() {
			t.Fatalf("no-op truncate: %d %v", leo, err)
		}
		// Mid-batch: the containing batch goes too.
		mid := want[90]
		if mid.LastOffset() == mid.BaseOffset() {
			mid = want[91]
		}
		k := 90
		if mid.BaseOffset() != want[90].BaseOffset() {
			k = 91
		}
		leo, err := l.TruncateSuffix(mid.BaseOffset() + 1)
		if err != nil || leo != mid.BaseOffset() {
			t.Fatalf("mid-batch truncate: leo %d err %v, want %d", leo, err, mid.BaseOffset())
		}
		want = want[:k]
		equalBatches(t, checkInvariants(t, l), want)

		// Far back, across several segments, at a batch boundary.
		cut := want[20].BaseOffset()
		if leo, err := l.TruncateSuffix(cut); err != nil || leo != cut {
			t.Fatalf("truncate to %d: %d %v", cut, leo, err)
		}
		want = want[:20]
		equalBatches(t, checkInvariants(t, l), want)
		if l.Stats().Segments >= segsBefore {
			t.Fatal("whole later segments were not deleted")
		}
		// The new term can be lower than removed terms but not than the last kept.
		more := appendN(t, l, 30, 2)
		want = append(want, more...)
		equalBatches(t, checkInvariants(t, l), want)
		if _, err := l.Append(mkBatch(t, 1, 1, 0, 'x'), 1); !errors.Is(err, ErrTermRegression) {
			t.Fatalf("term regression accepted: %v", err)
		}
		// Reopen (clean) and crash-reopen both preserve the truncated state.
		l.Close()
		l = mustOpen(t, cfg)
		equalBatches(t, checkInvariants(t, l), want)
		abandon(l)
		l = mustOpen(t, cfg)
		equalBatches(t, checkInvariants(t, l), want)
		// No *.deleted leftovers.
		ents, _ := e.fsys.ReadDir(e.dir)
		for _, en := range ents {
			if strings.HasSuffix(en.Name(), deletedSuffix) {
				t.Fatalf("leftover %s", en.Name())
			}
		}
		// Truncate everything.
		if leo, err := l.TruncateSuffix(0); err != nil || leo != 0 {
			t.Fatalf("truncate to 0: %d %v", leo, err)
		}
		if bs := checkInvariants(t, l); len(bs) != 0 {
			t.Fatalf("%d batches after truncate to 0", len(bs))
		}
		if last, term := l.LastOffsetAndTerm(); last != -1 || term != 0 {
			t.Fatalf("LastOffsetAndTerm of empty log = %d,%d", last, term)
		}
		appendN(t, l, 3, 5)
		l.Close()
	})
}

func TestTruncatePrefixAndTerms(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		cfg := smallConfig(e)
		l := mustOpen(t, cfg)
		var want []Batch
		for term := uint64(1); term <= 5; term++ {
			want = append(want, appendN(t, l, 20, term)...)
		}
		if got, err := l.TermAt(-1); err != nil || got != 0 {
			t.Fatalf("TermAt(-1) = %d, %v", got, err)
		}
		for _, b := range want {
			if got, err := l.TermAt(b.LastOffset()); err != nil || got != b.Term() {
				t.Fatalf("TermAt(%d) = %d, %v; want %d", b.LastOffset(), got, err, b.Term())
			}
		}
		for term := uint64(0); term <= 6; term++ {
			got, err := l.TermStart(term)
			wantOff := l.LogEndOffset()
			for _, b := range want {
				if b.Term() >= term {
					wantOff = b.BaseOffset()
					break
				}
			}
			if err != nil || got != wantOff {
				t.Fatalf("TermStart(%d) = %d, %v; want %d", term, got, err, wantOff)
			}
		}
		cut := want[37]
		segsBefore := l.Stats().Segments
		if err := l.TruncatePrefix(cut.BaseOffset()); err != nil {
			t.Fatal(err)
		}
		if l.LogStartOffset() != cut.BaseOffset() || l.StartTerm() != want[36].Term() {
			t.Fatalf("start %d term %d", l.LogStartOffset(), l.StartTerm())
		}
		if got, err := l.TermAt(cut.BaseOffset() - 1); err != nil || got != want[36].Term() {
			t.Fatalf("TermAt(start-1) = %d, %v", got, err)
		}
		if _, err := l.TermAt(cut.BaseOffset() - 2); !errors.Is(err, ErrCompacted) {
			t.Fatalf("TermAt(start-2) err = %v", err)
		}
		if _, err := l.Read(cut.BaseOffset()-1, 100); !errors.Is(err, ErrOffsetOutOfRange) {
			t.Fatalf("Read below start: %v", err)
		}
		if l.Stats().Segments >= segsBefore {
			t.Fatal("no segment deleted by TruncatePrefix")
		}
		want = want[37:]
		equalBatches(t, checkInvariants(t, l), want)
		if err := l.TruncatePrefix(1); err != nil {
			t.Fatalf("backwards TruncatePrefix should be a no-op: %v", err)
		}
		if err := l.TruncatePrefix(l.LogEndOffset() + 1); !errors.Is(err, ErrOffsetOutOfRange) {
			t.Fatalf("TruncatePrefix beyond LEO: %v", err)
		}
		if _, err := l.TruncateSuffix(cut.BaseOffset() - 1); !errors.Is(err, ErrOffsetOutOfRange) {
			t.Fatalf("TruncateSuffix below start: %v", err)
		}
		startTerm := l.StartTerm()
		l.Close()
		l = mustOpen(t, cfg)
		if l.LogStartOffset() != cut.BaseOffset() || l.StartTerm() != startTerm {
			t.Fatalf("after reopen: start %d term %d, want %d %d", l.LogStartOffset(), l.StartTerm(), cut.BaseOffset(), startTerm)
		}
		equalBatches(t, checkInvariants(t, l), want)
		// Prefix truncation to LEO empties the log logically.
		if err := l.TruncatePrefix(l.LogEndOffset()); err != nil {
			t.Fatal(err)
		}
		if last, term := l.LastOffsetAndTerm(); last != l.LogStartOffset()-1 || term != l.StartTerm() {
			t.Fatalf("empty log LastOffsetAndTerm = %d,%d (start %d, term %d)", last, term, l.LogStartOffset(), l.StartTerm())
		}
		if bs := checkInvariants(t, l); len(bs) != 0 {
			t.Fatalf("%d batches readable after TruncatePrefix(LEO)", len(bs))
		}
		l.Close()
	})
}

func TestResetTo(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		cfg := smallConfig(e)
		l := mustOpen(t, cfg)
		appendN(t, l, 50, 1)
		// Hold a region on the old log across the reset.
		r, err := l.Read(0, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if err := l.ResetTo(10_000, 7); err != nil {
			t.Fatal(err)
		}
		if data, err := r.Bytes(); err != nil || len(data) == 0 {
			t.Fatalf("pinned region unreadable after reset: %v", err)
		}
		r.Release()
		if l.LogStartOffset() != 10_000 || l.LogEndOffset() != 10_000 {
			t.Fatalf("start %d end %d", l.LogStartOffset(), l.LogEndOffset())
		}
		if last, term := l.LastOffsetAndTerm(); last != 9_999 || term != 7 {
			t.Fatalf("LastOffsetAndTerm = %d,%d", last, term)
		}
		if _, err := l.Append(mkBatch(t, 1, 1, 0, 'x'), 6); !errors.Is(err, ErrTermRegression) {
			t.Fatalf("append with term below snapshot term: %v", err)
		}
		want := appendN(t, l, 10, 7)
		if want[0].BaseOffset() != 10_000 {
			t.Fatalf("first offset after reset %d", want[0].BaseOffset())
		}
		l.Close()
		l = mustOpen(t, cfg)
		equalBatches(t, checkInvariants(t, l), want)
		if got, _ := l.TermAt(9_999); got != 7 {
			t.Fatalf("TermAt(snapshot) = %d", got)
		}
		// Reset to a lower offset than the current log end works too.
		if err := l.ResetTo(5, 8); err != nil {
			t.Fatal(err)
		}
		appendN(t, l, 2, 8)
		l.Close()
		l = mustOpen(t, cfg)
		if l.LogStartOffset() != 5 || len(checkInvariants(t, l)) != 2 {
			t.Fatalf("after second reset: start %d", l.LogStartOffset())
		}
		l.Close()
	})
}

func TestInterruptedResetCompletesOnOpen(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		cfg := smallConfig(e)
		l := mustOpen(t, cfg)
		appendN(t, l, 30, 1)
		// Simulate a crash right after ResetTo's first step.
		l.mu.Lock()
		l.logStart.Store(20)
		l.startTerm.Store(3)
		l.recoveryPoint = 20
		if err := l.writeCheckpointLocked(false, true); err != nil {
			t.Fatal(err)
		}
		l.mu.Unlock()
		abandon(l)
		l = mustOpen(t, cfg)
		defer l.Close()
		if l.LogStartOffset() != 20 || l.LogEndOffset() != 20 || l.StartTerm() != 3 {
			t.Fatalf("start %d end %d term %d", l.LogStartOffset(), l.LogEndOffset(), l.StartTerm())
		}
		if bs := checkInvariants(t, l); len(bs) != 0 {
			t.Fatalf("%d stale batches survived the reset", len(bs))
		}
	})
}

func TestAppendStamped(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		l := mustOpen(t, smallConfig(e))
		defer l.Close()
		b := mkBatch(t, 3, 10, 0, 'f')
		b.Stamp(0, 4)
		if _, err := l.AppendStamped(b); err != nil {
			t.Fatal(err)
		}
		c := mkBatch(t, 1, 10, 0, 'g')
		c.Stamp(5, 4) // gap
		if _, err := l.AppendStamped(c); !errors.Is(err, ErrOffsetMismatch) {
			t.Fatalf("gap accepted: %v", err)
		}
		c.Stamp(3, 3)
		if _, err := l.AppendStamped(c); !errors.Is(err, ErrTermRegression) {
			t.Fatalf("term regression accepted: %v", err)
		}
		unstamped := mkBatch(t, 1, 1, 0, 'u')
		unstamped.Stamp(3, 0)
		copy(unstamped[offLeaderEpoch:], []byte{0xff, 0xff, 0xff, 0xff})
		if _, err := l.AppendStamped(unstamped); err == nil {
			t.Fatal("unstamped batch accepted")
		}
		c.Stamp(3, 9)
		if _, err := l.AppendStamped(c); err != nil {
			t.Fatal(err)
		}
		bad := bytes.Clone(c)
		bad[len(bad)-1] ^= 1
		Batch(bad).Stamp(4, 9)
		if _, err := l.AppendStamped(bad); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("corrupt batch accepted: %v", err)
		}
		if _, err := l.Append(mkBatch(t, 1, 1, 0, 'x'), math.MaxInt32+1); !errors.Is(err, ErrTermOverflow) {
			t.Fatalf("term overflow accepted: %v", err)
		}
		big := mkBatch(t, 1, DefaultMaxBatchBytes, 0, 'B')
		if _, err := l.Append(big, 9); !errors.Is(err, ErrTooLarge) {
			t.Fatalf("oversized batch accepted: %v", err)
		}
	})
}

func TestSingleBatchLargerThanSegment(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		cfg := smallConfig(e)
		l := mustOpen(t, cfg)
		defer l.Close()
		want := appendN(t, l, 3, 1)
		big := mkBatch(t, 2, 10_000, 0, 'L') // > SegmentBytes (4096)
		if _, err := l.Append(big, 1); err != nil {
			t.Fatal(err)
		}
		want = append(want, bytes.Clone(big))
		want = append(want, appendN(t, l, 3, 1)...)
		equalBatches(t, checkInvariants(t, l), want)
	})
}

func TestRollBySegmentAge(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		cfg := smallConfig(e)
		cfg.SegmentBytes = 1 << 30
		cfg.SegmentAge = time.Hour
		now := time.Unix(1_000_000, 0)
		var mu sync.Mutex
		cfg.Now = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
		advance := func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }
		l := mustOpen(t, cfg)
		defer l.Close()
		appendN(t, l, 5, 1)
		advance(59 * time.Minute)
		appendN(t, l, 1, 1)
		if n := l.Stats().Segments; n != 1 {
			t.Fatalf("rolled too early: %d segments", n)
		}
		advance(2 * time.Minute)
		appendN(t, l, 1, 1)
		if n := l.Stats().Segments; n != 2 {
			t.Fatalf("expected an age roll: %d segments", n)
		}
		// An idle empty segment is never rolled.
		advance(10 * time.Hour)
		if err := l.Roll(); err != nil {
			t.Fatal(err)
		}
		if err := l.Roll(); err != nil {
			t.Fatal(err)
		}
		if n := l.Stats().Segments; n != 3 {
			t.Fatalf("Roll on empty active segment created a segment: %d", n)
		}
	})
}

func TestRollWhenIndexFull(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		cfg := smallConfig(e)
		cfg.SegmentBytes = 1 << 30
		cfg.IndexIntervalBytes = 1
		cfg.MaxIndexBytes = 4 * timeEntrySize // 6 offset entries, 4 time entries
		l := mustOpen(t, cfg)
		defer l.Close()
		want := appendN(t, l, 40, 1)
		if n := l.Stats().Segments; n < 5 {
			t.Fatalf("index-full rolls did not happen: %d segments", n)
		}
		equalBatches(t, checkInvariants(t, l), want)
	})
}

func TestWriteErrorIsNotSticky(t *testing.T) {
	ff := fs.NewFaultFS(3)
	cfg := Config{Dir: "/p", FS: ff, SegmentBytes: 4096, Sync: SyncPolicy{Mode: SyncNever}}
	l := mustOpen(t, cfg)
	defer l.Close()
	want := appendN(t, l, 3, 1)
	ff.FailNextWrites(1)
	if _, err := l.Append(mkBatch(t, 2, 100, 0, 'e'), 1); !errors.Is(err, fs.ErrInjected) {
		t.Fatalf("append err = %v", err)
	}
	want = append(want, appendN(t, l, 3, 1)...)
	equalBatches(t, checkInvariants(t, l), want)
}

func TestRegionPinsSegmentAcrossClose(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		l := mustOpen(t, smallConfig(e))
		want := appendN(t, l, 5, 1)
		r, err := l.Read(0, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		if _, err := r.WriteTo(&buf); err != nil {
			t.Fatal(err)
		}
		bs, _ := SplitBatches(buf.Bytes(), math.MaxInt32)
		equalBatches(t, bs, want[:len(bs)])
		r.Release()
		r.Release() // idempotent
	})
}

func TestConcurrentReadersAndWriter(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		cfg := smallConfig(e)
		cfg.Sync = SyncPolicy{Mode: SyncAlways}
		l := mustOpen(t, cfg)
		defer l.Close()
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		errc := make(chan error, 8)
		for r := 0; r < 4; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				off := int64(0)
				for ctx.Err() == nil {
					hw := l.DurableOffset() + 1
					reg, err := l.ReadUpTo(off, 2000, hw)
					if err != nil {
						if errors.Is(err, ErrOffsetOutOfRange) && off < l.LogStartOffset() {
							off = l.LogStartOffset()
							continue
						}
						errc <- err
						return
					}
					data, err := reg.Bytes()
					reg.Release()
					if err != nil {
						errc <- err
						return
					}
					bs, err := SplitBatches(data, math.MaxInt32)
					if err != nil {
						errc <- err
						return
					}
					if len(bs) > 0 {
						if bs[0].BaseOffset() > off {
							errc <- errors.New("gap in read")
							return
						}
						off = bs[len(bs)-1].LastOffset() + 1
					}
					_, _ = l.TermAt(off - 1)
				}
			}()
		}
		for i := 0; i < 300; i++ {
			b := mkBatch(t, 1+i%3, 100, int64(i), 'c')
			if _, err := l.Append(b, 1); err != nil {
				t.Fatal(err)
			}
			if i%50 == 49 {
				if _, err := l.ApplyRetention(RetentionPolicy{MaxBytes: 8000}, l.DurableOffset()+1); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := l.WaitDurable(context.Background(), l.LogEndOffset()-1); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
		cancel()
		wg.Wait()
		close(errc)
		for err := range errc {
			t.Fatal(err)
		}
	})
}

// TestRecoveryChecksTermOrder: partitionLeaderEpoch is outside the CRC, so a
// corrupted epoch is caught by the log's own invariants (epochs are stamped,
// terms never decrease): above the recovery point it is treated as a torn
// tail, below it as fatal corruption.
func TestRecoveryChecksTermOrder(t *testing.T) {
	forEachEnv(t, func(t *testing.T, e testEnv) {
		cfg := smallConfig(e)
		cfg.SegmentBytes = 1 << 20
		l := mustOpen(t, cfg)
		var want []Batch
		for term := uint64(1); term <= 3; term++ {
			want = append(want, appendN(t, l, 5, term)...)
		}
		victim := want[7]
		info, err := l.BatchInfoAt(victim.BaseOffset())
		if err != nil {
			t.Fatal(err)
		}
		abandon(l)
		modifyFile(t, e.fsys, lastLogFile(t, e), func(f fs.File, size int64) {
			f.WriteAt([]byte{0xff, 0xff, 0xff, 0xff}, info.Position+offLeaderEpoch) // epoch -1
		})
		l2 := mustOpen(t, cfg)
		equalBatches(t, checkInvariants(t, l2), want[:7])
		appendN(t, l2, 3, 3)
		abandon(l2)
		// Now the first 7 batches are below the recovery point: corrupting a
		// term there (making batch 2's term exceed batch 3's) is fatal.
		info, _ = func() (BatchInfo, error) {
			l3 := mustOpen(t, cfg)
			defer abandon(l3)
			return l3.BatchInfoAt(want[2].BaseOffset())
		}()
		modifyFile(t, e.fsys, lastLogFile(t, e), func(f fs.File, size int64) {
			f.WriteAt([]byte{0, 0, 0, 9}, info.Position+offLeaderEpoch)
		})
		if _, err := Open(cfg); !errors.Is(err, ErrCorruptLog) {
			t.Fatalf("term corruption below the recovery point: Open = %v, want ErrCorruptLog", err)
		}
	})
}
