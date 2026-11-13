package raftlog

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"testing"

	"go.uber.org/goleak"

	"github.com/Rahul-Nikhate/quorumlog/internal/fs"
	"github.com/Rahul-Nikhate/quorumlog/internal/log"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// mkEntry builds a raft entry whose Data is a valid batch of span records
// stamped with (first, term).
func mkEntry(t testing.TB, first, span, term uint64, tag byte) raft.Entry {
	t.Helper()
	recs := make([]log.Record, span)
	for i := range recs {
		recs[i] = log.Record{Value: []byte{tag, byte(i)}}
	}
	b, err := log.NewBatch(log.DefaultBatchOptions(), recs...)
	if err != nil {
		t.Fatal(err)
	}
	b.Stamp(int64(first), term)
	return raft.Entry{Term: term, FirstIndex: first, LastIndex: first + span - 1, Data: b}
}

func smallLog() log.Config {
	return log.Config{SegmentBytes: 700, IndexIntervalBytes: 64, MaxIndexBytes: 4096, Sync: log.SyncPolicy{Mode: log.SyncNever}}
}

func mustOpen(t testing.TB, fsys fs.FS, dir string) *Store {
	t.Helper()
	s, err := Open(Config{Dir: dir, FS: fsys, Log: smallLog()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// compare checks every LogStore read of s against the reference store.
func compare(t *testing.T, step string, s *Store, ref *raft.MemoryStore) {
	t.Helper()
	if s.FirstIndex() != ref.FirstIndex() || s.LastIndex() != ref.LastIndex() {
		t.Fatalf("%s: first/last = %d/%d, want %d/%d", step, s.FirstIndex(), s.LastIndex(), ref.FirstIndex(), ref.LastIndex())
	}
	gs, err := s.Snapshot()
	must(t, err)
	ws, _ := ref.Snapshot()
	if gs.Index != ws.Index || gs.Term != ws.Term || !bytes.Equal(gs.Data, ws.Data) || !slices.Equal(gs.Peers, ws.Peers) {
		t.Fatalf("%s: snapshot = %+v, want %+v", step, gs, ws)
	}
	first, last := ref.FirstIndex(), ref.LastIndex()
	lo := first - 1
	if lo > 0 {
		lo-- // one below: ErrCompacted
	}
	for i := lo; i <= last+1; i++ {
		gt, gerr := s.Term(i)
		wt, werr := ref.Term(i)
		if gt != wt || !errors.Is(gerr, werr) && (gerr != nil || werr != nil) {
			t.Fatalf("%s: Term(%d) = (%d, %v), want (%d, %v)", step, i, gt, gerr, wt, werr)
		}
	}
	for lo := first; lo <= last; lo++ {
		for _, mb := range []uint64{0, 100, 1 << 20} {
			ge, gerr := s.Entries(lo, last+1, mb)
			we, werr := ref.Entries(lo, last+1, 1<<20)
			if gerr != nil || werr != nil {
				t.Fatalf("%s: Entries(%d) errors %v / %v", step, lo, gerr, werr)
			}
			if mb == 0 && len(ge) != 1 {
				t.Fatalf("%s: Entries(%d, maxBytes=0) returned %d entries", step, lo, len(ge))
			}
			if len(ge) == 0 || len(ge) > len(we) {
				t.Fatalf("%s: Entries(%d,%d,%d) returned %d entries (ref %d)", step, lo, last+1, mb, len(ge), len(we))
			}
			for k := range ge {
				g, w := ge[k], we[k]
				if g.FirstIndex != w.FirstIndex || g.LastIndex != w.LastIndex || g.Term != w.Term || !bytes.Equal(g.Data, w.Data) {
					t.Fatalf("%s: Entries(%d)[%d] = %v, want %v", step, lo, k, g, w)
				}
			}
		}
	}
	if _, err := s.Entries(first-1, last+1, 0); first > 1 && !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("%s: Entries below first: %v", step, err)
	}
	if _, err := s.Entries(first, last+2, 0); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatalf("%s: Entries beyond last: %v", step, err)
	}
}

// TestStoreMatchesMemoryStore drives the log-backed store and the reference
// MemoryStore with the same random operations (multi-offset batches, term
// changes, suffix truncation at batch boundaries, compaction, snapshot
// install, clean reopen, power-loss crash after Sync) and compares every read.
func TestStoreMatchesMemoryStore(t *testing.T) {
	for seed := uint64(1); seed <= 40; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 7))
			ffs := fs.NewFaultFS(seed)
			dir := "/p"
			s := mustOpen(t, ffs, dir)
			defer func() { _ = s.Close() }()
			ref := raft.NewMemoryStore()
			term := uint64(1)
			for op := 0; op < 120; op++ {
				step := fmt.Sprintf("op %d", op)
				switch r := rng.IntN(100); {
				case r < 55: // append 1..3 batches
					var ents []raft.Entry
					next := ref.LastIndex() + 1
					for range 1 + rng.IntN(3) {
						if rng.IntN(5) == 0 {
							term += uint64(rng.IntN(3))
						}
						e := mkEntry(t, next, 1+rng.Uint64N(4), term, byte(op))
						if rng.IntN(8) == 0 {
							e = raft.Entry{Term: term, FirstIndex: next, LastIndex: next, Data: NoopBatch(next, term)}
						}
						ents = append(ents, e)
						next = e.LastIndex + 1
					}
					must(t, s.Append(ents))
					must(t, ref.Append(ents))
					step += " append"
				case r < 70: // truncate at a random batch boundary
					all := ref.AllEntries()
					if len(all) == 0 {
						continue
					}
					e := all[rng.IntN(len(all))]
					must(t, s.TruncateSuffix(e.FirstIndex))
					must(t, ref.TruncateSuffix(e.FirstIndex))
					term++ // a new leader writes the replacement suffix
					step += fmt.Sprintf(" truncate(%d)", e.FirstIndex)
				case r < 78: // compact at a random batch end
					all := ref.AllEntries()
					if len(all) == 0 {
						continue
					}
					e := all[rng.IntN(len(all))]
					data := []byte(fmt.Sprintf("state@%d", e.LastIndex))
					must(t, s.Compact(e.LastIndex, []raft.NodeID{1, 2, 3}, data))
					must(t, ref.Compact(e.LastIndex, []raft.NodeID{1, 2, 3}, data))
					step += fmt.Sprintf(" compact(%d)", e.LastIndex)
				case r < 83: // install a snapshot beyond the log
					term++
					snap := raft.Snapshot{Index: ref.LastIndex() + 1 + rng.Uint64N(10), Term: term, Peers: []raft.NodeID{1, 2}, Data: []byte("snap")}
					must(t, s.ApplySnapshot(snap))
					must(t, ref.ApplySnapshot(snap))
					step += fmt.Sprintf(" snapshot(%d)", snap.Index)
				case r < 91: // clean reopen
					must(t, s.Close())
					s = mustOpen(t, ffs, dir)
					step += " reopen"
				default: // power loss after a full sync: nothing may be lost
					must(t, s.Sync())
					ffs = ffs.Crash(fs.CrashDropUnsynced)
					_ = s.Close() // fails on the crashed FS; releases resources
					s = mustOpen(t, ffs, dir)
					step += " crash"
				}
				compare(t, step, s, ref)
			}
		})
	}
}

func TestOSDir(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, fs.OS, dir)
	must(t, s.Append([]raft.Entry{mkEntry(t, 1, 3, 1, 1), mkEntry(t, 4, 2, 2, 2), mkEntry(t, 6, 1, 2, 3)}))
	must(t, s.Compact(3, []raft.NodeID{1}, []byte("x")))
	must(t, s.Close())
	s = mustOpen(t, fs.OS, dir)
	defer s.Close()
	snap, err := s.Snapshot()
	must(t, err)
	if snap.Index != 3 || snap.Term != 1 || string(snap.Data) != "x" || s.FirstIndex() != 4 || s.LastIndex() != 6 {
		t.Fatalf("after reopen: snap=%+v first=%d last=%d", snap, s.FirstIndex(), s.LastIndex())
	}
}

// A crash between writing snapshot.meta and moving the log start must be
// completed by the next Open, in each of its forms.
func TestReconcileInterruptedCompaction(t *testing.T) {
	setup := func(t *testing.T) (*fs.FaultFS, *Store) {
		ffs := fs.NewFaultFS(3)
		s := mustOpen(t, ffs, "/p")
		must(t, s.Append([]raft.Entry{mkEntry(t, 1, 3, 1, 1), mkEntry(t, 4, 2, 2, 2), mkEntry(t, 6, 4, 3, 3)}))
		must(t, s.Sync())
		return ffs, s
	}
	crashReopen := func(t *testing.T, ffs *fs.FaultFS, s *Store) *Store {
		ffs = ffs.Crash(fs.CrashDropUnsynced)
		_ = s.Close()
		return mustOpen(t, ffs, "/p")
	}
	t.Run("compact", func(t *testing.T) { // meta matches a batch end in the log → TruncatePrefix
		ffs, s := setup(t)
		must(t, writeSnapMeta(ffs, s.metaPath(), raft.Snapshot{Index: 5, Term: 2, Data: []byte("ps")}))
		s = crashReopen(t, ffs, s)
		defer s.Close()
		if s.FirstIndex() != 6 || s.LastIndex() != 9 {
			t.Fatalf("first/last %d/%d", s.FirstIndex(), s.LastIndex())
		}
		if tm, _ := s.Term(5); tm != 2 {
			t.Fatalf("Term(5)=%d", tm)
		}
		if snap, err := s.Snapshot(); err != nil || snap.Index != 5 || string(snap.Data) != "ps" {
			t.Fatalf("snapshot %+v %v", snap, err)
		}
	})
	t.Run("conflicting-snapshot", func(t *testing.T) { // same index, other term → ResetTo
		ffs, s := setup(t)
		must(t, writeSnapMeta(ffs, s.metaPath(), raft.Snapshot{Index: 5, Term: 7}))
		s = crashReopen(t, ffs, s)
		defer s.Close()
		if s.FirstIndex() != 6 || s.LastIndex() != 5 {
			t.Fatalf("first/last %d/%d", s.FirstIndex(), s.LastIndex())
		}
		if tm, _ := s.Term(5); tm != 7 {
			t.Fatalf("Term(5)=%d", tm)
		}
	})
	t.Run("snapshot-beyond-log", func(t *testing.T) { // → ResetTo
		ffs, s := setup(t)
		must(t, writeSnapMeta(ffs, s.metaPath(), raft.Snapshot{Index: 40, Term: 9}))
		s = crashReopen(t, ffs, s)
		defer s.Close()
		if s.FirstIndex() != 41 || s.LastIndex() != 40 {
			t.Fatalf("first/last %d/%d", s.FirstIndex(), s.LastIndex())
		}
		must(t, s.Append([]raft.Entry{mkEntry(t, 41, 2, 9, 1)}))
	})
	t.Run("mid-batch-meta", func(t *testing.T) { // index inside a batch → ResetTo (never a torn batch)
		ffs, s := setup(t)
		must(t, writeSnapMeta(ffs, s.metaPath(), raft.Snapshot{Index: 7, Term: 3}))
		s = crashReopen(t, ffs, s)
		defer s.Close()
		if s.FirstIndex() != 8 || s.LastIndex() != 7 {
			t.Fatalf("first/last %d/%d", s.FirstIndex(), s.LastIndex())
		}
	})
	t.Run("lost-meta", func(t *testing.T) { // log start ahead of the meta → re-anchor
		ffs, s := setup(t)
		must(t, s.Compact(5, nil, []byte("ps")))
		must(t, ffs.Remove(s.metaPath()))
		must(t, ffs.SyncDir(filepath.Dir(s.metaPath())))
		s = crashReopen(t, ffs, s)
		defer s.Close()
		snap, err := s.Snapshot()
		if err != nil || snap.Index != 5 || snap.Term != 2 || snap.Data != nil {
			t.Fatalf("snapshot %+v %v", snap, err)
		}
	})
}

func TestContractErrors(t *testing.T) {
	s := mustOpen(t, fs.NewFaultFS(1), "/p")
	defer s.Close()
	must(t, s.Append([]raft.Entry{mkEntry(t, 1, 3, 1, 1), mkEntry(t, 4, 1, 1, 2)}))
	bad := []raft.Entry{
		{Term: 1, FirstIndex: 5, LastIndex: 6, Data: nil},             // nil data spanning 2
		{Term: 1, FirstIndex: 5, LastIndex: 5, Data: []byte("short")}, // not a batch
		mkEntry(t, 6, 1, 1, 1),                                        // not contiguous
	}
	e := mkEntry(t, 5, 2, 1, 1)
	e.LastIndex = 5 // header says [5..6]
	bad = append(bad, e)
	e = mkEntry(t, 5, 1, 1, 1)
	e.Term = 2 // header epoch 1
	bad = append(bad, e)
	for i, e := range bad {
		if err := s.Append([]raft.Entry{e}); err == nil {
			t.Errorf("bad entry %d accepted", i)
		}
	}
	if err := s.TruncateSuffix(2); !errors.Is(err, ErrContract) {
		t.Errorf("mid-batch truncate: %v", err)
	}
	if err := s.Compact(2, nil, nil); !errors.Is(err, ErrContract) {
		t.Errorf("mid-batch compact: %v", err)
	}
	if err := s.Append([]raft.Entry{{Term: 1, FirstIndex: 5, LastIndex: 5}}); err != nil {
		t.Fatalf("nil-data no-op: %v", err)
	}
	ents, err := s.Entries(5, 6, 0)
	must(t, err)
	if ct, err := log.ControlType(log.Batch(ents[0].Data)); err != nil || ct != log.ControlLeaderChange {
		t.Fatalf("no-op not a control batch: %v %v", ct, err)
	}
}

func TestSnapMetaCodec(t *testing.T) {
	m := raft.Snapshot{Index: 9, Term: 4, Peers: []raft.NodeID{1, 2, 3}, Data: []byte("hello")}
	b := EncodeSnapMeta(m)
	got, err := DecodeSnapMeta(b)
	if err != nil || got.Index != 9 || got.Term != 4 || !slices.Equal(got.Peers, m.Peers) || string(got.Data) != "hello" {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	for i := range b {
		c := slices.Clone(b)
		c[i] ^= 0x10
		if _, err := DecodeSnapMeta(c); !errors.Is(err, ErrCorruptSnapshotMeta) {
			t.Fatalf("bit flip at %d not detected: %v", i, err)
		}
	}
	for n := range len(b) {
		if _, err := DecodeSnapMeta(b[:n]); err == nil {
			t.Fatalf("truncated meta (%d bytes) accepted", n)
		}
	}
}

func TestHardStateOnFaultFS(t *testing.T) {
	ffs := fs.NewFaultFS(5)
	must(t, ffs.MkdirAll("/p/raft", 0o755))
	hs := NewHardStateStore(ffs, "/p")
	if got, err := hs.Load(); err != nil || !got.IsEmpty() {
		t.Fatalf("fresh load: %+v %v", got, err)
	}
	must(t, hs.Save(raft.HardState{Term: 3, Vote: 2, Commit: 10}))
	ffs = ffs.Crash(fs.CrashDropUnsynced)
	hs = NewHardStateStore(ffs, "/p")
	if got, err := hs.Load(); err != nil || got != (raft.HardState{Term: 3, Vote: 2, Commit: 10}) {
		t.Fatalf("after crash: %+v %v", got, err)
	}
	ffs.FailNextSyncs(1)
	if err := hs.Save(raft.HardState{Term: 4}); err == nil {
		t.Fatal("Save with failing fsync succeeded")
	}
}
