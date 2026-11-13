package raft

import (
	"errors"
	"math/rand/v2"
	"testing"
)

// Property test: MemoryStore with random multi-offset batches agrees with a
// one-offset-per-entry reference model (spec §13.1: property-test the
// LogStore against a 1-offset reference), across appends, truncations and
// compactions.
func TestMemoryStoreAgainstReference(t *testing.T) {
	for seed := uint64(1); seed <= 200; seed++ {
		rng := rand.New(rand.NewPCG(seed, 1))
		st := NewMemoryStore()
		var ref []uint64 // ref[o] = term of offset o (ref[0] unused)
		ref = append(ref, 0)
		first := uint64(1)
		var bounds []uint64 // batch first indices still present
		term := uint64(1)
		for op := 0; op < 200; op++ {
			switch r := rng.IntN(10); {
			case r < 6:
				span := 1 + rng.Uint64N(5)
				if rng.IntN(4) == 0 {
					term++
				}
				f := uint64(len(ref))
				must(t, st.Append([]Entry{{Term: term, FirstIndex: f, LastIndex: f + span - 1}}))
				for range span {
					ref = append(ref, term)
				}
				bounds = append(bounds, f)
			case r < 8 && len(bounds) > 1:
				k := 1 + rng.IntN(len(bounds)-1)
				from := bounds[k]
				must(t, st.TruncateSuffix(from))
				ref = ref[:from]
				bounds = bounds[:k]
				term = ref[len(ref)-1]
			case r < 9 && len(bounds) > 1:
				k := rng.IntN(len(bounds) - 1)
				idx := bounds[k+1] - 1
				must(t, st.Compact(idx, nil, nil))
				first = idx + 1
				bounds = bounds[k+1:]
			}
			last := uint64(len(ref) - 1)
			if st.LastIndex() != last || st.FirstIndex() != first {
				t.Fatalf("seed %d: first/last %d/%d want %d/%d", seed, st.FirstIndex(), st.LastIndex(), first, last)
			}
			for o := first; o <= last; o++ {
				if got, err := st.Term(o); err != nil || got != ref[o] {
					t.Fatalf("seed %d: Term(%d)=%d,%v want %d", seed, o, got, err, ref[o])
				}
			}
			if first > 1 {
				if _, err := st.Term(first - 2); !errors.Is(err, ErrCompacted) {
					t.Fatalf("seed %d: Term below snapshot: %v", seed, err)
				}
			}
			// Entries covers exactly the offsets requested, batch-aligned.
			if last >= first {
				lo := first + rng.Uint64N(last-first+1)
				ents, err := st.Entries(lo, last+1, 1<<30)
				if err != nil {
					t.Fatal(err)
				}
				if ents[0].FirstIndex > lo || ents[0].LastIndex < lo || ents[len(ents)-1].LastIndex != last {
					t.Fatalf("seed %d: Entries(%d) = %v..%v", seed, lo, ents[0], ents[len(ents)-1])
				}
				for i := 1; i < len(ents); i++ {
					if ents[i].FirstIndex != ents[i-1].LastIndex+1 {
						t.Fatalf("seed %d: non-contiguous Entries", seed)
					}
				}
			}
		}
	}
}

func TestMemoryStoreErrors(t *testing.T) {
	st := gappedStore(t)
	if err := st.Append([]Entry{{Term: 3, FirstIndex: 12, LastIndex: 12}}); err == nil {
		t.Error("gap accepted")
	}
	if err := st.Append([]Entry{{Term: 1, FirstIndex: 11, LastIndex: 11}}); err == nil {
		t.Error("term regression accepted")
	}
	if err := st.TruncateSuffix(6); err == nil {
		t.Error("mid-batch truncation accepted")
	}
	if err := st.Compact(6, nil, nil); err == nil {
		t.Error("mid-batch compaction accepted")
	}
	if _, err := st.Entries(1, 12, 0); !errors.Is(err, ErrUnavailable) {
		t.Errorf("Entries beyond last: %v", err)
	}
	must(t, st.ApplySnapshot(Snapshot{Index: 20, Term: 4}))
	if st.FirstIndex() != 21 || st.LastIndex() != 20 {
		t.Errorf("after ApplySnapshot first=%d last=%d", st.FirstIndex(), st.LastIndex())
	}
	if tm, _ := st.Term(20); tm != 4 {
		t.Errorf("snapshot term %d", tm)
	}
	if err := st.ApplySnapshot(Snapshot{Index: 5}); err == nil {
		t.Error("older snapshot accepted")
	}
}
