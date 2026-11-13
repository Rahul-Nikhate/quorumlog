package raft

import (
	"errors"
	"testing"
)

// Gapped (batch) index space: entries [1], [2..4], [5..9](t2), [10..10](t3).
func gappedStore(t *testing.T) *MemoryStore {
	st := NewMemoryStore()
	must(t, st.Append([]Entry{
		{Term: 1, FirstIndex: 1, LastIndex: 1},
		{Term: 1, FirstIndex: 2, LastIndex: 4},
		{Term: 2, FirstIndex: 5, LastIndex: 9},
		{Term: 3, FirstIndex: 10, LastIndex: 10},
	}))
	return st
}

func TestRaftLogTermAndBatchLookups(t *testing.T) {
	l := newRaftLog(gappedStore(t))
	l.appendLeader(Entry{Term: 4, FirstIndex: 11, LastIndex: 13}, Entry{Term: 4, FirstIndex: 14, LastIndex: 14})
	for i, want := range map[uint64]uint64{0: 0, 1: 1, 3: 1, 4: 1, 5: 2, 9: 2, 10: 3, 11: 4, 12: 4, 14: 4} {
		if got, err := l.term(i); err != nil || got != want {
			t.Errorf("term(%d)=%d,%v want %d", i, got, err, want)
		}
	}
	if _, err := l.term(15); !errors.Is(err, ErrUnavailable) {
		t.Errorf("term(15) err=%v", err)
	}
	for i, want := range map[uint64]uint64{1: 1, 3: 2, 4: 2, 7: 5, 10: 10, 12: 11, 14: 14} {
		if got, err := l.batchStart(i); err != nil || got != want {
			t.Errorf("batchStart(%d)=%d,%v want %d", i, got, err, want)
		}
	}
	for term, want := range map[uint64]uint64{1: 4, 2: 9, 3: 10, 4: 14} {
		if got, ok := l.lastIndexOfTerm(term); !ok || got != want {
			t.Errorf("lastIndexOfTerm(%d)=%d,%v want %d", term, got, ok, want)
		}
	}
	if _, ok := l.lastIndexOfTerm(5); ok {
		t.Error("lastIndexOfTerm(5) found")
	}
	// matchTerm at batch ends and (by the same-term argument) mid-batch.
	if !l.matchTerm(9, 2) || l.matchTerm(9, 3) || !l.matchTerm(0, 0) {
		t.Error("matchTerm")
	}
}

func TestRaftLogConflictHint(t *testing.T) {
	l := newRaftLog(gappedStore(t))
	cases := []struct{ prev, ci, ct uint64 }{
		{20, 11, 0}, // too short
		{9, 5, 2},   // first offset of term 2
		{7, 5, 2},   // mid-batch prev
		{10, 10, 3},
		{4, 1, 1},
	}
	for _, c := range cases {
		ci, ct := l.conflictHint(c.prev)
		if ci != c.ci || ct != c.ct {
			t.Errorf("conflictHint(%d)=(%d,t%d) want (%d,t%d)", c.prev, ci, ct, c.ci, c.ct)
		}
	}
	l.committed = 4
	if ci, ct := l.conflictHint(4); ci != 4 || ct != 1 {
		t.Errorf("hint below commit: (%d,%d)", ci, ct)
	}
	if ci, _ := l.conflictHint(9); ci != 5 {
		t.Errorf("hint clamped to commit+1: %d", ci)
	}
}

func TestRaftLogTruncateAndAppend(t *testing.T) {
	st := gappedStore(t)
	l := newRaftLog(st)
	l.committed = 4
	// Conflict inside the stable log → TruncateFrom.
	l.truncateAndAppend([]Entry{{Term: 5, FirstIndex: 5, LastIndex: 6}})
	if l.truncateFrom != 5 || l.lastIndex() != 6 || l.stableLast() != 4 {
		t.Fatalf("truncateFrom=%d last=%d stableLast=%d", l.truncateFrom, l.lastIndex(), l.stableLast())
	}
	if tm, _ := l.term(5); tm != 5 {
		t.Fatalf("term(5)=%d after truncation", tm)
	}
	// Further conflict inside the unstable tail.
	l.truncateAndAppend([]Entry{{Term: 5, FirstIndex: 5, LastIndex: 6}, {Term: 6, FirstIndex: 7, LastIndex: 7}})
	l.truncateAndAppend([]Entry{{Term: 7, FirstIndex: 7, LastIndex: 8}})
	if len(l.ents) != 2 || l.lastIndex() != 8 || l.truncateFrom != 5 {
		t.Fatalf("ents=%v truncateFrom=%d", l.ents, l.truncateFrom)
	}
	ents, err := l.entries(1, 9, 1<<20)
	if err != nil || len(ents) != 4 || ents[2].Term != 5 || ents[3].Term != 7 {
		t.Fatalf("entries=%v err=%v", ents, err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("truncating a committed entry did not panic")
		}
	}()
	l.truncateAndAppend([]Entry{{Term: 9, FirstIndex: 2, LastIndex: 2}})
}

func TestRaftLogEntriesSizeLimit(t *testing.T) {
	st := NewMemoryStore()
	must(t, st.Append([]Entry{{Term: 1, FirstIndex: 1, LastIndex: 1, Data: make([]byte, 100)}, {Term: 1, FirstIndex: 2, LastIndex: 2, Data: make([]byte, 100)}}))
	l := newRaftLog(st)
	l.appendLeader(Entry{Term: 1, FirstIndex: 3, LastIndex: 3, Data: make([]byte, 100)})
	if e, _ := l.entries(1, 4, 0); len(e) != 1 {
		t.Fatalf("maxBytes=0 returned %d entries", len(e))
	}
	if e, _ := l.entries(1, 4, 250); len(e) != 2 {
		t.Fatalf("maxBytes=250 returned %d entries", len(e))
	}
	if e, _ := l.entries(1, 4, 1000); len(e) != 3 {
		t.Fatalf("unlimited returned %d entries", len(e))
	}
	if e, _ := l.entries(2, 4, 1000); len(e) != 2 || e[0].FirstIndex != 2 {
		t.Fatalf("from 2: %v", e)
	}
	must(t, st.Compact(1, nil, nil))
	if _, err := l.entries(1, 4, 1000); !errors.Is(err, ErrCompacted) {
		t.Fatalf("compacted read err=%v", err)
	}
}
