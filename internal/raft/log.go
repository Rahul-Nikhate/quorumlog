package raft

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
)

// raftLog is the Node's view of the Raft log: the stable LogStore plus an
// unstable tail that has not yet been handed to the driver for persistence,
// plus the commit and apply indices.
//
// Unstable state (everything here is emitted in the next Ready and cleared by
// Advance):
//   - snap: a snapshot received via InstallSnapshot. While set, the stable log
//     is logically empty and ends at snap.Index.
//   - truncateFrom: the driver must drop stable batches with FirstIndex >=
//     truncateFrom. While set, the stable log logically ends at truncateFrom-1.
//   - ents: entries to append; ents[0].FirstIndex == stableLast()+1.
type raftLog struct {
	store LogStore

	snap         *Snapshot
	truncateFrom uint64
	ents         []Entry

	committed uint64 // highest index known committed (a batch LastIndex)
	applied   uint64 // highest index handed to the application (Ready.CommittedEntries)
}

func newRaftLog(store LogStore) *raftLog {
	return &raftLog{store: store}
}

// stableLast is the last index of the stable log as it will look once the
// pending snapshot/truncation has been applied by the driver.
func (l *raftLog) stableLast() uint64 {
	switch {
	case l.snap != nil:
		return l.snap.Index
	case l.truncateFrom != 0:
		return l.truncateFrom - 1
	}
	return l.store.LastIndex()
}

func (l *raftLog) firstIndex() uint64 {
	if l.snap != nil {
		return l.snap.Index + 1
	}
	return l.store.FirstIndex()
}

func (l *raftLog) lastIndex() uint64 {
	if n := len(l.ents); n > 0 {
		return l.ents[n-1].LastIndex
	}
	return l.stableLast()
}

func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		panic(fmt.Sprintf("raft: term of last index %d: %v", l.lastIndex(), err))
	}
	return t
}

// findUnstable returns the position in l.ents of the entry containing offset i.
func (l *raftLog) findUnstable(i uint64) int {
	return sort.Search(len(l.ents), func(k int) bool { return l.ents[k].LastIndex >= i })
}

// term returns the term of the batch containing offset i.
func (l *raftLog) term(i uint64) (uint64, error) {
	if i == 0 {
		return 0, nil
	}
	if n := len(l.ents); n > 0 && i >= l.ents[0].FirstIndex {
		if i > l.ents[n-1].LastIndex {
			return 0, ErrUnavailable
		}
		return l.ents[l.findUnstable(i)].Term, nil
	}
	if i > l.stableLast() {
		return 0, ErrUnavailable
	}
	if l.snap != nil {
		if i == l.snap.Index {
			return l.snap.Term, nil
		}
		return 0, ErrCompacted
	}
	return l.store.Term(i)
}

// matchTerm reports whether the log contains (i, t) (spec §5.4.2 rule 1).
// With batch entries, Term(i)==t for a batch-end index i on the leader implies
// the follower's batch containing i is the very same batch (only one leader
// creates term-t batches), so containment by term is sufficient.
func (l *raftLog) matchTerm(i, t uint64) bool {
	tt, err := l.term(i)
	return err == nil && tt == t
}

// isUpToDate implements the election restriction (Raft §5.4.1).
func (l *raftLog) isUpToDate(lastIndex, lastTerm uint64) bool {
	myTerm := l.lastTerm()
	return lastTerm > myTerm || (lastTerm == myTerm && lastIndex >= l.lastIndex())
}

// entries returns the batches covering offsets [lo, hi), size-limited like
// LogStore.Entries (at least one entry is returned if any is available).
func (l *raftLog) entries(lo, hi, maxBytes uint64) ([]Entry, error) {
	if lo >= hi {
		return nil, nil
	}
	if lo < l.firstIndex() {
		return nil, ErrCompacted
	}
	if hi > l.lastIndex()+1 {
		return nil, ErrUnavailable
	}
	var out []Entry
	var size uint64
	sl := l.stableLast()
	if lo <= sl {
		stableHi := min(hi, sl+1)
		ents, err := l.store.Entries(lo, stableHi, maxBytes)
		if err != nil {
			return nil, err
		}
		if len(ents) == 0 {
			return nil, fmt.Errorf("raft: LogStore.Entries(%d,%d) returned no entries", lo, stableHi)
		}
		out = slices.Clip(ents) // never append into the store's backing array
		for _, e := range ents {
			size += e.size()
		}
		if out[len(out)-1].LastIndex < stableHi-1 {
			return out, nil // size limit reached inside the stable part
		}
		lo = sl + 1
	}
	if lo < hi && len(l.ents) > 0 {
		for p := l.findUnstable(lo); p < len(l.ents) && l.ents[p].FirstIndex < hi; p++ {
			e := l.ents[p]
			size += e.size()
			if len(out) > 0 && size > maxBytes {
				break
			}
			out = append(out, e)
		}
	}
	return out, nil
}

// batchStart returns the FirstIndex of the batch containing offset i, which
// must be in [firstIndex, lastIndex].
func (l *raftLog) batchStart(i uint64) (uint64, error) {
	if n := len(l.ents); n > 0 && i >= l.ents[0].FirstIndex {
		if i > l.ents[n-1].LastIndex {
			return 0, ErrUnavailable
		}
		return l.ents[l.findUnstable(i)].FirstIndex, nil
	}
	ents, err := l.entries(i, i+1, 0)
	if err != nil {
		return 0, err
	}
	return ents[0].FirstIndex, nil
}

// searchOffsets returns the smallest offset o in [lo, hi] for which pred(term(o))
// holds, or hi+1 if none. pred must be monotone over the log's (non-decreasing)
// terms. Term lookups that fail are treated as "pred false". O(log(hi-lo)) Term
// calls: this is how the gapped (batch) index space is navigated without
// walking back batch by batch.
func (l *raftLog) searchOffsets(lo, hi uint64, pred func(t uint64) bool) uint64 {
	if hi == math.MaxUint64 {
		hi-- // offsets never reach MaxUint64; keeps hi+1 from overflowing
	}
	end := hi + 1
	for lo < end {
		mid := lo + (end-lo)/2
		if t, err := l.term(mid); err == nil && pred(t) {
			end = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

// lastIndexOfTerm returns the last index (a batch LastIndex) of term t in the
// log, if the log has any entry (or snapshot) with term t.
func (l *raftLog) lastIndexOfTerm(t uint64) (uint64, bool) {
	lo := l.firstIndex() - 1
	hi := l.lastIndex()
	// First offset whose term is > t; the offset before it ends the last batch
	// with term <= t.
	o := l.searchOffsets(lo, hi, func(tt uint64) bool { return tt > t })
	if o == lo {
		return 0, false
	}
	tt, err := l.term(o - 1)
	if err != nil || tt != t {
		return 0, false
	}
	return o - 1, true
}

// conflictHint computes the fast-backup hint for a rejected MsgApp whose
// prevLogIndex is prev: (ConflictIndex, ConflictTerm). ConflictIndex is the
// first offset of ConflictTerm in this log (never below committed+1), or
// lastIndex+1 with ConflictTerm 0 when the log is too short.
func (l *raftLog) conflictHint(prev uint64) (uint64, uint64) {
	last := l.lastIndex()
	if prev > last {
		return last + 1, 0
	}
	ct, err := l.term(prev)
	if err != nil {
		return l.committed + 1, 0
	}
	lo := max(l.committed+1, l.firstIndex())
	if lo > prev {
		return prev, ct
	}
	ci := l.searchOffsets(lo, prev, func(tt uint64) bool { return tt >= ct })
	return ci, ct
}

// appendLeader appends new entries at the tail (leader path).
func (l *raftLog) appendLeader(ents ...Entry) {
	l.ents = append(l.ents, ents...)
}

// truncateAndAppend appends ents, first discarding every entry with
// FirstIndex >= ents[0].FirstIndex (follower conflict path). ents[0].FirstIndex
// must be a batch boundary of this log and greater than committed.
func (l *raftLog) truncateAndAppend(ents []Entry) {
	first := ents[0].FirstIndex
	if first <= l.committed {
		panic(fmt.Sprintf("raft: truncating committed entry at %d (committed=%d)", first, l.committed))
	}
	switch {
	case first == l.lastIndex()+1:
		l.ents = append(l.ents, ents...)
	case len(l.ents) > 0 && first >= l.ents[0].FirstIndex:
		p := l.findUnstable(first)
		if p >= len(l.ents) || l.ents[p].FirstIndex != first {
			panic(fmt.Sprintf("raft: truncation point %d is not a batch boundary in the unstable log", first))
		}
		l.ents = append(slices.Clip(l.ents[:p]), ents...)
	default:
		if l.snap != nil {
			panic(fmt.Sprintf("raft: truncation at %d below pending snapshot %d", first, l.snap.Index))
		}
		// The conflict is in the stable log: the driver must truncate it.
		l.truncateFrom = first
		l.ents = slices.Clone(ents)
	}
}

func (l *raftLog) commitTo(i uint64) {
	if i <= l.committed {
		return
	}
	if i > l.lastIndex() {
		panic(fmt.Sprintf("raft: commit %d beyond last index %d", i, l.lastIndex()))
	}
	l.committed = i
}

// restore replaces the log with a snapshot (follower InstallSnapshot path).
func (l *raftLog) restore(s Snapshot) {
	s = cloneSnapshot(s)
	l.snap = &s
	l.truncateFrom = 0
	l.ents = nil
	l.committed = s.Index
}

// applyFrom is the first offset that has not been handed to the application.
func (l *raftLog) applyFrom() uint64 {
	a := l.applied
	if l.snap != nil {
		a = max(a, l.snap.Index)
	}
	return a + 1
}

func (l *raftLog) hasNextCommittedEnts() bool {
	return l.committed >= l.applyFrom()
}

func (l *raftLog) nextCommittedEnts(maxBytes uint64) []Entry {
	lo := l.applyFrom()
	if l.committed < lo {
		return nil
	}
	ents, err := l.entries(lo, l.committed+1, maxBytes)
	if err != nil {
		if errors.Is(err, ErrCompacted) {
			panic(fmt.Sprintf("raft: committed entries [%d,%d] were compacted before being applied (compact only up to the applied index)", lo, l.committed))
		}
		panic(fmt.Sprintf("raft: reading committed entries [%d,%d]: %v", lo, l.committed, err))
	}
	return ents
}
