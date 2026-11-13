package raft

import (
	"errors"
	"fmt"
	"slices"
	"sync"
)

// NodeID identifies a replica (broker) inside a Raft group. 0 means "none".
type NodeID uint32

// None is the zero NodeID, meaning "no node" (no leader, no vote).
const None NodeID = 0

// Entry is one Raft log entry. In QuorumLog one entry is one Kafka RecordBatch
// covering the contiguous offset range [FirstIndex, LastIndex]; the entry's Raft
// index is its LastIndex (spec §5.4.2). Raft indices are therefore not dense:
// the entry after [100..163] is [164..170] and its predecessor's index is 163.
//
// Invariants for every log (stable or in flight):
//   - FirstIndex <= LastIndex
//   - next.FirstIndex == prev.LastIndex + 1 (offsets are contiguous)
//   - terms are non-decreasing along the log
type Entry struct {
	Term       uint64 // Kafka partitionLeaderEpoch
	FirstIndex uint64 // Kafka baseOffset
	LastIndex  uint64 // Kafka lastOffset == this entry's Raft index
	Data       []byte // opaque payload (the encoded, stamped RecordBatch); nil for a no-op
}

// Span returns the number of offsets the entry covers.
func (e Entry) Span() uint64 { return e.LastIndex - e.FirstIndex + 1 }

func (e Entry) String() string {
	if e.FirstIndex == e.LastIndex {
		return fmt.Sprintf("(%d,t%d)", e.LastIndex, e.Term)
	}
	return fmt.Sprintf("([%d..%d],t%d)", e.FirstIndex, e.LastIndex, e.Term)
}

// size is the accounting size of an entry for MaxSizePerMsg/inflight bytes.
func (e Entry) size() uint64 { return uint64(len(e.Data)) + 24 }

// Snapshot is the metadata (plus optional opaque state) that replaces a log
// prefix. For data partitions the log *is* the state, so Data is typically
// small (producer state reference, §5.12); for a KV-style state machine it is
// the serialized state.
type Snapshot struct {
	Index uint64   // last included Raft index (a batch LastIndex)
	Term  uint64   // term of the entry at Index
	Peers []NodeID // voter set at Index (static membership: informational)
	Data  []byte   // opaque application state
}

// IsEmpty reports whether s is the zero snapshot.
func (s Snapshot) IsEmpty() bool { return s.Index == 0 }

// Storage errors.
var (
	// ErrCompacted is returned when the requested index precedes the
	// first available index (it was compacted or deleted by retention).
	ErrCompacted = errors.New("raft: requested index is unavailable due to compaction")
	// ErrUnavailable is returned when the requested index is beyond the last index.
	ErrUnavailable = errors.New("raft: requested entry at index is unavailable")
	// ErrSnapshotTemporarilyUnavailable may be returned by LogStore.Snapshot when
	// a snapshot cannot be produced right now; the leader retries later.
	ErrSnapshotTemporarilyUnavailable = errors.New("raft: snapshot is temporarily unavailable")
)

// LogStore is the stable Raft log. internal/log implements it on top of the
// segmented partition log (one entry = one RecordBatch, index = lastOffset);
// MemoryStore implements it for tests.
//
// Concurrency: the Node only calls the read methods (FirstIndex, LastIndex,
// Term, Entries, Snapshot), and only from the driver goroutine. The driver calls
// the write methods (Append, TruncateSuffix, ApplySnapshot) between Ready() and
// Advance(). Retention may move FirstIndex forward concurrently; the Node
// tolerates ErrCompacted from any read at any time.
//
// Index semantics (spec §5.4.2):
//   - The log "contains" (i, t) iff i == 0, or i == the snapshot index and t
//     the snapshot term, or there is an entry with LastIndex == i and Term == t.
//   - Term(i) is defined for ANY offset i in [FirstIndex()-1, LastIndex()]: it
//     is the term of the batch containing offset i, and for i == FirstIndex()-1
//     it is the snapshot term (0 if there is no snapshot and i == 0). The Node
//     binary-searches offsets with Term, so mid-batch lookups must work.
type LogStore interface {
	// FirstIndex returns the first offset still present (snapshot.Index+1, or 1
	// for a fresh log).
	FirstIndex() uint64
	// LastIndex returns the last offset in the log (Kafka LEO-1), or the
	// snapshot index when the log holds no entries.
	LastIndex() uint64
	// Term returns the term of the batch containing offset i. It returns
	// ErrCompacted if i < FirstIndex()-1 and ErrUnavailable if i > LastIndex().
	Term(i uint64) (uint64, error)
	// Entries returns the batches overlapping offsets [lo, hi), in order. It
	// returns at least one entry if any overlaps, then stops before the total
	// size (len(Data)+24 per entry; an adapter may use the on-disk batch size)
	// would exceed maxBytes, so maxBytes == 0 returns exactly one entry.
	// ErrCompacted if lo < FirstIndex(), ErrUnavailable if hi > LastIndex()+1.
	Entries(lo, hi, maxBytes uint64) ([]Entry, error)
	// Snapshot returns the most recent snapshot (zero Snapshot if none).
	Snapshot() (Snapshot, error)

	// Append appends entries; entries[0].FirstIndex must equal LastIndex()+1.
	Append(entries []Entry) error
	// TruncateSuffix drops every batch with FirstIndex >= from. from must be a
	// batch boundary (the FirstIndex of some batch, or LastIndex()+1).
	TruncateSuffix(from uint64) error
	// ApplySnapshot discards the whole log and resets it to start right after
	// snap.Index (FirstIndex() == snap.Index+1, LastIndex() == snap.Index).
	ApplySnapshot(snap Snapshot) error
}

// MemoryStore is an in-memory LogStore for tests. It supports multi-offset
// entries and compaction. It is safe for concurrent use.
type MemoryStore struct {
	mu   sync.Mutex
	snap Snapshot
	ents []Entry // ents[i].FirstIndex == (i==0 ? snap.Index+1 : ents[i-1].LastIndex+1)
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

var _ LogStore = (*MemoryStore)(nil)

// FirstIndex implements LogStore.
func (s *MemoryStore) FirstIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snap.Index + 1
}

// LastIndex implements LogStore.
func (s *MemoryStore) LastIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastIndexLocked()
}

func (s *MemoryStore) lastIndexLocked() uint64 {
	if len(s.ents) == 0 {
		return s.snap.Index
	}
	return s.ents[len(s.ents)-1].LastIndex
}

// find returns the position of the batch containing offset i (i must be in
// [FirstIndex, LastIndex]).
func (s *MemoryStore) find(i uint64) int {
	pos, _ := slices.BinarySearchFunc(s.ents, i, func(e Entry, t uint64) int {
		switch {
		case e.LastIndex < t:
			return -1
		case e.FirstIndex > t:
			return 1
		}
		return 0
	})
	return pos
}

// Term implements LogStore.
func (s *MemoryStore) Term(i uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case i < s.snap.Index:
		return 0, ErrCompacted
	case i == s.snap.Index:
		return s.snap.Term, nil
	case i > s.lastIndexLocked():
		return 0, ErrUnavailable
	}
	return s.ents[s.find(i)].Term, nil
}

// Entries implements LogStore.
func (s *MemoryStore) Entries(lo, hi, maxBytes uint64) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if lo <= s.snap.Index {
		return nil, ErrCompacted
	}
	if hi > s.lastIndexLocked()+1 {
		return nil, ErrUnavailable
	}
	if lo >= hi {
		return nil, nil
	}
	var out []Entry
	var size uint64
	for p := s.find(lo); p < len(s.ents) && s.ents[p].FirstIndex < hi; p++ {
		e := s.ents[p]
		size += e.size()
		if len(out) > 0 && size > maxBytes {
			break
		}
		out = append(out, e)
	}
	return out, nil
}

// Snapshot implements LogStore.
func (s *MemoryStore) Snapshot() (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snap, nil
}

// Append implements LogStore.
func (s *MemoryStore) Append(entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.lastIndexLocked() + 1
	for _, e := range entries {
		if e.FirstIndex != next || e.LastIndex < e.FirstIndex {
			return fmt.Errorf("raft: MemoryStore.Append: entry %v not contiguous with next offset %d", e, next)
		}
		if n := len(s.ents); n > 0 && s.ents[n-1].Term > e.Term {
			return fmt.Errorf("raft: MemoryStore.Append: entry %v term regresses from %d", e, s.ents[n-1].Term)
		}
		s.ents = append(s.ents, e)
		next = e.LastIndex + 1
	}
	return nil
}

// TruncateSuffix implements LogStore.
func (s *MemoryStore) TruncateSuffix(from uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if from <= s.snap.Index {
		return fmt.Errorf("raft: MemoryStore.TruncateSuffix(%d): at or below snapshot %d", from, s.snap.Index)
	}
	if from > s.lastIndexLocked() {
		if from == s.lastIndexLocked()+1 {
			return nil
		}
		return fmt.Errorf("raft: MemoryStore.TruncateSuffix(%d): beyond last index %d", from, s.lastIndexLocked())
	}
	p := s.find(from)
	if s.ents[p].FirstIndex != from {
		return fmt.Errorf("raft: MemoryStore.TruncateSuffix(%d): not a batch boundary (batch %v)", from, s.ents[p])
	}
	s.ents = slices.Clip(s.ents[:p])
	return nil
}

// ApplySnapshot implements LogStore.
func (s *MemoryStore) ApplySnapshot(snap Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if snap.Index < s.snap.Index {
		return fmt.Errorf("raft: MemoryStore.ApplySnapshot: snapshot %d older than current %d", snap.Index, s.snap.Index)
	}
	s.snap = cloneSnapshot(snap)
	s.ents = nil
	return nil
}

// Compact discards all entries up to and including index (which must be a
// batch LastIndex in the log) and records a snapshot at index with the given
// peers and data. This is the log compaction hook: the application calls it
// with an index <= its applied index (for data partitions: retention deleting
// closed segments below the HW).
func (s *MemoryStore) Compact(index uint64, peers []NodeID, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index <= s.snap.Index {
		return ErrCompacted
	}
	if index > s.lastIndexLocked() {
		return ErrUnavailable
	}
	p := s.find(index)
	if s.ents[p].LastIndex != index {
		return fmt.Errorf("raft: MemoryStore.Compact(%d): not a batch end (batch %v)", index, s.ents[p])
	}
	s.snap = Snapshot{Index: index, Term: s.ents[p].Term, Peers: slices.Clone(peers), Data: slices.Clone(data)}
	s.ents = slices.Clone(s.ents[p+1:])
	return nil
}

// Clone returns a deep-enough copy of the store (entries are immutable values
// whose Data is never mutated, so Data slices are shared).
func (s *MemoryStore) Clone() *MemoryStore {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &MemoryStore{snap: cloneSnapshot(s.snap), ents: slices.Clone(s.ents)}
}

// AllEntries returns a copy of all entries currently in the store.
func (s *MemoryStore) AllEntries() []Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.ents)
}

func cloneSnapshot(s Snapshot) Snapshot {
	s.Peers = slices.Clone(s.Peers)
	s.Data = slices.Clone(s.Data)
	return s
}
