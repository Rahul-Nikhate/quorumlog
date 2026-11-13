// Package raftlog makes the segmented partition log (internal/log) the Raft
// log (raft.LogStore), following spec §5.4.2 "the Raft log IS the partition
// log":
//
//   - one Raft entry = one RecordBatch; Entry.FirstIndex = baseOffset,
//     Entry.LastIndex = lastOffset (the entry's Raft index), Entry.Term =
//     partitionLeaderEpoch;
//   - the log is opened with InitialOffset 1, so Kafka offset == Raft index and
//     index 0 stays the "empty log" sentinel;
//   - an election no-op is a Kafka control batch (ControlLeaderChange) that
//     occupies one offset on disk (NoopData builds it; Append also materialises
//     a nil-Data entry defensively);
//   - snapshots are metadata only (§5.12): Index = LogStartOffset-1, Term =
//     the term there, plus peers and opaque application data (the producer
//     state) kept in <dir>/raft/snapshot.meta.
//
// Crash safety of prefix truncation: Compact and ApplySnapshot write
// snapshot.meta (atomic replace + dir fsync) BEFORE moving the log start
// (TruncatePrefix / ResetTo, each crash-safe on its own). Open reconciles: if
// the meta is ahead of the log start, the interrupted truncation is redone
// (TruncatePrefix when the meta's (index, term) is a batch end present in the
// log, ResetTo otherwise). So Snapshot().Index == FirstIndex()-1 always holds
// after Open, as raft requires.
//
// Concurrency: the write methods (Append, TruncateSuffix, ApplySnapshot,
// Compact, Sync, Close) must be called from one goroutine (the partition
// driver). Read methods are safe from any goroutine.
package raftlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	iofs "io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/Rahul-Nikhate/quorumlog/internal/fs"
	"github.com/Rahul-Nikhate/quorumlog/internal/log"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
)

// Sub-directory (inside the partition directory) holding the Raft files. The
// segmented log ignores it.
const (
	RaftDir       = "raft"
	snapMetaName  = "snapshot.meta"
	hardStateName = "hardstate"
)

// Errors.
var (
	// ErrCorruptSnapshotMeta is returned by Open when snapshot.meta fails
	// validation. It is fatal: the file is written by atomic replace, so this
	// is bit rot or tampering, never a torn write.
	ErrCorruptSnapshotMeta = errors.New("raftlog: corrupt snapshot.meta")
	// ErrContract is returned for calls that violate the LogStore contract
	// (non-contiguous append, entry/batch header mismatch, truncation inside a
	// batch, ...).
	ErrContract = errors.New("raftlog: LogStore contract violation")
)

// Config configures a Store.
type Config struct {
	// Dir is the partition directory (log segments + raft/). Required.
	Dir string
	// FS defaults to fs.OS.
	FS fs.FS
	// Log configures the segmented log. Dir and FS are overridden by the
	// fields above; InitialOffset is forced to 1.
	Log log.Config
}

// Store implements raft.LogStore on top of *log.Log.
type Store struct {
	l    *log.Log
	fsys fs.FS
	dir  string

	mu   sync.Mutex
	meta raft.Snapshot // last snapshot.meta written/read (zero if none)
}

var _ raft.LogStore = (*Store)(nil)

// Open opens (creating if needed) the store in cfg.Dir, runs log recovery and
// reconciles the log start with snapshot.meta (see the package doc).
func Open(cfg Config) (*Store, error) {
	if cfg.Dir == "" {
		return nil, errors.New("raftlog: Config.Dir is required")
	}
	fsys := cfg.FS
	if fsys == nil {
		fsys = fs.OS
	}
	if err := fsys.MkdirAll(filepath.Join(cfg.Dir, RaftDir), 0o755); err != nil {
		return nil, err
	}
	meta, err := readSnapMeta(fsys, filepath.Join(cfg.Dir, RaftDir, snapMetaName))
	if err != nil {
		return nil, err
	}
	lc := cfg.Log
	lc.Dir, lc.FS, lc.InitialOffset = cfg.Dir, fsys, 1
	l, err := log.Open(lc)
	if err != nil {
		return nil, err
	}
	s := &Store{l: l, fsys: fsys, dir: cfg.Dir, meta: meta}
	if err := s.reconcile(); err != nil {
		_ = l.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) metaPath() string { return filepath.Join(s.dir, RaftDir, snapMetaName) }

// reconcile makes the log start agree with snapshot.meta after a crash.
func (s *Store) reconcile() error {
	l := s.l
	if l.LogStartOffset() == 0 {
		// A log created without InitialOffset 1 (or by an old version).
		if l.LogEndOffset() > 0 {
			return fmt.Errorf("%w: log starts at offset 0; raft logs must start at offset 1", ErrContract)
		}
		if err := l.ResetTo(1, 0); err != nil {
			return err
		}
	}
	start := uint64(l.LogStartOffset())
	want := s.meta.Index + 1
	switch {
	case s.meta.Index == 0 && start == 1:
		return nil
	case want > start:
		// Interrupted Compact/ApplySnapshot: redo it.
		keep := false
		if s.meta.Index < uint64(l.LogEndOffset()) {
			if info, err := l.BatchInfoAt(int64(s.meta.Index)); err == nil &&
				uint64(info.LastOffset) == s.meta.Index && info.Term == s.meta.Term {
				keep = true
			}
		}
		if keep {
			return l.TruncatePrefix(int64(want))
		}
		return l.ResetTo(int64(want), s.meta.Term)
	case want < start:
		// The log start moved without the meta (lost meta file, or retention
		// applied directly to the log): the snapshot data no longer matches.
		// Re-anchor the meta at the log start; the application state for the
		// missing prefix is lost (documented gap).
		m := raft.Snapshot{Index: start - 1, Term: l.StartTerm(), Peers: s.meta.Peers}
		if err := writeSnapMeta(s.fsys, s.metaPath(), m); err != nil {
			return err
		}
		s.meta = m
	}
	return nil
}

// Log returns the underlying segmented log (for reads: Fetch, ListOffsets).
// Writers must go through the Store.
func (s *Store) Log() *log.Log { return s.l }

// Dir returns the partition directory.
func (s *Store) Dir() string { return s.dir }

// Close closes the log (final fsync + clean-shutdown record).
func (s *Store) Close() error { return s.l.Close() }

// Sync fsyncs everything written so far (regardless of the log's policy).
func (s *Store) Sync() error { return s.l.Sync() }

// FirstIndex implements raft.LogStore.
func (s *Store) FirstIndex() uint64 { return uint64(s.l.LogStartOffset()) }

// LastIndex implements raft.LogStore.
func (s *Store) LastIndex() uint64 { return uint64(s.l.LogEndOffset() - 1) }

// snapTerm is the term at FirstIndex()-1: the meta's term when it describes
// that point (it survives a lost log checkpoint), else the log's StartTerm.
func (s *Store) snapTerm(first uint64) uint64 {
	s.mu.Lock()
	m := s.meta
	s.mu.Unlock()
	if m.Index+1 == first && m.Index > 0 {
		return m.Term
	}
	return s.l.StartTerm()
}

// Term implements raft.LogStore.
func (s *Store) Term(i uint64) (uint64, error) {
	if i > math.MaxInt64 {
		return 0, raft.ErrUnavailable
	}
	first := s.FirstIndex()
	switch {
	case i+1 < first:
		return 0, raft.ErrCompacted
	case i+1 == first:
		return s.snapTerm(first), nil
	case i >= uint64(s.l.LogEndOffset()):
		return 0, raft.ErrUnavailable
	}
	t, err := s.l.TermAt(int64(i))
	if err != nil {
		return 0, s.mapReadErr(i, err)
	}
	return t, nil
}

// mapReadErr maps log errors to raft's: a read below the (concurrently
// advanced) log start is ErrCompacted, beyond the end ErrUnavailable.
func (s *Store) mapReadErr(i uint64, err error) error {
	switch {
	case errors.Is(err, log.ErrCompacted):
		return raft.ErrCompacted
	case errors.Is(err, log.ErrOffsetOutOfRange):
		if i < s.FirstIndex() {
			return raft.ErrCompacted
		}
		return raft.ErrUnavailable
	}
	return err
}

// Entries implements raft.LogStore.
func (s *Store) Entries(lo, hi, maxBytes uint64) ([]raft.Entry, error) {
	if lo < s.FirstIndex() {
		return nil, raft.ErrCompacted
	}
	if hi > uint64(s.l.LogEndOffset()) {
		return nil, raft.ErrUnavailable
	}
	if lo >= hi {
		return nil, nil
	}
	mb := math.MaxInt
	if maxBytes < uint64(math.MaxInt) {
		mb = int(maxBytes)
	}
	bs, err := s.l.ReadBatches(int64(lo), int64(hi), mb)
	if err != nil {
		return nil, s.mapReadErr(lo, err)
	}
	if len(bs) == 0 {
		return nil, fmt.Errorf("raftlog: no batches in [%d,%d)", lo, hi)
	}
	out := make([]raft.Entry, len(bs))
	for i, b := range bs {
		out[i] = raft.Entry{Term: b.Term(), FirstIndex: uint64(b.BaseOffset()), LastIndex: uint64(b.LastOffset()), Data: b}
	}
	return out, nil
}

// Snapshot implements raft.LogStore. Index is always FirstIndex()-1.
func (s *Store) Snapshot() (raft.Snapshot, error) {
	first := s.FirstIndex()
	if first <= 1 {
		return raft.Snapshot{}, nil
	}
	s.mu.Lock()
	m := s.meta
	s.mu.Unlock()
	if m.Index != first-1 {
		// Only possible if retention moved the log start behind our back.
		return raft.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
	}
	return raft.Snapshot{Index: m.Index, Term: m.Term, Peers: slices.Clone(m.Peers), Data: slices.Clone(m.Data)}, nil
}

// SnapshotMeta returns the stored snapshot metadata without the Index check.
func (s *Store) SnapshotMeta() raft.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.meta
	m.Peers, m.Data = slices.Clone(m.Peers), slices.Clone(m.Data)
	return m
}

// NoopBatch builds the election no-op: a ControlLeaderChange control batch
// stamped with (index, term). It is the raft.Config.NoopData function.
func NoopBatch(index, term uint64) []byte {
	opts := log.DefaultBatchOptions()
	opts.BaseOffset = int64(index)
	opts.PartitionLeaderEpoch = int32(min(term, math.MaxInt32))
	b, err := log.NewControlBatch(opts, log.ControlLeaderChange, nil)
	if err != nil {
		panic(err) // cannot happen: fixed-size control record
	}
	return b
}

// Append implements raft.LogStore. Each entry's Data must be a valid batch
// stamped with (FirstIndex, Term) and covering exactly [FirstIndex,
// LastIndex]; a nil Data single-offset entry is materialised as a no-op
// control batch.
func (s *Store) Append(entries []raft.Entry) error {
	for _, e := range entries {
		data := e.Data
		if data == nil {
			if e.FirstIndex != e.LastIndex {
				return fmt.Errorf("%w: nil-data entry %v spans more than one offset", ErrContract, e)
			}
			data = NoopBatch(e.FirstIndex, e.Term)
		}
		if len(data) < log.BatchHeaderSize {
			return fmt.Errorf("%w: entry %v data too short for a batch (%d bytes)", ErrContract, e, len(data))
		}
		b := log.Batch(data)
		if uint64(b.BaseOffset()) != e.FirstIndex || uint64(b.LastOffset()) != e.LastIndex || b.Term() != e.Term ||
			b.PartitionLeaderEpoch() < 0 {
			return fmt.Errorf("%w: entry %v carries batch [%d..%d] epoch %d", ErrContract, e,
				b.BaseOffset(), b.LastOffset(), b.PartitionLeaderEpoch())
		}
		if _, err := s.l.AppendStamped(b); err != nil {
			return fmt.Errorf("raftlog: append %v: %w", e, err)
		}
	}
	return nil
}

// TruncateSuffix implements raft.LogStore.
func (s *Store) TruncateSuffix(from uint64) error {
	first, leo := s.FirstIndex(), uint64(s.l.LogEndOffset())
	if from < first {
		return fmt.Errorf("%w: TruncateSuffix(%d) below first index %d", ErrContract, from, first)
	}
	if from >= leo {
		if from == leo {
			return nil
		}
		return fmt.Errorf("%w: TruncateSuffix(%d) beyond last index %d", ErrContract, from, leo-1)
	}
	info, err := s.l.BatchInfoAt(int64(from))
	if err != nil {
		return err
	}
	if uint64(info.BaseOffset) != from {
		return fmt.Errorf("%w: TruncateSuffix(%d) not at a batch boundary (batch starts at %d)", ErrContract, from, info.BaseOffset)
	}
	_, err = s.l.TruncateSuffix(int64(from))
	return err
}

// ApplySnapshot implements raft.LogStore: it records the snapshot durably,
// then resets the log to start right after it.
func (s *Store) ApplySnapshot(snap raft.Snapshot) error {
	if snap.Index == 0 || snap.Index >= math.MaxInt64 {
		return fmt.Errorf("%w: ApplySnapshot index %d", ErrContract, snap.Index)
	}
	if first := s.FirstIndex(); snap.Index+1 < first {
		return fmt.Errorf("%w: snapshot %d older than log start %d", ErrContract, snap.Index, first)
	}
	m := raft.Snapshot{Index: snap.Index, Term: snap.Term, Peers: slices.Clone(snap.Peers), Data: slices.Clone(snap.Data)}
	if err := writeSnapMeta(s.fsys, s.metaPath(), m); err != nil {
		return err
	}
	s.mu.Lock()
	s.meta = m
	s.mu.Unlock()
	return s.l.ResetTo(int64(snap.Index+1), snap.Term)
}

// Compact discards the log prefix up to and including index (a batch
// LastIndex at or below the applied index) and records a snapshot there with
// the given peers and application data (spec §5.12: for data partitions the
// snapshot is metadata; data is the producer state as of index).
func (s *Store) Compact(index uint64, peers []raft.NodeID, data []byte) error {
	first := s.FirstIndex()
	if index+1 <= first {
		return raft.ErrCompacted
	}
	if index >= uint64(s.l.LogEndOffset()) {
		return raft.ErrUnavailable
	}
	info, err := s.l.BatchInfoAt(int64(index))
	if err != nil {
		return err
	}
	if uint64(info.LastOffset) != index {
		return fmt.Errorf("%w: Compact(%d) not a batch end (batch [%d..%d])", ErrContract, index, info.BaseOffset, info.LastOffset)
	}
	m := raft.Snapshot{Index: index, Term: info.Term, Peers: slices.Clone(peers), Data: slices.Clone(data)}
	if err := writeSnapMeta(s.fsys, s.metaPath(), m); err != nil {
		return err
	}
	s.mu.Lock()
	s.meta = m
	s.mu.Unlock()
	return s.l.TruncatePrefix(int64(index + 1))
}

// AllEntries returns every entry in the log (tests and debugging).
func (s *Store) AllEntries() ([]raft.Entry, error) {
	lo, hi := s.FirstIndex(), uint64(s.l.LogEndOffset())
	if lo >= hi {
		return nil, nil
	}
	var out []raft.Entry
	for lo < hi {
		ents, err := s.Entries(lo, hi, 4<<20)
		if err != nil {
			return out, err
		}
		out = append(out, ents...)
		lo = ents[len(ents)-1].LastIndex + 1
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// snapshot.meta: [u32 magic "QLSM"][u8 version=1][u64 index][u64 term]
// [u32 nPeers]{[u32 peer]}[u32 dataLen][data][u32 crc32c of all preceding].

const (
	snapMetaMagic   = 0x514C534D // "QLSM"
	snapMetaVersion = 1
	snapMetaMaxSize = 64 << 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// EncodeSnapMeta returns the snapshot.meta encoding of m.
func EncodeSnapMeta(m raft.Snapshot) []byte {
	b := make([]byte, 0, 4+1+8+8+4+4*len(m.Peers)+4+len(m.Data)+4)
	b = binary.BigEndian.AppendUint32(b, snapMetaMagic)
	b = append(b, snapMetaVersion)
	b = binary.BigEndian.AppendUint64(b, m.Index)
	b = binary.BigEndian.AppendUint64(b, m.Term)
	b = binary.BigEndian.AppendUint32(b, uint32(len(m.Peers)))
	for _, p := range m.Peers {
		b = binary.BigEndian.AppendUint32(b, uint32(p))
	}
	b = binary.BigEndian.AppendUint32(b, uint32(len(m.Data)))
	b = append(b, m.Data...)
	return binary.BigEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

// DecodeSnapMeta parses EncodeSnapMeta's output.
func DecodeSnapMeta(b []byte) (raft.Snapshot, error) {
	bad := func(why string) (raft.Snapshot, error) {
		return raft.Snapshot{}, fmt.Errorf("%w: %s", ErrCorruptSnapshotMeta, why)
	}
	if len(b) < 4+1+8+8+4+4+4 {
		return bad("short file")
	}
	body, sum := b[:len(b)-4], binary.BigEndian.Uint32(b[len(b)-4:])
	if crc32.Checksum(body, castagnoli) != sum {
		return bad("crc mismatch")
	}
	if binary.BigEndian.Uint32(body) != snapMetaMagic || body[4] != snapMetaVersion {
		return bad("bad magic/version")
	}
	p := body[5:]
	var m raft.Snapshot
	m.Index = binary.BigEndian.Uint64(p)
	m.Term = binary.BigEndian.Uint64(p[8:])
	n := binary.BigEndian.Uint32(p[16:])
	p = p[20:]
	if uint64(n)*4+4 > uint64(len(p)) {
		return bad("peer count")
	}
	for range n {
		m.Peers = append(m.Peers, raft.NodeID(binary.BigEndian.Uint32(p)))
		p = p[4:]
	}
	dl := binary.BigEndian.Uint32(p)
	p = p[4:]
	if uint64(dl) != uint64(len(p)) {
		return bad("data length")
	}
	if dl > 0 {
		m.Data = slices.Clone(p)
	}
	return m, nil
}

func readSnapMeta(fsys fs.FS, path string) (raft.Snapshot, error) {
	b, err := fs.ReadFile(fsys, path, snapMetaMaxSize)
	if errors.Is(err, iofs.ErrNotExist) {
		return raft.Snapshot{}, nil
	}
	if err != nil {
		return raft.Snapshot{}, err
	}
	return DecodeSnapMeta(b)
}

func writeSnapMeta(fsys fs.FS, path string, m raft.Snapshot) error {
	if err := fs.WriteFileAtomic(fsys, path, EncodeSnapMeta(m), 0o644); err != nil {
		return fmt.Errorf("raftlog: write %s: %w", path, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Hard state on an fs.FS.

// FileOps adapts an fs.FS to raft.FileOps, so the HardState file goes through
// the same (fault-injectable) file system as the log.
type FileOps struct{ FS fs.FS }

var _ raft.FileOps = FileOps{}

// ReadFile implements raft.FileOps.
func (o FileOps) ReadFile(name string) ([]byte, error) { return fs.ReadFile(o.FS, name, 1<<16) }

// WriteFileSync implements raft.FileOps.
func (o FileOps) WriteFileSync(name string, data []byte) error {
	f, err := o.FS.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// Rename implements raft.FileOps.
func (o FileOps) Rename(oldpath, newpath string) error { return o.FS.Rename(oldpath, newpath) }

// SyncDir implements raft.FileOps.
func (o FileOps) SyncDir(dir string) error { return o.FS.SyncDir(dir) }

// NewHardStateStore returns the HardState store of the partition in dir:
// <dir>/raft/hardstate, atomic replace + dir fsync on fsys.
func NewHardStateStore(fsys fs.FS, dir string) *raft.FileHardStateStore {
	if fsys == nil {
		fsys = fs.OS
	}
	return &raft.FileHardStateStore{Dir: filepath.Join(dir, RaftDir), Name: hardStateName, Ops: FileOps{FS: fsys}}
}
