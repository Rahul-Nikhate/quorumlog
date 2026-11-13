package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// HardState is the Raft state that must survive a crash.
//
// Term and Vote are safety-critical: they must be durable before any message
// that depends on them leaves the node (Ready.MustSync). Commit is a restart
// hint only (it lets a restarting node apply up to it immediately); the Node
// guarantees that an emitted Commit never exceeds the durable log, so a
// persisted Commit is always safe to restore.
type HardState struct {
	Term   uint64
	Vote   NodeID // None = not voted in Term
	Commit uint64
}

// IsEmpty reports whether hs is the zero HardState.
func (hs HardState) IsEmpty() bool { return hs == HardState{} }

// HardStateStore persists a HardState. Save must be durable (fsynced) when it
// returns nil. An fsync failure must be treated as fatal by the caller
// (fsyncgate, spec §13.2): do not retry and continue.
type HardStateStore interface {
	Load() (HardState, error)
	Save(HardState) error
}

// MemHardStateStore is an in-memory HardStateStore for tests.
type MemHardStateStore struct {
	mu sync.Mutex
	hs HardState
	// Saves counts successful Save calls (tests use it to assert fsync counts).
	Saves int
}

// Load implements HardStateStore.
func (m *MemHardStateStore) Load() (HardState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hs, nil
}

// Save implements HardStateStore.
func (m *MemHardStateStore) Save(hs HardState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hs = hs
	m.Saves++
	return nil
}

// On-disk format of the meta file (spec §5.4.4, extended with Commit):
//
//	[u32 magic "QLHS"][u8 version=1][u64 term][u32 vote][u64 commit][u32 crc32c of the preceding bytes]
//
// All integers are big-endian. Total 29 bytes.
const (
	hardStateMagic   = 0x514C4853 // "QLHS"
	hardStateVersion = 1
	hardStateSize    = 4 + 1 + 8 + 4 + 8 + 4
)

var crc32c = crc32.MakeTable(crc32.Castagnoli)

// ErrCorruptHardState is returned by Load when the meta file fails validation.
// The caller must treat this as fatal (a replica that forgot its vote can vote
// twice in one term).
var ErrCorruptHardState = errors.New("raft: corrupt hard state file")

// EncodeHardState returns the on-disk encoding of hs.
func EncodeHardState(hs HardState) []byte {
	b := make([]byte, hardStateSize)
	binary.BigEndian.PutUint32(b[0:], hardStateMagic)
	b[4] = hardStateVersion
	binary.BigEndian.PutUint64(b[5:], hs.Term)
	binary.BigEndian.PutUint32(b[13:], uint32(hs.Vote))
	binary.BigEndian.PutUint64(b[17:], hs.Commit)
	binary.BigEndian.PutUint32(b[25:], crc32.Checksum(b[:25], crc32c))
	return b
}

// DecodeHardState parses the on-disk encoding produced by EncodeHardState.
func DecodeHardState(b []byte) (HardState, error) {
	if len(b) != hardStateSize {
		return HardState{}, fmt.Errorf("%w: size %d", ErrCorruptHardState, len(b))
	}
	if binary.BigEndian.Uint32(b[0:]) != hardStateMagic || b[4] != hardStateVersion {
		return HardState{}, fmt.Errorf("%w: bad magic/version", ErrCorruptHardState)
	}
	if crc32.Checksum(b[:25], crc32c) != binary.BigEndian.Uint32(b[25:]) {
		return HardState{}, fmt.Errorf("%w: crc mismatch", ErrCorruptHardState)
	}
	return HardState{
		Term:   binary.BigEndian.Uint64(b[5:]),
		Vote:   NodeID(binary.BigEndian.Uint32(b[13:])),
		Commit: binary.BigEndian.Uint64(b[17:]),
	}, nil
}

// FileOps is the small slice of the file system FileHardStateStore needs. It
// exists so tests can inject failures; OSFileOps is the real implementation.
type FileOps interface {
	ReadFile(name string) ([]byte, error)
	// WriteFileSync creates/truncates name, writes data and fsyncs the file.
	WriteFileSync(name string, data []byte) error
	Rename(oldpath, newpath string) error
	// SyncDir fsyncs the directory so a rename/create survives a crash.
	SyncDir(dir string) error
}

// OSFileOps implements FileOps with package os.
type OSFileOps struct{}

// ReadFile implements FileOps.
func (OSFileOps) ReadFile(name string) ([]byte, error) { return os.ReadFile(name) }

// WriteFileSync implements FileOps.
func (OSFileOps) WriteFileSync(name string, data []byte) error {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// Rename implements FileOps.
func (OSFileOps) Rename(oldpath, newpath string) error { return os.Rename(oldpath, newpath) }

// SyncDir implements FileOps.
func (OSFileOps) SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}

// FileHardStateStore persists the HardState in a single small file using the
// atomic-replace discipline: write "<name>.tmp" → fsync → rename over "<name>"
// → fsync the directory. A crash at any point leaves either the old or the new
// state, never a torn one.
type FileHardStateStore struct {
	Dir  string
	Name string  // default "meta"
	Ops  FileOps // default OSFileOps{}
}

func (s *FileHardStateStore) ops() FileOps {
	if s.Ops == nil {
		return OSFileOps{}
	}
	return s.Ops
}

func (s *FileHardStateStore) path() string {
	name := s.Name
	if name == "" {
		name = "meta"
	}
	return filepath.Join(s.Dir, name)
}

// Load implements HardStateStore. A missing file yields the zero HardState.
func (s *FileHardStateStore) Load() (HardState, error) {
	b, err := s.ops().ReadFile(s.path())
	if errors.Is(err, fs.ErrNotExist) {
		return HardState{}, nil
	}
	if err != nil {
		return HardState{}, err
	}
	return DecodeHardState(b)
}

// Save implements HardStateStore.
func (s *FileHardStateStore) Save(hs HardState) error {
	ops := s.ops()
	p := s.path()
	tmp := p + ".tmp"
	if err := ops.WriteFileSync(tmp, EncodeHardState(hs)); err != nil {
		return fmt.Errorf("raft: write %s: %w", tmp, err)
	}
	if err := ops.Rename(tmp, p); err != nil {
		return fmt.Errorf("raft: rename %s: %w", tmp, err)
	}
	if err := ops.SyncDir(s.Dir); err != nil {
		return fmt.Errorf("raft: fsync dir %s: %w", s.Dir, err)
	}
	return nil
}
