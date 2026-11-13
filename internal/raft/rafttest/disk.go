package rafttest

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Rahul-Nikhate/quorumlog/internal/fs"
	"github.com/Rahul-Nikhate/quorumlog/internal/log"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
	"github.com/Rahul-Nikhate/quorumlog/internal/raftlog"
)

// DiskKind selects the storage behind each node's raft.LogStore.
type DiskKind int

const (
	// DiskMem is raft.MemoryStore with a separate durable image (MemDisk).
	DiskMem DiskKind = iota
	// DiskFaultFS is the real segmented log (internal/raftlog over
	// internal/log) on an in-memory fault-injecting file system; Crash is a
	// power loss (unsynced writes dropped or torn) followed by log recovery.
	DiskFaultFS
	// DiskOS is the real segmented log on a temporary directory of the real
	// file system (mmap'd indexes); Crash is a process restart (clean close +
	// reopen). fsyncs are elided (see noSyncFS).
	DiskOS
)

func (k DiskKind) String() string {
	switch k {
	case DiskMem:
		return "mem"
	case DiskFaultFS:
		return "faultfs"
	case DiskOS:
		return "os"
	}
	return fmt.Sprintf("DiskKind(%d)", int(k))
}

// Store is the LogStore view the harness needs (reads + writes + the
// compaction hook + a full listing for invariant checks).
type Store interface {
	raft.LogStore
	AllEntries() []raft.Entry
	Compact(index uint64, peers []raft.NodeID, data []byte) error
}

// Disk is one node's persistent state as the harness drives it.
type Disk interface {
	Store() Store
	HardState() raft.HardStateStore
	// Written is called after the driver wrote one Ready's log changes. In
	// synchronous-durability mode they must be durable when it returns.
	Written(async bool)
	// BeginFlush starts a simulated fsync covering everything written so far;
	// CompleteFlush completes the last one started (async mode).
	BeginFlush()
	CompleteFlush()
	// SyncAll makes everything durable now (snapshot installation).
	SyncAll()
	// DurableLastIndex is the last index that survives a crash.
	DurableLastIndex() uint64
	// Crash simulates a crash; the store afterwards shows what survived.
	Crash(async bool)
	// Close releases resources at the end of the test.
	Close()
	// LogBacked reports whether entries must carry real RecordBatches.
	LogBacked() bool
}

// MemDisk models a node's disk for the harness: a current view (what the
// process has written, i.e. the page cache) and a durable image (what
// survives a crash). Sync makes the current view durable; Crash discards
// everything written since the last Sync. In synchronous-durability mode the
// harness treats every write as durable immediately and Crash keeps it.
type MemDisk struct {
	cur     *raft.MemoryStore
	durable *raft.MemoryStore
	img     *raft.MemoryStore
	hs      *raft.MemHardStateStore
}

// NewMemDisk returns an empty disk.
func NewMemDisk() *MemDisk {
	return &MemDisk{cur: raft.NewMemoryStore(), durable: raft.NewMemoryStore(), hs: &raft.MemHardStateStore{}}
}

type memStore struct{ *raft.MemoryStore }

// Store returns the current view.
func (d *MemDisk) Store() Store { return memStore{d.cur} }

// HardState returns the (always durable) hard state store.
func (d *MemDisk) HardState() raft.HardStateStore { return d.hs }

// Written implements Disk (sync mode: the current view counts as durable).
func (d *MemDisk) Written(bool) {}

// BeginFlush captures the image the simulated fsync will make durable.
func (d *MemDisk) BeginFlush() { d.img = d.cur.Clone() }

// CompleteFlush makes the captured image durable.
func (d *MemDisk) CompleteFlush() {
	if d.img != nil {
		d.durable, d.img = d.img, nil
	}
}

// SyncAll makes everything written so far durable.
func (d *MemDisk) SyncAll() { d.durable, d.img = d.cur.Clone(), nil }

// DurableLastIndex implements Disk.
func (d *MemDisk) DurableLastIndex() uint64 { return d.durable.LastIndex() }

// Crash discards unsynced writes (async mode only).
func (d *MemDisk) Crash(async bool) {
	d.img = nil
	if async {
		d.cur = d.durable.Clone()
	}
}

// Close implements Disk.
func (d *MemDisk) Close() {}

// LogBacked implements Disk.
func (d *MemDisk) LogBacked() bool { return false }

// ---------------------------------------------------------------------------

// LogDisk is the real QuorumLog storage stack: raftlog.Store (segmented log +
// snapshot.meta) and the file-based HardState store, on FaultFS or the OS.
//
// The log runs with SyncPolicy never and the harness decides when fsyncs
// happen (deterministically): after every Ready in sync mode, and at the
// simulated flush completion in async mode. Suffix truncations are made
// durable immediately, as the production SyncAlways policy does.
type LogDisk struct {
	kind  DiskKind
	ffs   *fs.FaultFS
	dir   string
	seed  uint64
	crash uint64
	st    *logStore
	hs    raft.HardStateStore
	last  uint64 // durable last index (as of the last fsync)
}

type logStore struct {
	*raftlog.Store
	disk *LogDisk
}

// AllEntries implements Store (panics on a read error; the harness reports
// panics with their seed).
func (s *logStore) AllEntries() []raft.Entry {
	ents, err := s.Store.AllEntries()
	if err != nil {
		panic(fmt.Sprintf("rafttest: LogDisk.AllEntries: %v", err))
	}
	return ents
}

// TruncateSuffix truncates and makes the truncation durable.
func (s *logStore) TruncateSuffix(from uint64) error {
	if err := s.Store.TruncateSuffix(from); err != nil {
		return err
	}
	return s.disk.sync()
}

// NewLogDisk opens a log-backed disk in dir (on a fresh FaultFS for
// DiskFaultFS; dir must be a temporary directory for DiskOS).
func NewLogDisk(kind DiskKind, dir string, seed uint64) (*LogDisk, error) {
	d := &LogDisk{kind: kind, dir: dir, seed: seed}
	if kind == DiskFaultFS {
		d.ffs = fs.NewFaultFS(seed)
	}
	if err := d.open(); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *LogDisk) fsys() fs.FS {
	if d.ffs != nil {
		return d.ffs
	}
	return noSyncFS{fs.OS}
}

func (d *LogDisk) open() error {
	st, err := raftlog.Open(raftlog.Config{Dir: d.dir, FS: d.fsys(), Log: log.Config{
		SegmentBytes:       2048, // small segments: rolls, multi-segment reads and truncations
		IndexIntervalBytes: 128,
		MaxIndexBytes:      4096,
		Sync:               log.SyncPolicy{Mode: log.SyncNever},
	}})
	if err != nil {
		return err
	}
	d.st = &logStore{Store: st, disk: d}
	d.hs = raftlog.NewHardStateStore(d.fsys(), d.dir)
	d.last = st.LastIndex()
	return nil
}

func (d *LogDisk) sync() error {
	if err := d.st.Sync(); err != nil {
		return err
	}
	d.last = d.st.LastIndex()
	return nil
}

func (d *LogDisk) mustSync() {
	if err := d.sync(); err != nil {
		panic(fmt.Sprintf("rafttest: LogDisk sync: %v", err))
	}
}

// Store implements Disk.
func (d *LogDisk) Store() Store { return d.st }

// HardState implements Disk.
func (d *LogDisk) HardState() raft.HardStateStore { return d.hs }

// Written implements Disk: in sync mode every Ready's writes are fsynced
// before its messages are sent.
func (d *LogDisk) Written(async bool) {
	if !async {
		d.mustSync()
	}
}

// BeginFlush implements Disk (the fsync happens at completion; it covers at
// least everything written before BeginFlush).
func (d *LogDisk) BeginFlush() {}

// CompleteFlush implements Disk.
func (d *LogDisk) CompleteFlush() { d.mustSync() }

// SyncAll implements Disk.
func (d *LogDisk) SyncAll() { d.mustSync() }

// DurableLastIndex implements Disk.
func (d *LogDisk) DurableLastIndex() uint64 { return d.last }

// Crash implements Disk. FaultFS: power loss (unsynced data dropped, or torn
// at a random byte, chosen by seed), then recovery. OS: clean close and
// reopen (a process restart).
func (d *LogDisk) Crash(bool) {
	d.crash++
	if d.ffs != nil {
		mode := fs.CrashDropUnsynced
		if (d.seed+d.crash)%2 == 0 {
			mode = fs.CrashTearUnsynced
		}
		d.ffs = d.ffs.Crash(mode)
	}
	_ = d.st.Close() // on a crashed FaultFS this fails; it only releases resources
	if err := d.open(); err != nil {
		panic(fmt.Sprintf("rafttest: reopen after crash: %v", err))
	}
}

// Close implements Disk.
func (d *LogDisk) Close() { _ = d.st.Close() }

// LogBacked implements Disk.
func (d *LogDisk) LogBacked() bool { return true }

// ---------------------------------------------------------------------------
// Payloads. On a log-backed disk every entry is a real RecordBatch: span
// records, the first record's value starting with the 8-byte proposal id.

func makePayload(logBacked bool, pid, span uint64, noise []byte) []byte {
	head := make([]byte, 8, 8+len(noise))
	binary.BigEndian.PutUint64(head, pid)
	head = append(head, noise...)
	if !logBacked {
		return head
	}
	recs := make([]log.Record, span)
	recs[0].Value = head
	for i := 1; i < len(recs); i++ {
		recs[i].Value = []byte{byte(i)}
	}
	b, err := log.NewBatch(log.DefaultBatchOptions(), recs...)
	if err != nil {
		panic(err)
	}
	return b
}

func stampBatch(e *raft.Entry) { log.Batch(e.Data).Stamp(int64(e.FirstIndex), e.Term) }

func payloadID(logBacked bool, d []byte) uint64 {
	if logBacked {
		b := log.Batch(d)
		if len(d) < log.BatchHeaderSize || b.IsControl() {
			return 0
		}
		recs, err := b.Records()
		if err != nil || len(recs) == 0 {
			return 0
		}
		d = recs[0].Value
	}
	if len(d) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(d)
}

func diskDir(base string, id raft.NodeID) string { return filepath.Join(base, fmt.Sprintf("n%d", id)) }

// noSyncFS is the real OS file system with fsync elided. DiskOS crashes are
// process restarts (everything written survives), so fsyncs cannot change an
// outcome there, and on macOS each one is an F_FULLFSYNC of several
// milliseconds. Power-loss durability is DiskFaultFS's job. Files keep
// implementing fs.OSFile, so the mmap index path is exercised.
type noSyncFS struct{ fs.FS }

type noSyncFile struct{ fs.File }

type noSyncOSFile struct {
	noSyncFile
	os fs.OSFile
}

func (f noSyncFile) Sync() error { return nil }

func (f noSyncOSFile) OSFile() *os.File { return f.os.OSFile() }

func wrapNoSync(f fs.File, err error) (fs.File, error) {
	if err != nil {
		return nil, err
	}
	if of, ok := f.(fs.OSFile); ok {
		return noSyncOSFile{noSyncFile{f}, of}, nil
	}
	return noSyncFile{f}, nil
}

func (n noSyncFS) OpenFile(name string, flag int, perm os.FileMode) (fs.File, error) {
	return wrapNoSync(n.FS.OpenFile(name, flag, perm))
}
func (n noSyncFS) Create(name string) (fs.File, error) { return wrapNoSync(n.FS.Create(name)) }
func (n noSyncFS) Open(name string) (fs.File, error)   { return wrapNoSync(n.FS.Open(name)) }
func (n noSyncFS) SyncDir(string) error                { return nil }
