package fs

import (
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"syscall"
	"time"
)

// ErrInjected is the error returned by FaultFS for injected I/O failures. It
// wraps syscall.EIO, so errors.Is(err, syscall.EIO) also holds.
var ErrInjected = fmt.Errorf("fs: injected fault: %w", syscall.EIO)

// ErrCrashed is returned by every operation on a FaultFS instance (and on files
// opened through it) after Crash has been called on it. Use the FaultFS
// returned by Crash to "reboot".
var ErrCrashed = errors.New("fs: file system instance crashed")

// CrashMode selects what survives a simulated crash.
type CrashMode int

const (
	// CrashDropUnsynced models power loss: every file reverts to its contents
	// as of its last successful Sync, and every directory reverts to its
	// entries as of its last successful SyncDir.
	CrashDropUnsynced CrashMode = iota
	// CrashTearUnsynced is CrashDropUnsynced, except that for each file a random
	// prefix of the writes issued since the last Sync is persisted in order, and
	// the first write that is not fully persisted is torn at a random byte.
	CrashTearUnsynced
	// CrashProcess models a process crash (kill -9) with the OS still up: all
	// written data and directory changes survive, only open handles are lost.
	CrashProcess
)

// Faults configures probabilistic and latency fault injection. The zero value
// injects nothing.
type Faults struct {
	// SyncErrorRate is the probability in [0,1] that File.Sync or SyncDir fails
	// with ErrInjected.
	SyncErrorRate float64
	// WriteErrorRate is the probability in [0,1] that a write (Write, WriteAt,
	// Truncate) fails with ErrInjected. A failed write changes nothing.
	WriteErrorRate float64
	// SyncLatency is added to every Sync and SyncDir.
	SyncLatency time.Duration
	// WriteLatency is added to every write.
	WriteLatency time.Duration
	// DropDirtyOnSyncError models Linux "fsyncgate": when a Sync fails, the
	// dirty data is marked clean without being persisted. Reads still see it,
	// a later Sync "succeeds", and a crash loses it.
	DropDirtyOnSyncError bool
	// Match restricts fault injection to paths for which it returns true
	// (slash-separated, absolute). Nil matches every path.
	Match func(name string) bool
}

// FaultStats counts operations performed through a FaultFS (across crashes).
type FaultStats struct {
	Syncs               int64 // successful File.Sync calls
	DirSyncs            int64 // successful SyncDir calls
	Writes              int64 // successful write calls
	BytesWritten        int64
	InjectedSyncErrors  int64
	InjectedWriteErrors int64
	Crashes             int64
}

// FaultFS is a deterministic in-memory file system with fault injection, for
// tests. It models the durability semantics of a POSIX file system: data is
// durable only after File.Sync, and directory entries (create, rename, remove)
// are durable only after SyncDir of the parent. Directories created by MkdirAll
// are durable immediately.
//
// All randomness comes from a PRNG seeded at construction, so a test that uses
// FaultFS from a single goroutine is reproducible from its seed. FaultFS is
// safe for concurrent use.
type FaultFS struct {
	st  *faultState
	gen uint64
}

type faultState struct {
	mu         sync.Mutex
	gen        uint64
	rng        *rand.Rand
	dirs       map[string]*memDir
	faults     Faults
	failSyncs  int
	failWrites int
	now        func() time.Time
	stats      FaultStats
}

type memDir struct {
	entries map[string]*inode // volatile view
	durable map[string]*inode // as of the last SyncDir
	modTime time.Time
}

type inode struct {
	data    []byte      // what reads observe (page cache)
	synced  []byte      // what survives power loss
	pending []pendingOp // changes since the last successful Sync, in order
	modTime time.Time
}

type pendingOp struct {
	truncate bool
	off      int64 // WriteAt offset, or the new size for truncate
	data     []byte
}

var _ FS = (*FaultFS)(nil)

// NewFaultFS returns an empty fault-injecting file system whose randomness is
// derived from seed. The root directory "/" exists.
func NewFaultFS(seed uint64) *FaultFS {
	st := &faultState{
		rng:  rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		dirs: map[string]*memDir{},
		now:  time.Now,
	}
	st.dirs["/"] = newMemDir(st.now())
	return &FaultFS{st: st}
}

func newMemDir(t time.Time) *memDir {
	return &memDir{entries: map[string]*inode{}, durable: map[string]*inode{}, modTime: t}
}

// SetFaults replaces the probabilistic fault configuration.
func (f *FaultFS) SetFaults(fl Faults) {
	f.st.mu.Lock()
	f.st.faults = fl
	f.st.mu.Unlock()
}

// FailNextSyncs makes the next n Sync/SyncDir calls (on matching paths) fail
// with ErrInjected, deterministically.
func (f *FaultFS) FailNextSyncs(n int) {
	f.st.mu.Lock()
	f.st.failSyncs = n
	f.st.mu.Unlock()
}

// FailNextWrites makes the next n write calls (on matching paths) fail with
// ErrInjected, deterministically.
func (f *FaultFS) FailNextWrites(n int) {
	f.st.mu.Lock()
	f.st.failWrites = n
	f.st.mu.Unlock()
}

// SetClock sets the clock used for modification times.
func (f *FaultFS) SetClock(now func() time.Time) {
	f.st.mu.Lock()
	f.st.now = now
	f.st.mu.Unlock()
}

// Stats returns operation counters.
func (f *FaultFS) Stats() FaultStats {
	f.st.mu.Lock()
	defer f.st.mu.Unlock()
	return f.st.stats
}

// Crash simulates a crash according to mode and returns a fresh FaultFS
// instance (the "rebooted" machine) that shares the surviving state. The
// receiver and every file opened through it are dead afterwards: all of their
// operations fail with ErrCrashed, so code still running against the old
// instance cannot change what survived.
func (f *FaultFS) Crash(mode CrashMode) *FaultFS {
	st := f.st
	st.mu.Lock()
	defer st.mu.Unlock()
	st.gen++
	st.stats.Crashes++
	seen := map[*inode]bool{}
	// Iterate in a deterministic order so the PRNG stream is reproducible.
	dirNames := make([]string, 0, len(st.dirs))
	for name := range st.dirs {
		dirNames = append(dirNames, name)
	}
	sort.Strings(dirNames)
	for _, dn := range dirNames {
		d := st.dirs[dn]
		if mode != CrashProcess {
			d.entries = maps(d.durable)
		} else {
			d.durable = maps(d.entries)
		}
		names := make([]string, 0, len(d.entries))
		for n := range d.entries {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			ino := d.entries[n]
			if seen[ino] {
				continue
			}
			seen[ino] = true
			var img []byte
			switch mode {
			case CrashProcess:
				img = ino.data
			case CrashTearUnsynced:
				img = st.tornImage(ino)
			default:
				img = slices.Clone(ino.synced)
			}
			ino.data = img
			ino.synced = slices.Clone(img)
			ino.pending = nil
		}
	}
	return &FaultFS{st: st, gen: st.gen}
}

func maps(m map[string]*inode) map[string]*inode {
	out := make(map[string]*inode, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// tornImage applies a random prefix of the pending operations to the synced
// image, tearing the first unapplied write at a random byte.
func (st *faultState) tornImage(ino *inode) []byte {
	img := slices.Clone(ino.synced)
	if len(ino.pending) == 0 {
		return img
	}
	k := st.rng.IntN(len(ino.pending) + 1)
	for _, op := range ino.pending[:k] {
		img = applyOp(img, op)
	}
	if k < len(ino.pending) {
		op := ino.pending[k]
		if !op.truncate && len(op.data) > 0 {
			m := st.rng.IntN(len(op.data))
			img = applyOp(img, pendingOp{off: op.off, data: op.data[:m]})
		}
	}
	return img
}

func applyOp(img []byte, op pendingOp) []byte {
	if op.truncate {
		return resize(img, op.off)
	}
	end := op.off + int64(len(op.data))
	if end > int64(len(img)) {
		img = resize(img, end)
	}
	copy(img[op.off:], op.data)
	return img
}

func resize(b []byte, n int64) []byte {
	if n <= int64(len(b)) {
		return b[:n]
	}
	if n <= int64(cap(b)) {
		old := len(b)
		b = b[:n]
		clear(b[old:])
		return b
	}
	nb := make([]byte, n, max(n, int64(2*cap(b))))
	copy(nb, b)
	return nb
}

// clean converts any path into the canonical absolute slash form.
func clean(name string) string {
	return path.Clean("/" + filepath.ToSlash(name))
}

func (st *faultState) matches(p string) bool {
	return st.faults.Match == nil || st.faults.Match(p)
}

// injectSync decides (under st.mu) whether a sync on p fails, and returns the
// latency to apply.
func (st *faultState) injectSync(p string) (time.Duration, bool) {
	if !st.matches(p) {
		return 0, false
	}
	fail := false
	if st.failSyncs > 0 {
		st.failSyncs--
		fail = true
	} else if st.faults.SyncErrorRate > 0 && st.rng.Float64() < st.faults.SyncErrorRate {
		fail = true
	}
	if fail {
		st.stats.InjectedSyncErrors++
	}
	return st.faults.SyncLatency, fail
}

func (st *faultState) injectWrite(p string) (time.Duration, bool) {
	if !st.matches(p) {
		return 0, false
	}
	fail := false
	if st.failWrites > 0 {
		st.failWrites--
		fail = true
	} else if st.faults.WriteErrorRate > 0 && st.rng.Float64() < st.faults.WriteErrorRate {
		fail = true
	}
	if fail {
		st.stats.InjectedWriteErrors++
	}
	return st.faults.WriteLatency, fail
}

// lock acquires the state lock and verifies the instance is alive.
func (f *FaultFS) lock(op, name string) error {
	f.st.mu.Lock()
	if f.gen != f.st.gen {
		f.st.mu.Unlock()
		return &os.PathError{Op: op, Path: name, Err: ErrCrashed}
	}
	return nil
}

// sleepUnlocked releases the lock for d (if positive), re-acquires it and
// re-checks liveness. It always returns with the lock held.
func (f *FaultFS) sleepUnlocked(d time.Duration, op, name string) error {
	if d <= 0 {
		return nil
	}
	f.st.mu.Unlock()
	time.Sleep(d)
	f.st.mu.Lock()
	if f.gen != f.st.gen {
		return &os.PathError{Op: op, Path: name, Err: ErrCrashed}
	}
	return nil
}

func (st *faultState) parent(p string) (*memDir, string, error) {
	d, ok := st.dirs[path.Dir(p)]
	if !ok {
		return nil, "", os.ErrNotExist
	}
	return d, path.Base(p), nil
}

// OpenFile opens a file. Supported flags: O_RDONLY, O_WRONLY, O_RDWR, O_CREATE,
// O_EXCL, O_TRUNC, O_APPEND. Directories cannot be opened as files.
func (f *FaultFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	p := clean(name)
	if err := f.lock("open", name); err != nil {
		return nil, err
	}
	defer f.st.mu.Unlock()
	st := f.st
	if _, isDir := st.dirs[p]; isDir {
		return nil, &os.PathError{Op: "open", Path: name, Err: syscall.EISDIR}
	}
	d, base, err := st.parent(p)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	acc := flag & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)
	writable := acc == os.O_WRONLY || acc == os.O_RDWR
	ino, exists := d.entries[base]
	switch {
	case exists && flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0:
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrExist}
	case !exists && flag&os.O_CREATE == 0:
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrNotExist}
	case !exists:
		now := st.now()
		ino = &inode{modTime: now}
		d.entries[base] = ino
		d.modTime = now
	}
	if flag&os.O_TRUNC != 0 && writable && len(ino.data) > 0 {
		ino.data = ino.data[:0]
		ino.pending = append(ino.pending, pendingOp{truncate: true, off: 0})
		ino.modTime = st.now()
	}
	return &memFile{
		fsys:     f,
		ino:      ino,
		name:     name,
		path:     p,
		readable: acc == os.O_RDONLY || acc == os.O_RDWR,
		writable: writable,
		append:   flag&os.O_APPEND != 0,
	}, nil
}

// Create creates or truncates a file for reading and writing.
func (f *FaultFS) Create(name string) (File, error) {
	return f.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
}

// Open opens a file read-only.
func (f *FaultFS) Open(name string) (File, error) { return f.OpenFile(name, os.O_RDONLY, 0) }

// Rename renames a file (directories cannot be renamed). Durable after SyncDir.
func (f *FaultFS) Rename(oldpath, newpath string) error {
	op, np := clean(oldpath), clean(newpath)
	if err := f.lock("rename", oldpath); err != nil {
		return err
	}
	defer f.st.mu.Unlock()
	st := f.st
	od, ob, err := st.parent(op)
	if err != nil {
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: err}
	}
	nd, nb, err := st.parent(np)
	if err != nil {
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: err}
	}
	if _, isDir := st.dirs[np]; isDir {
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: syscall.EISDIR}
	}
	ino, ok := od.entries[ob]
	if !ok {
		if _, isDir := st.dirs[op]; isDir {
			return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: syscall.EINVAL}
		}
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: os.ErrNotExist}
	}
	if op == np {
		return nil
	}
	delete(od.entries, ob)
	nd.entries[nb] = ino
	now := st.now()
	od.modTime, nd.modTime = now, now
	return nil
}

// Remove removes a file, or an empty directory (directory removal is
// immediately durable). File removal is durable after SyncDir.
func (f *FaultFS) Remove(name string) error {
	p := clean(name)
	if err := f.lock("remove", name); err != nil {
		return err
	}
	defer f.st.mu.Unlock()
	st := f.st
	if d, isDir := st.dirs[p]; isDir {
		if p == "/" {
			return &os.PathError{Op: "remove", Path: name, Err: syscall.EINVAL}
		}
		if len(d.entries) > 0 || st.hasChildDir(p) {
			return &os.PathError{Op: "remove", Path: name, Err: syscall.ENOTEMPTY}
		}
		delete(st.dirs, p)
		return nil
	}
	d, base, err := st.parent(p)
	if err != nil {
		return &os.PathError{Op: "remove", Path: name, Err: err}
	}
	if _, ok := d.entries[base]; !ok {
		return &os.PathError{Op: "remove", Path: name, Err: os.ErrNotExist}
	}
	delete(d.entries, base)
	d.modTime = st.now()
	return nil
}

func (st *faultState) hasChildDir(p string) bool {
	for dn := range st.dirs {
		if dn != p && path.Dir(dn) == p {
			return true
		}
	}
	return false
}

// MkdirAll creates a directory and its parents. Directory creation is durable
// immediately (a simplification: the storage engine creates its directories
// once, before writing any data).
func (f *FaultFS) MkdirAll(name string, perm os.FileMode) error {
	p := clean(name)
	if err := f.lock("mkdir", name); err != nil {
		return err
	}
	defer f.st.mu.Unlock()
	st := f.st
	var chain []string
	for q := p; ; q = path.Dir(q) {
		chain = append(chain, q)
		if q == "/" {
			break
		}
	}
	for i := len(chain) - 1; i >= 0; i-- {
		q := chain[i]
		if _, ok := st.dirs[q]; ok {
			continue
		}
		d, base, err := st.parent(q)
		if err != nil {
			return &os.PathError{Op: "mkdir", Path: name, Err: err}
		}
		if _, isFile := d.entries[base]; isFile {
			return &os.PathError{Op: "mkdir", Path: name, Err: syscall.ENOTDIR}
		}
		st.dirs[q] = newMemDir(st.now())
	}
	return nil
}

// ReadDir lists the (volatile) entries of a directory, sorted by name.
func (f *FaultFS) ReadDir(name string) ([]os.DirEntry, error) {
	p := clean(name)
	if err := f.lock("readdir", name); err != nil {
		return nil, err
	}
	defer f.st.mu.Unlock()
	st := f.st
	d, ok := st.dirs[p]
	if !ok {
		return nil, &os.PathError{Op: "readdir", Path: name, Err: os.ErrNotExist}
	}
	out := make([]os.DirEntry, 0, len(d.entries))
	for n, ino := range d.entries {
		out = append(out, iofs.FileInfoToDirEntry(&memInfo{name: n, size: int64(len(ino.data)), modTime: ino.modTime}))
	}
	for dn, sub := range st.dirs {
		if dn != p && path.Dir(dn) == p {
			out = append(out, iofs.FileInfoToDirEntry(&memInfo{name: path.Base(dn), dir: true, modTime: sub.modTime}))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out, nil
}

// SyncDir makes the directory's current entries durable.
func (f *FaultFS) SyncDir(name string) error {
	p := clean(name)
	if err := f.lock("syncdir", name); err != nil {
		return err
	}
	defer f.st.mu.Unlock()
	st := f.st
	if _, ok := st.dirs[p]; !ok {
		return &os.PathError{Op: "syncdir", Path: name, Err: os.ErrNotExist}
	}
	lat, fail := st.injectSync(p)
	if err := f.sleepUnlocked(lat, "syncdir", name); err != nil {
		return err
	}
	d, ok := st.dirs[p]
	if !ok {
		return &os.PathError{Op: "syncdir", Path: name, Err: os.ErrNotExist}
	}
	if fail {
		return &os.PathError{Op: "syncdir", Path: name, Err: ErrInjected}
	}
	d.durable = maps(d.entries)
	st.stats.DirSyncs++
	return nil
}

// Stat returns metadata for a file or directory.
func (f *FaultFS) Stat(name string) (os.FileInfo, error) {
	p := clean(name)
	if err := f.lock("stat", name); err != nil {
		return nil, err
	}
	defer f.st.mu.Unlock()
	st := f.st
	if d, ok := st.dirs[p]; ok {
		return &memInfo{name: path.Base(p), dir: true, modTime: d.modTime}, nil
	}
	d, base, err := st.parent(p)
	if err != nil {
		return nil, &os.PathError{Op: "stat", Path: name, Err: err}
	}
	ino, ok := d.entries[base]
	if !ok {
		return nil, &os.PathError{Op: "stat", Path: name, Err: os.ErrNotExist}
	}
	return &memInfo{name: base, size: int64(len(ino.data)), modTime: ino.modTime}, nil
}

// memFile is an open handle on an inode. Like a POSIX descriptor it keeps the
// inode alive after Remove/Rename.
type memFile struct {
	fsys     *FaultFS
	ino      *inode
	name     string
	path     string
	readable bool
	writable bool
	append   bool
	off      int64 // sequential Read/Write offset, guarded by fsys.st.mu
	closed   bool
}

func (m *memFile) lock(op string) error {
	if err := m.fsys.lock(op, m.name); err != nil {
		return err
	}
	if m.closed {
		m.fsys.st.mu.Unlock()
		return &os.PathError{Op: op, Path: m.name, Err: os.ErrClosed}
	}
	return nil
}

func (m *memFile) Name() string { return m.name }

func (m *memFile) Read(p []byte) (int, error) {
	if err := m.lock("read"); err != nil {
		return 0, err
	}
	defer m.fsys.st.mu.Unlock()
	n, err := m.readAtLocked(p, m.off)
	m.off += int64(n)
	return n, err
}

func (m *memFile) ReadAt(p []byte, off int64) (int, error) {
	if err := m.lock("read"); err != nil {
		return 0, err
	}
	defer m.fsys.st.mu.Unlock()
	return m.readAtLocked(p, off)
}

func (m *memFile) readAtLocked(p []byte, off int64) (int, error) {
	if !m.readable {
		return 0, &os.PathError{Op: "read", Path: m.name, Err: syscall.EBADF}
	}
	if off < 0 {
		return 0, &os.PathError{Op: "read", Path: m.name, Err: syscall.EINVAL}
	}
	if off >= int64(len(m.ino.data)) {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	n := copy(p, m.ino.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *memFile) Write(p []byte) (int, error) {
	if err := m.lock("write"); err != nil {
		return 0, err
	}
	defer m.fsys.st.mu.Unlock()
	off := m.off
	if m.append {
		off = int64(len(m.ino.data))
	}
	n, err := m.writeAtLocked(p, off)
	if err == nil || n > 0 {
		m.off = off + int64(n)
	}
	return n, err
}

func (m *memFile) WriteAt(p []byte, off int64) (int, error) {
	if err := m.lock("write"); err != nil {
		return 0, err
	}
	defer m.fsys.st.mu.Unlock()
	if m.append {
		return 0, errors.New("fs: invalid use of WriteAt on file opened with O_APPEND")
	}
	return m.writeAtLocked(p, off)
}

func (m *memFile) writeAtLocked(p []byte, off int64) (int, error) {
	if !m.writable {
		return 0, &os.PathError{Op: "write", Path: m.name, Err: syscall.EBADF}
	}
	if off < 0 {
		return 0, &os.PathError{Op: "write", Path: m.name, Err: syscall.EINVAL}
	}
	st := m.fsys.st
	lat, fail := st.injectWrite(m.path)
	if err := m.relockAfter(lat, "write"); err != nil {
		return 0, err
	}
	if fail {
		return 0, &os.PathError{Op: "write", Path: m.name, Err: ErrInjected}
	}
	if len(p) == 0 {
		return 0, nil
	}
	op := pendingOp{off: off, data: slices.Clone(p)}
	m.ino.data = applyOp(m.ino.data, op)
	m.ino.pending = append(m.ino.pending, op)
	m.ino.modTime = st.now()
	st.stats.Writes++
	st.stats.BytesWritten += int64(len(p))
	return len(p), nil
}

// relockAfter sleeps without the lock and re-validates the handle.
func (m *memFile) relockAfter(d time.Duration, op string) error {
	if d <= 0 {
		return nil
	}
	m.fsys.st.mu.Unlock()
	time.Sleep(d)
	m.fsys.st.mu.Lock()
	if m.fsys.gen != m.fsys.st.gen {
		return &os.PathError{Op: op, Path: m.name, Err: ErrCrashed}
	}
	if m.closed {
		return &os.PathError{Op: op, Path: m.name, Err: os.ErrClosed}
	}
	return nil
}

func (m *memFile) Truncate(size int64) error {
	if err := m.lock("truncate"); err != nil {
		return err
	}
	defer m.fsys.st.mu.Unlock()
	if !m.writable {
		return &os.PathError{Op: "truncate", Path: m.name, Err: syscall.EBADF}
	}
	if size < 0 {
		return &os.PathError{Op: "truncate", Path: m.name, Err: syscall.EINVAL}
	}
	st := m.fsys.st
	lat, fail := st.injectWrite(m.path)
	if err := m.relockAfter(lat, "truncate"); err != nil {
		return err
	}
	if fail {
		return &os.PathError{Op: "truncate", Path: m.name, Err: ErrInjected}
	}
	op := pendingOp{truncate: true, off: size}
	m.ino.data = applyOp(m.ino.data, op)
	m.ino.pending = append(m.ino.pending, op)
	m.ino.modTime = st.now()
	return nil
}

func (m *memFile) Sync() error {
	if err := m.lock("sync"); err != nil {
		return err
	}
	defer m.fsys.st.mu.Unlock()
	st := m.fsys.st
	lat, fail := st.injectSync(m.path)
	if err := m.relockAfter(lat, "sync"); err != nil {
		return err
	}
	if fail {
		if st.faults.DropDirtyOnSyncError {
			m.ino.pending = nil
		}
		return &os.PathError{Op: "sync", Path: m.name, Err: ErrInjected}
	}
	for _, op := range m.ino.pending {
		m.ino.synced = applyOp(m.ino.synced, op)
	}
	m.ino.pending = nil
	st.stats.Syncs++
	return nil
}

func (m *memFile) Close() error {
	if err := m.lock("close"); err != nil {
		return err
	}
	defer m.fsys.st.mu.Unlock()
	m.closed = true
	return nil
}

func (m *memFile) Stat() (os.FileInfo, error) {
	if err := m.lock("stat"); err != nil {
		return nil, err
	}
	defer m.fsys.st.mu.Unlock()
	return &memInfo{name: path.Base(m.path), size: int64(len(m.ino.data)), modTime: m.ino.modTime}, nil
}

type memInfo struct {
	name    string
	size    int64
	dir     bool
	modTime time.Time
}

func (i *memInfo) Name() string { return i.name }
func (i *memInfo) Size() int64  { return i.size }
func (i *memInfo) Mode() os.FileMode {
	if i.dir {
		return os.ModeDir | 0o755
	}
	return 0o644
}
func (i *memInfo) ModTime() time.Time { return i.modTime }
func (i *memInfo) IsDir() bool        { return i.dir }
func (i *memInfo) Sys() any           { return nil }
