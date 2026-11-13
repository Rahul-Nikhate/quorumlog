package fs

import (
	"os"
)

// OS is the real operating-system file system.
var OS FS = osFS{}

type osFS struct{}

// osFile wraps *os.File so that Sync can use fdatasync on Linux and so that the
// descriptor is reachable through the OSFile interface.
type osFile struct {
	*os.File
}

var _ OSFile = (*osFile)(nil)

// OSFile returns the underlying *os.File.
func (f *osFile) OSFile() *os.File { return f.File }

// Sync flushes data and size to stable storage (fdatasync on Linux,
// F_FULLFSYNC on macOS, fsync elsewhere).
func (f *osFile) Sync() error { return datasync(f.File) }

func (osFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &osFile{File: f}, nil
}

func (o osFS) Create(name string) (File, error) {
	return o.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
}

func (o osFS) Open(name string) (File, error) { return o.OpenFile(name, os.O_RDONLY, 0) }

func (osFS) Rename(oldpath, newpath string) error { return os.Rename(oldpath, newpath) }

func (osFS) Remove(name string) error { return os.Remove(name) }

func (osFS) MkdirAll(path string, perm os.FileMode) error { return os.MkdirAll(path, perm) }

func (osFS) ReadDir(name string) ([]os.DirEntry, error) { return os.ReadDir(name) }

func (osFS) Stat(name string) (os.FileInfo, error) { return os.Stat(name) }

// SyncDir fsyncs a directory so that entries created, renamed or removed in it
// survive a crash.
func (osFS) SyncDir(name string) error {
	d, err := os.Open(name)
	if err != nil {
		return err
	}
	serr := d.Sync()
	cerr := d.Close()
	if serr != nil {
		return serr
	}
	return cerr
}
