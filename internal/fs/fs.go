// Package fs abstracts the file-system operations used by the storage engine
// (internal/log) and the Raft persistence code, so that tests can substitute a
// deterministic, fault-injecting in-memory implementation (FaultFS) for the real
// operating-system file system (OS).
//
// The interface is deliberately small: it contains only the calls the storage
// engine needs, and every durability-relevant operation (Sync, SyncDir) is
// explicit, so that the fault file system can model exactly what survives a
// crash: file contents up to the last successful Sync, and directory entries up
// to the last successful SyncDir of the parent directory.
package fs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// File is an open file. *os.File-backed files returned by OS also implement
// OSFile, which callers use for mmap and sendfile.
//
// Sync must make all data written so far, and the file size, durable. On Linux
// the OS implementation uses fdatasync(2); on macOS it uses F_FULLFSYNC (via
// (*os.File).Sync).
type File interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.WriterAt
	io.Closer
	// Name returns the name the file was opened with.
	Name() string
	// Stat returns the file's metadata.
	Stat() (os.FileInfo, error)
	// Sync flushes written data and the file size to stable storage.
	Sync() error
	// Truncate changes the size of the file.
	Truncate(size int64) error
}

// OSFile is implemented by files backed by a real operating-system file
// descriptor. The returned *os.File is owned by the File: callers must not
// close it, and must not use it after the File is closed.
type OSFile interface {
	OSFile() *os.File
}

// FS is the file-system abstraction. Paths use the host's separator conventions
// for OS; FaultFS accepts both relative and absolute slash-separated paths.
type FS interface {
	// OpenFile opens name with the given os.O_* flags.
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	// Create creates or truncates name for reading and writing (mode 0o644).
	Create(name string) (File, error)
	// Open opens name read-only.
	Open(name string) (File, error)
	// Rename atomically replaces newpath with oldpath. The change is durable
	// only after SyncDir of the affected directories.
	Rename(oldpath, newpath string) error
	// Remove removes a file or an empty directory. Durable after SyncDir.
	Remove(name string) error
	// MkdirAll creates a directory and any missing parents.
	MkdirAll(path string, perm os.FileMode) error
	// ReadDir lists a directory, sorted by file name.
	ReadDir(name string) ([]os.DirEntry, error)
	// SyncDir makes the directory's entries (creates, renames, removes) durable.
	SyncDir(name string) error
	// Stat returns metadata for name.
	Stat(name string) (os.FileInfo, error)
}

// ReadFile reads the whole named file. It refuses files larger than maxBytes
// (use a small bound: this helper is meant for metadata files).
func ReadFile(fsys FS, name string, maxBytes int64) ([]byte, error) {
	f, err := fsys.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() > maxBytes {
		return nil, fmt.Errorf("fs: %s is %d bytes, larger than the %d byte limit", name, st.Size(), maxBytes)
	}
	buf := make([]byte, st.Size())
	n, err := f.ReadAt(buf, 0)
	if err != nil && !(errors.Is(err, io.EOF) && n == len(buf)) {
		return nil, err
	}
	return buf, nil
}

// WriteFileAtomic replaces name with data crash-atomically: it writes
// name+".tmp", fsyncs it, renames it over name and fsyncs the parent directory.
// After a crash the file holds either the old or the new contents, never a mix.
func WriteFileAtomic(fsys FS, name string, data []byte, perm os.FileMode) (err error) {
	tmp := name + ".tmp"
	f, err := fsys.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = fsys.Remove(tmp)
		}
	}()
	if _, err = f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = fsys.Rename(tmp, name); err != nil {
		return err
	}
	return fsys.SyncDir(filepath.Dir(name))
}
