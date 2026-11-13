//go:build linux

package fs

import (
	"os"

	"golang.org/x/sys/unix"
)

// datasync uses fdatasync(2): for appends it flushes the data and the metadata
// needed to read it back (the file size) but skips mtime, saving a journal write.
// The descriptor is obtained through SyscallConn rather than Fd so the file is
// not switched to blocking mode.
func datasync(f *os.File) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var serr error
	cerr := rc.Control(func(fd uintptr) {
		for {
			serr = unix.Fdatasync(int(fd))
			if serr != unix.EINTR {
				return
			}
		}
	})
	if cerr != nil {
		return cerr
	}
	if serr != nil {
		return &os.PathError{Op: "fdatasync", Path: f.Name(), Err: serr}
	}
	return nil
}
