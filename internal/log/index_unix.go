//go:build unix

package log

import (
	"errors"

	"golang.org/x/sys/unix"

	"github.com/Rahul-Nikhate/quorumlog/internal/fs"
)

// canMmap reports whether f is backed by a real descriptor that can be mapped.
func canMmap(f fs.File) bool {
	_, ok := f.(fs.OSFile)
	return ok
}

// mmapFile maps the first size bytes of f with MAP_SHARED. The descriptor is
// obtained through SyscallConn().Control rather than Fd(); the mapping stays
// valid after the descriptor is closed.
func mmapFile(f fs.File, size int, writable bool) ([]byte, error) {
	of, ok := f.(fs.OSFile)
	if !ok {
		return nil, errors.New("log: file does not support mmap")
	}
	rc, err := of.OSFile().SyscallConn()
	if err != nil {
		return nil, err
	}
	prot := unix.PROT_READ
	if writable {
		prot |= unix.PROT_WRITE
	}
	var m []byte
	var merr error
	if cerr := rc.Control(func(fd uintptr) {
		m, merr = unix.Mmap(int(fd), 0, size, prot, unix.MAP_SHARED)
	}); cerr != nil {
		return nil, cerr
	}
	if merr != nil {
		return nil, merr
	}
	return m, nil
}

func munmap(b []byte) error { return unix.Munmap(b) }

// msync flushes a writable mapping's dirty pages to the file.
func msync(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return unix.Msync(b, unix.MS_SYNC)
}
