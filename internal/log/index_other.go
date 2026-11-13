//go:build !unix

package log

import (
	"errors"

	"github.com/Rahul-Nikhate/quorumlog/internal/fs"
)

// On non-Unix platforms indexes always use the portable heap-buffer mode.

func canMmap(fs.File) bool { return false }

func mmapFile(fs.File, int, bool) ([]byte, error) {
	return nil, errors.New("log: mmap not supported on this platform")
}

func munmap([]byte) error { return nil }

func msync([]byte) error { return nil }
