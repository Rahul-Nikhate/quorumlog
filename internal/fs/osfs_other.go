//go:build !linux

package fs

import "os"

// datasync falls back to (*os.File).Sync, which is fsync(2) on most Unixes and
// F_FULLFSYNC on macOS (correct, but much slower than Linux fdatasync: never
// compare macOS fsync numbers with Linux ones).
func datasync(f *os.File) error { return f.Sync() }
