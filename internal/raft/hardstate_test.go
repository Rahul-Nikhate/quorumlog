package raft

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestHardStateCodec(t *testing.T) {
	hs := HardState{Term: 1<<40 + 3, Vote: 7, Commit: 99}
	b := EncodeHardState(hs)
	got, err := DecodeHardState(b)
	if err != nil || got != hs {
		t.Fatalf("round trip %+v %v", got, err)
	}
	for i := range b {
		c := append([]byte(nil), b...)
		c[i] ^= 0x40
		if _, err := DecodeHardState(c); !errors.Is(err, ErrCorruptHardState) {
			t.Fatalf("flip at byte %d not detected: %v", i, err)
		}
	}
	if _, err := DecodeHardState(b[:10]); !errors.Is(err, ErrCorruptHardState) {
		t.Fatal("short file accepted")
	}
}

func TestFileHardStateStore(t *testing.T) {
	dir := t.TempDir()
	s := &FileHardStateStore{Dir: dir}
	if hs, err := s.Load(); err != nil || !hs.IsEmpty() {
		t.Fatalf("fresh load %+v %v", hs, err)
	}
	for _, hs := range []HardState{{Term: 1, Vote: 2}, {Term: 5, Vote: 0, Commit: 10}} {
		must(t, s.Save(hs))
		if got, err := s.Load(); err != nil || got != hs {
			t.Fatalf("load %+v %v want %+v", got, err, hs)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "meta.tmp")); !os.IsNotExist(err) {
		t.Fatalf("tmp file left behind: %v", err)
	}
	// A torn tmp file from a crash mid-save is ignored.
	must(t, os.WriteFile(filepath.Join(dir, "meta.tmp"), []byte("garbage"), 0o644))
	if got, err := s.Load(); err != nil || got.Term != 5 {
		t.Fatalf("load with stray tmp: %+v %v", got, err)
	}
	// Corruption of the real file is reported, never silently zeroed.
	must(t, os.WriteFile(filepath.Join(dir, "meta"), []byte("garbage"), 0o644))
	if _, err := s.Load(); !errors.Is(err, ErrCorruptHardState) {
		t.Fatalf("corrupt meta: %v", err)
	}
}

type failingOps struct {
	OSFileOps
	failRename, failSync bool
}

func (f failingOps) Rename(a, b string) error {
	if f.failRename {
		return errors.New("injected rename failure")
	}
	return f.OSFileOps.Rename(a, b)
}

func (f failingOps) SyncDir(dir string) error {
	if f.failSync {
		return errors.New("injected fsync failure")
	}
	return f.OSFileOps.SyncDir(dir)
}

// A failed save leaves the previous state intact and reports the error.
func TestFileHardStateStoreFailures(t *testing.T) {
	dir := t.TempDir()
	good := &FileHardStateStore{Dir: dir}
	must(t, good.Save(HardState{Term: 3, Vote: 1}))
	bad := &FileHardStateStore{Dir: dir, Ops: failingOps{failRename: true}}
	if err := bad.Save(HardState{Term: 4, Vote: 2}); err == nil {
		t.Fatal("rename failure not reported")
	}
	if got, _ := good.Load(); got.Term != 3 || got.Vote != 1 {
		t.Fatalf("failed save clobbered state: %+v", got)
	}
	bad = &FileHardStateStore{Dir: dir, Ops: failingOps{failSync: true}}
	if err := bad.Save(HardState{Term: 4, Vote: 2}); err == nil {
		t.Fatal("dir fsync failure not reported")
	}
	var mem MemHardStateStore
	must(t, mem.Save(HardState{Term: 2}))
	if hs, _ := mem.Load(); hs.Term != 2 || mem.Saves != 1 {
		t.Fatal("MemHardStateStore")
	}
}
