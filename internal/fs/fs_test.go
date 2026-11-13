package fs

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// forEachFS runs a conformance test against the real OS file system (in a
// temp dir) and against FaultFS.
func forEachFS(t *testing.T, fn func(t *testing.T, fsys FS, dir string)) {
	t.Run("os", func(t *testing.T) { fn(t, OS, t.TempDir()) })
	t.Run("fault", func(t *testing.T) {
		f := NewFaultFS(1)
		if err := f.MkdirAll("/data", 0o755); err != nil {
			t.Fatal(err)
		}
		fn(t, f, "/data")
	})
}

func TestConformanceReadWrite(t *testing.T) {
	forEachFS(t, func(t *testing.T, fsys FS, dir string) {
		p := filepath.Join(dir, "a")
		f, err := fsys.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte("hello ")); err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte("world")); err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt([]byte("W"), 6); err != nil {
			t.Fatal(err)
		}
		// Writing past EOF zero-fills the gap.
		if _, err := f.WriteAt([]byte("!"), 13); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 14)
		n, err := f.ReadAt(buf, 0)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
		if want := "hello World\x00\x00!"; n != 14 || string(buf) != want {
			t.Fatalf("got %q (%d), want %q", buf[:n], n, want)
		}
		n, err = f.ReadAt(buf, 10)
		if n != 4 || !errors.Is(err, io.EOF) {
			t.Fatalf("short ReadAt: n=%d err=%v", n, err)
		}
		if err := f.Truncate(5); err != nil {
			t.Fatal(err)
		}
		st, err := f.Stat()
		if err != nil || st.Size() != 5 {
			t.Fatalf("stat after truncate: %v %v", st, err)
		}
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		got, err := ReadFile(fsys, p, 100)
		if err != nil || string(got) != "hello" {
			t.Fatalf("ReadFile = %q, %v", got, err)
		}
		if _, err := ReadFile(fsys, p, 2); err == nil {
			t.Fatal("ReadFile ignored the size limit")
		}
	})
}

func TestConformanceNamespace(t *testing.T) {
	forEachFS(t, func(t *testing.T, fsys FS, dir string) {
		sub := filepath.Join(dir, "x", "y")
		if err := fsys.MkdirAll(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		for _, n := range []string{"b", "a", "c"} {
			f, err := fsys.Create(filepath.Join(sub, n))
			if err != nil {
				t.Fatal(err)
			}
			f.Close()
		}
		if err := fsys.Rename(filepath.Join(sub, "c"), filepath.Join(sub, "d")); err != nil {
			t.Fatal(err)
		}
		if err := fsys.Remove(filepath.Join(sub, "a")); err != nil {
			t.Fatal(err)
		}
		if err := fsys.SyncDir(sub); err != nil {
			t.Fatal(err)
		}
		ents, err := fsys.ReadDir(sub)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, e := range ents {
			names = append(names, e.Name())
		}
		if got := filepath.Join(names...); got != filepath.Join("b", "d") {
			t.Fatalf("ReadDir = %v", names)
		}
		ents, err = fsys.ReadDir(filepath.Join(dir, "x"))
		if err != nil || len(ents) != 1 || !ents[0].IsDir() || ents[0].Name() != "y" {
			t.Fatalf("ReadDir(parent) = %v, %v", ents, err)
		}
		if _, err := fsys.Stat(filepath.Join(sub, "a")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Stat(removed) err = %v", err)
		}
		if _, err := fsys.Open(filepath.Join(sub, "zzz")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Open(missing) err = %v", err)
		}
		if _, err := fsys.OpenFile(filepath.Join(sub, "b"), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644); !errors.Is(err, os.ErrExist) {
			t.Fatalf("O_EXCL on existing file err = %v", err)
		}
		st, err := fsys.Stat(sub)
		if err != nil || !st.IsDir() {
			t.Fatalf("Stat(dir) = %v, %v", st, err)
		}
	})
}

func TestConformanceOpenHandleSurvivesRemove(t *testing.T) {
	forEachFS(t, func(t *testing.T, fsys FS, dir string) {
		p := filepath.Join(dir, "f")
		f, err := fsys.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		f.Write([]byte("data"))
		if err := fsys.Rename(p, p+".deleted"); err != nil {
			t.Fatal(err)
		}
		if err := fsys.Remove(p + ".deleted"); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 4)
		if n, err := f.ReadAt(buf, 0); n != 4 || string(buf) != "data" {
			t.Fatalf("read after unlink: %q %v", buf[:n], err)
		}
	})
}

func TestConformanceReadOnly(t *testing.T) {
	forEachFS(t, func(t *testing.T, fsys FS, dir string) {
		p := filepath.Join(dir, "ro")
		if err := WriteFileAtomic(fsys, p, []byte("v1"), 0o644); err != nil {
			t.Fatal(err)
		}
		f, err := fsys.Open(p)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, err := f.Write([]byte("x")); err == nil {
			t.Fatal("write on read-only handle succeeded")
		}
		b, _ := io.ReadAll(f)
		if string(b) != "v1" {
			t.Fatalf("read %q", b)
		}
		// The tmp file must be gone after WriteFileAtomic.
		if _, err := fsys.Stat(p + ".tmp"); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("tmp file left behind: %v", err)
		}
	})
}

func TestOSFileExposesDescriptor(t *testing.T) {
	f, err := OS.Create(filepath.Join(t.TempDir(), "f"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	of, ok := f.(OSFile)
	if !ok || of.OSFile() == nil {
		t.Fatal("OS file does not implement OSFile")
	}
	ff, err := NewFaultFS(1).Create("/x")
	if err != nil {
		t.Fatal(err)
	}
	if _, isOS := ff.(OSFile); isOS {
		t.Fatal("FaultFS file must not implement OSFile")
	}
}

func TestFaultCrashDropsUnsynced(t *testing.T) {
	f := NewFaultFS(7)
	h, _ := f.Create("/a")
	h.Write([]byte("synced"))
	if err := h.Sync(); err != nil {
		t.Fatal(err)
	}
	h.Write([]byte("-lost"))
	// File created but directory never synced: vanishes.
	g, _ := f.Create("/b")
	g.Write([]byte("x"))
	g.Sync()
	// Directory entry for /a is not durable either until SyncDir.
	if err := f.SyncDir("/"); err != nil {
		t.Fatal(err)
	}
	r, _ := f.Create("/c")
	r.Sync()

	f2 := f.Crash(CrashDropUnsynced)
	if _, err := h.Write([]byte("zombie")); !errors.Is(err, ErrCrashed) {
		t.Fatalf("old handle after crash: %v", err)
	}
	if _, err := f.Stat("/a"); !errors.Is(err, ErrCrashed) {
		t.Fatalf("old FS after crash: %v", err)
	}
	got, err := ReadFile(f2, "/a", 100)
	if err != nil || string(got) != "synced" {
		t.Fatalf("after crash /a = %q, %v", got, err)
	}
	if got, _ := ReadFile(f2, "/b", 100); string(got) != "x" {
		t.Fatalf("/b (synced, dir synced) = %q", got)
	}
	if _, err := f2.Stat("/c"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("/c should vanish (dir not synced after create): %v", err)
	}
	if s := f2.Stats(); s.Crashes != 1 {
		t.Fatalf("stats %+v", s)
	}
}

func TestFaultCrashRenameNeedsDirSync(t *testing.T) {
	f := NewFaultFS(1)
	if err := WriteFileAtomic(f, "/meta", []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Rename without SyncDir: after a crash the old name/content is back.
	tmp, _ := f.Create("/meta.tmp")
	tmp.Write([]byte("new"))
	tmp.Sync()
	tmp.Close()
	f.Rename("/meta.tmp", "/meta")
	f2 := f.Crash(CrashDropUnsynced)
	if got, _ := ReadFile(f2, "/meta", 10); string(got) != "old" {
		t.Fatalf("got %q, want old", got)
	}
	// WriteFileAtomic syncs the dir: survives.
	if err := WriteFileAtomic(f2, "/meta", []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}
	f3 := f2.Crash(CrashDropUnsynced)
	if got, _ := ReadFile(f3, "/meta", 10); string(got) != "new" {
		t.Fatalf("got %q, want new", got)
	}
}

func TestFaultCrashTearIsPrefixOfWrites(t *testing.T) {
	sawPartial, sawNone, sawAll := false, false, false
	for seed := uint64(0); seed < 200; seed++ {
		f := NewFaultFS(seed)
		h, _ := f.Create("/f")
		f.SyncDir("/")
		h.Write([]byte("AAAA"))
		h.Sync()
		h.Write([]byte("BBBB"))
		h.Write([]byte("CCCC"))
		f2 := f.Crash(CrashTearUnsynced)
		got, _ := ReadFile(f2, "/f", 100)
		full := "AAAABBBBCCCC"
		if len(got) < 4 || !bytes.Equal(got, []byte(full[:len(got)])) {
			t.Fatalf("seed %d: torn image %q is not a prefix of %q", seed, got, full)
		}
		switch len(got) {
		case 4:
			sawNone = true
		case 12:
			sawAll = true
		default:
			sawPartial = true
		}
	}
	if !sawPartial || !sawNone || !sawAll {
		t.Fatalf("tear distribution too narrow: partial=%v none=%v all=%v", sawPartial, sawNone, sawAll)
	}
}

func TestFaultCrashDeterministic(t *testing.T) {
	run := func() string {
		f := NewFaultFS(42)
		h, _ := f.Create("/f")
		f.SyncDir("/")
		for i := 0; i < 20; i++ {
			h.Write(bytes.Repeat([]byte{byte('a' + i)}, 10))
		}
		got, _ := ReadFile(f.Crash(CrashTearUnsynced), "/f", 1000)
		return string(got)
	}
	if a, b := run(), run(); a != b {
		t.Fatalf("same seed, different crash images: %q vs %q", a, b)
	}
}

func TestFaultCrashProcessKeepsEverything(t *testing.T) {
	f := NewFaultFS(1)
	h, _ := f.Create("/f")
	h.Write([]byte("unsynced"))
	f2 := f.Crash(CrashProcess)
	if got, _ := ReadFile(f2, "/f", 100); string(got) != "unsynced" {
		t.Fatalf("got %q", got)
	}
}

func TestFaultInjectedErrors(t *testing.T) {
	f := NewFaultFS(1)
	h, _ := f.Create("/f")
	f.FailNextSyncs(1)
	if err := h.Sync(); !errors.Is(err, ErrInjected) || !errors.Is(err, syscall.EIO) {
		t.Fatalf("sync err = %v", err)
	}
	if err := h.Sync(); err != nil {
		t.Fatalf("second sync: %v", err)
	}
	f.FailNextWrites(1)
	if _, err := h.WriteAt([]byte("x"), 0); !errors.Is(err, ErrInjected) {
		t.Fatalf("write err = %v", err)
	}
	if st, _ := h.Stat(); st.Size() != 0 {
		t.Fatal("failed write changed the file")
	}
	f.SetFaults(Faults{SyncErrorRate: 1, Match: func(p string) bool { return p == "/other" }})
	if err := h.Sync(); err != nil {
		t.Fatalf("Match filter ignored: %v", err)
	}
	f.SetFaults(Faults{WriteErrorRate: 1})
	if _, err := h.Write([]byte("x")); !errors.Is(err, ErrInjected) {
		t.Fatalf("rate-based write err = %v", err)
	}
	s := f.Stats()
	if s.InjectedSyncErrors != 1 || s.InjectedWriteErrors != 2 {
		t.Fatalf("stats = %+v", s)
	}
}

// TestFaultFsyncgate: with DropDirtyOnSyncError, a failed fsync silently
// discards the dirty data; a later fsync succeeds, and a crash loses it.
func TestFaultFsyncgate(t *testing.T) {
	f := NewFaultFS(1)
	f.SetFaults(Faults{DropDirtyOnSyncError: true})
	h, _ := f.Create("/f")
	f.SyncDir("/")
	h.Write([]byte("lost"))
	f.FailNextSyncs(1)
	if err := h.Sync(); err == nil {
		t.Fatal("expected injected failure")
	}
	if err := h.Sync(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if n, _ := h.ReadAt(buf, 0); n != 4 {
		t.Fatal("page cache should still show the data")
	}
	if got, _ := ReadFile(f.Crash(CrashDropUnsynced), "/f", 10); len(got) != 0 {
		t.Fatalf("fsyncgate data survived: %q", got)
	}
}

func TestFaultLatency(t *testing.T) {
	f := NewFaultFS(1)
	f.SetFaults(Faults{SyncLatency: 20 * time.Millisecond})
	h, _ := f.Create("/f")
	start := time.Now()
	h.Sync()
	if d := time.Since(start); d < 20*time.Millisecond {
		t.Fatalf("sync latency not applied: %v", d)
	}
}

func TestFaultCrashDuringLatentSync(t *testing.T) {
	f := NewFaultFS(1)
	f.SetFaults(Faults{SyncLatency: 50 * time.Millisecond})
	h, _ := f.Create("/f")
	h.Write([]byte("x"))
	done := make(chan error)
	go func() { done <- h.Sync() }()
	time.Sleep(10 * time.Millisecond)
	f2 := f.Crash(CrashDropUnsynced)
	if err := <-done; !errors.Is(err, ErrCrashed) {
		t.Fatalf("sync spanning a crash returned %v", err)
	}
	if _, err := f2.Stat("/f"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsynced dir entry survived: %v", err)
	}
}

func TestFaultRemoveDir(t *testing.T) {
	f := NewFaultFS(1)
	f.MkdirAll("/a/b", 0o755)
	if err := f.Remove("/a"); err == nil {
		t.Fatal("removed non-empty dir")
	}
	if err := f.Remove("/a/b"); err != nil {
		t.Fatal(err)
	}
	if err := f.Remove("/a"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Create("/a/x"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("create in removed dir: %v", err)
	}
	if err := f.MkdirAll("/q", 0o755); err != nil {
		t.Fatal(err)
	}
	h, _ := f.Create("/file")
	h.Close()
	if err := f.MkdirAll("/file/sub", 0o755); err == nil {
		t.Fatal("mkdir through a file succeeded")
	}
	if _, err := h.Write([]byte("x")); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("write after close: %v", err)
	}
}
