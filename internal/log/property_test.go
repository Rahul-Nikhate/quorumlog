package log

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"pgregory.net/rapid"

	"github.com/Rahul-Nikhate/quorumlog/internal/fs"
)

// logModel is the reference model: the ordered batches the log must contain.
type logModel struct {
	batches   []Batch // batches with LastOffset >= logStart
	logStart  int64
	startTerm uint64
	leo       int64
}

func (m *logModel) lastTerm() uint64 {
	if len(m.batches) == 0 {
		return m.startTerm
	}
	return m.batches[len(m.batches)-1].Term()
}

// containing returns the index of the model batch containing off.
func (m *logModel) containing(off int64) int {
	return sort.Search(len(m.batches), func(i int) bool { return m.batches[i].LastOffset() >= off })
}

// logMachine drives random operations against a Log and the model.
type logMachine struct {
	cfg   Config
	l     *Log
	m     logModel
	crash bool // FaultFS only: process crashes allowed
}

func (lm *logMachine) open(t *rapid.T) {
	l, err := Open(lm.cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	lm.l = l
}

func (lm *logMachine) Append(t *rapid.T) {
	n := rapid.IntRange(0, 5).Draw(t, "records")
	size := rapid.IntRange(0, 700).Draw(t, "valueSize")
	ts := rapid.Int64Range(-1, 1<<40).Draw(t, "ts")
	term := lm.m.lastTerm() + uint64(rapid.IntRange(0, 1).Draw(t, "termBump"))
	recs := make([]Record, n)
	for i := range recs {
		recs[i] = Record{Value: bytes.Repeat([]byte{byte(i + len(lm.m.batches))}, size)}
	}
	opts := DefaultBatchOptions()
	opts.BaseTimestamp = ts
	b, err := NewBatch(opts, recs...)
	if err != nil {
		t.Fatal(err)
	}
	res, err := lm.l.Append(b, term)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if res.BaseOffset != lm.m.leo {
		t.Fatalf("base offset %d, want %d", res.BaseOffset, lm.m.leo)
	}
	lm.m.batches = append(lm.m.batches, bytes.Clone(b))
	lm.m.leo = b.LastOffset() + 1
}

func (lm *logMachine) Roll(t *rapid.T) {
	if err := lm.l.Roll(); err != nil {
		t.Fatalf("Roll: %v", err)
	}
}

func (lm *logMachine) Sync(t *rapid.T) {
	if err := lm.l.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}

func (lm *logMachine) Reopen(t *rapid.T) {
	if err := lm.l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	lm.open(t)
}

// CrashReopen simulates kill -9 (page cache survives): the log must recover
// exactly, with no data loss.
func (lm *logMachine) CrashReopen(t *rapid.T) {
	abandon(lm.l)
	if ff, ok := lm.cfg.FS.(*fs.FaultFS); ok && lm.crash {
		lm.cfg.FS = ff.Crash(fs.CrashProcess)
	}
	lm.open(t)
}

func (lm *logMachine) TruncateSuffix(t *rapid.T) {
	if lm.m.leo == lm.m.logStart {
		t.Skip("empty")
	}
	from := rapid.Int64Range(lm.m.logStart, lm.m.leo).Draw(t, "from")
	leo, err := lm.l.TruncateSuffix(from)
	if from == lm.m.leo {
		if err != nil || leo != lm.m.leo {
			t.Fatalf("no-op truncate: %d %v", leo, err)
		}
		return
	}
	i := lm.m.containing(from)
	if lm.m.batches[i].BaseOffset() < lm.m.logStart {
		if !errors.Is(err, ErrOffsetOutOfRange) {
			t.Fatalf("truncate into the batch straddling log start: %v", err)
		}
		return
	}
	if err != nil {
		t.Fatalf("TruncateSuffix(%d): %v", from, err)
	}
	cut := lm.m.batches[i].BaseOffset()
	if leo != cut {
		t.Fatalf("TruncateSuffix(%d) = %d, want %d", from, leo, cut)
	}
	lm.m.batches = lm.m.batches[:i]
	lm.m.leo = cut
}

func (lm *logMachine) TruncatePrefix(t *rapid.T) {
	off := rapid.Int64Range(lm.m.logStart, lm.m.leo).Draw(t, "offset")
	if err := lm.l.TruncatePrefix(off); err != nil {
		t.Fatalf("TruncatePrefix(%d): %v", off, err)
	}
	if off == lm.m.logStart {
		return
	}
	i := lm.m.containing(off - 1)
	lm.m.startTerm = lm.m.batches[i].Term()
	lm.m.logStart = off
	lm.m.batches = lm.m.batches[lm.m.containing(off):]
}

func (lm *logMachine) Read(t *rapid.T) {
	off := rapid.Int64Range(lm.m.logStart, lm.m.leo).Draw(t, "offset")
	maxBytes := rapid.IntRange(0, 5000).Draw(t, "maxBytes")
	r, err := lm.l.Read(off, maxBytes)
	if err != nil {
		t.Fatalf("Read(%d): %v", off, err)
	}
	data, err := r.Bytes()
	r.Release()
	if err != nil {
		t.Fatal(err)
	}
	if off == lm.m.leo {
		if len(data) != 0 {
			t.Fatalf("Read(LEO) returned %d bytes", len(data))
		}
		return
	}
	bs, err := SplitBatches(data, math.MaxInt32)
	if err != nil || len(bs) == 0 {
		t.Fatalf("Read(%d, %d): %d batches, %v", off, maxBytes, len(bs), err)
	}
	if len(bs) > 1 && len(data) > maxBytes {
		t.Fatalf("Read(%d, %d) returned %d bytes", off, maxBytes, len(data))
	}
	i := lm.m.containing(off)
	for j, b := range bs {
		if i+j >= len(lm.m.batches) || !bytes.Equal(b, lm.m.batches[i+j]) {
			t.Fatalf("Read(%d) batch %d differs from model", off, j)
		}
	}
}

func (lm *logMachine) TermAt(t *rapid.T) {
	off := rapid.Int64Range(lm.m.logStart-1, lm.m.leo).Draw(t, "offset")
	got, err := lm.l.TermAt(off)
	switch {
	case off == lm.m.leo && off != lm.m.logStart-1:
		if !errors.Is(err, ErrOffsetOutOfRange) {
			t.Fatalf("TermAt(LEO) err = %v", err)
		}
	case off == lm.m.logStart-1:
		if err != nil || got != lm.m.startTerm {
			t.Fatalf("TermAt(start-1) = %d, %v; want %d", got, err, lm.m.startTerm)
		}
	default:
		want := lm.m.batches[lm.m.containing(off)].Term()
		if err != nil || got != want {
			t.Fatalf("TermAt(%d) = %d, %v; want %d", off, got, err, want)
		}
	}
}

// Check runs after every action.
func (lm *logMachine) Check(t *rapid.T) {
	if got := lm.l.LogEndOffset(); got != lm.m.leo {
		t.Fatalf("LEO %d, model %d", got, lm.m.leo)
	}
	if got := lm.l.LogStartOffset(); got != lm.m.logStart {
		t.Fatalf("log start %d, model %d", got, lm.m.logStart)
	}
	if got := lm.l.StartTerm(); got != lm.m.startTerm {
		t.Fatalf("start term %d, model %d", got, lm.m.startTerm)
	}
	if last, term := lm.l.LastOffsetAndTerm(); last != lm.m.leo-1 || term != lm.m.lastTerm() {
		t.Fatalf("LastOffsetAndTerm = %d,%d; model %d,%d", last, term, lm.m.leo-1, lm.m.lastTerm())
	}
	got, err := lm.l.ReadBatches(lm.m.logStart, lm.m.leo, math.MaxInt32)
	if err != nil {
		t.Fatalf("ReadBatches: %v", err)
	}
	if len(got) != len(lm.m.batches) {
		t.Fatalf("log has %d batches, model %d", len(got), len(lm.m.batches))
	}
	for i := range got {
		if !bytes.Equal(got[i], lm.m.batches[i]) {
			t.Fatalf("batch %d differs from model", i)
		}
	}
}

func runLogMachine(t *rapid.T, cfg Config, crash bool) {
	lm := &logMachine{cfg: cfg, crash: crash}
	lm.m.leo = cfg.InitialOffset
	lm.m.logStart = cfg.InitialOffset
	lm.open(t)
	defer func() {
		if lm.l != nil {
			_ = lm.l.Close()
		}
	}()
	t.Repeat(map[string]func(*rapid.T){
		"append":         lm.Append,
		"append2":        lm.Append,
		"append3":        lm.Append,
		"roll":           lm.Roll,
		"sync":           lm.Sync,
		"reopen":         lm.Reopen,
		"crashReopen":    lm.CrashReopen,
		"truncateSuffix": lm.TruncateSuffix,
		"truncatePrefix": lm.TruncatePrefix,
		"read":           lm.Read,
		"termAt":         lm.TermAt,
		"":               lm.Check,
	})
}

func propertyConfig(t *rapid.T, fsys fs.FS, dir string) Config {
	return Config{
		Dir:                dir,
		FS:                 fsys,
		SegmentBytes:       int64(rapid.IntRange(200, 6000).Draw(t, "segmentBytes")),
		IndexIntervalBytes: rapid.IntRange(1, 2000).Draw(t, "indexInterval"),
		MaxIndexBytes:      rapid.IntRange(4*timeEntrySize, 600).Draw(t, "maxIndexBytes"),
		InitialOffset:      rapid.Int64Range(0, 1<<40).Draw(t, "initialOffset"),
		Sync:               SyncPolicy{Mode: SyncMode(rapid.IntRange(0, 2).Draw(t, "syncMode")), Interval: 1e6},
	}
}

// TestPropertyFaultFS: a random sequence of append/roll/reopen/crash/
// truncate/read operations returns exactly what the model says was appended.
// Iterations: -rapid.checks (default 100).
func TestPropertyFaultFS(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		seed := rapid.Uint64().Draw(t, "seed")
		runLogMachine(t, propertyConfig(t, fs.NewFaultFS(seed), "/p"), true)
	})
}

// TestPropertyOSFS runs the same machine on real files with mmap'd indexes.
// fsync is stubbed out (eatmydata-style) because this test checks file and
// mapping behaviour, not power-loss durability (the FaultFS crash tests do),
// and macOS F_FULLFSYNC would otherwise dominate the runtime.
func TestPropertyOSFS(t *testing.T) {
	root := t.TempDir()
	n := 0
	rapid.Check(t, func(rt *rapid.T) {
		n++
		dir := filepath.Join(root, fmt.Sprint(n))
		runLogMachine(rt, propertyConfig(rt, noSyncFS{fs.OS}, dir), false)
	})
}

// noSyncFS wraps a file system and turns Sync/SyncDir into no-ops, keeping
// the OSFile capability so the mmap index path is exercised.
type noSyncFS struct{ fs.FS }

func (n noSyncFS) OpenFile(name string, flag int, perm os.FileMode) (fs.File, error) {
	f, err := n.FS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return noSyncFile{f}, nil
}

func (n noSyncFS) Create(name string) (fs.File, error) {
	return n.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
}

func (n noSyncFS) Open(name string) (fs.File, error) { return n.OpenFile(name, os.O_RDONLY, 0) }

func (noSyncFS) SyncDir(string) error { return nil }

type noSyncFile struct{ fs.File }

func (noSyncFile) Sync() error { return nil }

func (f noSyncFile) OSFile() *os.File { return f.File.(fs.OSFile).OSFile() }
