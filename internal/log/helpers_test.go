package log

import (
	"bytes"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"testing"

	"go.uber.org/goleak"

	"github.com/Rahul-Nikhate/quorumlog/internal/fs"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// mkBatch builds an uncompressed batch of n records with values of valueSize
// bytes. The first value byte encodes tag so batches are distinguishable.
func mkBatch(t testing.TB, n, valueSize int, baseTS int64, tag byte) Batch {
	t.Helper()
	recs := make([]Record, n)
	for i := range recs {
		v := bytes.Repeat([]byte{tag}, valueSize)
		recs[i] = Record{TimestampDelta: int64(i), Key: []byte(fmt.Sprintf("k%d", i)), Value: v}
	}
	opts := DefaultBatchOptions()
	opts.BaseTimestamp = baseTS
	b, err := NewBatch(opts, recs...)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// testEnv is a log directory on either the real OS file system (exercising
// the mmap index path) or FaultFS (heap index path, crash simulation).
type testEnv struct {
	name string
	fsys fs.FS
	dir  string
}

func envs(t *testing.T) []testEnv {
	ff := NewFaultFSForTest(1)
	return []testEnv{
		{"os", fs.OS, t.TempDir()},
		{"faultfs", ff, "/p0"},
	}
}

// NewFaultFSForTest wraps fs.NewFaultFS for readability in tests.
func NewFaultFSForTest(seed uint64) *fs.FaultFS { return fs.NewFaultFS(seed) }

func forEachEnv(t *testing.T, fn func(t *testing.T, e testEnv)) {
	for _, e := range envs(t) {
		t.Run(e.name, func(t *testing.T) { fn(t, e) })
	}
}

func smallConfig(e testEnv) Config {
	return Config{
		Dir:                e.dir,
		FS:                 e.fsys,
		SegmentBytes:       4096,
		IndexIntervalBytes: 256,
		MaxIndexBytes:      1024,
		Sync:               SyncPolicy{Mode: SyncNever},
	}
}

func mustOpen(t testing.TB, cfg Config) *Log {
	t.Helper()
	l, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return l
}

// readAllBatches reads every batch in [LogStartOffset, LogEndOffset).
func readAllBatches(t testing.TB, l *Log) []Batch {
	t.Helper()
	bs, err := l.ReadBatches(l.LogStartOffset(), l.LogEndOffset(), math.MaxInt32)
	if err != nil {
		t.Fatalf("ReadBatches: %v", err)
	}
	return bs
}

// checkInvariants verifies that the readable log is a contiguous sequence of
// valid batches with non-decreasing terms that ends at LogEndOffset.
func checkInvariants(t testing.TB, l *Log) []Batch {
	t.Helper()
	bs := readAllBatches(t, l)
	next := int64(-1)
	var term uint64
	for i, b := range bs {
		if err := b.Validate(math.MaxInt32); err != nil {
			t.Fatalf("batch %d invalid: %v", i, err)
		}
		if next >= 0 && b.BaseOffset() != next {
			t.Fatalf("batch %d base %d, want %d", i, b.BaseOffset(), next)
		}
		if b.Term() < term {
			t.Fatalf("batch %d term %d regressed from %d", i, b.Term(), term)
		}
		term = b.Term()
		next = b.LastOffset() + 1
	}
	if len(bs) > 0 && next != l.LogEndOffset() {
		t.Fatalf("batches end at %d, LEO %d", next, l.LogEndOffset())
	}
	return bs
}

// newTestLogger returns a text logger writing to w (warnings and above).
func newTestLogger(w *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(&syncWriter{w: w}, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

type syncWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}
