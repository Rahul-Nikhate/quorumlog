package log

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/Rahul-Nikhate/quorumlog/internal/fs"
)

// benchBatch builds a batch of 1 KiB records totalling about size bytes.
func benchBatch(b *testing.B, size int) Batch {
	const recordValue = 1024 - 16 // ~1 KiB per encoded record
	n := max(size/1024, 1)
	recs := make([]Record, n)
	val := make([]byte, recordValue)
	for i := range recs {
		recs[i] = Record{Value: val}
	}
	opts := DefaultBatchOptions()
	opts.BaseTimestamp = 1_700_000_000_000
	bt, err := NewBatch(opts, recs...)
	if err != nil {
		b.Fatal(err)
	}
	return bt
}

// BenchmarkAppend measures single-goroutine Append throughput on the real file
// system (page cache; segments roll every 128 MiB) for 1/16/64 KiB batches.
//
//   - never:  write(2) only, the benchmark ceiling.
//   - always: Append + WaitDurable per batch, so every batch pays a full
//     fsync round trip (no group commit possible with one producer). On
//     macOS this is F_FULLFSYNC: do not compare with Linux fdatasync.
func BenchmarkAppend(b *testing.B) {
	for _, policy := range []SyncPolicy{{Mode: SyncNever}, {Mode: SyncAlways}} {
		for _, size := range []int{1 << 10, 16 << 10, 64 << 10} {
			b.Run(fmt.Sprintf("sync=%s/batch=%dKiB", policy, size>>10), func(b *testing.B) {
				l, err := Open(Config{Dir: b.TempDir(), FS: fs.OS, Sync: policy})
				if err != nil {
					b.Fatal(err)
				}
				defer l.Close()
				bt := benchBatch(b, size)
				b.SetBytes(int64(len(bt)))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					res, err := l.Append(bt, 1)
					if err != nil {
						b.Fatal(err)
					}
					if policy.Mode == SyncAlways {
						if err := l.WaitDurable(context.Background(), res.LastOffset); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.StopTimer()
			})
		}
	}
}

// BenchmarkAppendFaultFS isolates the log's CPU cost from the OS (in-memory FS).
func BenchmarkAppendFaultFS(b *testing.B) {
	for _, size := range []int{1 << 10, 16 << 10, 64 << 10} {
		b.Run(fmt.Sprintf("batch=%dKiB", size>>10), func(b *testing.B) {
			l, err := Open(Config{Dir: "/p", FS: fs.NewFaultFS(1), SegmentBytes: 64 << 20, Sync: SyncPolicy{Mode: SyncNever}})
			if err != nil {
				b.Fatal(err)
			}
			defer l.Close()
			bt := benchBatch(b, size)
			b.SetBytes(int64(len(bt)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := l.Append(bt, 1); err != nil {
					b.Fatal(err)
				}
				if i%2000 == 1999 {
					// Bound FaultFS memory: it keeps unsynced writes for crash
					// simulation.
					b.StopTimer()
					if _, err := l.TruncateSuffix(l.LogStartOffset()); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
			}
		})
	}
}

// BenchmarkGroupCommit: 64 concurrent producers under policy always; reports
// fsync rounds per batch.
func BenchmarkGroupCommit(b *testing.B) {
	l, err := Open(Config{Dir: b.TempDir(), FS: fs.OS, Sync: SyncPolicy{Mode: SyncAlways}})
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	bt := benchBatch(b, 1<<10)
	b.SetBytes(int64(len(bt)))
	before := l.Stats().Syncs
	b.SetParallelism(64 / max(1, testingProcs()))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		local := append(Batch(nil), bt...)
		for pb.Next() {
			res, err := l.Append(local, 1)
			if err != nil {
				b.Error(err)
				return
			}
			if err := l.WaitDurable(context.Background(), res.LastOffset); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(l.Stats().Syncs-before)/float64(b.N), "fsyncs/batch")
}

// BenchmarkRead measures Read (index lookup + header scan) plus copying the
// region out, at random offsets across a 64 MiB log of 16 KiB batches.
func BenchmarkRead(b *testing.B) {
	l, err := Open(Config{Dir: b.TempDir(), FS: fs.OS, SegmentBytes: 16 << 20, Sync: SyncPolicy{Mode: SyncNever}})
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	bt := benchBatch(b, 16<<10)
	for l.Stats().Bytes < 64<<20 {
		if _, err := l.Append(bt, 1); err != nil {
			b.Fatal(err)
		}
	}
	leo := l.LogEndOffset()
	b.SetBytes(1 << 20)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := int64(uint64(i)*2654435761) % leo
		r, err := l.Read(off, 1<<20)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := r.Bytes(); err != nil {
			b.Fatal(err)
		}
		r.Release()
	}
}

func BenchmarkValidate64KiB(b *testing.B) {
	bt := benchBatch(b, 64<<10)
	b.SetBytes(int64(len(bt)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := bt.Validate(1 << 20); err != nil {
			b.Fatal(err)
		}
	}
}

func testingProcs() int { return runtime.GOMAXPROCS(0) }
