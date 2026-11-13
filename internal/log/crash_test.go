package log

import (
	"bytes"
	"context"
	"flag"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/Rahul-Nikhate/quorumlog/internal/fs"
)

// -crash.iters overrides the iteration count of each crash test (the DoD run
// uses -crash.iters=10000).
var crashItersFlag = flag.Int("crash.iters", 0, "iterations per crash test (0 = default)")

func crashIters(def int) int {
	if *crashItersFlag > 0 {
		return *crashItersFlag
	}
	if testing.Short() {
		return max(def/10, 1)
	}
	return def
}

func randBatch(t testing.TB, rng *rand.Rand, tag int) Batch {
	n := rng.IntN(5)
	recs := make([]Record, n)
	for i := range recs {
		recs[i] = Record{
			TimestampDelta: int64(i),
			Key:            []byte{byte(tag), byte(i)},
			Value:          bytes.Repeat([]byte{byte(tag + i)}, rng.IntN(400)),
		}
	}
	opts := DefaultBatchOptions()
	opts.BaseTimestamp = int64(1_000_000 + tag)
	b, err := NewBatch(opts, recs...)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func randCrashConfig(rng *rand.Rand, fsys fs.FS, p SyncPolicy) Config {
	return Config{
		Dir:                "/data/p0",
		FS:                 fsys,
		SegmentBytes:       int64(300 + rng.IntN(4000)),
		IndexIntervalBytes: 1 + rng.IntN(800),
		MaxIndexBytes:      4*timeEntrySize + rng.IntN(400),
		Sync:               p,
	}
}

func isPrefix(got, model []Batch) bool {
	if len(got) > len(model) {
		return false
	}
	for i := range got {
		if !bytes.Equal(got[i], model[i]) {
			return false
		}
	}
	return true
}

// TestCrashRecoversExactlySyncedPrefix: with policy never, append N batches
// (with rolls, explicit Syncs and clean reopens in between), simulate power
// loss dropping every unsynced byte, reopen: the log must equal exactly what
// was appended up to the last successful Sync/Close.
func TestCrashRecoversExactlySyncedPrefix(t *testing.T) {
	n := crashIters(300)
	lossSeen := 0
	defer func() {
		t.Logf("%d iterations, %d lost unsynced batches", n, lossSeen)
		if n >= 100 && lossSeen < n/10 {
			t.Errorf("only %d of %d iterations had unsynced data to lose", lossSeen, n)
		}
	}()
	for it := 0; it < n; it++ {
		seed := uint64(it)
		rng := rand.New(rand.NewPCG(seed, 0xc0ffee))
		ff := fs.NewFaultFS(seed)
		cfg := randCrashConfig(rng, ff, SyncPolicy{Mode: SyncNever})
		l := mustOpen(t, cfg)
		var model, synced []Batch
		steps := 1 + rng.IntN(80)
		for s := 0; s < steps; s++ {
			switch r := rng.IntN(20); {
			case r < 14:
				b := randBatch(t, rng, s)
				if _, err := l.Append(b, 1); err != nil {
					t.Fatalf("seed %d: append: %v", seed, err)
				}
				model = append(model, bytes.Clone(b))
			case r < 16:
				if err := l.Roll(); err != nil {
					t.Fatal(err)
				}
			case r < 19:
				if err := l.Sync(); err != nil {
					t.Fatal(err)
				}
				synced = append([]Batch(nil), model...)
			default:
				if err := l.Close(); err != nil {
					t.Fatal(err)
				}
				synced = append([]Batch(nil), model...)
				l = mustOpen(t, cfg)
			}
		}
		cfg.FS = ff.Crash(fs.CrashDropUnsynced)
		abandon(l)
		l2, err := Open(cfg)
		if err != nil {
			t.Fatalf("seed %d: reopen after crash: %v", seed, err)
		}
		got := checkInvariants(t, l2)
		if len(got) != len(synced) || !isPrefix(got, synced) {
			t.Fatalf("seed %d: recovered %d batches, synced %d (appended %d)", seed, len(got), len(synced), len(model))
		}
		if len(synced) < len(model) {
			lossSeen++
		}
		// The recovered log accepts appends at the right offset.
		leo := l2.LogEndOffset()
		if res, err := l2.Append(randBatch(t, rng, 99), 1); err != nil || res.BaseOffset != leo {
			t.Fatalf("seed %d: append after recovery: %+v %v (LEO was %d)", seed, res, err, leo)
		}
		if err := l2.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestCrashTornWrites: like the exact test, but the crash persists a random
// in-order prefix of the unsynced writes and tears the next one at a random
// byte. The recovered log must be a prefix of what was appended that contains
// at least everything synced.
func TestCrashTornWrites(t *testing.T) {
	n := crashIters(300)
	tornSeen := 0
	for it := 0; it < n; it++ {
		seed := uint64(it) + 1<<32
		rng := rand.New(rand.NewPCG(seed, 0xbeef))
		ff := fs.NewFaultFS(seed)
		cfg := randCrashConfig(rng, ff, SyncPolicy{Mode: SyncNever})
		l := mustOpen(t, cfg)
		var model []Batch
		synced := 0
		steps := 1 + rng.IntN(80)
		for s := 0; s < steps; s++ {
			switch r := rng.IntN(20); {
			case r < 16:
				b := randBatch(t, rng, s)
				if _, err := l.Append(b, 1); err != nil {
					t.Fatal(err)
				}
				model = append(model, bytes.Clone(b))
			case r < 18:
				if err := l.Roll(); err != nil {
					t.Fatal(err)
				}
			default:
				if err := l.Sync(); err != nil {
					t.Fatal(err)
				}
				synced = len(model)
			}
		}
		cfg.FS = ff.Crash(fs.CrashTearUnsynced)
		abandon(l)
		l2, err := Open(cfg)
		if err != nil {
			t.Fatalf("seed %d: reopen after torn crash: %v", seed, err)
		}
		got := checkInvariants(t, l2)
		if len(got) < synced || !isPrefix(got, model) {
			t.Fatalf("seed %d: recovered %d batches; synced %d, appended %d, prefix=%v",
				seed, len(got), synced, len(model), isPrefix(got, model))
		}
		if len(got) > synced && len(got) < len(model) {
			tornSeen++
		}
		if err := l2.Close(); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("%d iterations, %d recovered a partial unsynced tail", n, tornSeen)
	if n >= 100 && tornSeen == 0 {
		t.Fatal("no iteration recovered a partial unsynced tail; the tear model is not being exercised")
	}
}

// TestCrashDurableWatermark: with group commit (always) or interval fsync and
// the background flusher running, mix appends, durability waits, Raft-style
// suffix truncations, rolls and reopens; then crash (drop or tear) at an
// arbitrary moment. Every batch at or below the DurableOffset observed before
// the crash must survive, and the recovered log must be a prefix of the
// current model (no resurrected truncated data, no garbage).
func TestCrashDurableWatermark(t *testing.T) {
	n := crashIters(200)
	var lossSeen, durableSeen int
	defer func() {
		t.Logf("%d iterations: %d lost a non-durable tail, %d had durable data", n, lossSeen, durableSeen)
		if n >= 100 && (lossSeen == 0 || durableSeen < n/4) {
			t.Errorf("crash scenarios not exercised: loss %d, durable %d", lossSeen, durableSeen)
		}
	}()
	policies := []SyncPolicy{{Mode: SyncAlways}, {Mode: SyncInterval, Interval: 200 * time.Microsecond}}
	for it := 0; it < n; it++ {
		seed := uint64(it) + 2<<32
		rng := rand.New(rand.NewPCG(seed, 0xd00d))
		ff := fs.NewFaultFS(seed)
		if rng.IntN(4) == 0 {
			ff.SetFaults(fs.Faults{SyncLatency: time.Duration(rng.IntN(300)) * time.Microsecond})
		}
		cfg := randCrashConfig(rng, ff, policies[it%len(policies)])
		l := mustOpen(t, cfg)
		var model []Batch
		term := uint64(1)
		steps := 1 + rng.IntN(60)
		for s := 0; s < steps; s++ {
			switch r := rng.IntN(20); {
			case r < 13:
				b := randBatch(t, rng, s)
				if _, err := l.Append(b, term); err != nil {
					t.Fatal(err)
				}
				model = append(model, bytes.Clone(b))
			case r < 15:
				if len(model) > 0 {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					err := l.WaitDurable(ctx, model[len(model)-1].LastOffset())
					cancel()
					if err != nil {
						t.Fatalf("seed %d: WaitDurable: %v", seed, err)
					}
				}
			case r < 17:
				if len(model) > 0 {
					k := rng.IntN(len(model))
					if _, err := l.TruncateSuffix(model[k].BaseOffset()); err != nil {
						t.Fatalf("seed %d: TruncateSuffix: %v", seed, err)
					}
					model = model[:k]
					term++
				}
			case r < 18:
				if err := l.Roll(); err != nil {
					t.Fatal(err)
				}
			case r < 19:
				if err := l.Close(); err != nil {
					t.Fatal(err)
				}
				l = mustOpen(t, cfg)
			default:
				time.Sleep(time.Duration(rng.IntN(300)) * time.Microsecond)
			}
		}
		durable := l.DurableOffset()
		mode := fs.CrashDropUnsynced
		if rng.IntN(2) == 0 {
			mode = fs.CrashTearUnsynced
		}
		cfg.FS = ff.Crash(mode)
		abandon(l)
		cfg.FS.(*fs.FaultFS).SetFaults(fs.Faults{})
		l2, err := Open(cfg)
		if err != nil {
			t.Fatalf("seed %d: reopen: %v", seed, err)
		}
		got := checkInvariants(t, l2)
		if !isPrefix(got, model) {
			t.Fatalf("seed %d: recovered log (%d batches) is not a prefix of the model (%d)", seed, len(got), len(model))
		}
		if len(got) < len(model) {
			lossSeen++
		}
		if durable >= 0 {
			durableSeen++
		}
		for _, b := range model[len(got):] {
			if b.LastOffset() <= durable {
				t.Fatalf("seed %d: lost durable batch [%d,%d] (durable offset %d, recovered %d of %d)",
					seed, b.BaseOffset(), b.LastOffset(), durable, len(got), len(model))
			}
		}
		if err := l2.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestCrashAfterPrefixTruncationAndRetention: crash right after TruncatePrefix
// or ApplyRetention: the log start never moves backwards past durable state,
// and data below it never reappears.
func TestCrashAfterPrefixTruncationAndRetention(t *testing.T) {
	n := crashIters(100)
	for it := 0; it < n; it++ {
		seed := uint64(it) + 3<<32
		rng := rand.New(rand.NewPCG(seed, 7))
		ff := fs.NewFaultFS(seed)
		cfg := randCrashConfig(rng, ff, SyncPolicy{Mode: SyncAlways})
		l := mustOpen(t, cfg)
		var model []Batch
		for s := 0; s < 40; s++ {
			b := randBatch(t, rng, s)
			if _, err := l.Append(b, 1); err != nil {
				t.Fatal(err)
			}
			model = append(model, bytes.Clone(b))
		}
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
		if rng.IntN(2) == 0 {
			k := rng.IntN(len(model))
			if err := l.TruncatePrefix(model[k].BaseOffset()); err != nil {
				t.Fatal(err)
			}
		} else if _, err := l.ApplyRetention(RetentionPolicy{MaxBytes: int64(1 + rng.IntN(5000))}, math.MaxInt64); err != nil {
			t.Fatal(err)
		}
		start := l.LogStartOffset()
		cfg.FS = ff.Crash(fs.CrashTearUnsynced)
		abandon(l)
		l2, err := Open(cfg)
		if err != nil {
			t.Fatalf("seed %d: reopen: %v", seed, err)
		}
		if l2.LogStartOffset() != start {
			t.Fatalf("seed %d: log start %d after crash, want %d", seed, l2.LogStartOffset(), start)
		}
		got := checkInvariants(t, l2)
		var want []Batch
		for _, b := range model {
			if b.LastOffset() >= start {
				want = append(want, b)
			}
		}
		equalBatches(t, got, want)
		if err := l2.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
