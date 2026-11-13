package rafttest

import (
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
)

// Seed control:
//
//	RAFT_SEEDS=N      run N seeds per randomized test (default: per-test value)
//	RAFT_SEED=S       run only seed S (reproduce a failure)
//	RAFT_SEED_BASE=B  first seed (default 1)
//	RAFT_DEBUG=1      print the per-node debug log to stderr
//	RAFT_DISK=K       storage: mem, faultfs, os, all, or unset (default: every
//	                  seed on mem, plus the first max(2, N/6) seeds on faultfs
//	                  — the real segmented log with power-loss crashes — and
//	                  the first max(1, N/30) on os, the real log in a temp dir)
func envInt(name string, def uint64) uint64 {
	if v := os.Getenv(name); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			panic(fmt.Sprintf("%s=%q: %v", name, v, err))
		}
		return n
	}
	return def
}

var seedRuns atomic.Int64 // total seed runs across all tests, reported by TestMain

// forSeeds runs f once per seed as parallel subtests named seed=N. A panic
// inside raft (an internal invariant assertion) is reported with its seed.
func forSeeds(t *testing.T, defSeeds uint64, f func(t *testing.T, seed uint64)) {
	t.Helper()
	inner := f
	f = func(t *testing.T, seed uint64) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic (rerun with RAFT_SEED=%d [RAFT_DEBUG=1]): %v\n%s", seed, r, debug.Stack())
			}
		}()
		inner(t, seed)
	}
	n := envInt("RAFT_SEEDS", defSeeds)
	base := envInt("RAFT_SEED_BASE", 1)
	type plan struct {
		kind DiskKind
		n    uint64
	}
	var plans []plan
	switch d := os.Getenv("RAFT_DISK"); d {
	case "":
		plans = []plan{{DiskMem, n}, {DiskFaultFS, max(2, n/6)}, {DiskOS, max(1, n/30)}}
	case "all":
		plans = []plan{{DiskMem, n}, {DiskFaultFS, n}, {DiskOS, n}}
	case "mem":
		plans = []plan{{DiskMem, n}}
	case "faultfs":
		plans = []plan{{DiskFaultFS, n}}
	case "os":
		plans = []plan{{DiskOS, n}}
	default:
		panic(fmt.Sprintf("RAFT_DISK=%q: want mem, faultfs, os or all", d))
	}
	single := os.Getenv("RAFT_SEED") != ""
	for _, pl := range plans {
		run := func(seed uint64, parallel bool) {
			name := fmt.Sprintf("seed=%d", seed)
			if pl.kind != DiskMem {
				name = pl.kind.String() + "/" + name
			}
			t.Run(name, func(t *testing.T) {
				if parallel {
					t.Parallel()
				}
				if pl.kind != DiskMem {
					diskByTest.Store(t, pl.kind)
					defer diskByTest.Delete(t)
				}
				f(t, seed)
				seedRuns.Add(1)
				if pl.kind != DiskMem {
					logSeedRuns.Add(1)
				}
			})
		}
		if single {
			run(envInt("RAFT_SEED", 0), false)
			continue
		}
		for i := uint64(0); i < pl.n; i++ {
			run(base+i, true)
		}
	}
}

var logSeedRuns atomic.Int64 // seed runs on the real segmented log (faultfs/os)

func TestMain(m *testing.M) {
	code := m.Run()
	if testing.Verbose() || os.Getenv("RAFT_SEEDS") != "" {
		fmt.Printf("rafttest: %d seeded cluster runs completed (%d on the real segmented log)\n", seedRuns.Load(), logSeedRuns.Load())
	}
	os.Exit(code)
}

func ids(xs ...int) []raft.NodeID {
	out := make([]raft.NodeID, len(xs))
	for i, x := range xs {
		out[i] = raft.NodeID(x)
	}
	return out
}

func others(all []raft.NodeID, except ...raft.NodeID) []raft.NodeID {
	var out []raft.NodeID
outer:
	for _, id := range all {
		for _, e := range except {
			if id == e {
				continue outer
			}
		}
		out = append(out, id)
	}
	return out
}
