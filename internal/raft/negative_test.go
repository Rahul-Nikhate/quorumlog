package raft_test

import (
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"

	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft/rafttest"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft/transport"
)

// Negative controls (spec §13.5 "a checker that can't fail"): each test
// re-enables one classic Raft bug through a test-only knob and asserts that
// the rafttest invariant checkers catch it within a bounded number of seeds.
// If one of these tests fails, the harness has lost its teeth.

var safetyMarkers = []string{
	"ELECTION SAFETY", "LOG MATCHING", "STATE MACHINE SAFETY", "LEADER COMPLETENESS",
	"LINEARIZABILITY", "raft: ", // raft's own invariant panics
}

// detect runs scenario under the knob for up to maxSeeds seeds and returns
// the seed and message of the first safety violation.
func detect(t *testing.T, knob raft.Knob, maxSeeds uint64, opts func(seed uint64) rafttest.Options,
	scenario func(c *rafttest.Cluster, rng *rand.Rand)) (uint64, string, bool) {
	t.Helper()
	for seed := uint64(1); seed <= maxSeeds; seed++ {
		msg, violated := func() (msg string, violated bool) {
			defer func() {
				if r := recover(); r != nil {
					msg, violated = fmt.Sprint(r), true
				}
			}()
			o := opts(seed)
			o.PanicOnViolation = true
			if knob >= 0 {
				o.ConfigHook = func(c *raft.Config) { raft.SetKnob(c, knob) }
			}
			c := rafttest.New(t, o)
			scenario(c, rand.New(rand.NewPCG(seed, 7)))
			return "", false
		}()
		if !violated {
			continue
		}
		for _, mk := range safetyMarkers {
			if strings.Contains(msg, mk) {
				return seed, msg, true
			}
		}
		// A liveness failure (e.g. "no agreement") is not proof of detection.
	}
	return 0, "", false
}

// noKnob runs a scenario without any unsafe knob.
const noKnob raft.Knob = -1

func requireDetected(t *testing.T, knob raft.Knob, maxSeeds uint64, opts func(uint64) rafttest.Options,
	scenario func(*rafttest.Cluster, *rand.Rand)) {
	t.Helper()
	seed, msg, ok := detect(t, knob, maxSeeds, opts, scenario)
	if !ok {
		t.Fatalf("%v: no safety violation detected in %d seeds — the checkers cannot see this bug", knob, maxSeeds)
	}
	first := msg
	if i := strings.IndexByte(first, '\n'); i > 0 {
		first = first[:i]
	}
	t.Logf("%v detected at seed %d: %s", knob, seed, first)
	if os.Getenv("RAFT_DEBUG") != "" {
		t.Log(msg)
	}
}

func leaderOf(c *rafttest.Cluster, timeout int) raft.NodeID {
	l, _ := c.StableLeader(nil, timeout)
	return l
}

// rejoinScenario: an isolated leader accumulates uncommitted entries, the
// majority moves on, then the old leader rejoins (TestRejoin shape).
func rejoinScenario(c *rafttest.Cluster, rng *rand.Rand) {
	ids := c.IDs()
	c.One(len(ids), true, 200)
	l1 := leaderOf(c, 200)
	c.Disconnect(l1)
	for range 5 {
		c.Propose(l1)
	}
	c.RunTicks(5)
	for range 3 {
		c.One(len(ids)-1, true, 200)
	}
	c.Connect(l1)
	for range 3 {
		c.One(len(ids), true, 400)
	}
}

// chaosScenario: Figure 8 (unreliable) style churn.
func chaosScenario(c *rafttest.Cluster, rng *rand.Rand) {
	ids := c.IDs()
	n := len(ids)
	for range 300 {
		leader := raft.None
		for _, id := range ids {
			if rng.IntN(2) == 0 {
				if _, _, ok := c.Propose(id); ok {
					leader = id
				}
			}
		}
		c.RunTicks(rng.IntN(4))
		switch r := rng.IntN(100); {
		case r < 25 && leader != raft.None:
			c.Crash(leader)
		case r < 35:
			c.Disconnect(ids[rng.IntN(n)])
		case r < 45:
			c.Heal()
		}
		for _, id := range ids {
			if !c.Alive(id) && rng.IntN(100) < 30 {
				c.Restart(id)
			}
		}
	}
	c.Heal()
	for _, id := range ids {
		c.Restart(id)
	}
	c.One(n, true, 2000)
}

func staleReadScenario(c *rafttest.Cluster, rng *rand.Rand) {
	ids := c.IDs()
	c.One(len(ids), true, 200)
	l := leaderOf(c, 200)
	c.Disconnect(l)
	for range 3 {
		c.One(len(ids)-1, true, 400)
	}
	_, _ = c.ReadIndex(l)
	c.RunTicks(20)
}

func optsFor(n int, mutate func(*rafttest.Options)) func(uint64) rafttest.Options {
	return func(seed uint64) rafttest.Options {
		o := rafttest.Options{N: n, Seed: seed}
		if mutate != nil {
			mutate(&o)
		}
		return o
	}
}

func unreliableLink() *transport.LinkConfig {
	return &transport.LinkConfig{DropRate: 0.1, DupRate: 0.02, MinDelay: 1, MaxDelay: 8, ReorderRate: 0.2, ReorderDelay: 60}
}

func TestNegativeControlReadIndexWithoutQuorum(t *testing.T) {
	requireDetected(t, raft.KnobReadIndexNoQuorum, 10, optsFor(3, nil), staleReadScenario)
}

func TestNegativeControlHeartbeatFullCommit(t *testing.T) {
	requireDetected(t, raft.KnobHeartbeatFullCommit, 50, optsFor(3, nil), rejoinScenario)
}

func TestNegativeControlVoteIgnoresLog(t *testing.T) {
	requireDetected(t, raft.KnobVoteIgnoreLog, 50, optsFor(5, nil), chaosScenario)
}

func TestNegativeControlCommitOldTerms(t *testing.T) {
	requireDetected(t, raft.KnobCommitOldTerms, 300, optsFor(5, func(o *rafttest.Options) {
		o.MaxSizePerMsg = 1 // one entry per MsgApp: old-term entries are acked alone
		o.Link = unreliableLink()
	}), chaosScenario)
}

func TestNegativeControlTruncateOnAppend(t *testing.T) {
	requireDetected(t, raft.KnobTruncateOnAppend, 300, optsFor(5, func(o *rafttest.Options) {
		o.Link = unreliableLink()
	}), chaosScenario)
}

// Lease reads are unsafe under clock-rate skew (spec §5.13): with the
// leader's clock running slow, the majority elects a new leader and commits
// while the old leader still believes its lease is valid and serves a stale
// read. The harness must flag it. (No knob: this is the documented, real
// weakness of Config.UnsafeLeaseReads.)
func TestNegativeControlLeaseReadsUnderClockSkew(t *testing.T) {
	requireDetected(t, noKnob, 20, optsFor(3, func(o *rafttest.Options) {
		o.CheckQuorum = true
		o.UnsafeLeaseReads = true
	}), skewedLeaderReadScenario)
	// The same scenario with ReadIndex (no lease) is safe on every seed.
	if seed, msg, bad := detect(t, noKnob, 20, optsFor(3, func(o *rafttest.Options) { o.CheckQuorum = true }), skewedLeaderReadScenario); bad {
		t.Fatalf("ReadIndex violated linearizability under clock skew at seed %d: %s", seed, msg)
	}
}

func skewedLeaderReadScenario(c *rafttest.Cluster, _ *rand.Rand) {
	c.One(3, true, 200)
	l := leaderOf(c, 200)
	c.SetTickSkip(l, 0.9) // the leader's clock runs at ~1/10 speed
	c.RunTicks(1)
	c.Disconnect(l)
	for range 3 {
		c.One(2, true, 400)
	}
	for range 5 {
		_, _ = c.ReadIndex(l)
		c.RunTicks(1)
	}
}
