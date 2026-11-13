package rafttest

import (
	"errors"
	"testing"

	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft/transport"
)

// 6.5840 3B "basic agreement".
func TestBasicAgree(t *testing.T) {
	forSeeds(t, 60, func(t *testing.T, seed uint64) {
		c := New(t, baseOpts(seed, 5))
		mustLeader(t, c, nil)
		prev := uint64(0)
		for range 10 {
			idx := c.One(5, false, leaderWait)
			if idx <= prev {
				c.fatalf("index did not advance: %d after %d", idx, prev)
			}
			prev = idx
		}
	})
}

// Proposals on a follower fail with ErrNotLeader and a leader hint.
func TestProposeNotLeader(t *testing.T) {
	forSeeds(t, 15, func(t *testing.T, seed uint64) {
		c := New(t, baseOpts(seed, 3))
		l := mustLeader(t, c, nil)
		f := others(c.IDs(), l)[0]
		_, err := c.Node(f).Propose([]byte("x"))
		var nle *raft.NotLeaderError
		if !errors.Is(err, raft.ErrNotLeader) || !errors.As(err, &nle) || nle.Leader != l {
			c.fatalf("follower Propose: got %v, want NotLeaderError{Leader: %d}", err, l)
		}
	})
}

// 6.5840 3B "agreement despite follower disconnection" and progressive
// failure of followers.
func TestFollowerFailure(t *testing.T) {
	forSeeds(t, 60, func(t *testing.T, seed uint64) {
		c := New(t, baseOpts(seed, 3))
		all := c.IDs()
		c.One(3, false, leaderWait)
		l := mustLeader(t, c, nil)
		f1 := others(all, l)[0]
		c.Disconnect(f1)
		c.One(2, false, leaderWait)
		c.One(2, false, leaderWait)
		c.Connect(f1)
		c.One(3, true, leaderWait)

		// Progressive failure: disconnect both followers of the current leader.
		l = mustLeader(t, c, nil)
		fs := others(all, l)
		c.Disconnect(fs[0])
		c.One(2, true, leaderWait)
		c.Disconnect(fs[1])
		pid, e, ok := c.Propose(l)
		if !ok {
			c.fatalf("leader N%d refused proposal", l)
		}
		c.RunTicks(4 * c.opts.ElectionTick)
		if _, ok := c.CommittedIndex(pid); ok {
			c.fatalf("entry %v committed without a majority", e)
		}
		c.Heal()
		c.One(3, true, leaderWait)
	})
}

// 6.5840 3B "test failure of leaders".
func TestLeaderFailure(t *testing.T) {
	forSeeds(t, 60, func(t *testing.T, seed uint64) {
		c := New(t, baseOpts(seed, 3))
		all := c.IDs()
		c.One(3, false, leaderWait)
		l1 := mustLeader(t, c, nil)
		c.Disconnect(l1)
		c.One(2, true, leaderWait)
		c.One(2, true, leaderWait)
		l2 := mustLeader(t, c, others(all, l1))
		c.Disconnect(l2)
		var pids []uint64
		for _, id := range all {
			if pid, _, ok := c.Propose(id); ok {
				pids = append(pids, pid)
			}
		}
		c.RunTicks(4 * c.opts.ElectionTick)
		for _, pid := range pids {
			if _, ok := c.CommittedIndex(pid); ok {
				c.fatalf("proposal %d committed with only one connected node", pid)
			}
		}
		c.Heal()
		c.One(3, true, leaderWait)
	})
}

// 6.5840 3B "no agreement if too many followers disconnect"; the uncommitted
// entry may be overwritten after reconnection.
func TestFailNoAgree(t *testing.T) {
	forSeeds(t, 60, func(t *testing.T, seed uint64) {
		c := New(t, baseOpts(seed, 5))
		all := c.IDs()
		c.One(5, false, leaderWait)
		l := mustLeader(t, c, nil)
		fs := others(all, l)
		for _, f := range fs[:3] {
			c.Disconnect(f)
		}
		pid, _, ok := c.Propose(l)
		if !ok {
			c.fatalf("leader refused proposal")
		}
		c.RunTicks(4 * c.opts.ElectionTick)
		if _, ok := c.CommittedIndex(pid); ok {
			c.fatalf("committed with 2 of 5")
		}
		for _, f := range fs[:3] {
			c.Connect(f)
		}
		mustLeader(t, c, nil)
		c.One(5, true, leaderWait)
	})
}

// 6.5840 3B "concurrent Start()s": many proposals in the same tick all commit.
func TestConcurrentProposals(t *testing.T) {
	forSeeds(t, 60, func(t *testing.T, seed uint64) {
		c := New(t, baseOpts(seed, 3))
	retry:
		for attempt := 0; attempt < 5; attempt++ {
			l := mustLeader(t, c, nil)
			term := c.Term(l)
			var pids []uint64
			for range 20 {
				pid, _, ok := c.Propose(l)
				if !ok {
					continue retry
				}
				pids = append(pids, pid)
			}
			for _, pid := range pids {
				if _, ok := c.WaitCommitted(pid, 3, leaderWait); !ok {
					if c.Term(l) != term {
						continue retry // leadership changed: try again
					}
					c.fatalf("proposal %d not committed", pid)
				}
			}
			return
		}
		c.fatalf("term changed too often")
	})
}

// 6.5840 3B "rejoin of partitioned leader": the old leader's uncommitted
// entries are discarded when it rejoins.
func TestRejoin(t *testing.T) {
	forSeeds(t, 60, func(t *testing.T, seed uint64) {
		c := New(t, baseOpts(seed, 3))
		all := c.IDs()
		c.One(3, true, leaderWait)
		l1 := mustLeader(t, c, nil)
		c.Disconnect(l1)
		for range 3 {
			c.Propose(l1) // never committed
		}
		c.One(2, true, leaderWait)
		l2 := mustLeader(t, c, others(all, l1))
		c.Disconnect(l2)
		c.Connect(l1)
		c.One(2, true, leaderWait)
		c.Connect(l2)
		c.One(3, true, leaderWait)
	})
}

// 6.5840 3B "leader backs up quickly over incorrect follower logs": divergent
// logs of 50 entries are repaired within a bounded time thanks to the
// ConflictTerm/ConflictIndex hints.
func TestBackup(t *testing.T) {
	forSeeds(t, 30, func(t *testing.T, seed uint64) {
		o := baseOpts(seed, 5)
		c := New(t, o)
		all := c.IDs()
		c.One(5, true, leaderWait)
		l1 := mustLeader(t, c, nil)
		rest := others(all, l1)

		// Leader + 1 follower in a minority: 50 uncommitted entries.
		c.Partition([]raft.NodeID{l1, rest[0]}, rest[1:])
		for range 50 {
			c.Propose(l1)
		}
		c.RunTicks(c.opts.ElectionTick)

		// The majority commits 50 entries of its own.
		for range 50 {
			c.One(3, true, leaderWait)
		}
		l2 := mustLeader(t, c, rest[1:])
		other := others(rest[1:], l2)[0]

		// Leader 2 + one follower: another 50 uncommitted entries.
		c.Partition([]raft.NodeID{l2, other}, others(all, l2, other))
		for range 50 {
			c.Propose(l2)
		}
		c.RunTicks(c.opts.ElectionTick)

		// The original pair + the third member of the majority: they must
		// elect the up-to-date one and repair the stale pair quickly.
		for range 50 {
			c.One(3, true, leaderWait)
		}
		c.Heal()
		start := c.Now()
		c.One(5, true, leaderWait)
		if took := c.Now() - start; took > 10*uint64(c.opts.ElectionTick) {
			c.fatalf("backup took %d ticks", took)
		}
	})
}

// 6.5840 3B "RPC counts aren't too high": an idle leader sends one heartbeat
// per follower per heartbeat interval, and agreement on 10 entries does not
// need a flood of messages.
func TestMessageCount(t *testing.T) {
	forSeeds(t, 15, func(t *testing.T, seed uint64) {
		o := baseOpts(seed, 3)
		o.Async = false
		c := New(t, o)
		mustLeader(t, c, nil)
		c.One(3, false, leaderWait)
		c.RunTicks(5)
		before := c.Net.Stats().Sent
		const idle = 50
		c.RunTicks(idle)
		sent := c.Net.Stats().Sent - before
		// heartbeat + response per follower per tick, small slack.
		if limit := uint64(2 * 2 * idle * 12 / 10); sent > limit {
			c.fatalf("idle cluster sent %d messages in %d ticks (limit %d)", sent, idle, limit)
		}
		before = c.Net.SentByType(raft.MsgApp)
		for range 10 {
			c.One(3, false, leaderWait)
		}
		if apps := c.Net.SentByType(raft.MsgApp) - before; apps > 10*2*3 {
			c.fatalf("10 agreements needed %d MsgApps", apps)
		}
	})
}

func unreliable() *transport.LinkConfig {
	return &transport.LinkConfig{
		DropRate: 0.1, DupRate: 0.02, MinDelay: 1, MaxDelay: 8,
		ReorderRate: 0.1, ReorderDelay: 60,
	}
}

// 6.5840 3C "unreliable agreement": loss, duplication, delay and heavy
// reordering; every proposal eventually commits.
func TestUnreliableAgree(t *testing.T) {
	forSeeds(t, 60, func(t *testing.T, seed uint64) {
		o := baseOpts(seed, 5)
		o.Link = unreliable()
		c := New(t, o)
		for iter := 0; iter < 10; iter++ {
			for range 4 {
				if l := c.Leader(); l != raft.None {
					c.Propose(l)
				}
				c.RunTicks(1)
			}
			c.One(1, true, 4*leaderWait)
		}
		c.SetLink(transport.LinkConfig{MinDelay: 1, MaxDelay: 2})
		c.One(5, true, 4*leaderWait)
		t.Log(c.Summary())
	})
}
