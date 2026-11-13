package rafttest

import (
	"testing"

	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
)

// baseOpts varies the feature flags across seeds so that every randomized
// test covers all combinations of pre-vote, check-quorum, async durability
// and single- vs multi-offset (batch) entries.
func baseOpts(seed uint64, n int) Options {
	return Options{
		N:           n,
		Seed:        seed,
		PreVote:     seed&1 == 1,
		CheckQuorum: seed&2 == 2,
		Async:       seed&4 == 4,
		MaxSpan:     []uint64{1, 4}[(seed>>3)&1],
	}
}

const leaderWait = 200 // ticks (20 election timeouts)

func mustLeader(t *testing.T, c *Cluster, among []raft.NodeID) raft.NodeID {
	t.Helper()
	l, ok := c.StableLeader(among, leaderWait)
	if !ok {
		c.fatalf("no stable leader among %v (leaders=%v)", among, c.Leaders(among))
	}
	return l
}

// 6.5840 3A "initial election": a leader is elected, and with a healthy
// network the term stays stable.
func TestInitialElection(t *testing.T) {
	forSeeds(t, 60, func(t *testing.T, seed uint64) {
		c := New(t, baseOpts(seed, 3))
		l := mustLeader(t, c, nil)
		term := c.Term(l)
		if term < 1 {
			c.fatalf("term %d after election", term)
		}
		c.RunTicks(10 * c.opts.ElectionTick)
		if l2 := mustLeader(t, c, nil); l2 != l || c.Term(l2) != term {
			c.fatalf("leader/term changed without failures: N%d t%d → N%d t%d", l, term, l2, c.Term(l2))
		}
	})
}

// 6.5840 3A "election after network failure".
func TestReElection(t *testing.T) {
	forSeeds(t, 60, func(t *testing.T, seed uint64) {
		c := New(t, baseOpts(seed, 3))
		all := c.IDs()
		l1 := mustLeader(t, c, nil)

		// Leader disconnects: a new one must be elected.
		c.Disconnect(l1)
		l2 := mustLeader(t, c, others(all, l1))

		// Old leader rejoins: it must not disturb the new leader's authority
		// (it may trigger a new election, but there must be one leader).
		c.Connect(l1)
		l3 := mustLeader(t, c, nil)

		// No quorum: no leader among the remaining node.
		rest := others(all, l3)
		c.Disconnect(l3)
		c.Disconnect(rest[0])
		c.RunTicks(10 * c.opts.ElectionTick)
		if ls := c.Leaders(rest[1:]); len(ls) != 0 {
			c.fatalf("leader %v elected without quorum", ls)
		}

		// Quorum restored: a leader is elected.
		c.Connect(rest[0])
		mustLeader(t, c, rest)

		// Everyone back.
		c.Connect(l3)
		mustLeader(t, c, nil)
		_ = l2
	})
}

// 6.5840 3A "multiple elections": 7 nodes, repeated random disconnections of
// up to 3 nodes; a majority always elects a leader.
func TestManyElections(t *testing.T) {
	forSeeds(t, 30, func(t *testing.T, seed uint64) {
		c := New(t, baseOpts(seed, 7))
		all := c.IDs()
		mustLeader(t, c, nil)
		for range 8 {
			perm := c.rng.Perm(7)
			down := []raft.NodeID{all[perm[0]], all[perm[1]], all[perm[2]]}
			for _, id := range down {
				c.Disconnect(id)
			}
			mustLeader(t, c, others(all, down...))
			for _, id := range down {
				c.Connect(id)
			}
		}
		mustLeader(t, c, nil)
	})
}

// Pre-vote (thesis §9.6): a partitioned follower must not inflate its term,
// so it cannot depose the healthy leader when it rejoins. The negative
// control (pre-vote off) shows the disruption the feature prevents.
func TestPreVotePreventsTermInflation(t *testing.T) {
	for _, preVote := range []bool{true, false} {
		name := map[bool]string{true: "prevote", false: "control-no-prevote"}[preVote]
		t.Run(name, func(t *testing.T) {
			forSeeds(t, 30, func(t *testing.T, seed uint64) {
				o := baseOpts(seed, 3)
				o.PreVote = preVote
				c := New(t, o)
				l := mustLeader(t, c, nil)
				term := c.Term(l)
				f := others(c.IDs(), l)[0]
				c.Disconnect(f)
				c.RunTicks(20 * c.opts.ElectionTick)
				if preVote && c.Term(f) != term {
					c.fatalf("isolated follower inflated its term %d → %d despite pre-vote", term, c.Term(f))
				}
				if !preVote && c.Term(f) <= term+5 {
					c.fatalf("control: expected term inflation without pre-vote, got %d → %d", term, c.Term(f))
				}
				c.Connect(f)
				c.RunTicks(10 * c.opts.ElectionTick)
				l2 := mustLeader(t, c, nil)
				if preVote && (l2 != l || c.Term(l2) != term) {
					c.fatalf("rejoining follower disrupted the leader: N%d t%d → N%d t%d", l, term, l2, c.Term(l2))
				}
				if !preVote && c.Term(l2) <= term {
					c.fatalf("control: expected the rejoining node to force a new term (still t%d)", c.Term(l2))
				}
				c.One(3, true, leaderWait)
			})
		})
	}
}

// Check-quorum (spec §5.4.5): a leader cut off from the majority steps down
// within ~2 election timeouts. Control: without it, it stays leader forever.
func TestCheckQuorumStepDown(t *testing.T) {
	for _, cq := range []bool{true, false} {
		name := map[bool]string{true: "checkquorum", false: "control-no-checkquorum"}[cq]
		t.Run(name, func(t *testing.T) {
			forSeeds(t, 30, func(t *testing.T, seed uint64) {
				o := baseOpts(seed, 5)
				o.CheckQuorum = cq
				c := New(t, o)
				all := c.IDs()
				l := mustLeader(t, c, nil)
				// Leader + one follower in the minority.
				f := others(all, l)[0]
				c.Partition([]raft.NodeID{l, f}, others(all, l, f))
				stepped := c.RunUntil(2*c.opts.ElectionTick+2, func() bool { return !c.isLeader(l) })
				if cq && !stepped {
					c.fatalf("minority leader N%d did not step down within 2 election timeouts", l)
				}
				if !cq {
					c.RunTicks(10 * c.opts.ElectionTick)
					if !c.isLeader(l) {
						c.fatalf("control: leader stepped down without check-quorum")
					}
				}
				mustLeader(t, c, others(all, l, f))
				c.Heal()
				mustLeader(t, c, nil)
				c.One(5, true, leaderWait)
			})
		})
	}
}

// Leadership transfer (MsgTimeoutNow): to an up-to-date follower, to a
// lagging follower (caught up first), and to an unreachable one (aborts).
func TestLeadershipTransfer(t *testing.T) {
	forSeeds(t, 30, func(t *testing.T, seed uint64) {
		o := baseOpts(seed, 3)
		o.PreVote = true // a rejoining lagging target must not disrupt by itself
		c := New(t, o)
		all := c.IDs()
		l := mustLeader(t, c, nil)
		c.One(3, false, leaderWait)

		// 1. Up-to-date target.
		target := others(all, l)[0]
		term := c.Term(l)
		if err := c.Node(l).TransferLeadership(target); err != nil {
			c.fatalf("transfer: %v", err)
		}
		if !c.RunUntil(3*c.opts.ElectionTick, func() bool { return c.isLeader(target) }) {
			c.fatalf("transfer to N%d did not complete", target)
		}
		if c.Term(target) != term+1 {
			c.fatalf("transfer should take exactly one term: t%d → t%d", term, c.Term(target))
		}
		mustLeader(t, c, nil)

		// 2. Lagging target: it misses entries, then must be caught up.
		l = target
		lag := others(all, l)[0]
		c.Disconnect(lag)
		for range 10 {
			c.One(2, false, leaderWait)
		}
		c.Connect(lag)
		if err := c.Node(l).TransferLeadership(lag); err != nil {
			c.fatalf("transfer: %v", err)
		}
		if _, _, ok := c.Propose(l); ok {
			c.fatalf("proposal accepted during transfer")
		}
		if !c.RunUntil(5*c.opts.ElectionTick, func() bool { return c.isLeader(lag) }) {
			c.fatalf("transfer to lagging N%d did not complete", lag)
		}
		l = mustLeader(t, c, nil)

		// 3. Unreachable target: transfer aborts, the leader keeps leading.
		dead := others(all, l)[0]
		c.Disconnect(dead)
		_ = c.Node(l).TransferLeadership(dead)
		if _, _, ok := c.Propose(l); ok {
			c.fatalf("proposal accepted during transfer")
		}
		c.RunTicks(c.opts.ElectionTick + 1)
		if !c.isLeader(l) {
			c.fatalf("leader lost leadership after failed transfer")
		}
		if _, _, ok := c.Propose(l); !ok {
			c.fatalf("proposals still rejected after transfer timeout")
		}
		c.Connect(dead)
		c.One(3, true, leaderWait)
	})
}
