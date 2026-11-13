package rafttest

import (
	"testing"

	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft/transport"
)

// ReadIndex (spec §5.13): reads on the leader and on followers (forwarded)
// are confirmed with an index >= everything committed before they were
// issued (checked by the harness on every ReadState), and a partitioned
// stale leader can never confirm a read.
func TestReadIndex(t *testing.T) {
	forSeeds(t, 60, func(t *testing.T, seed uint64) {
		o := baseOpts(seed, 3)
		o.PreVote = true
		c := New(t, o)
		all := c.IDs()
		l := mustLeader(t, c, nil)
		c.One(3, false, leaderWait)

		var ctxs []uint64
		for i := range 40 {
			if i%3 == 0 {
				c.Propose(l)
			}
			id := all[c.rng.IntN(3)]
			ctx, err := c.ReadIndex(id)
			if err != nil {
				c.fatalf("ReadIndex on N%d: %v", id, err)
			}
			ctxs = append(ctxs, ctx)
			c.RunTicks(c.rng.IntN(2))
		}
		if !c.RunUntil(leaderWait, func() bool { return len(c.Reads()) == len(ctxs) }) {
			c.fatalf("only %d of %d reads confirmed on a healthy cluster", len(c.Reads()), len(ctxs))
		}

		// Stale leader: isolate it (without check-quorum it keeps believing it
		// leads), let the majority commit new writes, then read on it.
		o2 := baseOpts(seed, 3)
		o2.CheckQuorum = false
		c = New(t, o2)
		all = c.IDs()
		l = mustLeader(t, c, nil)
		c.One(3, false, leaderWait)
		c.Disconnect(l)
		for range 3 {
			c.One(2, true, leaderWait)
		}
		if !c.isLeader(l) {
			c.fatalf("isolated leader stepped down without check-quorum")
		}
		ctx, err := c.ReadIndex(l)
		if err != nil {
			c.fatalf("ReadIndex on stale leader: %v", err)
		}
		c.RunTicks(10 * c.opts.ElectionTick)
		if r, ok := c.ReadDone(ctx); ok {
			c.fatalf("stale leader confirmed a read at index %d (writes up to %d were committed before it)", r.Index, c.MaxCommit())
		}
		c.Connect(l)
		mustLeader(t, c, nil)
		c.One(3, true, leaderWait)
	})
}

// ReadIndex under faults: random reads everywhere while the cluster churns;
// every confirmed read is checked against the global commit history.
func TestReadIndexChaos(t *testing.T) {
	forSeeds(t, 60, func(t *testing.T, seed uint64) {
		o := baseOpts(seed, 5)
		o.Link = unreliable()
		c := New(t, o)
		all := c.IDs()
		for range 300 {
			for _, id := range all {
				switch r := c.rng.IntN(100); {
				case r < 30:
					c.Propose(id)
				case r < 60:
					_, _ = c.ReadIndex(id)
				}
			}
			c.RunTicks(1 + c.rng.IntN(3))
			id := all[c.rng.IntN(5)]
			switch r := c.rng.IntN(100); {
			case r < 8:
				c.Disconnect(id)
			case r < 16:
				c.Connect(id)
			case r < 20:
				c.Crash(id)
			case r < 26:
				c.Restart(id)
			}
		}
		c.SetLink(transport.LinkConfig{MinDelay: 1, MaxDelay: 2})
		c.Heal()
		for _, id := range all {
			c.Restart(id)
		}
		c.One(5, true, 8*leaderWait)
		l := mustLeader(t, c, nil)
		ctx, err := c.ReadIndex(l)
		if err != nil {
			c.fatalf("ReadIndex: %v", err)
		}
		if !c.RunUntil(leaderWait, func() bool { _, ok := c.ReadDone(ctx); return ok }) {
			c.fatalf("final read on healed cluster not confirmed")
		}
		t.Log(c.Summary())
	})
}

// Lease reads (UnsafeLeaseReads, flagged unsafe under clock-rate skew):
// with equal tick rates, leader reads skip the heartbeat round yet remain
// linearizable, and an isolated leader's lease expires before a new leader
// can be elected.
func TestLeaseReads(t *testing.T) {
	forSeeds(t, 30, func(t *testing.T, seed uint64) {
		o := baseOpts(seed, 3)
		o.CheckQuorum = true
		o.UnsafeLeaseReads = true
		c := New(t, o)
		l := mustLeader(t, c, nil)
		c.One(3, false, leaderWait)
		c.RunTicks(2)
		ctx, err := c.ReadIndex(l)
		if err != nil {
			c.fatalf("ReadIndex: %v", err)
		}
		c.processReady(c.peer[l])
		if _, ok := c.ReadDone(ctx); !ok {
			c.fatalf("lease read was not served immediately")
		}
		for range 30 {
			c.Propose(l)
			_, _ = c.ReadIndex(l)
			c.RunTicks(1)
		}
		c.Disconnect(l)
		for range 3 {
			c.One(2, true, leaderWait)
		}
		if c.isLeader(l) {
			ctx, _ := c.ReadIndex(l)
			c.RunTicks(c.opts.ElectionTick)
			if _, ok := c.ReadDone(ctx); ok {
				c.fatalf("stale leader served a lease read after a new leader committed")
			}
		}
		c.Connect(l)
		c.One(3, true, leaderWait)
	})
}

// Lease reads under partitions, crashes/restarts and an unreliable network
// (but equal tick rates): every lease-served read must still be linearizable.
func TestLeaseReadsChaos(t *testing.T) {
	forSeeds(t, 60, func(t *testing.T, seed uint64) {
		o := baseOpts(seed, 5)
		o.CheckQuorum = true
		o.UnsafeLeaseReads = true
		o.Link = unreliable()
		c := New(t, o)
		all := c.IDs()
		for range 300 {
			for _, id := range all {
				switch r := c.rng.IntN(100); {
				case r < 30:
					c.Propose(id)
				case r < 70:
					_, _ = c.ReadIndex(id)
				}
			}
			c.RunTicks(c.rng.IntN(3))
			id := all[c.rng.IntN(5)]
			switch r := c.rng.IntN(100); {
			case r < 8:
				c.Disconnect(id)
			case r < 16:
				c.Connect(id)
			case r < 22:
				c.Crash(id)
			case r < 30:
				c.Restart(id)
			case r < 33:
				perm := c.rng.Perm(5)
				c.Partition([]raft.NodeID{all[perm[0]], all[perm[1]]}, []raft.NodeID{all[perm[2]], all[perm[3]], all[perm[4]]})
			case r < 40:
				c.Heal()
			}
		}
		c.SetLink(transport.LinkConfig{MinDelay: 1, MaxDelay: 2})
		c.Heal()
		for _, id := range all {
			c.Restart(id)
		}
		c.One(5, true, 8*leaderWait)
		t.Log(c.Summary())
	})
}
