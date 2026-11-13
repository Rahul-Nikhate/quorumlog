package rafttest

import (
	"testing"

	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft/transport"
)

// 6.5840 3C "basic persistence": whole-cluster and leader crash/restart.
func TestPersist1(t *testing.T) {
	forSeeds(t, 60, func(t *testing.T, seed uint64) {
		c := New(t, baseOpts(seed, 3))
		all := c.IDs()
		c.One(3, true, leaderWait)
		for _, id := range all {
			c.Crash(id)
		}
		for _, id := range all {
			c.Restart(id)
		}
		c.One(3, true, leaderWait)

		l := mustLeader(t, c, nil)
		c.Crash(l)
		c.Restart(l)
		c.One(3, true, leaderWait)

		l = mustLeader(t, c, nil)
		c.Crash(l)
		c.One(2, true, leaderWait)
		c.Restart(l)
		c.One(3, true, leaderWait)

		f := others(all, mustLeader(t, c, nil))[0]
		c.Crash(f)
		c.One(2, true, leaderWait)
		c.Restart(f)
		c.One(3, true, leaderWait)
	})
}

// 6.5840 3C "more persistence": rotating crashes of up to two of five nodes
// combined with disconnections.
func TestPersist2(t *testing.T) {
	forSeeds(t, 30, func(t *testing.T, seed uint64) {
		c := New(t, baseOpts(seed, 5))
		all := c.IDs()
		at := func(l raft.NodeID, k int) raft.NodeID { return all[(int(l)-1+k)%5] }
		for range 5 {
			c.One(5, true, leaderWait)
			l1 := mustLeader(t, c, nil)
			c.Disconnect(at(l1, 1))
			c.Disconnect(at(l1, 2))
			c.One(3, true, leaderWait)

			c.Crash(l1)
			c.Crash(at(l1, 3))
			c.Crash(at(l1, 4))
			c.Connect(at(l1, 1))
			c.Connect(at(l1, 2))
			c.Restart(at(l1, 3))
			c.One(3, true, 2*leaderWait)

			c.Restart(l1)
			c.Restart(at(l1, 4))
		}
		c.One(5, true, 2*leaderWait)
	})
}

// 6.5840 3C "partitioned leader and one follower crash, leader restarts".
func TestPersist3(t *testing.T) {
	forSeeds(t, 60, func(t *testing.T, seed uint64) {
		c := New(t, baseOpts(seed, 3))
		all := c.IDs()
		c.One(3, true, leaderWait)
		l := mustLeader(t, c, nil)
		fs := others(all, l)
		c.Disconnect(fs[1])
		c.One(2, true, leaderWait)
		c.Crash(l)
		c.Crash(fs[0])
		c.Connect(fs[1])
		c.Restart(l)
		c.One(2, true, leaderWait)
		c.Restart(fs[0])
		c.One(3, true, leaderWait)
	})
}

// 6.5840 3C "Figure 8": leaders crash right after accepting proposals, so
// entries of old terms are replicated to various subsets. The Figure 8 rule
// (count only current-term entries) must keep every committed entry intact.
func TestFigure8(t *testing.T) {
	forSeeds(t, 60, func(t *testing.T, seed uint64) {
		c := New(t, baseOpts(seed, 5))
		all := c.IDs()
		c.One(1, true, leaderWait)
		up := 5
		for range 300 {
			leader := raft.None
			for _, id := range all {
				if _, _, ok := c.Propose(id); ok {
					leader = id
				}
			}
			if c.rng.IntN(1000) < 100 {
				c.RunTicks(c.rng.IntN(2 * c.opts.ElectionTick))
			} else {
				c.RunTicks(c.rng.IntN(c.opts.ElectionTick / 2))
			}
			if leader != raft.None && c.Alive(leader) {
				c.Crash(leader)
				up--
			}
			if up < 3 {
				for {
					id := all[c.rng.IntN(5)]
					if !c.Alive(id) {
						c.Restart(id)
						up++
						break
					}
				}
			}
		}
		for _, id := range all {
			c.Restart(id)
		}
		c.One(5, true, 4*leaderWait)
		t.Log(c.Summary())
	})
}

// 6.5840 3C "Figure 8 (unreliable)": the same churn on a network that drops,
// duplicates, delays and heavily reorders messages, plus random partitions,
// crashes and log compaction (so snapshots are installed mid-chaos).
func TestFigure8Unreliable(t *testing.T) {
	forSeeds(t, 60, func(t *testing.T, seed uint64) {
		o := baseOpts(seed, 5)
		o.Link = unreliable()
		o.TickSkipRate = 0.02
		c := New(t, o)
		all := c.IDs()
		c.One(1, true, 4*leaderWait)
		for iter := range 400 {
			if iter == 300 {
				// Long reordering phase (6.5840 setlongreordering).
				c.SetLink(transport.LinkConfig{DropRate: 0.1, MinDelay: 1, MaxDelay: 20, ReorderRate: 0.3, ReorderDelay: 120})
			}
			leader := raft.None
			for _, id := range all {
				if c.rng.IntN(1000) < 500 {
					if _, _, ok := c.Propose(id); ok {
						leader = id
					}
				}
			}
			if c.rng.IntN(1000) < 100 {
				c.RunTicks(c.rng.IntN(2 * c.opts.ElectionTick))
			} else {
				c.RunTicks(c.rng.IntN(4))
			}
			switch r := c.rng.IntN(100); {
			case r < 20 && leader != raft.None:
				c.Disconnect(leader)
			case r < 25:
				c.Crash(all[c.rng.IntN(5)])
			case r < 30:
				perm := c.rng.Perm(5)
				k := 1 + c.rng.IntN(4)
				var a, b []raft.NodeID
				for i, p := range perm {
					if i < k {
						a = append(a, all[p])
					} else {
						b = append(b, all[p])
					}
				}
				c.Partition(a, b)
			case r < 55:
				c.Compact(all[c.rng.IntN(5)])
			}
			// Keep a majority connected and alive most of the time.
			if c.rng.IntN(100) < 20 {
				c.Heal()
			}
			for _, id := range all {
				if !c.Alive(id) && c.rng.IntN(100) < 30 {
					c.Restart(id)
				}
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

// 6.5840 3C "churn": continuous proposals while random nodes crash, restart
// and disconnect; at the end everything heals and agrees.
func TestChurn(t *testing.T) {
	for _, reliable := range []bool{true, false} {
		name := map[bool]string{true: "reliable", false: "unreliable"}[reliable]
		t.Run(name, func(t *testing.T) {
			forSeeds(t, 30, func(t *testing.T, seed uint64) {
				o := baseOpts(seed, 5)
				if !reliable {
					o.Link = unreliable()
				}
				c := New(t, o)
				all := c.IDs()
				for range 150 {
					for _, id := range all {
						if c.rng.IntN(100) < 70 {
							c.Propose(id)
						}
					}
					c.RunTicks(1 + c.rng.IntN(c.opts.ElectionTick))
					id := all[c.rng.IntN(5)]
					switch r := c.rng.IntN(100); {
					case r < 10:
						c.Disconnect(id)
					case r < 30:
						c.Connect(id)
					case r < 40:
						c.Crash(id)
					case r < 60:
						c.Restart(id)
					case r < 70:
						c.Compact(id)
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
		})
	}
}

// InstallSnapshot: a follower that is behind the leader's compacted log
// catches up through a snapshot (spec §5.12).
func TestSnapshotInstall(t *testing.T) {
	forSeeds(t, 60, func(t *testing.T, seed uint64) {
		c := New(t, baseOpts(seed, 3))
		all := c.IDs()
		c.One(3, true, leaderWait)
		l := mustLeader(t, c, nil)
		lag := others(all, l)[0]
		c.Disconnect(lag)
		for range 30 {
			c.One(2, true, leaderWait)
		}
		for _, id := range others(all, lag) {
			c.Compact(id)
		}
		lagLast := c.Status(lag).LastIndex
		if first := c.Status(c.Leader()).FirstIndex; first <= lagLast+1 {
			c.fatalf("compaction did not pass the lagging follower (leader first=%d, lag last=%d)", first, lagLast)
		}
		c.Connect(lag)
		c.One(3, true, 2*leaderWait)
		snap, _ := c.Store(lag).Snapshot()
		if snap.Index == 0 {
			c.fatalf("lagging follower caught up without a snapshot")
		}
		// And it keeps working after restart from the snapshot.
		c.Crash(lag)
		c.Restart(lag)
		c.One(3, true, 2*leaderWait)
		t.Log(c.Summary())
	})
}
