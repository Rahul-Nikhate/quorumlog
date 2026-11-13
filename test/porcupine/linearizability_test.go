// Package porcupine checks that produce(acks=all) + read is linearizable,
// with the Porcupine checker (github.com/anishathalye/porcupine), on an
// in-process 3-replica partition (real Raft, real segmented log on a
// fault-injecting file system) over the in-memory network while a nemesis
// partitions the network and crashes/restarts replicas (losing unsynced
// writes).
//
// The model is a single append-only log. A produce returns the message's
// offset; a read returns the whole committed log [(offset, value)...].
// Produces whose outcome is unknown (leader lost after proposing, timeout)
// are recorded with an infinite return time, so the checker may place them
// anywhere after their call — or never observe them.
//
// Negative controls show the checker is not vacuous: reads served from a
// follower's local log, and acks=leader writes lost by a failover, MUST be
// flagged as non-linearizable.
//
//	QLOG_PORCUPINE_HISTORIES=200 go test ./test/porcupine/   # more histories (default 20)
package porcupine

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"go.uber.org/goleak"

	"github.com/Rahul-Nikhate/quorumlog/internal/fs"
	"github.com/Rahul-Nikhate/quorumlog/internal/partition"
	pt "github.com/Rahul-Nikhate/quorumlog/internal/partition/partitiontest"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// ---------------------------------------------------------------- model

type opKind int

const (
	opProduce opKind = iota
	opRead
)

type input struct {
	kind  opKind
	value string // produce
}

type entry struct {
	off   int64 // -1: not known yet (produce with unknown outcome)
	value string
}

type output struct {
	ok      bool    // produce: definitely appended at off (false = outcome unknown)
	off     int64   // produce
	entries []entry // read
}

// logModel: state is the committed log, a []entry (never mutated in place).
var logModel = porcupine.Model{
	Init: func() any { return []entry(nil) },
	Step: func(state, in, out any) (bool, any) {
		log, i, o := state.([]entry), in.(input), out.(output)
		switch i.kind {
		case opProduce:
			if !o.ok {
				return true, append(slices.Clip(log), entry{off: -1, value: i.value})
			}
			for _, e := range log {
				if e.off >= o.off {
					return false, nil // offsets grow along the log
				}
			}
			return true, append(slices.Clip(log), entry{off: o.off, value: i.value})
		default: // read: must return exactly the log
			if len(o.entries) != len(log) {
				return false, nil
			}
			for k, e := range log {
				if e.value != o.entries[k].value || (e.off >= 0 && e.off != o.entries[k].off) {
					return false, nil
				}
			}
			return true, o.entries // the read reveals the unknown offsets
		}
	},
	Equal: func(a, b any) bool { return slices.Equal(a.([]entry), b.([]entry)) },
	Hash: func(s any) uint64 {
		h := fnv.New64a()
		for _, e := range s.([]entry) {
			fmt.Fprintf(h, "%d:%s;", e.off, e.value)
		}
		return h.Sum64()
	},
	DescribeOperation: func(in, out any) string {
		i, o := in.(input), out.(output)
		if i.kind == opProduce {
			if !o.ok {
				return fmt.Sprintf("produce(%s) -> ?", i.value)
			}
			return fmt.Sprintf("produce(%s) -> %d", i.value, o.off)
		}
		return fmt.Sprintf("read() -> %v", o.entries)
	},
}

// ---------------------------------------------------------------- recorder

type recorder struct {
	start time.Time
	mu    sync.Mutex
	ops   []porcupine.Operation
}

func newRecorder() *recorder { return &recorder{start: time.Now()} }

func (r *recorder) now() int64 { return time.Since(r.start).Nanoseconds() }

func (r *recorder) add(client int, in input, out output, call, ret int64) {
	r.mu.Lock()
	r.ops = append(r.ops, porcupine.Operation{ClientId: client, Input: in, Output: out, Call: call, Return: ret})
	r.mu.Unlock()
}

// produce runs one produce(acks) and records it: definite failures are not
// recorded (nothing was appended), unknown outcomes get Return = +inf.
func (r *recorder) produce(ctx context.Context, client int, p *partition.Partition, value string, acks partition.Acks) error {
	call := r.now()
	pctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	res, err := p.ProduceMessages(pctx, partition.ProduceRequest{Messages: []partition.Message{{Value: []byte(value)}}, Acks: acks})
	switch {
	case err == nil:
		r.add(client, input{kind: opProduce, value: value}, output{ok: true, off: res.BaseOffset}, call, r.now())
	case errors.Is(err, partition.ErrOutcomeUnknown) || errors.Is(err, context.DeadlineExceeded):
		r.add(client, input{kind: opProduce, value: value}, output{}, call, math.MaxInt64)
	}
	return err
}

// read reads the whole log with consistency c and records it (failed reads
// have no effect and are not recorded).
func (r *recorder) read(ctx context.Context, client int, p *partition.Partition, c partition.Consistency) error {
	call := r.now()
	rctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	rr, err := p.ReadMessages(rctx, partition.ReadRequest{Offset: 0, Max: 1 << 20, MaxBytes: 1 << 30, Consistency: c}) // the whole log
	if err != nil {
		return err
	}
	out := output{entries: []entry{}}
	for _, m := range rr.Messages {
		out.entries = append(out.entries, entry{off: m.Offset, value: string(m.Value)})
	}
	r.add(client, input{kind: opRead}, out, call, r.now())
	return nil
}

func (r *recorder) check(t *testing.T) porcupine.CheckResult {
	t.Helper()
	res, info := porcupine.CheckOperationsVerbose(logModel, r.ops, 60*time.Second)
	if res != porcupine.Ok {
		if dir := os.Getenv("QLOG_PORCUPINE_VIZ"); dir != "" {
			f, err := os.CreateTemp(dir, "history-*.html")
			if err == nil {
				_ = porcupine.Visualize(logModel, info, f)
				_ = f.Close()
				t.Logf("visualization: %s", f.Name())
			}
		}
	}
	return res
}

// ---------------------------------------------------------------- histories

func histories() int {
	if n, err := strconv.Atoi(os.Getenv("QLOG_PORCUPINE_HISTORIES")); err == nil && n > 0 {
		return n
	}
	return 20
}

// TestLinearizableProduceRead: random concurrent produce(acks=all)/read
// (ReadIndex) histories under partitions and crash-restarts must all be
// linearizable.
func TestLinearizableProduceRead(t *testing.T) {
	var totalOps, totalUnknown int
	var mu sync.Mutex
	t.Run("group", func(t *testing.T) {
		for seed := range histories() {
			t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
				t.Parallel()
				ops, unknown := runHistory(t, uint64(seed))
				mu.Lock()
				totalOps += ops
				totalUnknown += unknown
				mu.Unlock()
			})
		}
	})
	if !t.Failed() {
		t.Logf("%d histories, %d operations (%d with unknown outcome), all linearizable", histories(), totalOps, totalUnknown)
	}
}

func runHistory(t *testing.T, seed uint64) (ops, unknown int) {
	c := pt.New(t, pt.Options{N: 3, Seed: seed + 1000})
	tp := c.TPs()[0]
	if _, err := c.WaitLeader(tp, nil, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	rec := newRecorder()
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()
	var wg sync.WaitGroup

	// Nemesis: one fault at a time — isolate a node, or crash it (dropping
	// unsynced writes) — then heal/restart.
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewPCG(seed, 7))
		for ctx.Err() == nil {
			sleep(ctx, time.Duration(80+rng.IntN(150))*time.Millisecond)
			victim := raft.NodeID(1 + rng.IntN(3))
			if rng.IntN(3) == 0 {
				if l, _ := c.Leader(tp); l != raft.None {
					victim = l // target the leader more often
				}
			}
			switch rng.IntN(3) {
			case 0:
				c.Net.Disconnect(victim)
				sleep(ctx, time.Duration(100+rng.IntN(200))*time.Millisecond)
				c.Net.Connect(victim)
			case 1:
				others := []raft.NodeID{}
				for _, id := range c.IDs() {
					if id != victim {
						others = append(others, id)
					}
				}
				c.Net.Partition([]raft.NodeID{victim}, others)
				sleep(ctx, time.Duration(100+rng.IntN(200))*time.Millisecond)
				c.Net.Heal()
			default:
				c.Crash(victim, fs.CrashDropUnsynced)
				sleep(ctx, time.Duration(50+rng.IntN(150))*time.Millisecond)
				_ = c.Restart(victim)
			}
		}
	}()

	for client := range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, uint64(client)+100))
			for k := 0; ctx.Err() == nil; k++ {
				id, p := c.Leader(tp)
				if p == nil || rng.IntN(4) == 0 { // sometimes ask a random (maybe stale) replica
					id = raft.NodeID(1 + rng.IntN(3))
					p = c.Partition(id, tp)
				}
				if p == nil {
					sleep(ctx, 5*time.Millisecond)
					continue
				}
				// Errors are part of the history (unknown outcomes) or have no
				// effect (definite failures): either way, just go on.
				if rng.IntN(5) < 3 {
					_ = rec.produce(ctx, client, p, fmt.Sprintf("c%d-%d", client, k), partition.AcksAll)
				} else {
					_ = rec.read(ctx, client, p, partition.ConsistencyReadIndex)
				}
				// Pace the clients: ~100 ops per history keeps the checker's
				// search (exponential in concurrency) fast.
				sleep(ctx, time.Duration(10+rng.IntN(30))*time.Millisecond)
			}
		}()
	}
	wg.Wait()
	for _, op := range rec.ops {
		if op.Return == math.MaxInt64 {
			unknown++
		}
	}
	reads := 0
	for _, op := range rec.ops {
		if op.Input.(input).kind == opRead {
			reads++
		}
	}
	if res := rec.check(t); res != porcupine.Ok {
		t.Fatalf("seed %d: history of %d ops is %v (rerun: QLOG_PORCUPINE_VIZ=/tmp go test -run 'TestLinearizableProduceRead/group/seed=%d$' ./test/porcupine/)",
			seed, len(rec.ops), res, seed)
	}
	if len(rec.ops)-reads < 3 || reads < 3 {
		t.Fatalf("seed %d: history too thin to be useful: %d ops, %d reads", seed, len(rec.ops), reads)
	}
	return len(rec.ops), unknown
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// ---------------------------------------------------------------- negative controls

func others(c *pt.Cluster, id raft.NodeID) []raft.NodeID {
	var out []raft.NodeID
	for _, x := range c.IDs() {
		if x != id {
			out = append(out, x)
		}
	}
	return out
}

func mustLeader(t *testing.T, c *pt.Cluster, among []raft.NodeID) (raft.NodeID, *partition.Partition) {
	t.Helper()
	tp := c.TPs()[0]
	id, err := c.WaitLeader(tp, among, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return id, c.Partition(id, tp)
}

// NEGATIVE CONTROL 1: serving reads from a follower's local log (no ReadIndex
// on the leader) is stale after a partition. The checker must flag it.
func TestNegativeControlFollowerRead(t *testing.T) {
	c := pt.New(t, pt.Options{N: 3, Seed: 77})
	tp := c.TPs()[0]
	ctx := context.Background()
	rec := newRecorder()
	l, lp := mustLeader(t, c, nil)
	if err := rec.produce(ctx, 0, lp, "a", partition.AcksAll); err != nil {
		t.Fatal(err)
	}
	f := others(c, l)[0]
	deadline := time.Now().Add(5 * time.Second)
	for c.Partition(f, tp).HighWatermark() < lp.HighWatermark() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	c.Net.Disconnect(f) // the follower stops hearing about new commits
	if err := rec.produce(ctx, 0, lp, "b", partition.AcksAll); err != nil {
		t.Fatal(err)
	}
	// The unsafe read: follower-local, after "b" was acknowledged.
	if err := rec.read(ctx, 1, c.Partition(f, tp), partition.ConsistencyLocal); err != nil {
		t.Fatal(err)
	}
	if got := rec.ops[len(rec.ops)-1].Output.(output).entries; len(got) != 1 || got[0].value != "a" {
		t.Fatalf("expected a stale read [a], got %v", got)
	}
	if res := rec.check(t); res != porcupine.Illegal {
		t.Fatalf("checker returned %v for a stale follower read; it must flag it (Illegal)", res)
	}
	// The same read done safely (ReadIndex on the leader) is linearizable.
	safe := &recorder{start: rec.start} // same time base
	safe.ops = slices.Clone(rec.ops[:2])
	if err := safe.read(ctx, 1, lp, partition.ConsistencyReadIndex); err != nil {
		t.Fatal(err)
	}
	if res := safe.check(t); res != porcupine.Ok {
		t.Fatalf("safe read: %v", res)
	}
}

// NEGATIVE CONTROL 2: acks=leader acknowledges before replication; when the
// leader is cut off and replaced, the acknowledged write is lost. The checker
// must flag the history; the same scenario with acks=all never acknowledges
// the doomed write.
func TestNegativeControlAcksLeaderLoss(t *testing.T) {
	c := pt.New(t, pt.Options{N: 3, Seed: 78})
	ctx := context.Background()
	rec := newRecorder()
	l, lp := mustLeader(t, c, nil)
	if err := rec.produce(ctx, 0, lp, "a", partition.AcksAll); err != nil {
		t.Fatal(err)
	}
	c.Net.Disconnect(l)
	if err := rec.produce(ctx, 0, lp, "lost", partition.AcksLeader); err != nil {
		t.Fatalf("acks=leader on the isolated leader should succeed: %v", err)
	}
	// acks=all on the isolated leader cannot succeed (no majority).
	if err := rec.produce(ctx, 2, lp, "never", partition.AcksAll); err == nil {
		t.Fatal("acks=all succeeded without a majority")
	}
	_, np := mustLeader(t, c, others(c, l))
	if err := rec.produce(ctx, 0, np, "b", partition.AcksAll); err != nil {
		t.Fatal(err)
	}
	c.Net.Connect(l) // the old leader truncates "lost"
	if err := rec.read(ctx, 1, np, partition.ConsistencyReadIndex); err != nil {
		t.Fatal(err)
	}
	var vals []string
	for _, e := range rec.ops[len(rec.ops)-1].Output.(output).entries {
		vals = append(vals, e.value)
	}
	if strings.Join(vals, ",") != "a,b" && strings.Join(vals, ",") != "a,never,b" {
		t.Fatalf("unexpected log %v", vals)
	}
	if res := rec.check(t); res != porcupine.Illegal {
		t.Fatalf("checker returned %v for a lost acks=leader write; it must flag it (Illegal)", res)
	}
}
