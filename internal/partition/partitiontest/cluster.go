// Package partitiontest is an in-process, real-time cluster of brokers for
// tests: each broker is a partition.Manager hosting replicas of the same
// partitions, with its own file system (FaultFS, or a temporary directory on
// the OS), connected by the fault-injecting in-memory network
// (transport.Network advanced in real time). Brokers can be crashed (power
// loss on FaultFS: unsynced writes dropped or torn) and restarted from disk.
package partitiontest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Rahul-Nikhate/quorumlog/internal/fs"
	"github.com/Rahul-Nikhate/quorumlog/internal/log"
	"github.com/Rahul-Nikhate/quorumlog/internal/partition"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft/transport"
)

// Options configures a Cluster.
type Options struct {
	N          int    // brokers (default 3)
	Partitions int    // partitions, each replicated on every broker (default 1)
	Topic      string // default "t"
	Seed       uint64
	// OSDir, if set, puts each broker's data under OSDir/n<ID> on the real
	// file system; crashes are then process crashes (clean close). Default:
	// one FaultFS per broker.
	OSDir string

	TickInterval time.Duration // default 10ms
	ElectionTick int           // default 10
	NetUnit      time.Duration // real time per network unit (default 1ms)
	Link         transport.LinkConfig
	Sync         log.SyncPolicy // default SyncAlways
	SegmentBytes int64          // default 64 KiB (rolls happen)
	// ConfigHook adjusts every partition config.
	ConfigHook func(*partition.Config)
}

// Broker is one node of the cluster.
type Broker struct {
	ID  raft.NodeID
	Mgr *partition.Manager // nil while crashed
	FS  *fs.FaultFS        // nil with OSDir
	ep  *transport.Endpoint
}

// Cluster is the harness. Its methods are safe for concurrent use.
type Cluster struct {
	t    testing.TB
	opts Options
	Net  *transport.Network
	ids  []raft.NodeID

	mu      sync.Mutex
	brokers map[raft.NodeID]*Broker
	closed  bool // set by Close: Restart then refuses (no leaked replicas)

	cancel  context.CancelFunc
	netDone chan struct{}
}

// TPs returns the cluster's topic-partitions.
func (c *Cluster) TPs() []partition.TopicPartition {
	out := make([]partition.TopicPartition, c.opts.Partitions)
	for i := range out {
		out[i] = partition.TopicPartition{Topic: c.opts.Topic, Partition: int32(i)}
	}
	return out
}

// IDs returns the broker ids.
func (c *Cluster) IDs() []raft.NodeID { return append([]raft.NodeID(nil), c.ids...) }

// New starts a cluster; it is closed by t.Cleanup.
func New(t testing.TB, o Options) *Cluster {
	t.Helper()
	if o.N == 0 {
		o.N = 3
	}
	if o.Partitions == 0 {
		o.Partitions = 1
	}
	if o.Topic == "" {
		o.Topic = "t"
	}
	if o.TickInterval == 0 {
		o.TickInterval = 10 * time.Millisecond
	}
	if o.ElectionTick == 0 {
		o.ElectionTick = 10
	}
	if o.NetUnit == 0 {
		o.NetUnit = time.Millisecond
	}
	if o.Link == (transport.LinkConfig{}) {
		o.Link = transport.LinkConfig{MinDelay: 1, MaxDelay: 3}
	}
	if o.SegmentBytes == 0 {
		o.SegmentBytes = 64 << 10
	}
	c := &Cluster{t: t, opts: o, Net: transport.NewNetwork(o.Seed), brokers: map[raft.NodeID]*Broker{}}
	c.Net.SetDefaultLink(o.Link)
	for i := 1; i <= o.N; i++ {
		id := raft.NodeID(i)
		c.ids = append(c.ids, id)
		b := &Broker{ID: id}
		if o.OSDir == "" {
			b.FS = fs.NewFaultFS(o.Seed*7919 + uint64(i))
		}
		c.brokers[id] = b
	}
	for _, id := range c.ids {
		if err := c.start(c.brokers[id]); err != nil {
			t.Fatalf("start broker %d: %v", id, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.netDone = make(chan struct{})
	go func() {
		defer close(c.netDone)
		c.Net.RunRealtime(ctx, o.NetUnit)
	}()
	t.Cleanup(c.Close)
	return c
}

func (c *Cluster) fsys(b *Broker) fs.FS {
	if b.FS != nil {
		return b.FS
	}
	return fs.OS
}

func (c *Cluster) dir(b *Broker, tp partition.TopicPartition) string {
	base := "/data"
	if c.opts.OSDir != "" {
		base = c.opts.OSDir
	}
	return filepath.Join(base, fmt.Sprintf("n%d", b.ID), tp.String())
}

// PartitionConfig returns the config broker id uses for tp.
func (c *Cluster) PartitionConfig(id raft.NodeID, tp partition.TopicPartition) partition.Config {
	b := c.brokers[id]
	cfg := partition.Config{
		TopicPartition: tp,
		Group:          uint64(tp.Partition) + 1,
		ID:             id,
		Peers:          c.IDs(),
		Dir:            c.dir(b, tp),
		FS:             c.fsys(b),
		Log:            log.Config{Sync: c.opts.Sync, SegmentBytes: c.opts.SegmentBytes, IndexIntervalBytes: 512, MaxIndexBytes: 64 << 10},
		TickInterval:   c.opts.TickInterval,
		ElectionTick:   c.opts.ElectionTick,
		Seed:           c.opts.Seed*1000003 + uint64(id)*101 + uint64(tp.Partition),
	}
	if os.Getenv("PARTITION_DEBUG") != "" {
		start := time.Now()
		cfg.RaftLogger = func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, "%9.3fms %v "+format+"\n", append([]any{float64(time.Since(start).Microseconds()) / 1000, tp}, args...)...)
		}
	}
	if c.opts.ConfigHook != nil {
		c.opts.ConfigHook(&cfg)
	}
	return cfg
}

// start opens broker b's manager and replicas (from disk if present).
func (c *Cluster) start(b *Broker) error {
	mgr := partition.NewManager(b.ID)
	b.ep = c.Net.Register(b.ID, mgr.Handle)
	mgr.SetTransport(b.ep)
	for _, tp := range c.TPs() {
		if _, err := mgr.Create(c.PartitionConfig(b.ID, tp)); err != nil {
			_ = mgr.Close()
			return err
		}
	}
	b.Mgr = mgr
	return nil
}

// Broker returns broker id (Mgr is nil while crashed).
func (c *Cluster) Broker(id raft.NodeID) *Broker {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.brokers[id]
}

// Partition returns broker id's replica of tp (nil while crashed).
func (c *Cluster) Partition(id raft.NodeID, tp partition.TopicPartition) *partition.Partition {
	c.mu.Lock()
	b := c.brokers[id]
	mgr := b.Mgr
	c.mu.Unlock()
	if mgr == nil {
		return nil
	}
	p, _ := mgr.Get(tp)
	return p
}

// Alive reports whether broker id is running.
func (c *Cluster) Alive(id raft.NodeID) bool { return c.Partition(id, c.TPs()[0]) != nil }

// Crash kills broker id. With FaultFS the disk loses unsynced writes (mode
// CrashDropUnsynced / CrashTearUnsynced) or keeps them (CrashProcess): the
// replicas are halted (no further writes) and then the file system crashes,
// so nothing written after the crash instant reaches the disk. With OSDir the
// files simply stay as written (a process crash).
func (c *Cluster) Crash(id raft.NodeID, mode fs.CrashMode) {
	c.mu.Lock()
	b := c.brokers[id]
	mgr := b.Mgr
	if mgr == nil {
		c.mu.Unlock()
		return
	}
	b.Mgr = nil
	c.Net.Crash(id)
	// Stop the drivers first without any further disk writes, then crash the
	// file system: the disk keeps exactly what was synced before this instant.
	mgr.Crash(func() {
		if b.FS != nil {
			b.FS = b.FS.Crash(mode)
		}
	})
	c.mu.Unlock()
}

// Restart recovers broker id from its disk.
func (c *Cluster) Restart(id raft.NodeID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.brokers[id]
	if c.closed {
		return errors.New("partitiontest: cluster closed")
	}
	if b.Mgr != nil {
		return nil
	}
	if err := c.start(b); err != nil {
		return err
	}
	c.Net.Restart(id)
	return nil
}

// Status returns the status of every live replica of tp.
func (c *Cluster) Status(tp partition.TopicPartition) map[raft.NodeID]partition.Status {
	out := map[raft.NodeID]partition.Status{}
	for _, id := range c.ids {
		if p := c.Partition(id, tp); p != nil {
			out[id] = p.Status()
		}
	}
	return out
}

// Leader returns the live replica of tp that believes it leads with the
// highest term (None if none).
func (c *Cluster) Leader(tp partition.TopicPartition) (raft.NodeID, *partition.Partition) {
	var best raft.NodeID
	var bp *partition.Partition
	var bestTerm uint64
	for _, id := range c.ids {
		p := c.Partition(id, tp)
		if p == nil {
			continue
		}
		if st := p.Status(); st.Role == raft.Leader && st.Term >= bestTerm {
			best, bp, bestTerm = id, p, st.Term
		}
	}
	return best, bp
}

// WaitLeader waits until exactly one live replica of tp leads and every live
// replica among `among` (nil = all) agrees on it.
func (c *Cluster) WaitLeader(tp partition.TopicPartition, among []raft.NodeID, timeout time.Duration) (raft.NodeID, error) {
	if among == nil {
		among = c.ids
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var lead raft.NodeID
		var term uint64
		ok := true
		n := 0
		for _, id := range among {
			p := c.Partition(id, tp)
			if p == nil {
				continue
			}
			st := p.Status()
			if st.Role == raft.Leader {
				n++
				lead, term = id, st.Term
			}
		}
		if n == 1 {
			for _, id := range among {
				if p := c.Partition(id, tp); p != nil {
					if st := p.Status(); st.Leader != lead || st.Term != term {
						ok = false
					}
				}
			}
			if ok {
				return lead, nil
			}
		}
		time.Sleep(c.opts.TickInterval)
	}
	return raft.None, fmt.Errorf("no stable leader for %v within %v: %v", tp, timeout, c.Status(tp))
}

// Produce sends batch to the leader of tp, following NotLeader hints and
// retrying definite NotLeader/ProposalDropped errors until ctx expires. The
// batch is copied per attempt (Produce takes ownership).
func (c *Cluster) Produce(ctx context.Context, tp partition.TopicPartition, batch log.Batch, acks partition.Acks) (partition.ProduceResult, error) {
	var hint raft.NodeID
	for attempt := 0; ; attempt++ {
		id := hint
		if id == raft.None {
			id, _ = c.Leader(tp)
		}
		if id == raft.None {
			id = c.ids[attempt%len(c.ids)]
		}
		p := c.Partition(id, tp)
		if p == nil {
			hint = raft.None
		} else {
			res, err := p.Produce(ctx, append(log.Batch(nil), batch...), acks)
			var nl *partition.NotLeaderError
			switch {
			case err == nil:
				return res, nil
			case errors.Is(err, partition.ErrOutcomeUnknown):
				return res, err
			case errors.As(err, &nl):
				hint = nl.Leader
			case errors.Is(err, partition.ErrProposalDropped), errors.Is(err, partition.ErrClosed), errors.Is(err, partition.ErrStorage):
				hint = raft.None
			default:
				return res, err
			}
		}
		select {
		case <-ctx.Done():
			return partition.ProduceResult{}, ctx.Err()
		case <-time.After(c.opts.TickInterval):
		}
	}
}

// Record is one decoded record.
type Record struct {
	Offset int64
	Key    []byte
	Value  []byte
}

// DecodeRecords decodes the data records of concatenated batches (control
// batches skipped). It also returns the offset after the last batch.
func DecodeRecords(b []byte) ([]Record, int64, error) {
	bs, err := log.SplitBatches(b, 64<<20)
	if err != nil {
		return nil, 0, err
	}
	var out []Record
	next := int64(-1)
	for _, bt := range bs {
		next = bt.LastOffset() + 1
		if bt.IsControl() {
			continue
		}
		recs, err := bt.Records()
		if err != nil {
			return nil, 0, err
		}
		for _, r := range recs {
			out = append(out, Record{Offset: bt.BaseOffset() + int64(r.OffsetDelta), Key: append([]byte(nil), r.Key...), Value: append([]byte(nil), r.Value...)})
		}
	}
	return out, next, nil
}

// ReadAll reads every committed record of p's replica from the log start up
// to a single read point: the HW returned by the first fetch (which applies
// the consistency rule c); later calls are bounded by it with UpTo, so the
// result is the committed log as of one instant.
func ReadAll(ctx context.Context, p *partition.Partition, c partition.Consistency) ([]Record, int64, error) {
	off := p.Log().LogStartOffset()
	res, err := p.Fetch(ctx, partition.FetchRequest{Offset: off, MaxBytes: 1 << 20, Consistency: c})
	if err != nil {
		return nil, 0, err
	}
	hw := res.HighWatermark
	var out []Record
	for {
		data, err := res.Region.Bytes()
		res.Region.Release()
		if err != nil {
			return nil, 0, err
		}
		if len(data) == 0 {
			if off < hw {
				return nil, 0, fmt.Errorf("partitiontest: empty read at %d below read point %d", off, hw)
			}
			return out, hw, nil
		}
		recs, next, err := DecodeRecords(data)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, recs...)
		off = next
		if off >= hw {
			return out, hw, nil
		}
		res, err = p.Fetch(ctx, partition.FetchRequest{Offset: off, MaxBytes: 1 << 20, Consistency: partition.ConsistencyLocal, UpTo: hw})
		if err != nil {
			return nil, 0, err
		}
	}
}

// Close stops every broker and the network.
func (c *Cluster) Close() {
	c.mu.Lock()
	c.closed = true
	var mgrs []*partition.Manager
	for _, id := range c.ids {
		if b := c.brokers[id]; b.Mgr != nil {
			mgrs = append(mgrs, b.Mgr)
			b.Mgr = nil
		}
	}
	c.mu.Unlock()
	for _, m := range mgrs {
		_ = m.Close()
	}
	if c.cancel != nil {
		c.cancel()
		<-c.netDone
		c.cancel = nil
	}
}

// TempDir returns a temp dir for OSDir that the test cleans up.
func TempDir(t testing.TB) string {
	d, err := os.MkdirTemp("", "quorumlog-partitiontest-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	return d
}
