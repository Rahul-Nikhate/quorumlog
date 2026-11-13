package cluster_test

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/Rahul-Nikhate/quorumlog/internal/cluster"
	"github.com/Rahul-Nikhate/quorumlog/internal/log"
	"github.com/Rahul-Nikhate/quorumlog/internal/partition"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft/transport"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

type testCluster struct {
	t      *testing.T
	dir    string
	addrs  map[raft.NodeID]string
	nodes  map[raft.NodeID]*cluster.Node
	events *cluster.Events
}

func newCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	c := &testCluster{t: t, dir: t.TempDir(), addrs: map[raft.NodeID]string{}, nodes: map[raft.NodeID]*cluster.Node{}, events: cluster.NewEvents(500)}
	lns := map[raft.NodeID]net.Listener{}
	for i := 1; i <= n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[raft.NodeID(i)] = ln
		c.addrs[raft.NodeID(i)] = ln.Addr().String()
	}
	for id, ln := range lns {
		c.start(id, ln)
	}
	t.Cleanup(func() {
		for _, nd := range c.nodes {
			_ = nd.Close()
		}
	})
	return c
}

func (c *testCluster) start(id raft.NodeID, ln net.Listener) {
	c.t.Helper()
	if ln == nil {
		var err error
		if ln, err = net.Listen("tcp", c.addrs[id]); err != nil {
			c.t.Fatal(err)
		}
	}
	nd, err := cluster.Open(cluster.Config{
		ID: id, Peers: c.addrs, DataDir: filepath.Join(c.dir, "n"+string(rune('0'+id))),
		Sync: log.SyncPolicy{Mode: log.SyncNever}, TickInterval: 10 * time.Millisecond, Events: c.events,
		NewTransport: func(h transport.Handler) (transport.Transport, error) {
			return transport.NewTCP(transport.TCPConfig{ID: id, Listener: ln, Peers: c.addrs, Handler: h,
				MinBackoff: 5 * time.Millisecond, MaxBackoff: 50 * time.Millisecond})
		},
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id] = nd
}

func (c *testCluster) stop(id raft.NodeID) {
	_ = c.nodes[id].Close()
	delete(c.nodes, id)
}

// leader waits for a node that leads group (as seen by itself) and that
// every live node agrees on.
func (c *testCluster) leader(group string) *cluster.Node {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var lead raft.NodeID
		agree := true
		for _, nd := range c.nodes {
			l := nd.Leader(group)
			if lead == raft.None {
				lead = l
			}
			agree = agree && l != raft.None && l == lead
		}
		if nd := c.nodes[lead]; agree && nd != nil {
			if p, err := nd.Group(group); err == nil && p.Status().Role == raft.Leader {
				return nd
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("no leader for %s", group)
	return nil
}

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// retry calls fn until it succeeds or fails with a non-retriable error.
func retry(ctx context.Context, fn func() error) error {
	for {
		err := fn()
		if err == nil || !partition.IsRetriable(err) || ctx.Err() != nil {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestTopicsOffsetsAndRestart(t *testing.T) {
	c := newCluster(t, 3)
	ctx := ctxT(t)

	ml := c.leader(cluster.MetaGroup)
	for _, nd := range c.nodes {
		if nd != ml {
			if _, err := nd.CreateTopic(ctx, "orders"); !errors.Is(err, partition.ErrNotLeader) {
				t.Fatalf("create on follower: %v", err)
			}
			break
		}
	}
	created, err := ml.CreateTopic(ctx, "orders")
	if err != nil || !created {
		t.Fatalf("create: %v %v", created, err)
	}
	if created, err := ml.CreateTopic(ctx, "orders"); err != nil || created {
		t.Fatalf("second create: %v %v", created, err)
	}
	if _, err := ml.CreateTopic(ctx, "__nope"); !errors.Is(err, cluster.ErrBadName) {
		t.Fatalf("bad name: %v", err)
	}
	if topics, err := ml.Topics(ctx); err != nil || !slices.Equal(topics, []string{"orders"}) {
		t.Fatalf("topics: %v %v", topics, err)
	}
	// Every node opens the topic group; it elects a leader and replicates.
	tl := c.leader("orders")
	p, _ := tl.Group("orders")
	res, err := p.ProduceMessages(ctx, partition.ProduceRequest{Messages: []partition.Message{{Value: []byte("hello")}}, Acks: partition.AcksAll})
	if err != nil {
		t.Fatal(err)
	}

	// Consumer offsets.
	ol := c.leader(cluster.OffsetsGroup)
	if off, err := ol.FetchOffset(ctx, "g1", "orders"); err != nil || off != -1 {
		t.Fatalf("fetch before commit: %d %v", off, err)
	}
	if err := ol.CommitOffset(ctx, "g1", "orders", res.LastOffset+1); err != nil {
		t.Fatal(err)
	}
	if off, err := ol.FetchOffset(ctx, "g1", "orders"); err != nil || off != res.LastOffset+1 {
		t.Fatalf("fetch: %d %v", off, err)
	}
	if err := ol.CommitOffset(ctx, "", "orders", 1); !errors.Is(err, cluster.ErrBadName) {
		t.Fatalf("empty group: %v", err)
	}

	// Stop every node, restart all from disk: topics, messages and offsets
	// are rebuilt from the logs.
	for _, id := range []raft.NodeID{1, 2, 3} {
		c.stop(id)
	}
	for _, id := range []raft.NodeID{1, 2, 3} {
		c.start(id, nil)
	}
	ml = c.leader(cluster.MetaGroup)
	if topics, err := ml.Topics(ctx); err != nil || !slices.Equal(topics, []string{"orders"}) {
		t.Fatalf("topics after restart: %v %v", topics, err)
	}
	tl = c.leader("orders")
	p, _ = tl.Group("orders")
	var rr partition.ReadResult
	if err := retry(ctx, func() (err error) { rr, err = p.ReadMessages(ctx, partition.ReadRequest{}); return }); err != nil ||
		len(rr.Messages) != 1 || string(rr.Messages[0].Value) != "hello" || rr.Messages[0].Offset != res.BaseOffset {
		t.Fatalf("messages after restart: %+v %v", rr, err)
	}
	ol = c.leader(cluster.OffsetsGroup)
	var off int64
	if err := retry(ctx, func() (err error) { off, err = ol.FetchOffset(ctx, "g1", "orders"); return }); err != nil || off != res.LastOffset+1 {
		t.Fatalf("offset after restart: %d %v", off, err)
	}

	// Status and events.
	gs := ml.Groups()
	if len(gs) != 3 || gs[0].Name != cluster.MetaGroup || gs[1].Name != cluster.OffsetsGroup || gs[2].Name != "orders" {
		t.Fatalf("groups: %+v", gs)
	}
	for _, g := range gs {
		if g.HighWatermark > g.Commit+1 || g.HighWatermark > g.LogEndOffset || g.Leader == raft.None {
			t.Fatalf("inconsistent status %+v", g)
		}
	}
	var leaderEvents int
	for _, ev := range c.events.List() {
		if ev.Msg != "" && ev.Group != "" && len(ev.Msg) > 6 && ev.Msg[:6] == "became" {
			leaderEvents++
		}
	}
	if leaderEvents < 6 { // 3 groups × 2 incarnations
		t.Fatalf("only %d leader events: %+v", leaderEvents, c.events.List())
	}
}

// The leader of __offsets fails over; the committed offset survives and the
// new leader serves it.
func TestOffsetsSurviveFailover(t *testing.T) {
	c := newCluster(t, 3)
	ctx := ctxT(t)
	ol := c.leader(cluster.OffsetsGroup)
	if err := ol.CommitOffset(ctx, "g", "t", 42); err != nil {
		t.Fatal(err)
	}
	c.stop(ol.ID())
	nl := c.leader(cluster.OffsetsGroup)
	var off int64
	if err := retry(ctx, func() (err error) { off, err = nl.FetchOffset(ctx, "g", "t"); return }); err != nil || off != 42 {
		t.Fatalf("after failover: %d %v", off, err)
	}
	if err := retry(ctx, func() error { return nl.CommitOffset(ctx, "g", "t", 43) }); err != nil {
		t.Fatal(err)
	}
	c.start(ol.ID(), nil)
	nl = c.leader(cluster.OffsetsGroup)
	if err := retry(ctx, func() (err error) { off, err = nl.FetchOffset(ctx, "g", "t"); return }); err != nil || off != 43 {
		t.Fatalf("after restart: %d %v", off, err)
	}
}

func TestEventsRing(t *testing.T) {
	e := cluster.NewEvents(3)
	for i := range 5 {
		e.Add(cluster.Event{Node: uint32(i)})
	}
	got := e.List()
	if len(got) != 3 || got[0].Node != 2 || got[2].Node != 4 {
		t.Fatalf("ring: %+v", got)
	}
}

// Topic directories are named after topics, so two names that differ only in
// case would share a directory on a case-insensitive file system (macOS,
// Windows) and corrupt each other's log (regression: the second group's
// driver panicked on the first one's batches). The second name is refused,
// both at create time and when applying a __meta record that slipped in.
func TestTopicNamesDifferingOnlyInCaseAreRefused(t *testing.T) {
	c := newCluster(t, 1)
	ctx := ctxT(t)
	nd := c.leader(cluster.MetaGroup)
	if created, err := nd.CreateTopic(ctx, "orders"); err != nil || !created {
		t.Fatalf("create orders: %v %v", created, err)
	}
	if _, err := nd.CreateTopic(ctx, "Orders"); !errors.Is(err, cluster.ErrBadName) {
		t.Fatalf("create Orders: %v, want ErrBadName", err)
	}
	// A record proposed directly (as two concurrent creates could) is
	// ignored by every node's apply.
	meta, _ := nd.Group(cluster.MetaGroup)
	res, err := meta.ProduceMessages(ctx, partition.ProduceRequest{Messages: []partition.Message{{Value: []byte(`{"op":"create_topic","name":"ORDERS"}`)}}, Acks: partition.AcksAll})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nd.CreateTopic(ctx, "after"); err != nil { // applied after the ORDERS record
		t.Fatal(err)
	}
	if topics, err := nd.Topics(ctx); err != nil || !slices.Equal(topics, []string{"after", "orders"}) {
		t.Fatalf("topics after ORDERS at %d: %v %v", res.BaseOffset, topics, err)
	}
	tl := c.leader("orders")
	p, _ := tl.Group("orders")
	if _, err := p.ProduceMessages(ctx, partition.ProduceRequest{Messages: []partition.Message{{Value: []byte("v")}}, Acks: partition.AcksAll}); err != nil {
		t.Fatal(err)
	}
}
