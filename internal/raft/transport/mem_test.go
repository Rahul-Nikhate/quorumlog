package transport

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
)

type recorder struct {
	mu  sync.Mutex
	got []raft.Message
}

func (r *recorder) handler(m raft.Message) {
	r.mu.Lock()
	r.got = append(r.got, m)
	r.mu.Unlock()
}

func (r *recorder) contexts() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []uint64
	for _, m := range r.got {
		out = append(out, m.Context)
	}
	return out
}

func hb(from, to raft.NodeID, ctx uint64) raft.Message {
	return raft.Message{Type: raft.MsgHeartbeat, Group: 9, From: from, To: to, Term: 1, Context: ctx}
}

func setup(seed uint64, n int) (*Network, []*Endpoint, []*recorder) {
	net := NewNetwork(seed)
	var eps []*Endpoint
	var recs []*recorder
	for i := 1; i <= n; i++ {
		r := &recorder{}
		recs = append(recs, r)
		eps = append(eps, net.Register(raft.NodeID(i), r.handler))
	}
	return net, eps, recs
}

func run(net *Network, steps int) {
	for range steps {
		net.Advance(1)
	}
}

func TestMemReliableDelivery(t *testing.T) {
	net, eps, recs := setup(1, 2)
	for i := range 100 {
		eps[0].Send([]raft.Message{hb(1, 2, uint64(i))})
	}
	run(net, 5)
	got := recs[1].contexts()
	if len(got) != 100 || !slices.IsSorted(got) {
		t.Fatalf("got %d messages in order=%v", len(got), slices.IsSorted(got))
	}
	if m := recs[1].got[0]; m.Group != 9 || m.From != 1 || m.Type != raft.MsgHeartbeat {
		t.Fatalf("message mangled in transit: %v", m)
	}
}

func TestMemFaults(t *testing.T) {
	net, eps, recs := setup(2, 3)
	net.SetDefaultLink(LinkConfig{DropRate: 0.3, DupRate: 0.1, MinDelay: 1, MaxDelay: 10, ReorderRate: 0.2, ReorderDelay: 50})
	for i := range 2000 {
		eps[0].Send([]raft.Message{hb(1, 2, uint64(i))})
	}
	run(net, 100)
	got := recs[1].contexts()
	st := net.Stats()
	if st.Dropped < 400 || st.Dropped > 800 || st.Duplicated < 50 {
		t.Fatalf("stats %+v", st)
	}
	if slices.IsSorted(got) {
		t.Fatal("no reordering observed")
	}
	if uint64(len(got)) != st.Delivered {
		t.Fatalf("delivered %d, recorded %d", st.Delivered, len(got))
	}
}

func TestMemPartitionsAndCrashes(t *testing.T) {
	net, eps, recs := setup(3, 3)
	net.Partition([]raft.NodeID{1}, []raft.NodeID{2, 3})
	eps[0].Send([]raft.Message{hb(1, 2, 1)})
	eps[1].Send([]raft.Message{hb(2, 3, 2)})
	run(net, 3)
	if len(recs[1].got) != 0 || len(recs[2].got) != 1 {
		t.Fatalf("partition not enforced: %d %d", len(recs[1].got), len(recs[2].got))
	}
	net.Heal()
	// In-flight messages to a node that crashes are lost.
	net.SetDefaultLink(LinkConfig{MinDelay: 5, MaxDelay: 5})
	eps[0].Send([]raft.Message{hb(1, 3, 3)})
	net.Crash(3)
	run(net, 10)
	net.Restart(3)
	if len(recs[2].got) != 1 {
		t.Fatal("message delivered to a crashed node")
	}
	// Disconnect drops messages already in flight when they arrive.
	eps[0].Send([]raft.Message{hb(1, 2, 4)})
	net.Disconnect(2)
	run(net, 10)
	net.Connect(2)
	if len(recs[1].got) != 0 {
		t.Fatal("message delivered to a disconnected node")
	}
	if !net.Connected(1, 2) || net.Connected(1, 9) {
		t.Fatal("Connected")
	}
	// Interposer drops selected messages.
	net.SetInterposer(func(m *raft.Message) bool { return m.Context != 5 })
	eps[0].Send([]raft.Message{hb(1, 2, 5), hb(1, 2, 6)})
	run(net, 10)
	if c := recs[1].contexts(); len(c) != 1 || c[0] != 6 {
		t.Fatalf("interposer: %v", c)
	}
}

func TestMemDeterministic(t *testing.T) {
	trace := func() []uint64 {
		net, eps, recs := setup(42, 3)
		net.SetDefaultLink(LinkConfig{DropRate: 0.2, DupRate: 0.1, MinDelay: 1, MaxDelay: 20, ReorderRate: 0.3, ReorderDelay: 40})
		for i := range 500 {
			eps[i%2].Send([]raft.Message{hb(raft.NodeID(i%2+1), 3, uint64(i))})
		}
		run(net, 100)
		return recs[2].contexts()
	}
	if a, b := trace(), trace(); !slices.Equal(a, b) {
		t.Fatal("same seed produced different delivery orders")
	}
}

func TestMemConcurrentAndRealtime(t *testing.T) {
	net, eps, recs := setup(7, 3)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { net.RunRealtime(ctx, time.Millisecond); close(done) }()
	var wg sync.WaitGroup
	for g := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				eps[g].Send([]raft.Message{hb(raft.NodeID(g+1), 3, uint64(i))})
			}
		}()
	}
	wg.Wait()
	deadline := time.Now().Add(5 * time.Second)
	for len(recs[2].contexts()) < 400 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if n := len(recs[2].contexts()); n != 400 {
		t.Fatalf("delivered %d/400", n)
	}
	_ = eps[2].Close()
	var _ Transport = eps[0]
}
