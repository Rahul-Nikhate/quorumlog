package transport

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

type inbox struct {
	mu  sync.Mutex
	got []raft.Message
}

func (b *inbox) handle(m raft.Message) {
	b.mu.Lock()
	b.got = append(b.got, m)
	b.mu.Unlock()
}

func (b *inbox) count(pred func(raft.Message) bool) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, m := range b.got {
		if pred(m) {
			n++
		}
	}
	return n
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

// tcpCluster starts n transports on localhost with fast backoff.
func tcpCluster(t *testing.T, n int) ([]*TCP, []*inbox, map[raft.NodeID]string) {
	t.Helper()
	lns := make([]net.Listener, n)
	addrs := map[raft.NodeID]string{}
	for i := range lns {
		lns[i] = listen(t)
		addrs[raft.NodeID(i+1)] = lns[i].Addr().String()
	}
	trs := make([]*TCP, n)
	boxes := make([]*inbox, n)
	for i := range trs {
		boxes[i] = &inbox{}
		tr, err := NewTCP(tcpConfig(raft.NodeID(i+1), lns[i], addrs, boxes[i].handle))
		if err != nil {
			t.Fatal(err)
		}
		trs[i] = tr
		t.Cleanup(func() { _ = tr.Close() })
	}
	return trs, boxes, addrs
}

func tcpConfig(id raft.NodeID, ln net.Listener, addrs map[raft.NodeID]string, h Handler) TCPConfig {
	return TCPConfig{ID: id, Listener: ln, Peers: addrs, Handler: h, MinBackoff: 5 * time.Millisecond, MaxBackoff: 50 * time.Millisecond}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// Every pair exchanges messages of several groups over one connection; the
// group id and payload survive the trip.
func TestTCPDeliveryAllPairsAndGroups(t *testing.T) {
	trs, boxes, _ := tcpCluster(t, 3)
	for i, tr := range trs {
		from := raft.NodeID(i + 1)
		var msgs []raft.Message
		for to := raft.NodeID(1); to <= 3; to++ {
			if to == from {
				continue
			}
			for g := uint64(1); g <= 3; g++ {
				for k := range 50 {
					msgs = append(msgs, raft.Message{Type: raft.MsgHeartbeat, Group: g, From: from, To: to, Term: 1, Context: uint64(k)})
				}
			}
		}
		// The first messages may be queued before the connection is up; they
		// are delivered once it is.
		tr.Send(msgs)
	}
	for i, b := range boxes {
		to := raft.NodeID(i + 1)
		waitFor(t, "all messages", func() bool { return b.count(func(raft.Message) bool { return true }) == 2*3*50 })
		for from := raft.NodeID(1); from <= 3; from++ {
			if from == to {
				continue
			}
			for g := uint64(1); g <= 3; g++ {
				if n := b.count(func(m raft.Message) bool { return m.From == from && m.Group == g && m.To == to }); n != 50 {
					t.Fatalf("N%d got %d msgs from %d group %d", to, n, from, g)
				}
			}
		}
	}
	// Per (peer, group) order is preserved on one connection.
	b := boxes[1]
	b.mu.Lock()
	var last uint64
	for _, m := range b.got {
		if m.From == 1 && m.Group == 2 {
			if m.Context < last {
				t.Fatalf("reordered: %d after %d", m.Context, last)
			}
			last = m.Context
		}
	}
	b.mu.Unlock()
	if !trs[0].Connected(2) || !trs[1].Connected(1) || trs[0].Connected(9) {
		t.Fatal("Connected() wrong")
	}
}

func TestTCPLargeMessage(t *testing.T) {
	trs, boxes, _ := tcpCluster(t, 2)
	data := bytes.Repeat([]byte("q"), 2<<20)
	trs[1].Send([]raft.Message{{Type: raft.MsgApp, Group: 7, From: 2, To: 1, Term: 3, PrevLogIndex: 4, PrevLogTerm: 2,
		Entries: []raft.Entry{{Term: 3, FirstIndex: 5, LastIndex: 5, Data: data}}}})
	waitFor(t, "large message", func() bool { return boxes[0].count(func(raft.Message) bool { return true }) == 1 })
	m := boxes[0].got[0]
	if m.Group != 7 || len(m.Entries) != 1 || !bytes.Equal(m.Entries[0].Data, data) {
		t.Fatalf("large message mangled: group=%d entries=%d", m.Group, len(m.Entries))
	}
}

// A peer that goes away and comes back on the same address is reconnected,
// whichever side dials.
func TestTCPReconnect(t *testing.T) {
	trs, boxes, addrs := tcpCluster(t, 3)
	ping := func(from raft.NodeID, to raft.NodeID, ctx uint64) {
		trs[from-1].Send([]raft.Message{{Type: raft.MsgHeartbeat, Group: 1, From: from, To: to, Term: 1, Context: ctx}})
	}
	got := func(b *inbox, from raft.NodeID, ctx uint64) func() bool {
		return func() bool {
			return b.count(func(m raft.Message) bool { return m.From == from && m.Context == ctx }) > 0
		}
	}
	ping(1, 2, 1)
	ping(3, 2, 1)
	waitFor(t, "initial delivery", func() bool { return got(boxes[1], 1, 1)() && got(boxes[1], 3, 1)() })

	// Stop node 2 (node 1 dials it; node 3 is dialled by it).
	_ = trs[1].Close()
	waitFor(t, "disconnect noticed", func() bool { return !trs[0].Connected(2) && !trs[2].Connected(2) })
	for k := uint64(100); k < 110; k++ { // queued or dropped while it is down
		ping(1, 2, k)
		ping(3, 2, k)
	}
	ln, err := net.Listen("tcp", addrs[2])
	if err != nil {
		t.Fatalf("relisten: %v", err)
	}
	nb := &inbox{}
	tr2, err := NewTCP(tcpConfig(2, ln, addrs, nb.handle))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tr2.Close() })
	trs[1] = tr2
	waitFor(t, "reconnected", func() bool { return trs[0].Connected(2) && trs[2].Connected(2) && tr2.Connected(1) && tr2.Connected(3) })
	ping(1, 2, 7)
	ping(3, 2, 7)
	tr2.Send([]raft.Message{{Type: raft.MsgHeartbeat, Group: 1, From: 2, To: 1, Term: 1, Context: 8}, {Type: raft.MsgHeartbeat, Group: 1, From: 2, To: 3, Term: 1, Context: 8}})
	waitFor(t, "delivery after reconnect", func() bool {
		return got(nb, 1, 7)() && got(nb, 3, 7)() && got(boxes[0], 2, 8)() && got(boxes[2], 2, 8)()
	})
}

// Send never blocks: with the peer unreachable and a tiny queue, the
// overflow is dropped and counted.
func TestTCPQueueFullDrops(t *testing.T) {
	dead := listen(t)
	deadAddr := dead.Addr().String()
	_ = dead.Close()
	ln := listen(t)
	tr, err := NewTCP(TCPConfig{ID: 1, Listener: ln, Peers: map[raft.NodeID]string{1: ln.Addr().String(), 2: deadAddr},
		Handler: func(raft.Message) {}, QueueSize: 4, MinBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	start := time.Now()
	for k := range 1000 {
		tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, Group: 1, From: 1, To: 2, Term: 1, Context: uint64(k)}})
	}
	tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: 1, To: 42}}) // unknown peer
	if el := time.Since(start); el > time.Second {
		t.Fatalf("Send blocked for %v", el)
	}
	if st := tr.Stats(); st.Dropped < 1000-4 {
		t.Fatalf("dropped %d, want >= %d", st.Dropped, 1000-4)
	}
}

// Garbage and wrong-identity connections are rejected without disturbing
// the real peer link.
func TestTCPRejectsBadHandshake(t *testing.T) {
	trs, boxes, addrs := tcpCluster(t, 2)
	for _, payload := range [][]byte{
		[]byte("GET / HTTP/1.1\r\n\r\n"),
		{0x51, 0x4C, 0x54, 0x31, 0, 0, 0, 9, 0, 0, 0, 2}, // unknown node 9
		{0x51, 0x4C, 0x54, 0x31, 0, 0, 0, 1, 0, 0, 0, 7}, // wrong "to"
	} {
		c, err := net.Dial("tcp", addrs[2])
		if err != nil {
			t.Fatal(err)
		}
		_, _ = c.Write(payload)
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := c.Read(make([]byte, 1)); err == nil {
			t.Fatal("bad connection got data")
		}
		_ = c.Close()
	}
	trs[0].Send([]raft.Message{{Type: raft.MsgHeartbeat, Group: 1, From: 1, To: 2, Term: 1, Context: 5}})
	waitFor(t, "real link still works", func() bool { return boxes[1].count(func(m raft.Message) bool { return m.Context == 5 }) == 1 })
}

// Close is idempotent, prompt, and stops all handler calls.
func TestTCPCloseStopsHandler(t *testing.T) {
	trs, boxes, _ := tcpCluster(t, 2)
	trs[0].Send([]raft.Message{{Type: raft.MsgHeartbeat, Group: 1, From: 1, To: 2, Term: 1}})
	waitFor(t, "first message", func() bool { return boxes[1].count(func(raft.Message) bool { return true }) == 1 })
	start := time.Now()
	_ = trs[1].Close()
	_ = trs[1].Close()
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("Close took %v", el)
	}
	n := boxes[1].count(func(raft.Message) bool { return true })
	for k := range 100 {
		trs[0].Send([]raft.Message{{Type: raft.MsgHeartbeat, Group: 1, From: 1, To: 2, Term: 1, Context: uint64(k)}})
	}
	trs[1].Send([]raft.Message{{Type: raft.MsgHeartbeat, Group: 1, From: 2, To: 1, Term: 1}}) // after Close: ignored
	time.Sleep(50 * time.Millisecond)
	if m := boxes[1].count(func(raft.Message) bool { return true }); m != n {
		t.Fatalf("handler called after Close (%d → %d)", n, m)
	}
}

// A peer that accepts the connection and then goes silent (no frames, not
// even keepalives — e.g. its host lost power) is dropped after IdleTimeout
// and redialled; a live but idle link is kept up by keepalive frames.
func TestTCPIdleTimeoutAndKeepalive(t *testing.T) {
	silent := listen(t)
	conns := make(chan net.Conn, 8)
	go func() {
		for {
			c, err := silent.Accept()
			if err != nil {
				return
			}
			conns <- c // read nothing, write nothing
		}
	}()
	ln := listen(t)
	tr, err := NewTCP(TCPConfig{ID: 1, Listener: ln, Peers: map[raft.NodeID]string{1: ln.Addr().String(), 2: silent.Addr().String()},
		Handler: func(raft.Message) {}, IdleTimeout: 200 * time.Millisecond, MinBackoff: 5 * time.Millisecond, MaxBackoff: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	var first net.Conn
	select {
	case first = <-conns:
	case <-time.After(5 * time.Second):
		t.Fatal("no connection")
	}
	select {
	case second := <-conns: // redialled after the idle timeout
		_ = second.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("silent peer not dropped and redialled")
	}
	_ = first.Close()
	_ = tr.Close()
	_ = silent.Close()
	for len(conns) > 0 {
		_ = (<-conns).Close()
	}

	// Two real transports with no traffic stay connected across several
	// idle timeouts (keepalive frames every second).
	l1, l2 := listen(t), listen(t)
	addrs := map[raft.NodeID]string{1: l1.Addr().String(), 2: l2.Addr().String()}
	var trs []*TCP
	for i, l := range []net.Listener{l1, l2} {
		cfg := tcpConfig(raft.NodeID(i+1), l, addrs, func(raft.Message) {})
		cfg.IdleTimeout = 1500 * time.Millisecond
		tr, err := NewTCP(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer tr.Close()
		trs = append(trs, tr)
	}
	waitFor(t, "connected", func() bool { return trs[0].Connected(2) && trs[1].Connected(1) })
	time.Sleep(3500 * time.Millisecond)
	if !trs[0].Connected(2) || !trs[1].Connected(1) {
		t.Fatal("idle link dropped despite keepalives")
	}
}
