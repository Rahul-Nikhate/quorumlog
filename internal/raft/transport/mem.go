package transport

import (
	"container/heap"
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
)

// LinkConfig describes the behaviour of a directed link. Time is measured in
// abstract network time units advanced by Network.Advance (the rafttest
// harness uses several units per Raft tick).
type LinkConfig struct {
	DropRate     float64 // probability a message is dropped
	DupRate      float64 // probability a message is delivered twice
	MinDelay     uint64  // delivery delay, uniform in [MinDelay, MaxDelay]
	MaxDelay     uint64
	ReorderRate  float64 // probability of an extra delay in [0, ReorderDelay]
	ReorderDelay uint64
}

// Stats counts network events.
type Stats struct {
	Sent, Delivered, Dropped, Duplicated uint64
}

// Network is a deterministic in-memory network for tests: nodes are
// addressed by raft.NodeID; links can drop, delay, duplicate and reorder
// messages; nodes can be partitioned into arbitrary groups, disconnected,
// crashed and restarted. All randomness comes from one seeded RNG, and
// delivery order is a pure function of (seed, sequence of calls), so a test
// run is reproducible from its seed.
//
// Network is safe for concurrent use. Handlers are invoked without the lock
// held, in delivery order, from the goroutine calling Advance.
type Network struct {
	mu        sync.Mutex
	rng       *rand.Rand
	now       uint64
	seq       uint64
	nodes     map[raft.NodeID]*memNode
	links     map[[2]raft.NodeID]LinkConfig
	def       LinkConfig
	group     map[raft.NodeID]int // partition group; nil = fully connected
	queue     msgQueue
	encode    bool
	stats     Stats
	perType   map[raft.MsgType]uint64
	interpose func(m *raft.Message) bool
}

type memNode struct {
	handler   Handler
	up        bool // false = crashed
	connected bool // false = disconnected from everyone
}

// NewNetwork returns a network with a reliable default link (delay 1) whose
// randomness derives from seed. Messages are round-tripped through the raft
// wire codec by default (SetEncode(false) disables it).
func NewNetwork(seed uint64) *Network {
	return &Network{
		rng:     rand.New(rand.NewPCG(seed, 0x5DEECE66D)),
		nodes:   map[raft.NodeID]*memNode{},
		links:   map[[2]raft.NodeID]LinkConfig{},
		def:     LinkConfig{MinDelay: 1, MaxDelay: 1},
		encode:  true,
		perType: map[raft.MsgType]uint64{},
	}
}

// SetEncode controls whether messages are encoded/decoded with the raft wire
// codec in transit (exercising the codec and isolating sender and receiver
// memory).
func (n *Network) SetEncode(on bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.encode = on
}

// Register attaches a node with its inbound handler and returns its endpoint.
// Re-registering an id replaces the handler (used on restart).
func (n *Network) Register(id raft.NodeID, h Handler) *Endpoint {
	n.mu.Lock()
	defer n.mu.Unlock()
	if nd, ok := n.nodes[id]; ok {
		nd.handler = h
	} else {
		n.nodes[id] = &memNode{handler: h, up: true, connected: true}
	}
	return &Endpoint{net: n, id: id}
}

// SetDefaultLink sets the configuration for links without an override.
func (n *Network) SetDefaultLink(c LinkConfig) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.def = c
}

// SetLink overrides the configuration of the directed link from → to.
func (n *Network) SetLink(from, to raft.NodeID, c LinkConfig) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.links[[2]raft.NodeID{from, to}] = c
}

// ClearLinks removes all per-link overrides.
func (n *Network) ClearLinks() {
	n.mu.Lock()
	defer n.mu.Unlock()
	clear(n.links)
}

// Partition splits the nodes into the given groups; messages only flow within
// a group. Registered nodes not listed in any group are isolated.
func (n *Network) Partition(groups ...[]raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = map[raft.NodeID]int{}
	for gi, g := range groups {
		for _, id := range g {
			n.group[id] = gi + 1
		}
	}
}

// Heal removes any partition and reconnects every node (crashed nodes stay
// crashed).
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = nil
	for _, nd := range n.nodes {
		nd.connected = true
	}
}

// Disconnect cuts a node off from everyone (in both directions) until
// Connect. In-flight messages to/from it are dropped at delivery time.
func (n *Network) Disconnect(id raft.NodeID) { n.setConnected(id, false) }

// Connect reverses Disconnect.
func (n *Network) Connect(id raft.NodeID) { n.setConnected(id, true) }

func (n *Network) setConnected(id raft.NodeID, on bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if nd, ok := n.nodes[id]; ok {
		nd.connected = on
	}
}

// Crash marks a node as down: messages to it are dropped (including those
// already in flight) and its sends are discarded, until Restart.
func (n *Network) Crash(id raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if nd, ok := n.nodes[id]; ok {
		nd.up = false
	}
}

// Restart marks a crashed node as up again. Messages sent to it while it was
// down are gone.
func (n *Network) Restart(id raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if nd, ok := n.nodes[id]; ok {
		nd.up = true
	}
}

// Connected reports whether from can currently reach to.
func (n *Network) Connected(from, to raft.NodeID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.reachableLocked(from, to)
}

// SetInterposer installs a hook called (under the network lock, so it must
// not call back into the Network) for every message at send time; returning
// false drops the message. Tests use it to drop specific message types.
func (n *Network) SetInterposer(f func(m *raft.Message) bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.interpose = f
}

func (n *Network) reachableLocked(from, to raft.NodeID) bool {
	a, b := n.nodes[from], n.nodes[to]
	if a == nil || b == nil || !a.up || !b.up || !a.connected || !b.connected {
		return false
	}
	if n.group != nil && (n.group[from] == 0 || n.group[from] != n.group[to]) {
		return false
	}
	return true
}

func (n *Network) linkLocked(from, to raft.NodeID) LinkConfig {
	if c, ok := n.links[[2]raft.NodeID{from, to}]; ok {
		return c
	}
	return n.def
}

func (n *Network) delayLocked(c LinkConfig) uint64 {
	d := c.MinDelay
	if c.MaxDelay > c.MinDelay {
		d += n.rng.Uint64N(c.MaxDelay - c.MinDelay + 1)
	}
	if c.ReorderRate > 0 && n.rng.Float64() < c.ReorderRate {
		d += n.rng.Uint64N(c.ReorderDelay + 1)
	}
	return d
}

func (n *Network) send(from raft.NodeID, msgs []raft.Message) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i := range msgs {
		m := msgs[i]
		n.stats.Sent++
		n.perType[m.Type]++
		if m.From != from {
			panic(fmt.Sprintf("transport: endpoint %d sending message from %d", from, m.From))
		}
		if !n.reachableLocked(m.From, m.To) {
			n.stats.Dropped++
			continue
		}
		if n.interpose != nil && !n.interpose(&m) {
			n.stats.Dropped++
			continue
		}
		c := n.linkLocked(m.From, m.To)
		if c.DropRate > 0 && n.rng.Float64() < c.DropRate {
			n.stats.Dropped++
			continue
		}
		var payload []byte
		if n.encode {
			b, err := raft.EncodeMessage(&m)
			if err != nil {
				panic(fmt.Sprintf("transport: encode %v: %v", m, err))
			}
			payload = b
		}
		copies := 1
		if c.DupRate > 0 && n.rng.Float64() < c.DupRate {
			copies = 2
			n.stats.Duplicated++
		}
		for range copies {
			n.seq++
			heap.Push(&n.queue, &inflightMsg{at: n.now + n.delayLocked(c), seq: n.seq, msg: m, payload: payload})
		}
	}
}

// Now returns the current network time.
func (n *Network) Now() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.now
}

// Pending returns the number of messages in flight.
func (n *Network) Pending() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.queue.Len()
}

// Stats returns network counters.
func (n *Network) Stats() Stats {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.stats
}

// SentByType returns how many messages of type t were sent so far.
func (n *Network) SentByType(t raft.MsgType) uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.perType[t]
}

// Advance moves network time forward by dt and delivers every message due
// at or before the new time that was in flight when Advance was called.
// Messages sent by handlers during delivery are delivered by a later call.
// It returns the number of messages delivered.
func (n *Network) Advance(dt uint64) int {
	n.mu.Lock()
	n.now += dt
	type delivery struct {
		h Handler
		m raft.Message
	}
	var due []delivery
	for n.queue.Len() > 0 && n.queue[0].at <= n.now {
		im := heap.Pop(&n.queue).(*inflightMsg)
		if !n.reachableLocked(im.msg.From, im.msg.To) {
			n.stats.Dropped++
			continue
		}
		m := im.msg
		if im.payload != nil {
			dm, err := raft.DecodeMessage(im.payload)
			if err != nil {
				panic(fmt.Sprintf("transport: decode %v: %v", im.msg, err))
			}
			m = dm
		}
		n.stats.Delivered++
		due = append(due, delivery{n.nodes[m.To].handler, m})
	}
	n.mu.Unlock()
	for _, d := range due {
		if d.h != nil {
			d.h(d.m)
		}
	}
	return len(due)
}

// RunRealtime advances the network by one unit every `unit` of wall-clock
// time until ctx is done. For tests that run real driver goroutines.
func (n *Network) RunRealtime(ctx context.Context, unit time.Duration) {
	t := time.NewTicker(unit)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n.Advance(1)
		}
	}
}

// Endpoint is one node's view of the Network; it implements Transport.
type Endpoint struct {
	net *Network
	id  raft.NodeID
}

var _ Transport = (*Endpoint)(nil)

// Send implements Transport.
func (e *Endpoint) Send(msgs []raft.Message) { e.net.send(e.id, msgs) }

// Close implements Transport; it detaches the handler.
func (e *Endpoint) Close() error {
	e.net.mu.Lock()
	defer e.net.mu.Unlock()
	if nd, ok := e.net.nodes[e.id]; ok {
		nd.handler = nil
	}
	return nil
}

type inflightMsg struct {
	at, seq uint64
	msg     raft.Message
	payload []byte
}

type msgQueue []*inflightMsg

func (q msgQueue) Len() int { return len(q) }
func (q msgQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q msgQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *msgQueue) Push(x any)   { *q = append(*q, x.(*inflightMsg)) }
func (q *msgQueue) Pop() any {
	old := *q
	it := old[len(old)-1]
	old[len(old)-1] = nil
	*q = old[:len(old)-1]
	return it
}
