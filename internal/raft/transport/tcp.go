package transport

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
)

// TCP is the node-to-node transport.
//
//   - One TCP connection per pair of nodes, used in both directions: the node
//     with the smaller id dials, the other accepts. (So there is never a race
//     between two connections for the same pair.)
//   - Handshake (dialer → acceptor, 12 bytes): magic "QLT1", from id, to id.
//   - Frames: [u32 big-endian length][raft wire-encoded message]. Messages of
//     every Raft group share the connection; the receiver demultiplexes on
//     Message.Group (the Handler routes it).
//   - Each peer has a bounded send queue drained by one writer goroutine.
//     Send never blocks: a full queue drops the message (Raft retransmits).
//   - A broken connection is redialled with exponential backoff + jitter by
//     the dialer side; the acceptor side waits for the dialer to come back.
//   - An idle connection carries an empty frame (length 0) every second, and
//     a reader that hears nothing for IdleTimeout drops the connection, so a
//     peer that vanished without closing its socket is noticed quickly.
type TCP struct {
	cfg    TCPConfig
	ln     net.Listener
	peers  map[raft.NodeID]*tcpPeer
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once

	mu      sync.Mutex
	pending map[net.Conn]struct{} // accepted, handshake not done yet

	sent, dropped, received atomic.Uint64
}

// TCPConfig configures a TCP transport.
type TCPConfig struct {
	ID raft.NodeID
	// Listener to accept peers on; if nil, ListenAddr is listened on.
	Listener   net.Listener
	ListenAddr string
	// Peers maps every other node id to its address (an entry for ID is
	// ignored).
	Peers   map[raft.NodeID]string
	Handler Handler

	QueueSize    int           // per-peer send queue in messages (default 4096)
	MaxFrameSize int           // default 64 MiB
	DialTimeout  time.Duration // default 1s
	WriteTimeout time.Duration // per flush (default 5s)
	MinBackoff   time.Duration // default 50ms
	MaxBackoff   time.Duration // default 2s
	IdleTimeout  time.Duration // drop a connection silent for this long (default 5s)
	Logger       *slog.Logger
}

type tcpPeer struct {
	id        raft.NodeID
	addr      string
	dialer    bool // we dial (our id is smaller)
	queue     chan raft.Message
	incoming  chan net.Conn // acceptor side: freshly handshaken conns
	connected atomic.Bool

	mu   sync.Mutex
	conn net.Conn // current connection (closed by Close to unblock writes)
}

const (
	tcpMagic     = 0x514C5431 // "QLT1"
	handshakeLen = 12
)

// NewTCP starts listening and connecting to the peers.
func NewTCP(cfg TCPConfig) (*TCP, error) {
	if cfg.Handler == nil {
		return nil, errors.New("transport: TCPConfig.Handler is required")
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 4096
	}
	if cfg.MaxFrameSize <= 0 {
		cfg.MaxFrameSize = 64 << 20
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 5 * time.Second
	}
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = 50 * time.Millisecond
	}
	if cfg.MaxBackoff < cfg.MinBackoff {
		cfg.MaxBackoff = max(2*time.Second, cfg.MinBackoff)
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 5 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	ln := cfg.Listener
	if ln == nil {
		var err error
		if ln, err = net.Listen("tcp", cfg.ListenAddr); err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &TCP{cfg: cfg, ln: ln, peers: map[raft.NodeID]*tcpPeer{}, ctx: ctx, cancel: cancel, pending: map[net.Conn]struct{}{}}
	for id, addr := range cfg.Peers {
		if id == cfg.ID {
			continue
		}
		t.peers[id] = &tcpPeer{id: id, addr: addr, dialer: cfg.ID < id,
			queue: make(chan raft.Message, cfg.QueueSize), incoming: make(chan net.Conn, 1)}
	}
	t.wg.Add(1)
	go t.acceptLoop()
	for _, p := range t.peers {
		t.wg.Add(1)
		go t.runPeer(p)
	}
	return t, nil
}

// Addr returns the listening address.
func (t *TCP) Addr() net.Addr { return t.ln.Addr() }

// Connected reports whether a connection to peer id is currently up.
func (t *TCP) Connected(id raft.NodeID) bool {
	p := t.peers[id]
	return p != nil && p.connected.Load()
}

// TCPStats counts messages.
type TCPStats struct{ Sent, Dropped, Received uint64 }

// Stats returns message counters.
func (t *TCP) Stats() TCPStats {
	return TCPStats{Sent: t.sent.Load(), Dropped: t.dropped.Load(), Received: t.received.Load()}
}

// Send implements Transport: it queues each message for its peer and never
// blocks (full queue or unknown peer → dropped).
func (t *TCP) Send(msgs []raft.Message) {
	if t.ctx.Err() != nil {
		return
	}
	for _, m := range msgs {
		p := t.peers[m.To]
		if p == nil {
			t.dropped.Add(1)
			continue
		}
		select {
		case p.queue <- m:
		default:
			t.dropped.Add(1)
		}
	}
}

// Close stops all goroutines and closes every connection. The handler is
// never called after Close returns.
func (t *TCP) Close() error {
	t.once.Do(func() {
		t.cancel()
		_ = t.ln.Close()
		t.mu.Lock()
		for c := range t.pending {
			_ = c.Close()
		}
		t.mu.Unlock()
		for _, p := range t.peers {
			p.mu.Lock()
			if p.conn != nil {
				_ = p.conn.Close()
			}
			p.mu.Unlock()
		}
		t.wg.Wait()
		for _, p := range t.peers { // conns handed over but never used
			select {
			case c := <-p.incoming:
				_ = c.Close()
			default:
			}
		}
	})
	return nil
}

func (t *TCP) acceptLoop() {
	defer t.wg.Done()
	for {
		c, err := t.ln.Accept()
		if err != nil {
			if t.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			t.cfg.Logger.Warn("transport: accept", "err", err)
			if !t.sleep(t.cfg.MinBackoff) {
				return
			}
			continue
		}
		t.mu.Lock()
		if t.ctx.Err() != nil {
			t.mu.Unlock()
			_ = c.Close()
			return
		}
		t.pending[c] = struct{}{}
		t.wg.Add(1)
		t.mu.Unlock()
		go t.handshakeIn(c)
	}
}

// handshakeIn validates an inbound connection and hands it to its peer loop.
func (t *TCP) handshakeIn(c net.Conn) {
	defer t.wg.Done()
	var hdr [handshakeLen]byte
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err := io.ReadFull(c, hdr[:])
	_ = c.SetReadDeadline(time.Time{})
	t.mu.Lock()
	delete(t.pending, c)
	t.mu.Unlock()
	from := raft.NodeID(binary.BigEndian.Uint32(hdr[4:]))
	to := raft.NodeID(binary.BigEndian.Uint32(hdr[8:]))
	p := t.peers[from]
	if err != nil || binary.BigEndian.Uint32(hdr[:]) != tcpMagic || to != t.cfg.ID || p == nil || p.dialer {
		t.cfg.Logger.Warn("transport: rejected inbound connection", "remote", c.RemoteAddr().String(), "from", from, "to", to, "err", err)
		_ = c.Close()
		return
	}
	// Replace any conn handed over earlier but not picked up yet.
	for {
		select {
		case p.incoming <- c:
			return
		case old := <-p.incoming:
			_ = old.Close()
		case <-t.ctx.Done():
			_ = c.Close()
			return
		}
	}
}

// runPeer owns the connection to one peer: connect, serve until it breaks,
// repeat.
func (t *TCP) runPeer(p *tcpPeer) {
	defer t.wg.Done()
	backoff := t.cfg.MinBackoff
	for {
		var c net.Conn
		if p.dialer {
			c = t.dial(p, &backoff)
		} else {
			select {
			case c = <-p.incoming:
			case <-t.ctx.Done():
			}
		}
		start := time.Now()
		for c != nil {
			c = t.serve(p, c) // non-nil: a replacement conn arrived
		}
		if t.ctx.Err() != nil {
			return
		}
		if p.dialer {
			// Back off before redialling too, so a peer that accepts and
			// immediately drops us is not hammered. A connection that
			// worked for a while resets the backoff.
			if time.Since(start) > time.Second {
				backoff = t.cfg.MinBackoff
			}
			if !t.backoff(&backoff) {
				return
			}
		}
	}
}

// backoff sleeps for the current backoff with jitter (uniform in
// [b/2, b]: peers that failed together do not retry in lockstep), then
// doubles it up to MaxBackoff. False when closing.
func (t *TCP) backoff(b *time.Duration) bool {
	wait := *b/2 + rand.N(*b/2+1)
	*b = min(2**b, t.cfg.MaxBackoff)
	return t.sleep(wait)
}

// dial connects to p with exponential backoff and jitter; nil when closing.
func (t *TCP) dial(p *tcpPeer, backoff *time.Duration) net.Conn {
	for {
		d := net.Dialer{Timeout: t.cfg.DialTimeout}
		c, err := d.DialContext(t.ctx, "tcp", p.addr)
		if err == nil {
			var hdr [handshakeLen]byte
			binary.BigEndian.PutUint32(hdr[:], tcpMagic)
			binary.BigEndian.PutUint32(hdr[4:], uint32(t.cfg.ID))
			binary.BigEndian.PutUint32(hdr[8:], uint32(p.id))
			_ = c.SetWriteDeadline(time.Now().Add(t.cfg.WriteTimeout))
			if _, err = c.Write(hdr[:]); err == nil {
				return c
			}
			_ = c.Close()
		}
		if t.ctx.Err() != nil || !t.backoff(backoff) {
			return nil
		}
	}
}

func (t *TCP) sleep(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-t.ctx.Done():
		return false
	}
}

// serve runs the writer on c (and a reader goroutine) until c breaks or the
// transport closes. It returns a replacement conn if the dialer reconnected
// (acceptor side), else nil.
func (t *TCP) serve(p *tcpPeer, c net.Conn) (next net.Conn) {
	p.mu.Lock()
	if t.ctx.Err() != nil {
		p.mu.Unlock()
		_ = c.Close()
		return nil
	}
	p.conn = c
	p.mu.Unlock()
	p.connected.Store(true)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		t.readLoop(p, c)
	}()
	defer func() {
		p.connected.Store(false)
		_ = c.Close()
		<-readDone
		p.mu.Lock()
		p.conn = nil
		p.mu.Unlock()
	}()
	w := bufio.NewWriterSize(c, 64<<10)
	var buf []byte
	ping := time.NewTicker(time.Second)
	defer ping.Stop()
	wrote := false
	for {
		select {
		case <-t.ctx.Done():
			return nil
		case <-readDone:
			return nil
		case nc := <-p.incoming:
			return nc
		case <-ping.C:
			if !wrote { // idle for a second: send an empty frame
				_ = c.SetWriteDeadline(time.Now().Add(t.cfg.WriteTimeout))
				if _, err := c.Write([]byte{0, 0, 0, 0}); err != nil {
					return nil
				}
			}
			wrote = false
		case m := <-p.queue:
			wrote = true
			// Write this message and up to 255 more already queued, then
			// flush once (fewer syscalls under load).
			_ = c.SetWriteDeadline(time.Now().Add(t.cfg.WriteTimeout))
			for i := 0; ; i++ {
				var err error
				buf, err = t.appendFrame(buf[:0], &m)
				if err == nil {
					if _, err := w.Write(buf); err != nil {
						return nil
					}
					t.sent.Add(1)
				}
				if i == 255 || len(p.queue) == 0 {
					break
				}
				m = <-p.queue // only this goroutine receives: cannot block
			}
			if err := w.Flush(); err != nil {
				t.cfg.Logger.Debug("transport: write failed", "peer", p.id, "err", err)
				return nil
			}
		}
	}
}

// appendFrame appends [u32 length][encoded m] to dst. Unencodable or
// oversized messages are dropped (returned as an error).
func (t *TCP) appendFrame(dst []byte, m *raft.Message) ([]byte, error) {
	n := raft.EncodedSize(m)
	if n > t.cfg.MaxFrameSize {
		t.dropped.Add(1)
		return dst, fmt.Errorf("transport: message of %d bytes exceeds the frame limit", n)
	}
	dst = binary.BigEndian.AppendUint32(dst, uint32(n))
	out, err := raft.AppendMessage(dst, m)
	if err != nil {
		t.dropped.Add(1)
		return dst[:len(dst)-4], err
	}
	return out, nil
}

// readLoop decodes frames from c and hands them to the handler until c
// fails.
func (t *TCP) readLoop(p *tcpPeer, c net.Conn) {
	r := bufio.NewReaderSize(c, 64<<10)
	var hdr [4]byte
	for {
		_ = c.SetReadDeadline(time.Now().Add(t.cfg.IdleTimeout))
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return
		}
		n := binary.BigEndian.Uint32(hdr[:])
		if n == 0 {
			continue // keepalive
		}
		if int64(n) > int64(t.cfg.MaxFrameSize) {
			t.cfg.Logger.Warn("transport: bad frame length", "peer", p.id, "len", n)
			return
		}
		frame := make([]byte, n)
		_ = c.SetReadDeadline(time.Now().Add(t.cfg.IdleTimeout))
		if _, err := io.ReadFull(r, frame); err != nil {
			return
		}
		m, err := raft.DecodeMessage(frame)
		if err != nil || m.From != p.id || m.To != t.cfg.ID {
			t.cfg.Logger.Warn("transport: bad message", "peer", p.id, "err", err)
			return
		}
		if t.ctx.Err() != nil {
			return
		}
		t.received.Add(1)
		t.cfg.Handler(m)
	}
}
