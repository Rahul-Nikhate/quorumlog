package partition

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft/transport"
)

// Manager hosts a broker's partition replicas: a map topic-partition →
// *Partition plus the Raft group index used to route inbound messages. One
// transport is shared by all partitions; its handler is Manager.Handle.
//
//	var mgr = partition.NewManager(id)
//	tr, _ := transport.NewTCP(transport.TCPConfig{ID: id, Handler: mgr.Handle, ...})
//	mgr.SetTransport(tr)
//	p, _ := mgr.Create(partition.Config{TopicPartition: tp, Group: g, Peers: peers, Dir: dir})
type Manager struct {
	id raft.NodeID

	mu      sync.RWMutex
	tr      transport.Transport
	byTP    map[TopicPartition]*Partition
	byGroup map[uint64]*Partition
	closed  bool

	unknownGroup atomic.Uint64
}

// NewManager returns an empty manager for broker id.
func NewManager(id raft.NodeID) *Manager {
	return &Manager{id: id, byTP: map[TopicPartition]*Partition{}, byGroup: map[uint64]*Partition{}}
}

// ID returns the broker id.
func (m *Manager) ID() raft.NodeID { return m.id }

// SetTransport sets the transport used by partitions created afterwards
// (partitions already open keep theirs).
func (m *Manager) SetTransport(tr transport.Transport) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tr = tr
}

// Handle routes an inbound Raft message to its partition by Group. It is the
// transport handler: it never blocks; messages for unknown groups (not yet
// created, or removed) are dropped and counted.
func (m *Manager) Handle(msg raft.Message) {
	m.mu.RLock()
	p := m.byGroup[msg.Group]
	m.mu.RUnlock()
	if p == nil || msg.To != m.id {
		m.unknownGroup.Add(1)
		return
	}
	p.Deliver(msg)
}

// UnroutableMessages counts inbound messages dropped for unknown groups or
// wrong recipients.
func (m *Manager) UnroutableMessages() uint64 { return m.unknownGroup.Load() }

// Create opens (or reopens from disk) a replica. cfg.ID and cfg.Transport
// default to the manager's. TopicPartition and Group must be unique.
func (m *Manager) Create(cfg Config) (*Partition, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrClosed
	}
	if cfg.ID == raft.None {
		cfg.ID = m.id
	}
	if cfg.ID != m.id {
		m.mu.Unlock()
		return nil, fmt.Errorf("partition: config ID %d != manager ID %d", cfg.ID, m.id)
	}
	if cfg.Transport == nil {
		cfg.Transport = m.tr
	}
	if _, ok := m.byTP[cfg.TopicPartition]; ok {
		m.mu.Unlock()
		return nil, fmt.Errorf("%w: %v", ErrExists, cfg.TopicPartition)
	}
	if _, ok := m.byGroup[cfg.Group]; ok {
		m.mu.Unlock()
		return nil, fmt.Errorf("%w: raft group %d", ErrExists, cfg.Group)
	}
	// Reserve the names while opening (recovery may take a while) without
	// holding the lock.
	m.byTP[cfg.TopicPartition] = nil
	m.byGroup[cfg.Group] = nil
	m.mu.Unlock()

	p, err := Open(cfg)

	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil || m.closed {
		delete(m.byTP, cfg.TopicPartition)
		delete(m.byGroup, cfg.Group)
		if err == nil {
			_ = p.Close()
			err = ErrClosed
		}
		return nil, err
	}
	m.byTP[cfg.TopicPartition] = p
	m.byGroup[cfg.Group] = p
	return p, nil
}

// Get returns the replica of tp.
func (m *Manager) Get(tp TopicPartition) (*Partition, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p := m.byTP[tp]
	return p, p != nil
}

// ByGroup returns the replica of Raft group g.
func (m *Manager) ByGroup(g uint64) (*Partition, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p := m.byGroup[g]
	return p, p != nil
}

// Partitions returns all open replicas, sorted by topic-partition.
func (m *Manager) Partitions() []*Partition {
	m.mu.RLock()
	out := make([]*Partition, 0, len(m.byTP))
	for _, p := range m.byTP {
		if p != nil {
			out = append(out, p)
		}
	}
	m.mu.RUnlock()
	slices.SortFunc(out, func(a, b *Partition) int {
		ta, tb := a.TopicPartition(), b.TopicPartition()
		if ta.Topic != tb.Topic {
			if ta.Topic < tb.Topic {
				return -1
			}
			return 1
		}
		return int(ta.Partition) - int(tb.Partition)
	})
	return out
}

// Remove closes and unregisters the replica of tp (its files stay on disk).
func (m *Manager) Remove(tp TopicPartition) error {
	m.mu.Lock()
	p := m.byTP[tp]
	if p == nil {
		m.mu.Unlock()
		return fmt.Errorf("%w: %v", ErrUnknownPartition, tp)
	}
	delete(m.byTP, tp)
	delete(m.byGroup, p.Group())
	m.mu.Unlock()
	return p.Close()
}

// Close closes every replica (in parallel). The transport is not closed: the
// caller owns it and should close it afterwards.
func (m *Manager) Close() error {
	m.mu.Lock()
	m.closed = true
	ps := make([]*Partition, 0, len(m.byTP))
	for _, p := range m.byTP {
		if p != nil {
			ps = append(ps, p)
		}
	}
	clear(m.byTP)
	clear(m.byGroup)
	m.mu.Unlock()
	errs := make([]error, len(ps))
	var wg sync.WaitGroup
	for i, p := range ps {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = p.Close()
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// Crash halts every replica (no further disk writes, see Partition.Halt),
// runs crashFS once all drivers are stopped, then releases the logs. Tests
// use it to simulate power loss of the whole broker.
func (m *Manager) Crash(crashFS func()) {
	m.mu.Lock()
	m.closed = true
	ps := make([]*Partition, 0, len(m.byTP))
	for _, p := range m.byTP {
		if p != nil {
			ps = append(ps, p)
		}
	}
	clear(m.byTP)
	clear(m.byGroup)
	m.mu.Unlock()
	for _, p := range ps {
		p.halt.Store(true)
		p.stop.Do(func() { close(p.stopc) })
	}
	for _, p := range ps {
		<-p.donec
	}
	if crashFS != nil {
		crashFS()
	}
	for _, p := range ps {
		_ = p.store.Close()
	}
}
