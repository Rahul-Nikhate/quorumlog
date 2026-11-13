// Package cluster is one QuorumLog node: a data directory, a transport and
// the node's replicas of every Raft group.
//
//   - __meta: the topic list. Creating a topic = proposing a record to
//     __meta. Every node tails the committed __meta log and, for each new
//     topic, opens that topic's Raft group (one partition, replicated on all
//     nodes).
//   - __offsets: consumer-group offsets. Committing = proposing a record;
//     every node rebuilds the map {group, topic} → offset by applying the
//     committed records in order.
//   - one group per topic: the messages.
//
// The state machines of __meta and __offsets are plain consumers of their own
// committed log (a "tailer" goroutine each), so a restart rebuilds them by
// reading the log from offset 0. Reads of that state are made linearizable
// with a Raft ReadIndex barrier on the group's leader followed by waiting for
// the local tailer to reach the barrier's high watermark.
//
// Membership is static: the peer list comes from configuration.
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Rahul-Nikhate/quorumlog/internal/log"
	"github.com/Rahul-Nikhate/quorumlog/internal/partition"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft/transport"
)

// Names of the internal groups. Topic names cannot start with '_'.
const (
	MetaGroup    = "__meta"
	OffsetsGroup = "__offsets"
)

// Raft group ids (tag every Raft message on the shared transport). A topic's
// group id is topicGroupBase + the __meta offset of its create record, which
// every node computes identically.
const (
	metaGroupID    = 1
	offsetsGroupID = 2
	topicGroupBase = 1000
)

// Errors.
var (
	ErrNotFound = errors.New("cluster: not found")
	ErrBadName  = errors.New("cluster: invalid name")
	ErrClosed   = errors.New("cluster: node closed")
)

// Config configures a Node.
type Config struct {
	ID raft.NodeID
	// Peers maps every node id (including ID) to its Raft TCP address.
	Peers   map[raft.NodeID]string
	DataDir string
	Sync    log.SyncPolicy // fsync policy of every group's log (default always)

	TickInterval time.Duration // Raft tick (default 50ms)
	ElectionTick int           // election timeout in ticks (default 10)

	// NewTransport builds the transport given the inbound handler. Default:
	// TCP listening on RaftListener (if set) or Peers[ID].
	NewTransport func(h transport.Handler) (transport.Transport, error)
	// Events is the event feed to write to (default: a private feed).
	Events *Events
	Logger *slog.Logger
}

// Node is one cluster member.
type Node struct {
	cfg     Config
	peers   []raft.NodeID
	mgr     *partition.Manager
	tr      transport.Transport
	events  *Events
	meta    *partition.Partition
	offsets *partition.Partition

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
	elections atomic.Uint64

	mu      sync.Mutex
	topics  map[string]topicInfo
	offs    map[offsetKey]int64
	applied map[uint64]int64 // group id → next offset applied by its tailer
	changed chan struct{}    // closed and replaced when applied/topics change
}

type topicInfo struct {
	group     uint64
	createdAt int64 // __meta offset of the create record
}

type offsetKey struct{ group, topic string }

// Records stored in the internal groups (JSON values).
type metaRecord struct {
	Op   string `json:"op"` // "create_topic"
	Name string `json:"name"`
}

type offsetRecord struct {
	Group  string `json:"group"`
	Topic  string `json:"topic"`
	Offset int64  `json:"offset"`
}

// Open opens (or creates) the node's data directory, starts the transport and
// the internal groups. Topic groups are opened as the __meta log is applied.
func Open(cfg Config) (*Node, error) {
	if cfg.ID == raft.None || cfg.Peers[cfg.ID] == "" && cfg.NewTransport == nil {
		return nil, fmt.Errorf("cluster: node %d is not in the peer list", cfg.ID)
	}
	if cfg.DataDir == "" {
		return nil, errors.New("cluster: DataDir is required")
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 50 * time.Millisecond
	}
	if cfg.ElectionTick <= 0 {
		cfg.ElectionTick = 10
	}
	if cfg.Events == nil {
		cfg.Events = NewEvents(200)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	n := &Node{
		cfg:     cfg,
		mgr:     partition.NewManager(cfg.ID),
		events:  cfg.Events,
		topics:  map[string]topicInfo{},
		offs:    map[offsetKey]int64{},
		applied: map[uint64]int64{},
		changed: make(chan struct{}),
	}
	for id := range cfg.Peers {
		n.peers = append(n.peers, id)
	}
	slices.Sort(n.peers)
	n.ctx, n.cancel = context.WithCancel(context.Background())

	var err error
	if cfg.NewTransport != nil {
		n.tr, err = cfg.NewTransport(n.mgr.Handle)
	} else {
		n.tr, err = transport.NewTCP(transport.TCPConfig{ID: cfg.ID, ListenAddr: cfg.Peers[cfg.ID], Peers: cfg.Peers,
			Handler: n.mgr.Handle, Logger: cfg.Logger})
	}
	if err != nil {
		n.cancel()
		return nil, err
	}
	n.mgr.SetTransport(n.tr)
	if n.meta, err = n.openGroup(MetaGroup, metaGroupID); err == nil {
		n.offsets, err = n.openGroup(OffsetsGroup, offsetsGroupID)
	}
	if err != nil {
		_ = n.Close()
		return nil, err
	}
	n.wg.Add(2)
	go n.tail(n.meta, metaGroupID, n.applyMeta)
	go n.tail(n.offsets, offsetsGroupID, n.applyOffset)
	return n, nil
}

func (n *Node) openGroup(name string, id uint64) (*partition.Partition, error) {
	return n.mgr.Create(partition.Config{
		TopicPartition: partition.TopicPartition{Topic: name},
		Group:          id,
		ID:             n.cfg.ID,
		Peers:          n.peers,
		Dir:            filepath.Join(n.cfg.DataDir, name),
		Log:            log.Config{Sync: n.cfg.Sync, SegmentBytes: 16 << 20},
		TickInterval:   n.cfg.TickInterval,
		ElectionTick:   n.cfg.ElectionTick,
		Logger:         n.cfg.Logger,
		OnStateChange:  n.onStateChange(name),
	})
}

// onStateChange turns role/term/leader changes into events.
func (n *Node) onStateChange(group string) func(partition.Status) {
	return func(st partition.Status) {
		var msg string
		switch st.Role {
		case raft.Leader:
			n.elections.Add(1)
			msg = fmt.Sprintf("became leader term=%d", st.Term)
		case raft.Candidate:
			msg = fmt.Sprintf("started election term=%d", st.Term)
		case raft.PreCandidate:
			msg = fmt.Sprintf("no leader heard: pre-vote, term=%d", st.Term)
		default:
			if st.Leader == raft.None {
				msg = fmt.Sprintf("follower term=%d, no leader known", st.Term)
			} else {
				msg = fmt.Sprintf("follower term=%d leader=%d", st.Term, st.Leader)
			}
		}
		n.events.Add(Event{Node: uint32(n.cfg.ID), Group: group, Msg: msg})
	}
}

// tail applies the committed records of p, in order, from offset 0.
func (n *Node) tail(p *partition.Partition, gid uint64, apply func(partition.Message)) {
	defer n.wg.Done()
	var off int64
	for {
		hwc := p.HWChanged()
		rr, err := p.ReadMessages(n.ctx, partition.ReadRequest{Offset: off, Max: 1000, Consistency: partition.ConsistencyLocal})
		if err != nil {
			if n.ctx.Err() == nil {
				n.cfg.Logger.Warn("cluster: tail", "group", gid, "err", err)
			}
			select {
			case <-n.ctx.Done():
				return
			case <-p.Done():
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		for _, m := range rr.Messages {
			apply(m)
		}
		off = rr.NextOffset
		n.mu.Lock()
		n.applied[gid] = off
		n.notifyLocked()
		n.mu.Unlock()
		if off < rr.HighWatermark {
			continue
		}
		select {
		case <-hwc:
		case <-n.ctx.Done():
			return
		case <-p.Done():
			return
		}
	}
}

func (n *Node) notifyLocked() {
	close(n.changed)
	n.changed = make(chan struct{})
}

func (n *Node) applyMeta(m partition.Message) {
	var r metaRecord
	if err := json.Unmarshal(m.Value, &r); err != nil || r.Op != "create_topic" || ValidTopicName(r.Name) != nil {
		n.cfg.Logger.Warn("cluster: ignoring bad __meta record", "offset", m.Offset)
		return
	}
	n.mu.Lock()
	other, exists := n.topicFoldLocked(r.Name)
	n.mu.Unlock()
	if exists {
		// A concurrent duplicate create: the first record wins. A name that
		// differs only in case would share the topic's directory on a
		// case-insensitive file system (macOS, Windows), so it is refused too.
		if other != r.Name {
			n.cfg.Logger.Warn("cluster: ignoring topic that differs only in case", "topic", r.Name, "existing", other)
		}
		return
	}
	t := topicInfo{group: topicGroupBase + uint64(m.Offset), createdAt: m.Offset}
	if _, err := n.openGroup(r.Name, t.group); err != nil && n.ctx.Err() == nil {
		n.cfg.Logger.Error("cluster: open topic group", "topic", r.Name, "err", err)
	}
	n.mu.Lock()
	n.topics[r.Name] = t
	n.notifyLocked()
	n.mu.Unlock()
}

func (n *Node) applyOffset(m partition.Message) {
	var r offsetRecord
	if err := json.Unmarshal(m.Value, &r); err != nil {
		n.cfg.Logger.Warn("cluster: ignoring bad __offsets record", "offset", m.Offset)
		return
	}
	n.mu.Lock()
	n.offs[offsetKey{r.Group, r.Topic}] = r.Offset
	n.mu.Unlock()
}

// waitApplied waits until the tailer of group gid has applied every record
// below offset h.
func (n *Node) waitApplied(ctx context.Context, gid uint64, h int64) error {
	for {
		n.mu.Lock()
		done, ch := n.applied[gid] >= h, n.changed
		n.mu.Unlock()
		if done {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-n.ctx.Done():
			return ErrClosed
		}
	}
}

// barrier makes a read of p's state machine linearizable: confirm leadership
// (ReadIndex), then wait until the tailer applied up to that point.
func (n *Node) barrier(ctx context.Context, p *partition.Partition, gid uint64) error {
	h, err := p.ReadBarrier(ctx)
	if err != nil {
		return err
	}
	return n.waitApplied(ctx, gid, h)
}

var topicName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`)

// ValidTopicName checks a topic name: 1–63 characters of [A-Za-z0-9._-],
// starting with a letter or digit (so it can never clash with __meta).
func ValidTopicName(s string) error {
	if !topicName.MatchString(s) {
		return fmt.Errorf("%w: topic %q (want 1-63 chars of [A-Za-z0-9._-], starting with a letter or digit)", ErrBadName, s)
	}
	return nil
}

// ValidGroupName checks a consumer group name: 1–128 printable characters.
func ValidGroupName(s string) error {
	if s == "" || len(s) > 128 || strings.ContainsFunc(s, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return fmt.Errorf("%w: group %q", ErrBadName, s)
	}
	return nil
}

// CreateTopic creates a topic through __meta (this node must lead __meta,
// else *partition.NotLeaderError). created is false if it already existed.
// It returns once the create is committed and applied locally, after waiting
// briefly (best effort) for the topic's group to elect a leader.
func (n *Node) CreateTopic(ctx context.Context, name string) (created bool, err error) {
	if err := ValidTopicName(name); err != nil {
		return false, err
	}
	if exists, err := n.checkNewTopic(name); exists || err != nil {
		return false, err
	}
	val, _ := json.Marshal(metaRecord{Op: "create_topic", Name: name})
	res, err := n.meta.ProduceMessages(ctx, partition.ProduceRequest{Messages: []partition.Message{{Value: val}}, Acks: partition.AcksAll})
	if err != nil {
		return false, err
	}
	if err := n.waitApplied(ctx, metaGroupID, res.LastOffset+1); err != nil {
		return false, err
	}
	if exists, err := n.checkNewTopic(name); !exists {
		return false, err // a concurrent create of the same name in another case won
	}
	n.mu.Lock()
	created = n.topics[name].createdAt == res.BaseOffset
	n.mu.Unlock()
	wctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for n.Leader(name) == raft.None && wctx.Err() == nil {
		select {
		case <-wctx.Done():
		case <-time.After(10 * time.Millisecond):
		}
	}
	return created, nil
}

// checkNewTopic reports whether topic name exists, or ErrBadName if a topic
// whose name differs only in case exists (topic directories are named after
// topics, and must not collide on case-insensitive file systems).
func (n *Node) checkNewTopic(name string) (exists bool, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	other, ok := n.topicFoldLocked(name)
	if ok && other != name {
		return false, fmt.Errorf("%w: topic %q conflicts with existing topic %q (names must differ in more than case)", ErrBadName, name, other)
	}
	return ok, nil
}

// topicFoldLocked returns the existing topic whose name equals name ignoring
// case (an exact match first).
func (n *Node) topicFoldLocked(name string) (string, bool) {
	if _, ok := n.topics[name]; ok {
		return name, true
	}
	for t := range n.topics {
		if strings.EqualFold(t, name) {
			return t, true
		}
	}
	return "", false
}

// Topics returns the topic names, linearizably (this node must lead __meta).
func (n *Node) Topics(ctx context.Context) ([]string, error) {
	if err := n.barrier(ctx, n.meta, metaGroupID); err != nil {
		return nil, err
	}
	return n.LocalTopics(), nil
}

// LocalTopics returns the topics this node has applied so far (may be
// stale; for status pages).
func (n *Node) LocalTopics() []string {
	n.mu.Lock()
	out := make([]string, 0, len(n.topics))
	for name := range n.topics {
		out = append(out, name)
	}
	n.mu.Unlock()
	slices.Sort(out)
	return out
}

// Group returns this node's replica of a group: MetaGroup, OffsetsGroup or a
// topic name. ErrNotFound if the topic is unknown to this node (so far).
func (n *Node) Group(name string) (*partition.Partition, error) {
	switch name {
	case MetaGroup:
		return n.meta, nil
	case OffsetsGroup:
		return n.offsets, nil
	}
	n.mu.Lock()
	t, ok := n.topics[name]
	n.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: topic %q", ErrNotFound, name)
	}
	p, ok := n.mgr.ByGroup(t.group)
	if !ok {
		return nil, fmt.Errorf("%w: topic %q is not open on node %d", ErrClosed, name, n.cfg.ID)
	}
	return p, nil
}

// Leader returns the leader this node knows for a group (None if unknown).
func (n *Node) Leader(name string) raft.NodeID {
	p, err := n.Group(name)
	if err != nil {
		return raft.None
	}
	return p.Status().Leader
}

// CommitOffset records a consumer group's offset for a topic through
// __offsets (this node must lead __offsets). Topic existence is not checked:
// offsets are opaque per (group, topic) name.
func (n *Node) CommitOffset(ctx context.Context, group, topic string, offset int64) error {
	if err := ValidGroupName(group); err != nil {
		return err
	}
	if err := ValidTopicName(topic); err != nil {
		return err
	}
	if offset < 0 {
		return fmt.Errorf("%w: negative offset %d", ErrBadName, offset)
	}
	val, _ := json.Marshal(offsetRecord{Group: group, Topic: topic, Offset: offset})
	res, err := n.offsets.ProduceMessages(ctx, partition.ProduceRequest{Messages: []partition.Message{{Value: val}}, Acks: partition.AcksAll})
	if err != nil {
		return err
	}
	return n.waitApplied(ctx, offsetsGroupID, res.LastOffset+1)
}

// FetchOffset returns a group's committed offset for a topic (-1 if none),
// linearizably (this node must lead __offsets).
func (n *Node) FetchOffset(ctx context.Context, group, topic string) (int64, error) {
	if err := ValidGroupName(group); err != nil {
		return 0, err
	}
	if err := n.barrier(ctx, n.offsets, offsetsGroupID); err != nil {
		return 0, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if off, ok := n.offs[offsetKey{group, topic}]; ok {
		return off, nil
	}
	return -1, nil
}

// GroupStatus is one group as seen by this node. Offsets are API (0-based)
// offsets: Commit is the last committed offset (-1 if none), HighWatermark the
// next offset after the last committed (and applied) one, LogEndOffset the
// next offset after the last one in this replica's log.
type GroupStatus struct {
	Name          string      `json:"name"`
	Leader        raft.NodeID `json:"leader"`
	Term          uint64      `json:"term"`
	Role          string      `json:"role"`
	Commit        int64       `json:"commit"`
	HighWatermark int64       `json:"high_watermark"`
	LogEndOffset  int64       `json:"log_end_offset"`
}

// Groups returns the status of every group on this node: __meta, __offsets,
// then topics by name.
func (n *Node) Groups() []GroupStatus {
	names := append([]string{MetaGroup, OffsetsGroup}, n.LocalTopics()...)
	out := make([]GroupStatus, 0, len(names))
	for _, name := range names {
		p, err := n.Group(name)
		if err != nil {
			continue
		}
		st := p.Status()
		out = append(out, GroupStatus{
			Name:          name,
			Leader:        st.Leader,
			Term:          st.Term,
			Role:          RoleName(st.Role),
			Commit:        partition.APIOffset(int64(st.Commit)),
			HighWatermark: partition.APIOffset(st.HighWatermark),
			LogEndOffset:  partition.APIOffset(st.LogEndOffset),
		})
	}
	return out
}

// RoleName is the lower-case role used by the API.
func RoleName(s raft.StateType) string {
	switch s {
	case raft.Leader:
		return "leader"
	case raft.Candidate:
		return "candidate"
	case raft.PreCandidate:
		return "pre-candidate"
	default:
		return "follower"
	}
}

// ID returns the node id.
func (n *Node) ID() raft.NodeID { return n.cfg.ID }

// Peers returns all node ids (including this one), sorted.
func (n *Node) Peers() []raft.NodeID { return slices.Clone(n.peers) }

// PeerUp reports whether this node currently has a transport connection to
// peer id (always true for itself, and for transports that cannot tell).
func (n *Node) PeerUp(id raft.NodeID) bool {
	if id == n.cfg.ID {
		return true
	}
	if c, ok := n.tr.(interface{ Connected(raft.NodeID) bool }); ok {
		return c.Connected(id)
	}
	return true
}

// Events returns the event feed.
func (n *Node) Events() *Events { return n.events }

// Elections counts how often a replica on this node became leader.
func (n *Node) Elections() uint64 { return n.elections.Load() }

// Transport returns the node's transport.
func (n *Node) Transport() transport.Transport { return n.tr }

// Close stops the groups (final fsync, clean shutdown record) and the
// transport. The data directory is kept; Open resumes from it.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		n.cancel()
		err := n.mgr.Close()
		n.wg.Wait()
		n.closeErr = errors.Join(err, n.tr.Close())
	})
	return n.closeErr
}
