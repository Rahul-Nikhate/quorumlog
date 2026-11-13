package partition

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Rahul-Nikhate/quorumlog/internal/fs"
	"github.com/Rahul-Nikhate/quorumlog/internal/log"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft/transport"
)

// TopicPartition names a partition.
type TopicPartition struct {
	Topic     string
	Partition int32
}

func (tp TopicPartition) String() string { return fmt.Sprintf("%s-%d", tp.Topic, tp.Partition) }

// Acks is the produce acknowledgement level.
type Acks int8

const (
	// AcksLeader (acks=leader): the batch is in the leader's log, durable
	// locally according to the fsync policy. Lost if the leader fails before a
	// majority has it.
	AcksLeader Acks = 1
	// AcksAll (acks=all): the batch is committed (on a majority; HW > its
	// last offset).
	AcksAll Acks = -1
)

// Consistency selects how Fetch decides that this replica may serve a read.
type Consistency uint8

const (
	// ConsistencyReadIndex (the default) confirms leadership with a heartbeat
	// quorum round (Raft ReadIndex) and waits until the read index is
	// applied: linearizable. Only the leader can serve it.
	ConsistencyReadIndex Consistency = iota
	// ConsistencyLocal serves this replica's local high watermark with no
	// leadership check. Everything returned is committed, but it may be
	// stale. Used by state machines that tail their own log (cluster
	// metadata) and as the negative control of the linearizability test.
	ConsistencyLocal
)

func (c Consistency) String() string {
	switch c {
	case ConsistencyReadIndex:
		return "readindex"
	case ConsistencyLocal:
		return "local"
	}
	return fmt.Sprintf("Consistency(%d)", uint8(c))
}

// Errors. Errors from Produce that leave the outcome unknown (the batch may
// or may not be committed) match ErrOutcomeUnknown; every other Produce error
// means the batch was definitely not appended.
var (
	// ErrNotLeader is raft.ErrNotLeader; errors.As(err, **NotLeaderError)
	// yields the leader hint for Kafka NOT_LEADER_OR_FOLLOWER (6).
	ErrNotLeader = raft.ErrNotLeader
	// ErrOutcomeUnknown marks an indeterminate produce result: leadership was
	// lost after proposing, the request timed out or the partition closed
	// while the batch was in flight. Clients retry (deduplicated by the
	// idempotent producer); history recorders must log it as "info", never
	// "fail" (spec §5.5).
	ErrOutcomeUnknown = errors.New("partition: produce outcome unknown")
	// ErrProposalDropped is returned while a leadership transfer is in
	// progress (retriable; nothing was appended).
	ErrProposalDropped = raft.ErrProposalDropped
	// ErrInvalidBatch: the produced batch is malformed (bad CRC/framing), a
	// control batch, or transactional (unsupported). Kafka CORRUPT_MESSAGE (2)
	// / INVALID_RECORD (87).
	ErrInvalidBatch = errors.New("partition: invalid batch")
	// ErrOffsetOutOfRange: fetch offset outside [LogStartOffset, HW]. Kafka
	// OFFSET_OUT_OF_RANGE (1).
	ErrOffsetOutOfRange = errors.New("partition: offset out of range")
	// ErrClosed: the partition is closed.
	ErrClosed = errors.New("partition: closed")
	// ErrStorage: the partition failed (fail-stop) after a storage error.
	// Kafka KAFKA_STORAGE_ERROR (56). Reopen the partition to recover.
	ErrStorage = errors.New("partition: storage failure")
	// ErrTimeout: a ReadIndex round did not complete in time (leadership in
	// doubt). Retriable.
	ErrTimeout = errors.New("partition: read index timed out")
	// ErrExists / ErrUnknownPartition: manager errors.
	ErrExists           = errors.New("partition: already exists")
	ErrUnknownPartition = errors.New("partition: unknown partition")
)

// NotLeaderError is raft's error type (leader hint + term).
type NotLeaderError = raft.NotLeaderError

// outcomeUnknown wraps the cause of an indeterminate produce result.
type outcomeUnknown struct{ cause error }

func (e *outcomeUnknown) Error() string {
	return "partition: produce outcome unknown: " + e.cause.Error()
}
func (e *outcomeUnknown) Unwrap() []error {
	return []error{ErrOutcomeUnknown, e.cause}
}

func unknown(cause error) error { return &outcomeUnknown{cause: cause} }

// Config configures one partition replica.
type Config struct {
	TopicPartition TopicPartition
	// Group is the Raft group id (unique per broker; tags every message).
	Group uint64
	// ID is this broker's node id; Peers are all replicas (including ID).
	ID    raft.NodeID
	Peers []raft.NodeID
	// Dir is the partition directory; FS defaults to fs.OS.
	Dir string
	FS  fs.FS
	// Log configures the segmented log (SegmentBytes, Sync, ...). Sync
	// defaults to SyncAlways (the policy the correctness claims assume).
	// Dir, FS, InitialOffset and OnDurable are set by the partition.
	Log log.Config
	// Transport sends Raft messages (shared by all partitions of a broker).
	Transport transport.Transport

	// TickInterval is the Raft tick (default 100ms). ElectionTick (default 10)
	// and HeartbeatTick (default 1) are in ticks.
	TickInterval  time.Duration
	ElectionTick  int
	HeartbeatTick int
	// DisablePreVote / DisableCheckQuorum turn off the (default-on) Raft
	// liveness/safety extensions.
	DisablePreVote     bool
	DisableCheckQuorum bool

	// MaxBatchBytes bounds produced batches (default: the log's MaxBatchBytes).
	MaxBatchBytes int
	// InboxSize bounds queued inbound Raft messages (default 4096; overflow
	// is dropped). ProposalQueue bounds queued produce requests (default 1024).
	InboxSize     int
	ProposalQueue int
	// ReadIndexTimeout bounds one ReadIndex round (default 2*election timeout,
	// at least 1s).
	ReadIndexTimeout time.Duration
	// HardStateSyncInterval rate-limits persisting commit-only HardState
	// changes (default 1s). Term/vote changes are always persisted before any
	// message leaves.
	HardStateSyncInterval time.Duration

	// Seed seeds election timeouts (0 = random).
	Seed uint64
	// Logger receives warnings; RaftLogger receives raft debug lines.
	Logger     *slog.Logger
	RaftLogger raft.Logger
	// OnStateChange, if set, is called on the driver goroutine whenever the
	// replica's role, term or known leader changes (event feed). It must not
	// block or call back into the partition.
	OnStateChange func(Status)
	// RaftConfigHook may adjust the raft.Config (tests).
	RaftConfigHook func(*raft.Config)
}

func (c *Config) withDefaults() error {
	if c.Dir == "" {
		return errors.New("partition: Config.Dir is required")
	}
	if c.Transport == nil {
		return errors.New("partition: Config.Transport is required")
	}
	if c.FS == nil {
		c.FS = fs.OS
	}
	if c.TickInterval <= 0 {
		c.TickInterval = 100 * time.Millisecond
	}
	if c.ElectionTick == 0 {
		c.ElectionTick = 10
	}
	if c.HeartbeatTick == 0 {
		c.HeartbeatTick = 1
	}
	if c.MaxBatchBytes <= 0 {
		c.MaxBatchBytes = c.Log.MaxBatchBytes
		if c.MaxBatchBytes <= 0 {
			c.MaxBatchBytes = log.DefaultMaxBatchBytes
		}
	}
	if c.InboxSize <= 0 {
		c.InboxSize = 4096
	}
	if c.ProposalQueue <= 0 {
		c.ProposalQueue = 1024
	}
	if c.ReadIndexTimeout <= 0 {
		c.ReadIndexTimeout = max(time.Second, 2*time.Duration(c.ElectionTick)*c.TickInterval)
	}
	if c.HardStateSyncInterval <= 0 {
		c.HardStateSyncInterval = time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	return nil
}

// Status is a point-in-time view of a replica (admin API, metadata, tests).
// Offsets follow Kafka conventions: HighWatermark = commit index + 1 (the
// first offset NOT yet committed), LogEndOffset = last offset + 1.
type Status struct {
	TopicPartition TopicPartition
	Group          uint64
	ID             raft.NodeID
	Role           raft.StateType
	Term           uint64
	Leader         raft.NodeID
	Commit         uint64 // Raft commit index (last committed offset)
	Applied        uint64 // last applied offset (HW-1)
	HighWatermark  int64
	LogStartOffset int64
	LogEndOffset   int64
	DurableOffset  int64 // last offset durable on this replica (-1/0 if none)
	Producers      int   // tracked idempotent producers
	Err            error // non-nil after fail-stop or Close
}

// ProduceResult describes an appended (or deduplicated) batch.
type ProduceResult struct {
	BaseOffset, LastOffset int64
	// Duplicate is true when the batch repeated one of its producer's last
	// ProducerWindow batches: the offsets are the original append's.
	Duplicate bool
}

// FetchRequest is one partition's part of a Kafka Fetch.
type FetchRequest struct {
	Offset int64
	// MaxBytes bounds the returned region (at least one whole batch is
	// always returned when one is available, for progress).
	MaxBytes int
	// MinBytes: wait (up to MaxWait) until at least this many committed bytes
	// are available from Offset. 0 or 1 = any data.
	MinBytes int
	MaxWait  time.Duration
	// Consistency selects the read rule (default ConsistencyReadIndex).
	Consistency Consistency
	// UpTo, if > 0, additionally excludes batches with lastOffset >= UpTo
	// (e.g. to continue a multi-call read at a fixed read point).
	UpTo int64
}

// FetchResult is the answer to a FetchRequest. Region holds whole batches
// from one segment, all below HighWatermark; the caller MUST Release it
// (nil/empty regions are safe to Release).
type FetchResult struct {
	Region         *log.FileRegion
	HighWatermark  int64
	LogStartOffset int64
}

// hwNotifier is a close-and-replace broadcast (spec §5.6.5): grab Wait()
// before checking the condition, then select on it.
type hwNotifier struct {
	mu sync.Mutex
	ch chan struct{}
}

func newHWNotifier() *hwNotifier { return &hwNotifier{ch: make(chan struct{})} }

func (n *hwNotifier) Wait() <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.ch
}

func (n *hwNotifier) Broadcast() {
	n.mu.Lock()
	close(n.ch)
	n.ch = make(chan struct{})
	n.mu.Unlock()
}
