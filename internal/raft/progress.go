package raft

import "fmt"

// ProgressState is the replication state the leader keeps for a follower.
type ProgressState uint8

const (
	// StateProbe: the leader does not know where the follower's log matches
	// and sends at most one MsgApp per heartbeat interval (or response) until
	// a success reveals the match point.
	StateProbe ProgressState = iota
	// StateReplicate: the match point is known; the leader optimistically
	// advances Next after sending and pipelines up to the inflight limits.
	StateReplicate
	// StateSnapshot: the follower needs entries the leader no longer has; a
	// snapshot has been sent and replication pauses until it is acknowledged
	// (or the snapshot attempt times out and the leader probes again).
	StateSnapshot
)

func (s ProgressState) String() string {
	switch s {
	case StateProbe:
		return "probe"
	case StateReplicate:
		return "replicate"
	case StateSnapshot:
		return "snapshot"
	}
	return fmt.Sprintf("ProgressState(%d)", uint8(s))
}

// Progress is the leader's view of one follower (spec §5.4.7).
type Progress struct {
	// Match is the highest index known to be replicated (durably) on the
	// follower. Always a batch LastIndex (or 0 / a snapshot index).
	Match uint64
	// Next is the first offset to send next; always a batch FirstIndex on the
	// leader's log, or the leader's LastIndex+1.
	Next  uint64
	State ProgressState
	// PendingSnapshot is the index of the snapshot in flight (StateSnapshot).
	PendingSnapshot uint64
	// RecentActive is set when any message from the follower arrives in the
	// current term; check-quorum resets it every election timeout.
	RecentActive bool
	// ProbeSent pauses a StateProbe follower after one MsgApp until a
	// response or heartbeat response arrives.
	ProbeSent bool
	// AckedHeartbeat is the highest heartbeat seq the follower acknowledged
	// in this term (ReadIndex confirmation and lease reads).
	AckedHeartbeat uint64

	inflights       *inflights
	snapshotElapsed int // ticks spent in StateSnapshot
	stallTicks      int // ticks in StateReplicate with inflight MsgApps and no progress
}

func newProgress(next uint64, maxMsgs int, maxBytes uint64) *Progress {
	return &Progress{Next: next, inflights: newInflights(maxMsgs, maxBytes)}
}

func (pr *Progress) resetState(state ProgressState) {
	pr.ProbeSent = false
	pr.PendingSnapshot = 0
	pr.State = state
	pr.snapshotElapsed = 0
	pr.stallTicks = 0
	pr.inflights.reset()
}

func (pr *Progress) becomeProbe() {
	if pr.State == StateSnapshot {
		// The snapshot may or may not have landed; resume after it.
		pending := pr.PendingSnapshot
		pr.resetState(StateProbe)
		pr.Next = max(pr.Match+1, pending+1)
		return
	}
	pr.resetState(StateProbe)
	pr.Next = pr.Match + 1
}

func (pr *Progress) becomeReplicate() {
	pr.resetState(StateReplicate)
	pr.Next = pr.Match + 1
}

func (pr *Progress) becomeSnapshot(index uint64) {
	pr.resetState(StateSnapshot)
	pr.PendingSnapshot = index
}

// maybeUpdate records a successful ack of index n. It returns false if the
// ack is stale (n <= Match).
func (pr *Progress) maybeUpdate(n uint64) bool {
	if n <= pr.Match {
		return false
	}
	pr.Match = n
	pr.Next = max(pr.Next, n+1)
	pr.ProbeSent = false
	pr.stallTicks = 0
	return true
}

// isPaused reports whether the leader should refrain from sending MsgApp.
func (pr *Progress) isPaused() bool {
	switch pr.State {
	case StateProbe:
		return pr.ProbeSent
	case StateReplicate:
		return pr.inflights.full()
	default:
		return true
	}
}

// sentEntries updates the progress after sending entries ending at last.
func (pr *Progress) sentEntries(last uint64, bytes uint64, nonEmpty bool) {
	switch pr.State {
	case StateReplicate:
		if nonEmpty {
			pr.Next = last + 1
			pr.inflights.add(last, bytes)
		}
	case StateProbe:
		pr.ProbeSent = true
	}
}

func (pr Progress) String() string {
	inflight := 0
	if pr.inflights != nil {
		inflight = pr.inflights.count
	}
	return fmt.Sprintf("{match=%d next=%d %v inflight=%d active=%v}", pr.Match, pr.Next, pr.State, inflight, pr.RecentActive)
}

// inflights is a bounded FIFO of the last index (and byte size) of each
// in-flight MsgApp to one follower: the pipelining window.
type inflights struct {
	maxMsgs  int
	maxBytes uint64 // 0 = unlimited
	start    int
	count    int
	bytes    uint64
	buf      []inflight
}

type inflight struct {
	index uint64
	bytes uint64
}

func newInflights(maxMsgs int, maxBytes uint64) *inflights {
	return &inflights{maxMsgs: maxMsgs, maxBytes: maxBytes, buf: make([]inflight, maxMsgs)}
}

func (in *inflights) full() bool {
	return in.count >= in.maxMsgs || (in.maxBytes > 0 && in.bytes >= in.maxBytes)
}

func (in *inflights) add(index, bytes uint64) {
	if in.count >= in.maxMsgs {
		panic("raft: inflights overflow")
	}
	in.buf[(in.start+in.count)%in.maxMsgs] = inflight{index, bytes}
	in.count++
	in.bytes += bytes
}

// freeLE frees every in-flight message whose last index is <= to.
func (in *inflights) freeLE(to uint64) {
	for in.count > 0 && in.buf[in.start].index <= to {
		in.bytes -= in.buf[in.start].bytes
		in.start = (in.start + 1) % in.maxMsgs
		in.count--
	}
	if in.count == 0 {
		in.start = 0
	}
}

// freeFirst frees the oldest in-flight message (used when a heartbeat
// response shows the follower is alive but the window is stuck full).
func (in *inflights) freeFirst() {
	if in.count == 0 {
		return
	}
	in.freeLE(in.buf[in.start].index)
}

func (in *inflights) reset() {
	in.start, in.count, in.bytes = 0, 0, 0
}
