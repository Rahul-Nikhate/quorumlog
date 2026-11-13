package raft

// Linearizable reads (spec §5.13, Raft thesis §6.4).
//
// ReadIndex protocol, as implemented here:
//  1. The leader must have committed an entry of its current term (the
//     election no-op); requests arriving earlier wait for it.
//  2. readIndex := commitIndex at the time the request is registered.
//  3. The request is tagged with the next heartbeat sequence number s. All
//     requests registered between two Ready() calls share one heartbeat round
//     (batching), which is sent when the driver next calls Ready/HasReady.
//  4. A follower acknowledging heartbeat seq s (in the leader's term) proves
//     it had not moved to a newer term when it received a message sent after
//     the request arrived. Once a quorum (counting the leader) has acked a seq
//     >= s, every request with seq <= s is confirmed and returned as a
//     ReadState{Index: readIndex, Context: ctx}.
//  5. The driver serves the read once its applied index >= ReadState.Index.
//
// Followers forward ReadIndex to the leader (MsgReadIndex) and receive the
// index back (MsgReadIndexResp); they then wait for their own apply.
//
// Lease reads (Config.UnsafeLeaseReads) skip the heartbeat round when a quorum
// acknowledged a heartbeat sent less than ElectionTick-LeaseMarginTicks ticks
// ago. This is ONLY safe if every node's tick rate is bounded relative to the
// others (no clock-rate skew) and the process cannot pause between the lease
// check and serving the read. A GC pause, SIGSTOP or a fast-ticking follower
// breaks it. It also requires CheckQuorum (voters must refuse votes while they
// hear from a leader) and is disabled for the rest of a term once the leader
// has sent MsgTimeoutNow (a transfer vote bypasses the voters' lease).

// ReadState is a confirmed ReadIndex request: once the local applied index is
// >= Index, a read issued with Context reflects every write committed before
// the request was made.
type ReadState struct {
	Index   uint64
	Context uint64
}

type readRequest struct {
	from  NodeID // None/self = local request
	ctx   uint64
	index uint64
	seq   uint64
}

type hbRecord struct {
	seq  uint64
	tick uint64
}

// readOnly is the leader-side ReadIndex state.
type readOnly struct {
	queue        []readRequest // ordered by seq
	waitCommit   []readRequest // waiting for the first commit of the current term
	needBcast    bool          // a heartbeat round must be sent for queued requests
	hbSeq        uint64        // last heartbeat seq sent in this term
	hbSent       []hbRecord    // seq → tick sent, for lease computation (pruned)
	leaseAllowed bool          // false once MsgTimeoutNow was sent in this term
}

func (ro *readOnly) reset() {
	*ro = readOnly{leaseAllowed: true}
}

// readIndex handles a ReadIndex request on the leader.
func (n *Node) readIndex(from NodeID, ctx uint64) {
	req := readRequest{from: from, ctx: ctx}
	if !n.committedInCurrentTerm() && !n.cfg.knobs.readIndexNoQuorum {
		n.ro.waitCommit = append(n.ro.waitCommit, req)
		return
	}
	n.registerRead(req)
}

func (n *Node) registerRead(req readRequest) {
	req.index = n.log.committed
	if len(n.peers) == 1 || n.leaseValid() || n.cfg.knobs.readIndexNoQuorum {
		n.respondRead(req)
		return
	}
	req.seq = n.ro.hbSeq + 1
	n.ro.queue = append(n.ro.queue, req)
	n.ro.needBcast = true
}

func (n *Node) respondRead(req readRequest) {
	if req.from == None || req.from == n.id {
		n.readStates = append(n.readStates, ReadState{Index: req.index, Context: req.ctx})
		return
	}
	n.send(Message{Type: MsgReadIndexResp, To: req.from, Context: req.ctx, Index: req.index})
}

// releaseWaitingReads registers requests that were waiting for the first
// commit of the term.
func (n *Node) releaseWaitingReads() {
	if len(n.ro.waitCommit) == 0 || !n.committedInCurrentTerm() {
		return
	}
	reqs := n.ro.waitCommit
	n.ro.waitCommit = nil
	for _, r := range reqs {
		n.registerRead(r)
	}
}

// quorumAckedSeq returns the highest heartbeat seq acknowledged by a quorum
// (the leader counts as having acked its latest seq).
func (n *Node) quorumAckedSeq() uint64 {
	seqs := make([]uint64, 0, len(n.peers))
	seqs = append(seqs, n.ro.hbSeq)
	for _, pr := range n.prs {
		seqs = append(seqs, pr.AckedHeartbeat)
	}
	return quorumValue(seqs)
}

// advanceReads confirms queued requests covered by the quorum-acked seq.
func (n *Node) advanceReads() {
	if len(n.ro.queue) == 0 && !n.cfg.UnsafeLeaseReads {
		return
	}
	q := n.quorumAckedSeq()
	k := 0
	for k < len(n.ro.queue) && n.ro.queue[k].seq <= q {
		n.respondRead(n.ro.queue[k])
		k++
	}
	n.ro.queue = n.ro.queue[k:]
	// Prune lease bookkeeping below the quorum seq (keep q itself).
	p := 0
	for p < len(n.ro.hbSent) && n.ro.hbSent[p].seq < q {
		p++
	}
	n.ro.hbSent = n.ro.hbSent[p:]
}

// leaseValid reports whether an (unsafe) lease read may skip the heartbeat
// round. See the package comment above for the assumptions.
func (n *Node) leaseValid() bool {
	if !n.cfg.UnsafeLeaseReads || !n.cfg.CheckQuorum || !n.ro.leaseAllowed || n.leadTransferee != None {
		return false
	}
	q := n.quorumAckedSeq()
	if q == 0 {
		return false
	}
	for _, r := range n.ro.hbSent {
		if r.seq == q {
			return n.ticks+uint64(n.cfg.LeaseMarginTicks) < r.tick+uint64(n.cfg.ElectionTick)
		}
	}
	return false
}

// flushReadBroadcast sends the batched heartbeat round for queued reads.
func (n *Node) flushReadBroadcast() {
	if n.state == Leader && n.ro.needBcast {
		n.bcastHeartbeat()
	}
}
