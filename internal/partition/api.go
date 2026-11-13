package partition

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/Rahul-Nikhate/quorumlog/internal/log"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
)

// TopicPartition returns the partition's name.
func (p *Partition) TopicPartition() TopicPartition { return p.cfg.TopicPartition }

// Group returns the Raft group id.
func (p *Partition) Group() uint64 { return p.cfg.Group }

// Status returns the replica's latest published status (updated by the
// driver after every Ready cycle).
func (p *Partition) Status() Status {
	p.stMu.Lock()
	defer p.stMu.Unlock()
	return p.st
}

// HighWatermark returns the replica's HW (first offset not known committed).
func (p *Partition) HighWatermark() int64 { return p.hw.Load() }

// DroppedMessages counts inbound Raft messages dropped on a full inbox.
func (p *Partition) DroppedMessages() uint64 { return p.dropped.Load() }

// Deliver hands an inbound Raft message to the driver. It never blocks: on a
// full inbox the message is dropped (Raft tolerates loss).
func (p *Partition) Deliver(m raft.Message) {
	select {
	case p.inbox <- m:
	default:
		p.dropped.Add(1)
	}
}

// Done is closed when the driver goroutine has exited (Close or fail-stop).
func (p *Partition) Done() <-chan struct{} { return p.donec }

// Close stops the driver, fails outstanding requests (produce waiters with
// an outcome-unknown error), persists the commit hint and closes the log
// (final fsync + clean-shutdown record). Outstanding FileRegions stay valid
// until released. It returns the fail-stop error if the partition had failed.
func (p *Partition) Close() error {
	p.stop.Do(func() { close(p.stopc) })
	<-p.donec
	if err := p.closedErr(); !errors.Is(err, ErrClosed) {
		return err
	}
	return nil
}

// Halt is Close for crash tests: it stops the driver WITHOUT any further
// disk writes (no final HardState save, no fsync, no clean-shutdown record),
// so the disk state is frozen at this instant. crashFS (e.g. FaultFS.Crash)
// runs next, then the log's resources are released (its writes then fail
// harmlessly). Without Halt, a driver still running while the file system
// "loses power" would see read errors that Raft treats as fatal.
func (p *Partition) Halt(crashFS func()) {
	p.halt.Store(true)
	p.stop.Do(func() { close(p.stopc) })
	<-p.donec
	if crashFS != nil {
		crashFS()
	}
	_ = p.store.Close()
}

// Produce appends one RecordBatch (Kafka v2, uncompressed or compressed —
// records are opaque) and waits for acks. The partition takes ownership of
// batch: it is stamped in place with baseOffset and partitionLeaderEpoch
// (both outside the CRC) and must not be modified afterwards.
//
// Errors: *NotLeaderError (errors.Is ErrNotLeader, with leader hint) when this
// replica is not the leader — definite; ErrOutOfOrderSequence /
// ErrInvalidProducerEpoch / ErrInvalidBatch / ErrProposalDropped — definite;
// anything matching ErrOutcomeUnknown — the batch may or may not be committed
// (leadership lost after proposing, ctx expired while waiting, close).
func (p *Partition) Produce(ctx context.Context, batch log.Batch, acks Acks) (ProduceResult, error) {
	if err := batch.Validate(p.cfg.MaxBatchBytes); err != nil {
		return ProduceResult{}, fmt.Errorf("%w: %w", ErrInvalidBatch, err)
	}
	if batch.IsControl() || batch.IsTransactional() {
		return ProduceResult{}, fmt.Errorf("%w: control/transactional batches are not accepted from producers", ErrInvalidBatch)
	}
	pr := &proposal{batch: batch, acks: acks, done: make(chan produceResult, 1)}
	select {
	case p.propc <- pr:
	case <-ctx.Done():
		return ProduceResult{}, ctx.Err()
	case <-p.donec:
		return ProduceResult{}, p.closedErr()
	}
	select {
	case r := <-pr.done:
		return r.res, r.err
	case <-ctx.Done():
		return ProduceResult{}, unknown(ctx.Err())
	case <-p.donec:
		// The driver answers everything it saw before exiting.
		select {
		case r := <-pr.done:
			return r.res, r.err
		default:
			return ProduceResult{}, p.closedErr() // queued after the final drain: never proposed
		}
	}
}

// readIndex runs one Raft ReadIndex round on the leader and waits until the
// read index is applied locally. Afterwards HighWatermark() > the index.
func (p *Partition) readIndex(ctx context.Context) error {
	r := &readReq{done: make(chan readResult, 1), deadline: time.Now().Add(p.cfg.ReadIndexTimeout)}
	select {
	case p.readc <- r:
	case <-ctx.Done():
		return ctx.Err()
	case <-p.donec:
		return p.closedErr()
	}
	select {
	case res := <-r.done:
		return res.err
	case <-ctx.Done():
		return ctx.Err()
	case <-p.donec:
		select {
		case res := <-r.done:
			return res.err
		default:
			return p.closedErr()
		}
	}
}

// checkRead applies the consistency rule before a read.
func (p *Partition) checkRead(ctx context.Context, c Consistency) error {
	if c == ConsistencyLocal {
		if st := p.Status(); st.Err != nil {
			return st.Err
		}
		return nil
	}
	return p.readIndex(ctx)
}

// Fetch returns committed batches starting with the one containing
// req.Offset (spec §4.4): never at or above the high watermark. If nothing
// (or less than MinBytes) is available it long-polls until the HW advances
// or MaxWait elapses. The caller must Release the returned region.
func (p *Partition) Fetch(ctx context.Context, req FetchRequest) (FetchResult, error) {
	if err := p.checkRead(ctx, req.Consistency); err != nil {
		return FetchResult{}, err
	}
	maxBytes := req.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	minBytes := int64(max(req.MinBytes, 1))
	var deadline <-chan time.Time
	if req.MaxWait > 0 {
		t := time.NewTimer(req.MaxWait)
		defer t.Stop()
		deadline = t.C
	}
	for {
		wait := p.hwN.Wait() // before reading the HW: no lost wakeup
		hw := p.hw.Load()
		start := p.lg.LogStartOffset()
		if req.Offset < start || req.Offset > hw {
			return FetchResult{HighWatermark: hw, LogStartOffset: start},
				fmt.Errorf("%w: %d not in [%d, %d]", ErrOffsetOutOfRange, req.Offset, start, hw)
		}
		upTo := hw
		if req.UpTo > 0 {
			upTo = min(upTo, req.UpTo)
		}
		r, err := p.lg.ReadUpTo(req.Offset, maxBytes, upTo)
		if err != nil {
			if errors.Is(err, log.ErrOffsetOutOfRange) {
				err = fmt.Errorf("%w: %w", ErrOffsetOutOfRange, err)
			} else if errors.Is(err, log.ErrClosed) {
				err = p.closedErr()
			}
			return FetchResult{HighWatermark: hw, LogStartOffset: start}, err
		}
		res := FetchResult{Region: r, HighWatermark: hw, LogStartOffset: start}
		if r.Len >= minBytes || deadline == nil || (req.UpTo > 0 && req.Offset >= upTo) {
			return res, nil
		}
		select {
		case <-wait:
			r.Release()
		case <-deadline:
			return res, nil
		case <-ctx.Done():
			r.Release()
			return FetchResult{}, ctx.Err()
		case <-p.donec:
			r.Release()
			return FetchResult{}, p.closedErr()
		}
	}
}

// TransferLeadership asks the leader to hand leadership to `to` (catch it up,
// then MsgTimeoutNow). Proposals fail with ErrProposalDropped meanwhile; raft
// aborts the transfer after an election timeout. Poll Status for the result.
func (p *Partition) TransferLeadership(ctx context.Context, to raft.NodeID) error {
	return p.ctrl(ctx, func() error { return p.node.TransferLeadership(to) })
}

// Campaign makes this replica start an election now (tests, admin).
func (p *Partition) Campaign(ctx context.Context) error {
	return p.ctrl(ctx, func() error { p.node.Campaign(); return nil })
}

// Compact discards the log prefix up to and including index — a batch last
// offset at or below the applied index (HW-1) — recording a Raft snapshot
// there whose data is the producer state as of index (spec §5.12). A
// follower that needs older entries then gets InstallSnapshot.
func (p *Partition) Compact(ctx context.Context, index uint64) error {
	return p.ctrl(ctx, func() error {
		if index > p.applied {
			return fmt.Errorf("%w: compact %d beyond applied %d", ErrOffsetOutOfRange, index, p.applied)
		}
		var data []byte
		if index == p.applied {
			data = p.committedPS.Encode()
		} else {
			snap, err := p.store.Snapshot()
			if err != nil {
				return err
			}
			ps, err := DecodeProducerState(snap.Data)
			if err != nil {
				return err
			}
			if err := replay(p.store, ps, p.store.FirstIndex(), index+1); err != nil {
				return err
			}
			data = ps.Encode()
		}
		return p.store.Compact(index, slices.Clone(p.cfg.Peers), data)
	})
}

// ProducerSnapshot returns the speculative producer state (tests/debug).
func (p *Partition) ProducerSnapshot(ctx context.Context) (*ProducerState, error) {
	var ps *ProducerState
	err := p.ctrl(ctx, func() error { ps = p.ps.Clone(); return nil })
	return ps, err
}

// Log exposes the underlying log for read-only use (e.g. sendfile of a
// FileRegion, diagnostics). Never write to it.
func (p *Partition) Log() *log.Log { return p.lg }

// HWChanged returns a channel that is closed the next time the high
// watermark advances (or the partition stops). Grab it before reading, then
// wait on it: no wakeup is lost.
func (p *Partition) HWChanged() <-chan struct{} { return p.hwN.Wait() }
