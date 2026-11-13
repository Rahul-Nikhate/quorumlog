package partition

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"time"

	"github.com/Rahul-Nikhate/quorumlog/internal/log"
)

// This file is the message-level API used by the cluster and the HTTP layer.
// It hides RecordBatches and converts offsets:
//
//	API offset (0-based, what clients see) = log offset (= Raft index) - 1
//
// The log starts at offset 1 because Raft reserves index 0 for "empty log".
// Every offset in this file's exported types is an API offset. Election
// no-ops are control batches that occupy one offset each, so API offsets can
// have gaps (as in Kafka); NextOffset always skips them.

// Message is one record as clients see it.
type Message struct {
	Offset    int64 // API offset (filled in by ReadMessages)
	Key       []byte
	Value     []byte
	Timestamp time.Time // set by the leader when the batch is produced
}

// ProduceRequest appends Messages as one batch (one Raft entry).
type ProduceRequest struct {
	Messages []Message // Key/Value only
	Acks     Acks
	// ProducerID enables idempotence when non-empty: the batch carries
	// sequences [Sequence, Sequence+len(Messages)-1]. A retry with the same
	// (ProducerID, Sequence, len) is answered with the original offsets
	// (Duplicate=true); a gap or a stale sequence is ErrOutOfOrderSequence.
	ProducerID string
	Sequence   int32
}

// ReadRequest asks for up to Max messages starting at Offset.
type ReadRequest struct {
	Offset int64 // API offset
	Max    int   // default 100
	// Wait long-polls up to this long when no committed message is available
	// at Offset.
	Wait        time.Duration
	Consistency Consistency
	// MaxBytes bounds the key+value bytes returned (default 1 MiB). At least
	// one message is returned when one is available, so one larger message
	// still makes progress; the result can exceed MaxBytes by one batch.
	MaxBytes int
}

// ReadResult holds committed messages only.
type ReadResult struct {
	Messages      []Message
	NextOffset    int64 // offset to ask for next
	HighWatermark int64 // next API offset after the last committed one
}

// LogOffset converts an API offset to a log offset (Raft index).
func LogOffset(apiOffset int64) int64 { return apiOffset + 1 }

// APIOffset converts a log offset (Raft index) to an API offset.
func APIOffset(logOffset int64) int64 { return logOffset - 1 }

// ProducerIDFor maps a client producer id string to the 63-bit numeric id
// stored in the batch header (FNV-1a). Every replica computes the same id, so
// deduplication state survives failover. A collision between two client ids
// (probability ~n²/2⁶⁴) would make them share a sequence space.
func ProducerIDFor(s string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return int64(h.Sum64() & math.MaxInt64)
}

// ProduceMessages builds a batch from req and calls Produce. The returned
// offsets are API offsets. Errors are those of Produce.
func (p *Partition) ProduceMessages(ctx context.Context, req ProduceRequest) (ProduceResult, error) {
	if len(req.Messages) == 0 {
		return ProduceResult{}, fmt.Errorf("%w: no messages", ErrInvalidBatch)
	}
	opts := log.DefaultBatchOptions()
	opts.BaseTimestamp = time.Now().UnixMilli()
	if req.ProducerID != "" {
		if req.Sequence < 0 {
			return ProduceResult{}, fmt.Errorf("%w: negative sequence %d", ErrOutOfOrderSequence, req.Sequence)
		}
		opts.ProducerID = ProducerIDFor(req.ProducerID)
		opts.ProducerEpoch = 0
		opts.BaseSequence = req.Sequence
	}
	recs := make([]log.Record, len(req.Messages))
	for i, m := range req.Messages {
		recs[i] = log.Record{Key: m.Key, Value: m.Value}
	}
	b, err := log.NewBatch(opts, recs...)
	if err != nil {
		return ProduceResult{}, fmt.Errorf("%w: %w", ErrInvalidBatch, err)
	}
	res, err := p.Produce(ctx, b, req.Acks)
	if err != nil {
		return ProduceResult{}, err
	}
	res.BaseOffset, res.LastOffset = APIOffset(res.BaseOffset), APIOffset(res.LastOffset)
	return res, nil
}

// ReadMessages returns committed messages from req.Offset (API offset). The
// consistency rule is applied once, before the first read. If nothing is
// committed at the offset yet it long-polls up to req.Wait (ctx cancels).
// An offset above the high watermark is ErrOffsetOutOfRange.
func (p *Partition) ReadMessages(ctx context.Context, req ReadRequest) (ReadResult, error) {
	if req.Offset < 0 {
		return ReadResult{}, fmt.Errorf("%w: negative offset %d", ErrOffsetOutOfRange, req.Offset)
	}
	limit := req.Max
	if limit <= 0 {
		limit = 100
	}
	maxBytes := req.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	deadline := time.Now().Add(req.Wait)
	off := LogOffset(req.Offset)
	cons := req.Consistency
	var out []Message
	nbytes := 0
	for {
		res, err := p.Fetch(ctx, FetchRequest{Offset: off, MaxBytes: max(min(maxBytes-nbytes, 1<<20), 1), MaxWait: max(time.Until(deadline), 0), Consistency: cons})
		cons = ConsistencyLocal // the read point was established by the first fetch
		if err != nil {
			return ReadResult{}, err
		}
		data, err := res.Region.Bytes()
		res.Region.Release()
		if err != nil {
			return ReadResult{}, err
		}
		before := len(out)
		next, err := decodeBatches(data, off, limit, &out)
		if err != nil {
			return ReadResult{}, err
		}
		for _, m := range out[before:] {
			nbytes += len(m.Key) + len(m.Value)
		}
		if next <= off && len(data) > 0 && len(out) < limit {
			return ReadResult{}, fmt.Errorf("partition: read made no progress at log offset %d", off) // cannot happen
		}
		off = max(off, next)
		hw := res.HighWatermark
		// Keep going while more committed data is immediately available (and
		// the byte budget is not spent), or while nothing was found and the
		// long-poll budget is not spent.
		if len(out) < limit && off < hw && nbytes < maxBytes {
			continue
		}
		if len(out) == 0 && off >= hw && time.Until(deadline) > 0 {
			continue
		}
		return ReadResult{Messages: out, NextOffset: APIOffset(off), HighWatermark: APIOffset(hw)}, nil
	}
}

// decodeBatches appends the data records at log offsets >= from (control
// batches skipped) to *out until it holds limit messages. It returns the log
// offset to continue from.
func decodeBatches(data []byte, from int64, limit int, out *[]Message) (int64, error) {
	batches, err := log.SplitBatches(data, len(data)+1)
	if err != nil {
		return 0, err
	}
	next := from
	for _, b := range batches {
		if b.IsControl() {
			next = max(next, b.LastOffset()+1)
			continue
		}
		recs, err := b.Records()
		if err != nil {
			return 0, err
		}
		for _, r := range recs {
			o := b.BaseOffset() + int64(r.OffsetDelta)
			if o < from {
				continue
			}
			if len(*out) >= limit {
				return next, nil
			}
			*out = append(*out, Message{
				Offset:    APIOffset(o),
				Key:       clone(r.Key),
				Value:     clone(r.Value),
				Timestamp: time.UnixMilli(b.BaseTimestamp() + r.TimestampDelta).UTC(),
			})
			next = o + 1
		}
		next = max(next, b.LastOffset()+1)
	}
	return next, nil
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte{}, b...)
}

// ReadBarrier confirms leadership (ReadIndex) and returns the high watermark
// (API offset) that every committed write before the call is below. Used to
// make reads of state-machine groups (__meta, __offsets) linearizable.
func (p *Partition) ReadBarrier(ctx context.Context) (int64, error) {
	if err := p.readIndex(ctx); err != nil {
		return 0, err
	}
	return APIOffset(p.hw.Load()), nil
}

// IsRetriable reports whether a produce/read error is transient (leader
// change, election in progress, timeout): clients should retry, with the same
// producer id and sequence for produce.
func IsRetriable(err error) bool {
	return errors.Is(err, ErrNotLeader) || errors.Is(err, ErrOutcomeUnknown) ||
		errors.Is(err, ErrProposalDropped) || errors.Is(err, ErrTimeout) || errors.Is(err, ErrClosed)
}
