package partition

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/Rahul-Nikhate/quorumlog/internal/log"
)

// Idempotent-producer errors (spec §5.9; Kafka error codes in comments).
var (
	// ErrOutOfOrderSequence: the batch's base sequence is not exactly one
	// past the producer's last appended sequence (and it is not a duplicate of
	// one of the last 5 batches). Kafka OUT_OF_ORDER_SEQUENCE_NUMBER (45).
	ErrOutOfOrderSequence = errors.New("partition: out of order sequence number")
	// ErrInvalidProducerEpoch: the batch's producer epoch is older than the
	// latest one seen for its producer id (a fenced zombie). Kafka
	// INVALID_PRODUCER_EPOCH (47).
	ErrInvalidProducerEpoch = errors.New("partition: invalid (fenced) producer epoch")
)

// ProducerWindow is the number of recent batches remembered per producer:
// Kafka's max.in.flight.requests.per.connection limit for idempotence.
const ProducerWindow = 5

// BatchMeta describes one appended idempotent batch.
type BatchMeta struct {
	FirstSeq, LastSeq      int32
	BaseOffset, LastOffset int64
	Term                   uint64 // partitionLeaderEpoch of the batch
	MaxTimestamp           int64
}

type producerEntry struct {
	epoch   int16
	batches [ProducerWindow]BatchMeta // ring buffer, oldest at head
	n, head int
}

func (e *producerEntry) last() *BatchMeta { return &e.batches[(e.head+e.n-1)%ProducerWindow] }

func (e *producerEntry) push(m BatchMeta) {
	if e.n < ProducerWindow {
		e.batches[(e.head+e.n)%ProducerWindow] = m
		e.n++
		return
	}
	e.batches[e.head] = m
	e.head = (e.head + 1) % ProducerWindow
}

// ProducerState is the per-partition idempotence state: producer id →
// (epoch, last ProducerWindow batches). It is owned by one goroutine (the
// partition driver), which makes check-then-append atomic (§5.9 subtlety 4).
//
// The driver keeps two of them: the speculative state of the whole log
// (stable + just-proposed batches; checked on the leader, subtlety 1) and the
// state as of the applied index (the base for rebuilding after a suffix
// truncation, subtlety 2, and for snapshots).
type ProducerState struct {
	m map[int64]*producerEntry
}

// NewProducerState returns an empty state.
func NewProducerState() *ProducerState { return &ProducerState{m: map[int64]*producerEntry{}} }

// Len returns the number of tracked producers.
func (s *ProducerState) Len() int { return len(s.m) }

// IncrementSequence adds inc to a sequence number, wrapping from MaxInt32 to
// 0 exactly as Kafka does (DefaultRecordBatch.incrementSequence).
func IncrementSequence(seq, inc int32) int32 {
	if seq > math.MaxInt32-inc {
		return inc - (math.MaxInt32 - seq) - 1
	}
	return seq + inc
}

// idempotent reports whether b carries producer metadata QuorumLog tracks.
func idempotent(b log.Batch) bool { return b.ProducerID() >= 0 && !b.IsControl() }

// Check validates an idempotent batch against the state before it is
// appended. It returns (dup, nil) when b repeats one of the producer's last
// ProducerWindow batches: the caller must reply with dup's ORIGINAL offsets
// and not append. Non-idempotent batches (producer id < 0) always pass.
func (s *ProducerState) Check(b log.Batch) (*BatchMeta, error) {
	if !idempotent(b) {
		return nil, nil
	}
	return s.check(b.ProducerID(), b.ProducerEpoch(), b.BaseSequence(), IncrementSequence(b.BaseSequence(), b.LastOffsetDelta()))
}

func (s *ProducerState) check(pid int64, epoch int16, first, last int32) (*BatchMeta, error) {
	if first < 0 {
		return nil, fmt.Errorf("%w: base sequence %d", ErrOutOfOrderSequence, first)
	}
	e, ok := s.m[pid]
	switch {
	case !ok:
		if first != 0 {
			return nil, fmt.Errorf("%w: unknown producer %d must start at sequence 0, got %d", ErrOutOfOrderSequence, pid, first)
		}
		return nil, nil
	case epoch < e.epoch:
		return nil, fmt.Errorf("%w: producer %d epoch %d < current %d", ErrInvalidProducerEpoch, pid, epoch, e.epoch)
	case epoch > e.epoch:
		if first != 0 {
			return nil, fmt.Errorf("%w: producer %d new epoch %d must start at sequence 0, got %d", ErrOutOfOrderSequence, pid, epoch, first)
		}
		return nil, nil
	}
	for i := range e.n {
		bm := &e.batches[(e.head+i)%ProducerWindow]
		if bm.FirstSeq == first && bm.LastSeq == last {
			dup := *bm
			return &dup, nil
		}
	}
	if want := IncrementSequence(e.last().LastSeq, 1); first != want {
		return nil, fmt.Errorf("%w: producer %d epoch %d expected sequence %d, got %d", ErrOutOfOrderSequence, pid, epoch, want, first)
	}
	return nil, nil
}

// Append records an appended (stamped) batch. It never fails: followers and
// replays record whatever the leader appended.
func (s *ProducerState) Append(b log.Batch) {
	if !idempotent(b) {
		return
	}
	pid, epoch := b.ProducerID(), b.ProducerEpoch()
	m := BatchMeta{
		FirstSeq:     b.BaseSequence(),
		LastSeq:      IncrementSequence(b.BaseSequence(), b.LastOffsetDelta()),
		BaseOffset:   b.BaseOffset(),
		LastOffset:   b.LastOffset(),
		Term:         b.Term(),
		MaxTimestamp: b.MaxTimestamp(),
	}
	e, ok := s.m[pid]
	if !ok || epoch != e.epoch {
		// A new producer, or a new epoch (sequences restart). An older epoch
		// can only appear when replaying a log written before fencing; the
		// latest batch wins, as in Kafka's log replay.
		e = &producerEntry{epoch: epoch}
		s.m[pid] = e
	}
	e.push(m)
}

// Lookup returns the producer's epoch and its most recent batch.
func (s *ProducerState) Lookup(pid int64) (epoch int16, last BatchMeta, ok bool) {
	e, ok := s.m[pid]
	if !ok {
		return 0, BatchMeta{}, false
	}
	return e.epoch, *e.last(), true
}

// Clone returns a deep copy.
func (s *ProducerState) Clone() *ProducerState {
	c := &ProducerState{m: make(map[int64]*producerEntry, len(s.m))}
	for pid, e := range s.m {
		cp := *e
		c.m[pid] = &cp
	}
	return c
}

// Equal reports whether two states hold the same producers and windows.
func (s *ProducerState) Equal(o *ProducerState) bool {
	if len(s.m) != len(o.m) {
		return false
	}
	for pid, e := range s.m {
		f, ok := o.m[pid]
		if !ok || e.epoch != f.epoch || e.n != f.n {
			return false
		}
		for i := range e.n {
			if e.batches[(e.head+i)%ProducerWindow] != f.batches[(f.head+i)%ProducerWindow] {
				return false
			}
		}
	}
	return true
}

// Encoding (Raft snapshot data, spec §5.12):
//
//	[u32 magic "QLPS"][u8 version=1][u32 nProducers]
//	{ [i64 pid][i16 epoch][u8 n] { [i32 firstSeq][i32 lastSeq][i64 base][i64 last][u64 term][i64 maxTS] } × n } × nProducers
//
// Producers are sorted by id and batches oldest first, so equal states
// encode identically. The snapshot.meta file carries the CRC.
const (
	psMagic    = 0x514C5053 // "QLPS"
	psVersion  = 1
	psBatchLen = 4 + 4 + 8 + 8 + 8 + 8
)

// ErrCorruptProducerState is returned by DecodeProducerState.
var ErrCorruptProducerState = errors.New("partition: corrupt producer state")

// Encode serializes the state.
func (s *ProducerState) Encode() []byte {
	pids := make([]int64, 0, len(s.m))
	for pid := range s.m {
		pids = append(pids, pid)
	}
	slices.Sort(pids)
	b := make([]byte, 0, 9+len(pids)*(11+ProducerWindow*psBatchLen))
	b = binary.BigEndian.AppendUint32(b, psMagic)
	b = append(b, psVersion)
	b = binary.BigEndian.AppendUint32(b, uint32(len(pids)))
	for _, pid := range pids {
		e := s.m[pid]
		b = binary.BigEndian.AppendUint64(b, uint64(pid))
		b = binary.BigEndian.AppendUint16(b, uint16(e.epoch))
		b = append(b, byte(e.n))
		for i := range e.n {
			m := e.batches[(e.head+i)%ProducerWindow]
			b = binary.BigEndian.AppendUint32(b, uint32(m.FirstSeq))
			b = binary.BigEndian.AppendUint32(b, uint32(m.LastSeq))
			b = binary.BigEndian.AppendUint64(b, uint64(m.BaseOffset))
			b = binary.BigEndian.AppendUint64(b, uint64(m.LastOffset))
			b = binary.BigEndian.AppendUint64(b, m.Term)
			b = binary.BigEndian.AppendUint64(b, uint64(m.MaxTimestamp))
		}
	}
	return b
}

// DecodeProducerState parses Encode's output. Empty input is an empty state
// (a snapshot without producer data).
func DecodeProducerState(b []byte) (*ProducerState, error) {
	s := NewProducerState()
	if len(b) == 0 {
		return s, nil
	}
	bad := func(why string) (*ProducerState, error) {
		return nil, fmt.Errorf("%w: %s", ErrCorruptProducerState, why)
	}
	if len(b) < 9 || binary.BigEndian.Uint32(b) != psMagic || b[4] != psVersion {
		return bad("bad header")
	}
	n := binary.BigEndian.Uint32(b[5:])
	p := b[9:]
	if uint64(n)*11 > uint64(len(p)) {
		return bad("producer count")
	}
	for range n {
		if len(p) < 11 {
			return bad("short producer")
		}
		pid := int64(binary.BigEndian.Uint64(p))
		e := &producerEntry{epoch: int16(binary.BigEndian.Uint16(p[8:]))}
		k := int(p[10])
		p = p[11:]
		if k < 1 || k > ProducerWindow || len(p) < k*psBatchLen {
			return bad("batch count")
		}
		if _, dup := s.m[pid]; dup || pid < 0 {
			return bad("producer id")
		}
		for range k {
			e.push(BatchMeta{
				FirstSeq:     int32(binary.BigEndian.Uint32(p)),
				LastSeq:      int32(binary.BigEndian.Uint32(p[4:])),
				BaseOffset:   int64(binary.BigEndian.Uint64(p[8:])),
				LastOffset:   int64(binary.BigEndian.Uint64(p[16:])),
				Term:         binary.BigEndian.Uint64(p[24:]),
				MaxTimestamp: int64(binary.BigEndian.Uint64(p[32:])),
			})
			p = p[psBatchLen:]
		}
		s.m[pid] = e
	}
	if len(p) != 0 {
		return bad("trailing bytes")
	}
	return s, nil
}
