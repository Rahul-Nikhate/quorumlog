package partition

import (
	"errors"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/Rahul-Nikhate/quorumlog/internal/log"
)

// pbatch builds an idempotent batch of n records stamped at base.
func pbatch(t testing.TB, pid int64, epoch int16, seq int32, n int, base int64, term uint64) log.Batch {
	t.Helper()
	opts := log.DefaultBatchOptions()
	opts.ProducerID, opts.ProducerEpoch, opts.BaseSequence = pid, epoch, seq
	recs := make([]log.Record, n)
	for i := range recs {
		recs[i].Value = []byte{byte(i)}
	}
	b, err := log.NewBatch(opts, recs...)
	if err != nil {
		t.Fatal(err)
	}
	b.Stamp(base, term)
	return b
}

func TestProducerStateRules(t *testing.T) {
	s := NewProducerState()
	check := func(b log.Batch) (*BatchMeta, error) { return s.Check(b) }
	// Unknown producer must start at 0.
	if _, err := check(pbatch(t, 7, 0, 3, 1, 1, 1)); !errors.Is(err, ErrOutOfOrderSequence) {
		t.Fatalf("unknown pid seq 3: %v", err)
	}
	off := int64(1)
	seq := int32(0)
	var metas []BatchMeta
	for range 7 {
		b := pbatch(t, 7, 0, seq, 3, off, 1)
		if dup, err := check(b); dup != nil || err != nil {
			t.Fatalf("in-sequence batch seq %d: dup=%v err=%v", seq, dup, err)
		}
		s.Append(b)
		metas = append(metas, BatchMeta{FirstSeq: seq, LastSeq: seq + 2, BaseOffset: off, LastOffset: off + 2, Term: 1, MaxTimestamp: b.MaxTimestamp()})
		seq += 3
		off += 3
	}
	// The last 5 batches are duplicates and report their ORIGINAL offsets.
	for _, m := range metas[2:] {
		dup, err := check(pbatch(t, 7, 0, m.FirstSeq, 3, 999, 9))
		if err != nil || dup == nil || *dup != m {
			t.Fatalf("dup of %+v: got %+v, %v", m, dup, err)
		}
	}
	// Older than the window: out of order.
	if _, err := check(pbatch(t, 7, 0, metas[1].FirstSeq, 3, 999, 9)); !errors.Is(err, ErrOutOfOrderSequence) {
		t.Fatalf("dup older than window: %v", err)
	}
	// Gap and overlap: out of order.
	if _, err := check(pbatch(t, 7, 0, seq+1, 1, off, 1)); !errors.Is(err, ErrOutOfOrderSequence) {
		t.Fatalf("gap: %v", err)
	}
	if _, err := check(pbatch(t, 7, 0, seq-1, 2, off, 1)); !errors.Is(err, ErrOutOfOrderSequence) {
		t.Fatalf("overlap: %v", err)
	}
	// Epoch bump resets sequences (must start at 0); then the old epoch is fenced.
	if _, err := check(pbatch(t, 7, 1, 5, 1, off, 1)); !errors.Is(err, ErrOutOfOrderSequence) {
		t.Fatalf("new epoch not at 0: %v", err)
	}
	b := pbatch(t, 7, 1, 0, 1, off, 2)
	if dup, err := check(b); dup != nil || err != nil {
		t.Fatalf("new epoch at 0: %v %v", dup, err)
	}
	s.Append(b)
	if _, err := check(pbatch(t, 7, 0, seq, 1, off+1, 2)); !errors.Is(err, ErrInvalidProducerEpoch) {
		t.Fatalf("zombie epoch: %v", err)
	}
	if ep, last, ok := s.Lookup(7); !ok || ep != 1 || last.BaseOffset != off || last.Term != 2 {
		t.Fatalf("lookup: %v %+v %v", ep, last, ok)
	}
	// Non-idempotent and control batches are ignored.
	plain, _ := log.NewBatch(log.DefaultBatchOptions(), log.Record{Value: []byte("x")})
	if dup, err := check(plain); dup != nil || err != nil {
		t.Fatalf("non-idempotent: %v %v", dup, err)
	}
	s.Append(plain)
	if s.Len() != 1 {
		t.Fatalf("Len = %d", s.Len())
	}
}

func TestSequenceWrap(t *testing.T) {
	if got := IncrementSequence(math.MaxInt32, 1); got != 0 {
		t.Fatalf("MaxInt32+1 = %d", got)
	}
	if got := IncrementSequence(math.MaxInt32-1, 3); got != 1 {
		t.Fatalf("MaxInt32-1+3 = %d", got)
	}
	s := NewProducerState()
	// Reach the wrap through an epoch whose first batch starts at 0 and a
	// long batch: rebuild the state directly via Append (replay semantics).
	s.Append(pbatch(t, 1, 0, math.MaxInt32-1, 2, 10, 1)) // seqs MaxInt32-1, MaxInt32
	b := pbatch(t, 1, 0, 0, 1, 12, 1)                    // next is 0
	if dup, err := s.Check(b); dup != nil || err != nil {
		t.Fatalf("after wrap: %v %v", dup, err)
	}
}

func TestProducerStateCodec(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	s := NewProducerState()
	off := int64(1)
	seqs := map[int64]int32{}
	for range 500 {
		pid := int64(rng.IntN(40))
		n := 1 + rng.IntN(4)
		b := pbatch(t, pid, int16(pid%3), seqs[pid], n, off, uint64(1+off/50))
		s.Append(b)
		seqs[pid] += int32(n)
		off += int64(n)
	}
	enc := s.Encode()
	got, err := DecodeProducerState(enc)
	if err != nil || !got.Equal(s) {
		t.Fatalf("round trip: equal=%v err=%v", got != nil && got.Equal(s), err)
	}
	if string(got.Encode()) != string(enc) {
		t.Fatal("encoding is not canonical")
	}
	c := s.Clone()
	s.Append(pbatch(t, 3, int16(3%3), seqs[3], 1, off, 99))
	if c.Equal(s) {
		t.Fatal("Clone shares state")
	}
	if e, err := DecodeProducerState(nil); err != nil || e.Len() != 0 {
		t.Fatalf("empty: %v %v", e, err)
	}
	for n := 1; n < len(enc); n += 1 + n/8 {
		if _, err := DecodeProducerState(enc[:n]); !errors.Is(err, ErrCorruptProducerState) {
			t.Fatalf("truncated to %d: %v", n, err)
		}
	}
}
