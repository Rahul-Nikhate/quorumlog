package log

import (
	"encoding/binary"
	"errors"
	"math"
)

// Record is one decoded record inside an uncompressed batch. On decode, Key,
// Value and header values alias the batch buffer (zero copy); clone them if
// they must outlive it.
type Record struct {
	Attributes     int8
	TimestampDelta int64
	// OffsetDelta is filled in on decode. NewBatch ignores it and assigns
	// sequential deltas 0, 1, 2, …, as Kafka producers do.
	OffsetDelta int32
	Key         []byte // nil encodes as null (length -1)
	Value       []byte // nil encodes as null (length -1)
	Headers     []Header
}

// Header is a record header. Header keys are non-null strings.
type Header struct {
	Key   string
	Value []byte // nil encodes as null
}

// BatchOptions are the producer-controlled header fields for NewBatch.
type BatchOptions struct {
	BaseOffset           int64
	PartitionLeaderEpoch int32
	// Attributes may carry AttrTimestampLogAppend, AttrTransactional and
	// AttrControl. Compression bits must be zero: NewBatch does not compress.
	Attributes    int16
	BaseTimestamp int64
	ProducerID    int64
	ProducerEpoch int16
	BaseSequence  int32
}

// DefaultBatchOptions returns options for a non-idempotent, unstamped batch.
func DefaultBatchOptions() BatchOptions {
	return BatchOptions{
		PartitionLeaderEpoch: NoPartitionLeaderEpoch,
		ProducerID:           NoProducerID,
		ProducerEpoch:        NoProducerEpoch,
		BaseSequence:         NoSequence,
	}
}

// Errors returned by the record codec.
var (
	// ErrCompressed is returned when decoding records of a compressed batch.
	ErrCompressed = errors.New("log: batch is compressed; records are opaque to the broker")
	errVarint     = errors.New("log: malformed varint")
)

// minRecordSize is the smallest possible encoded record: length, attributes,
// timestampDelta, offsetDelta, keyLen, valueLen and headerCount, one byte each.
const minRecordSize = 7

// minHeaderSize is the smallest possible encoded header: keyLen and valueLen.
const minHeaderSize = 2

// NewBatch encodes an uncompressed RecordBatch v2 with the given records and a
// valid CRC. lastOffsetDelta is len(records)-1 (0 for an empty batch, which
// still occupies one offset in the log); maxTimestamp is the largest
// BaseTimestamp+TimestampDelta.
func NewBatch(opts BatchOptions, records ...Record) (Batch, error) {
	if opts.Attributes&AttrCompressionMask != 0 {
		return nil, errors.New("log: NewBatch cannot build compressed batches")
	}
	if len(records) > math.MaxInt32 {
		return nil, ErrTooLarge
	}
	size := BatchHeaderSize
	maxTS := opts.BaseTimestamp
	for i := range records {
		r := &records[i]
		body := recordBodySize(r, int32(i))
		size += varintLen(int64(body)) + body
		if ts := opts.BaseTimestamp + r.TimestampDelta; ts > maxTS {
			maxTS = ts
		}
	}
	if size-LogOverhead > math.MaxInt32 {
		return nil, ErrTooLarge
	}
	buf := make([]byte, BatchHeaderSize, size)
	lastDelta := int32(0)
	if len(records) > 0 {
		lastDelta = int32(len(records) - 1)
	}
	h := BatchHeader{
		BaseOffset:           opts.BaseOffset,
		BatchLength:          int32(size - LogOverhead),
		PartitionLeaderEpoch: opts.PartitionLeaderEpoch,
		Magic:                MagicV2,
		Attributes:           opts.Attributes,
		LastOffsetDelta:      lastDelta,
		BaseTimestamp:        opts.BaseTimestamp,
		MaxTimestamp:         maxTS,
		ProducerID:           opts.ProducerID,
		ProducerEpoch:        opts.ProducerEpoch,
		BaseSequence:         opts.BaseSequence,
		RecordsCount:         int32(len(records)),
	}
	h.Put(buf)
	for i := range records {
		buf = appendRecord(buf, &records[i], int32(i))
	}
	b := Batch(buf)
	b.UpdateCRC()
	return b, nil
}

// Control record types (the key's "type" field). Kafka uses 0 (abort) and
// 1 (commit) for transaction markers and 2+ for KRaft; QuorumLog uses
// ControlLeaderChange for the Raft no-op a new leader appends.
const (
	ControlAbort        int16 = 0
	ControlCommit       int16 = 1
	ControlLeaderChange int16 = 2
)

// NewControlBatch builds a control batch holding exactly one control record
// (key = version 0 + controlType, value = value). It occupies one offset.
func NewControlBatch(opts BatchOptions, controlType int16, value []byte) (Batch, error) {
	key := make([]byte, 4)
	binary.BigEndian.PutUint16(key[0:], 0) // control record key version
	binary.BigEndian.PutUint16(key[2:], uint16(controlType))
	opts.Attributes |= AttrControl
	return NewBatch(opts, Record{Key: key, Value: value})
}

// ControlType returns the control record type of a control batch built by
// NewControlBatch (or by Kafka).
func ControlType(b Batch) (int16, error) {
	if len(b) < BatchHeaderSize || !b.IsControl() {
		return 0, ErrCorrupt
	}
	recs, err := b.Records()
	if err != nil {
		return 0, err
	}
	if len(recs) != 1 || len(recs[0].Key) < 4 {
		return 0, ErrCorrupt
	}
	return int16(binary.BigEndian.Uint16(recs[0].Key[2:])), nil
}

func recordBodySize(r *Record, offsetDelta int32) int {
	n := 1 // attributes
	n += varintLen(r.TimestampDelta)
	n += varintLen(int64(offsetDelta))
	n += bytesLen(r.Key)
	n += bytesLen(r.Value)
	n += varintLen(int64(len(r.Headers)))
	for _, h := range r.Headers {
		n += varintLen(int64(len(h.Key))) + len(h.Key)
		n += bytesLen(h.Value)
	}
	return n
}

func bytesLen(b []byte) int {
	if b == nil {
		return varintLen(-1)
	}
	return varintLen(int64(len(b))) + len(b)
}

func varintLen(v int64) int {
	var tmp [binary.MaxVarintLen64]byte
	return binary.PutVarint(tmp[:], v)
}

func appendRecord(buf []byte, r *Record, offsetDelta int32) []byte {
	buf = binary.AppendVarint(buf, int64(recordBodySize(r, offsetDelta)))
	buf = append(buf, byte(r.Attributes))
	buf = binary.AppendVarint(buf, r.TimestampDelta)
	buf = binary.AppendVarint(buf, int64(offsetDelta))
	buf = appendBytes(buf, r.Key)
	buf = appendBytes(buf, r.Value)
	buf = binary.AppendVarint(buf, int64(len(r.Headers)))
	for _, h := range r.Headers {
		buf = binary.AppendVarint(buf, int64(len(h.Key)))
		buf = append(buf, h.Key...)
		buf = appendBytes(buf, h.Value)
	}
	return buf
}

func appendBytes(buf, b []byte) []byte {
	if b == nil {
		return binary.AppendVarint(buf, -1)
	}
	buf = binary.AppendVarint(buf, int64(len(b)))
	return append(buf, b...)
}

// Records decodes the records of an uncompressed batch. The batch must already
// be framed (len(b) == b.Size()); the CRC is not checked here (use Validate).
// The decoder is bounded: it never allocates more than O(len(b)) regardless of
// the counts claimed in the input, and never panics.
func (b Batch) Records() ([]Record, error) {
	if len(b) < BatchHeaderSize || b.Size() != len(b) {
		return nil, ErrCorrupt
	}
	if b.Compression() != 0 {
		return nil, ErrCompressed
	}
	count := int(b.RecordsCount())
	if count < 0 {
		return nil, ErrCorrupt
	}
	p := []byte(b[BatchHeaderSize:])
	if count > len(p)/minRecordSize {
		return nil, ErrCorrupt
	}
	out := make([]Record, 0, count)
	for i := 0; i < count; i++ {
		n, rest, err := readVarint32(p)
		if err != nil {
			return nil, err
		}
		if n < 0 || int(n) > len(rest) {
			return nil, ErrCorrupt
		}
		body := rest[:n]
		p = rest[n:]
		r, err := decodeRecordBody(body)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if len(p) != 0 {
		return nil, ErrCorrupt
	}
	return out, nil
}

func decodeRecordBody(p []byte) (Record, error) {
	var r Record
	if len(p) < 1 {
		return r, ErrCorrupt
	}
	r.Attributes = int8(p[0])
	p = p[1:]
	var err error
	if r.TimestampDelta, p, err = readVarint64(p); err != nil {
		return r, err
	}
	if r.OffsetDelta, p, err = readVarint32(p); err != nil {
		return r, err
	}
	if r.Key, p, err = readNullableBytes(p); err != nil {
		return r, err
	}
	if r.Value, p, err = readNullableBytes(p); err != nil {
		return r, err
	}
	var hc int32
	if hc, p, err = readVarint32(p); err != nil {
		return r, err
	}
	if hc < 0 || int(hc) > len(p)/minHeaderSize {
		return r, ErrCorrupt
	}
	if hc > 0 {
		r.Headers = make([]Header, 0, hc)
	}
	for i := int32(0); i < hc; i++ {
		var kl int32
		if kl, p, err = readVarint32(p); err != nil {
			return r, err
		}
		if kl < 0 || int(kl) > len(p) {
			return r, ErrCorrupt
		}
		key := string(p[:kl])
		p = p[kl:]
		var val []byte
		if val, p, err = readNullableBytes(p); err != nil {
			return r, err
		}
		r.Headers = append(r.Headers, Header{Key: key, Value: val})
	}
	if len(p) != 0 {
		return r, ErrCorrupt
	}
	return r, nil
}

func readVarint64(p []byte) (int64, []byte, error) {
	v, n := binary.Varint(p)
	if n <= 0 {
		return 0, p, errVarint
	}
	return v, p[n:], nil
}

func readVarint32(p []byte) (int32, []byte, error) {
	v, n := binary.Varint(p)
	if n <= 0 || n > 5 || v < math.MinInt32 || v > math.MaxInt32 {
		return 0, p, errVarint
	}
	return int32(v), p[n:], nil
}

func readNullableBytes(p []byte) ([]byte, []byte, error) {
	l, rest, err := readVarint32(p)
	if err != nil {
		return nil, p, err
	}
	if l == -1 {
		return nil, rest, nil
	}
	if l < -1 || int(l) > len(rest) {
		return nil, p, ErrCorrupt
	}
	return rest[:l:l], rest[l:], nil
}
