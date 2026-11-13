// Package log implements QuorumLog's segmented, append-only partition log.
//
// The unit of storage is a Kafka RecordBatch v2 ("magic 2"), stored on disk
// byte-for-byte as the producer sent it, except for the two fields the leader
// stamps outside the CRC (baseOffset and partitionLeaderEpoch). A partition log
// is a directory of segments; each segment is a .log file of concatenated
// batches plus a sparse offset index (.index) and time index (.timeindex).
//
// The same log doubles as the Raft log (spec §5.4.2): one Raft entry is one
// batch, the entry's Raft index is the batch's last offset, and its term is the
// batch's partitionLeaderEpoch. See docs/IMPLEMENTATION_NOTES.md for the API the
// Raft LogStore adapter builds on.
package log

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// RecordBatch v2 layout constants (all integers big-endian).
const (
	// BatchHeaderSize is the size of the fixed RecordBatch v2 header.
	BatchHeaderSize = 61
	// LogOverhead is the size of baseOffset + batchLength, the part of the
	// header that precedes the bytes counted by batchLength.
	LogOverhead = 12
	// MagicV2 is the only supported batch format version.
	MagicV2 = 2

	offBaseOffset      = 0
	offBatchLength     = 8
	offLeaderEpoch     = 12
	offMagic           = 16
	offCRC             = 17
	offAttributes      = 21
	offLastOffsetDelta = 23
	offBaseTimestamp   = 27
	offMaxTimestamp    = 35
	offProducerID      = 43
	offProducerEpoch   = 51
	offBaseSequence    = 53
	offRecordsCount    = 57
)

// Attribute bits.
const (
	// AttrCompressionMask selects the compression codec bits (0 none, 1 gzip,
	// 2 snappy, 3 lz4, 4 zstd). The broker never decompresses.
	AttrCompressionMask = 0x07
	// AttrTimestampLogAppend marks LogAppendTime timestamps.
	AttrTimestampLogAppend = 1 << 3
	// AttrTransactional marks a transactional batch.
	AttrTransactional = 1 << 4
	// AttrControl marks a control batch (transaction markers, Raft no-ops).
	AttrControl = 1 << 5
	// AttrDeleteHorizon marks a batch with a delete horizon set (compaction).
	AttrDeleteHorizon = 1 << 6

	maxCompressionCodec = 4
)

// Sentinel values used by non-idempotent producers and unstamped batches.
const (
	NoProducerID           int64 = -1
	NoProducerEpoch        int16 = -1
	NoSequence             int32 = -1
	NoPartitionLeaderEpoch int32 = -1
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Batch validation errors.
var (
	// ErrCorrupt reports a structurally invalid batch or a CRC mismatch.
	ErrCorrupt = errors.New("log: corrupt batch")
	// ErrTooLarge reports a batch larger than the configured limit.
	ErrTooLarge = errors.New("log: batch too large")
	// ErrBadMagic reports a batch with an unsupported magic (format version).
	ErrBadMagic = errors.New("log: unsupported batch magic")
)

// Batch is a zero-copy view over one encoded RecordBatch v2. Accessors read
// only the 61-byte header, so they may be used on a header-only prefix; they
// panic if len(b) < BatchHeaderSize, so call Validate (or check the length)
// before using them on untrusted input.
type Batch []byte

// BaseOffset is the offset of the first record (stamped by the leader).
func (b Batch) BaseOffset() int64 { return int64(binary.BigEndian.Uint64(b[offBaseOffset:])) }

// BatchLength is the number of bytes after the batchLength field.
func (b Batch) BatchLength() int32 { return int32(binary.BigEndian.Uint32(b[offBatchLength:])) }

// PartitionLeaderEpoch is the leader epoch (the Raft term) stamped by the leader.
func (b Batch) PartitionLeaderEpoch() int32 {
	return int32(binary.BigEndian.Uint32(b[offLeaderEpoch:]))
}

// Term returns PartitionLeaderEpoch as a Raft term. Unstamped batches (epoch
// -1) report term 0.
func (b Batch) Term() uint64 {
	e := b.PartitionLeaderEpoch()
	if e < 0 {
		return 0
	}
	return uint64(e)
}

// Magic is the format version byte.
func (b Batch) Magic() int8 { return int8(b[offMagic]) }

// CRC is the stored CRC-32C of bytes [21, end).
func (b Batch) CRC() uint32 { return binary.BigEndian.Uint32(b[offCRC:]) }

// Attributes returns the attribute bits.
func (b Batch) Attributes() int16 { return int16(binary.BigEndian.Uint16(b[offAttributes:])) }

// Compression returns the compression codec id (attribute bits 0-2).
func (b Batch) Compression() int { return int(b.Attributes() & AttrCompressionMask) }

// IsControl reports whether this is a control batch.
func (b Batch) IsControl() bool { return b.Attributes()&AttrControl != 0 }

// IsTransactional reports whether the transactional bit is set.
func (b Batch) IsTransactional() bool { return b.Attributes()&AttrTransactional != 0 }

// LastOffsetDelta is lastOffset - baseOffset.
func (b Batch) LastOffsetDelta() int32 {
	return int32(binary.BigEndian.Uint32(b[offLastOffsetDelta:]))
}

// LastOffset is the offset of the last record (the Raft index of the entry).
func (b Batch) LastOffset() int64 { return b.BaseOffset() + int64(b.LastOffsetDelta()) }

// BaseTimestamp is the first record's timestamp.
func (b Batch) BaseTimestamp() int64 { return int64(binary.BigEndian.Uint64(b[offBaseTimestamp:])) }

// MaxTimestamp is the largest record timestamp in the batch (used by the time index).
func (b Batch) MaxTimestamp() int64 { return int64(binary.BigEndian.Uint64(b[offMaxTimestamp:])) }

// ProducerID is the idempotent producer id, or -1.
func (b Batch) ProducerID() int64 { return int64(binary.BigEndian.Uint64(b[offProducerID:])) }

// ProducerEpoch is the producer epoch.
func (b Batch) ProducerEpoch() int16 { return int16(binary.BigEndian.Uint16(b[offProducerEpoch:])) }

// BaseSequence is the sequence number of the first record, or -1.
func (b Batch) BaseSequence() int32 { return int32(binary.BigEndian.Uint32(b[offBaseSequence:])) }

// RecordsCount is the number of records in the batch.
func (b Batch) RecordsCount() int32 { return int32(binary.BigEndian.Uint32(b[offRecordsCount:])) }

// Size is the total encoded size implied by the header: LogOverhead + batchLength.
// It may be negative or huge for corrupt input; Validate checks it.
func (b Batch) Size() int { return LogOverhead + int(b.BatchLength()) }

// ComputeCRC computes the CRC-32C over bytes [21, len(b)).
func (b Batch) ComputeCRC() uint32 { return crc32.Checksum(b[offAttributes:], castagnoli) }

// UpdateCRC recomputes and stores the CRC. Only builders need this: stamping the
// offset and epoch does not change the CRC.
func (b Batch) UpdateCRC() { binary.BigEndian.PutUint32(b[offCRC:], b.ComputeCRC()) }

// Validate checks framing, format and CRC. maxBytes bounds the batch size
// (guarding against hostile lengths). It never panics and never allocates.
func (b Batch) Validate(maxBytes int) error {
	if err := validateHeader(b, maxBytes); err != nil {
		return err
	}
	if b.Size() != len(b) {
		return ErrCorrupt
	}
	if b.ComputeCRC() != b.CRC() {
		return ErrCorrupt
	}
	return nil
}

// validateHeader checks everything that can be checked from the first 61 bytes
// alone: length bounds, magic, attributes and deltas. It does not check the CRC
// or that len(hdr) covers the whole batch.
func validateHeader(hdr []byte, maxBytes int) error {
	if len(hdr) < BatchHeaderSize {
		return ErrCorrupt
	}
	b := Batch(hdr)
	bl := b.BatchLength()
	if bl < BatchHeaderSize-LogOverhead {
		return ErrCorrupt
	}
	if int64(bl)+LogOverhead > int64(maxBytes) {
		return ErrTooLarge
	}
	if b.Magic() != MagicV2 {
		return ErrBadMagic
	}
	if b.Compression() > maxCompressionCodec {
		return ErrCorrupt
	}
	if b.LastOffsetDelta() < 0 || b.RecordsCount() < 0 {
		return ErrCorrupt
	}
	return nil
}

// maxOffset bounds offsets so that offset arithmetic never overflows. The
// baseOffset of a client batch is ignored (the leader overwrites it), so it is
// checked against this bound only where the log trusts stored offsets.
const maxOffset = int64(1) << 62

// Stamp assigns the base offset and leader epoch in place. Both fields are
// outside the CRC'd region, so the CRC stays valid and is not recomputed.
func (b Batch) Stamp(baseOffset int64, term uint64) {
	binary.BigEndian.PutUint64(b[offBaseOffset:], uint64(baseOffset))
	binary.BigEndian.PutUint32(b[offLeaderEpoch:], uint32(term))
}

// BatchHeader is a decoded copy of the 61-byte header.
type BatchHeader struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                int8
	CRC                  uint32
	Attributes           int16
	LastOffsetDelta      int32
	BaseTimestamp        int64
	MaxTimestamp         int64
	ProducerID           int64
	ProducerEpoch        int16
	BaseSequence         int32
	RecordsCount         int32
}

// ParseHeader decodes the fixed header from p. It only checks that p is long
// enough; use Batch.Validate for semantic checks.
func ParseHeader(p []byte) (BatchHeader, error) {
	if len(p) < BatchHeaderSize {
		return BatchHeader{}, ErrCorrupt
	}
	b := Batch(p)
	return BatchHeader{
		BaseOffset:           b.BaseOffset(),
		BatchLength:          b.BatchLength(),
		PartitionLeaderEpoch: b.PartitionLeaderEpoch(),
		Magic:                b.Magic(),
		CRC:                  b.CRC(),
		Attributes:           b.Attributes(),
		LastOffsetDelta:      b.LastOffsetDelta(),
		BaseTimestamp:        b.BaseTimestamp(),
		MaxTimestamp:         b.MaxTimestamp(),
		ProducerID:           b.ProducerID(),
		ProducerEpoch:        b.ProducerEpoch(),
		BaseSequence:         b.BaseSequence(),
		RecordsCount:         b.RecordsCount(),
	}, nil
}

// Put encodes h into p[:BatchHeaderSize]. It panics if p is shorter.
func (h *BatchHeader) Put(p []byte) {
	_ = p[BatchHeaderSize-1]
	binary.BigEndian.PutUint64(p[offBaseOffset:], uint64(h.BaseOffset))
	binary.BigEndian.PutUint32(p[offBatchLength:], uint32(h.BatchLength))
	binary.BigEndian.PutUint32(p[offLeaderEpoch:], uint32(h.PartitionLeaderEpoch))
	p[offMagic] = byte(h.Magic)
	binary.BigEndian.PutUint32(p[offCRC:], h.CRC)
	binary.BigEndian.PutUint16(p[offAttributes:], uint16(h.Attributes))
	binary.BigEndian.PutUint32(p[offLastOffsetDelta:], uint32(h.LastOffsetDelta))
	binary.BigEndian.PutUint64(p[offBaseTimestamp:], uint64(h.BaseTimestamp))
	binary.BigEndian.PutUint64(p[offMaxTimestamp:], uint64(h.MaxTimestamp))
	binary.BigEndian.PutUint64(p[offProducerID:], uint64(h.ProducerID))
	binary.BigEndian.PutUint16(p[offProducerEpoch:], uint16(h.ProducerEpoch))
	binary.BigEndian.PutUint32(p[offBaseSequence:], uint32(h.BaseSequence))
	binary.BigEndian.PutUint32(p[offRecordsCount:], uint32(h.RecordsCount))
}

// SplitBatches splits a buffer of concatenated, complete batches (such as the
// contents of a FileRegion) into views, validating each one. It returns an
// error if the buffer does not consist exactly of valid batches.
func SplitBatches(p []byte, maxBatchBytes int) ([]Batch, error) {
	var out []Batch
	for len(p) > 0 {
		if err := validateHeader(p, maxBatchBytes); err != nil {
			return out, err
		}
		n := Batch(p).Size()
		if n > len(p) {
			return out, ErrCorrupt
		}
		b := Batch(p[:n:n])
		if err := b.Validate(maxBatchBytes); err != nil {
			return out, err
		}
		out = append(out, b)
		p = p[n:]
	}
	return out, nil
}
