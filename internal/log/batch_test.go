package log

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"reflect"
	"testing"
)

// The CRC-32C (Castagnoli) check value from RFC 3720 / the standard test
// vector "123456789" → 0xE3069283.
func TestCastagnoliCheckValue(t *testing.T) {
	if got := crc32.Checksum([]byte("123456789"), castagnoli); got != 0xE3069283 {
		t.Fatalf("crc32c = %#x", got)
	}
}

// TestGoldenBatch hand-encodes a one-record batch byte by byte (independently
// of NewBatch) and checks that the codec agrees on every field and the CRC.
func TestGoldenBatch(t *testing.T) {
	// Record: attributes 0, timestampDelta 0, offsetDelta 0, key "k",
	// value "v", no headers: an 8-byte body. Zigzag varints: 0→0x00,
	// 1→0x02, 8→0x10.
	rec := []byte{0x10, 0x00, 0x00, 0x00, 0x02, 'k', 0x02, 'v', 0x00}
	g := make([]byte, BatchHeaderSize, BatchHeaderSize+len(rec))
	binary.BigEndian.PutUint64(g[0:], 0)                                   // baseOffset
	binary.BigEndian.PutUint32(g[8:], uint32(BatchHeaderSize-12+len(rec))) // batchLength
	binary.BigEndian.PutUint32(g[12:], 0xffffffff)                         // partitionLeaderEpoch -1
	g[16] = 2                                                              // magic
	binary.BigEndian.PutUint16(g[21:], 0)                                  // attributes
	binary.BigEndian.PutUint32(g[23:], 0)                                  // lastOffsetDelta
	binary.BigEndian.PutUint64(g[27:], 1700000000000)                      // baseTimestamp
	binary.BigEndian.PutUint64(g[35:], 1700000000000)                      // maxTimestamp
	binary.BigEndian.PutUint64(g[43:], 0xffffffffffffffff)                 // producerId -1
	binary.BigEndian.PutUint16(g[51:], 0xffff)                             // producerEpoch -1
	binary.BigEndian.PutUint32(g[53:], 0xffffffff)                         // baseSequence -1
	binary.BigEndian.PutUint32(g[57:], 1)                                  // recordsCount
	g = append(g, rec...)
	binary.BigEndian.PutUint32(g[17:], crc32.Checksum(g[21:], crc32.MakeTable(crc32.Castagnoli)))

	opts := DefaultBatchOptions()
	opts.BaseTimestamp = 1700000000000
	b, err := NewBatch(opts, Record{Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, g) {
		t.Fatalf("NewBatch disagrees with hand encoding:\n got %x\nwant %x", []byte(b), g)
	}
	if err := Batch(g).Validate(1 << 20); err != nil {
		t.Fatal(err)
	}
}

func TestBatchAccessors(t *testing.T) {
	opts := BatchOptions{
		BaseOffset:           100,
		PartitionLeaderEpoch: 7,
		Attributes:           AttrTransactional,
		BaseTimestamp:        1000,
		ProducerID:           42,
		ProducerEpoch:        3,
		BaseSequence:         9,
	}
	recs := []Record{{TimestampDelta: 0, Value: []byte("a")}, {TimestampDelta: 50, Value: []byte("b")}, {TimestampDelta: 20}}
	b, err := NewBatch(opts, recs...)
	if err != nil {
		t.Fatal(err)
	}
	h, err := ParseHeader(b)
	if err != nil {
		t.Fatal(err)
	}
	want := BatchHeader{
		BaseOffset: 100, BatchLength: int32(len(b) - LogOverhead), PartitionLeaderEpoch: 7, Magic: 2,
		CRC: b.CRC(), Attributes: AttrTransactional, LastOffsetDelta: 2, BaseTimestamp: 1000,
		MaxTimestamp: 1050, ProducerID: 42, ProducerEpoch: 3, BaseSequence: 9, RecordsCount: 3,
	}
	if h != want {
		t.Fatalf("header\n got %+v\nwant %+v", h, want)
	}
	checks := []struct {
		name      string
		got, want any
	}{
		{"BaseOffset", b.BaseOffset(), int64(100)},
		{"LastOffset", b.LastOffset(), int64(102)},
		{"Term", b.Term(), uint64(7)},
		{"MaxTimestamp", b.MaxTimestamp(), int64(1050)},
		{"BaseTimestamp", b.BaseTimestamp(), int64(1000)},
		{"ProducerID", b.ProducerID(), int64(42)},
		{"ProducerEpoch", b.ProducerEpoch(), int16(3)},
		{"BaseSequence", b.BaseSequence(), int32(9)},
		{"RecordsCount", b.RecordsCount(), int32(3)},
		{"IsTransactional", b.IsTransactional(), true},
		{"IsControl", b.IsControl(), false},
		{"Compression", b.Compression(), 0},
		{"Magic", b.Magic(), int8(2)},
		{"Size", b.Size(), len(b)},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	// Re-encoding the parsed header reproduces the bytes.
	p := make([]byte, BatchHeaderSize)
	h.Put(p)
	if !bytes.Equal(p, b[:BatchHeaderSize]) {
		t.Fatal("Put(ParseHeader(b)) != header bytes")
	}
	var unstamped Batch = mkBatch(t, 1, 1, 0, 'x')
	if unstamped.Term() != 0 {
		t.Fatalf("unstamped term = %d", unstamped.Term())
	}
}

func TestStampKeepsCRC(t *testing.T) {
	b := mkBatch(t, 5, 100, 1, 'a')
	crc := b.CRC()
	b.Stamp(123456789, 42)
	if b.CRC() != crc {
		t.Fatal("Stamp changed the stored CRC")
	}
	if err := b.Validate(1 << 20); err != nil {
		t.Fatalf("stamped batch invalid: %v", err)
	}
	if b.BaseOffset() != 123456789 || b.Term() != 42 || b.LastOffset() != 123456793 {
		t.Fatalf("stamp: base %d term %d last %d", b.BaseOffset(), b.Term(), b.LastOffset())
	}
}

func TestValidateTable(t *testing.T) {
	good := mkBatch(t, 3, 10, 5, 'g')
	mut := func(f func(b []byte) []byte) Batch {
		c := bytes.Clone(good)
		return Batch(f(c))
	}
	setCRC := func(b []byte) []byte { Batch(b).UpdateCRC(); return b }
	tests := []struct {
		name string
		b    Batch
		max  int
		want error
	}{
		{"valid", good, 1 << 20, nil},
		{"empty", nil, 1 << 20, ErrCorrupt},
		{"short header", good[:60], 1 << 20, ErrCorrupt},
		{"truncated body", good[:len(good)-1], 1 << 20, ErrCorrupt},
		{"trailing byte", append(bytes.Clone(good), 0), 1 << 20, ErrCorrupt},
		{"too large", good, len(good) - 1, ErrTooLarge},
		{"exactly max", good, len(good), nil},
		{"batchLength too small", mut(func(b []byte) []byte { binary.BigEndian.PutUint32(b[8:], 48); return b }), 1 << 20, ErrCorrupt},
		{"batchLength negative", mut(func(b []byte) []byte { binary.BigEndian.PutUint32(b[8:], 0x80000000); return b }), 1 << 20, ErrCorrupt},
		{"batchLength huge", mut(func(b []byte) []byte { binary.BigEndian.PutUint32(b[8:], 0x7fffffff); return b }), 1 << 20, ErrTooLarge},
		{"magic 1", mut(func(b []byte) []byte { b[16] = 1; return b }), 1 << 20, ErrBadMagic},
		{"magic 3", mut(func(b []byte) []byte { b[16] = 3; return b }), 1 << 20, ErrBadMagic},
		{"crc mismatch", mut(func(b []byte) []byte { b[len(b)-1] ^= 1; return b }), 1 << 20, ErrCorrupt},
		{"crc field flipped", mut(func(b []byte) []byte { b[17] ^= 0x80; return b }), 1 << 20, ErrCorrupt},
		{"attributes flipped (in CRC)", mut(func(b []byte) []byte { b[22] ^= 0x10; return b }), 1 << 20, ErrCorrupt},
		{"negative lastOffsetDelta", mut(func(b []byte) []byte {
			binary.BigEndian.PutUint32(b[23:], 0xffffffff)
			return setCRC(b)
		}), 1 << 20, ErrCorrupt},
		{"negative recordsCount", mut(func(b []byte) []byte {
			binary.BigEndian.PutUint32(b[57:], 0xffffffff)
			return setCRC(b)
		}), 1 << 20, ErrCorrupt},
		{"unknown compression 5", mut(func(b []byte) []byte {
			binary.BigEndian.PutUint16(b[21:], 5)
			return setCRC(b)
		}), 1 << 20, ErrCorrupt},
		{"zstd passthrough ok", mut(func(b []byte) []byte {
			binary.BigEndian.PutUint16(b[21:], 4)
			return setCRC(b)
		}), 1 << 20, nil},
		{"baseOffset ignored (outside CRC)", mut(func(b []byte) []byte { binary.BigEndian.PutUint64(b[0:], 0xffffffffffffffff); return b }), 1 << 20, nil},
		{"epoch ignored (outside CRC)", mut(func(b []byte) []byte { binary.BigEndian.PutUint32(b[12:], 99); return b }), 1 << 20, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.b.Validate(tc.max)
			if !errors.Is(err, tc.want) || (tc.want == nil && err != nil) {
				t.Fatalf("Validate = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestValidateDoesNotAllocate(t *testing.T) {
	b := mkBatch(t, 10, 100, 0, 'z')
	if n := testing.AllocsPerRun(100, func() { _ = b.Validate(1 << 20) }); n != 0 {
		t.Fatalf("Validate allocates %v times", n)
	}
	bad := Batch(bytes.Repeat([]byte{0xff}, 200))
	if n := testing.AllocsPerRun(100, func() { _ = bad.Validate(1 << 20) }); n != 0 {
		t.Fatalf("Validate(garbage) allocates %v times", n)
	}
}

func TestRecordsRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		recs []Record
	}{
		{"single", []Record{{Key: []byte("k"), Value: []byte("v")}}},
		{"null key and value", []Record{{}}},
		{"empty key and value", []Record{{Key: []byte{}, Value: []byte{}}}},
		{"headers", []Record{{
			Value:   []byte("payload"),
			Headers: []Header{{Key: "a", Value: []byte("1")}, {Key: "", Value: nil}, {Key: "c", Value: []byte{}}},
		}}},
		{"negative timestamp delta", []Record{{TimestampDelta: -5, Value: []byte("x")}, {TimestampDelta: 1 << 40}}},
		{"many", func() []Record {
			r := make([]Record, 300)
			for i := range r {
				r[i] = Record{TimestampDelta: int64(i), Value: bytes.Repeat([]byte{byte(i)}, i)}
			}
			return r
		}()},
		{"large value", []Record{{Value: bytes.Repeat([]byte("x"), 70000)}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, err := NewBatch(DefaultBatchOptions(), tc.recs...)
			if err != nil {
				t.Fatal(err)
			}
			if err := b.Validate(1 << 24); err != nil {
				t.Fatal(err)
			}
			got, err := b.Records()
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tc.recs) {
				t.Fatalf("got %d records, want %d", len(got), len(tc.recs))
			}
			for i := range got {
				want := tc.recs[i]
				want.OffsetDelta = int32(i)
				if !reflect.DeepEqual(got[i], want) {
					t.Fatalf("record %d\n got %+v\nwant %+v", i, got[i], want)
				}
				if (got[i].Key == nil) != (want.Key == nil) || (got[i].Value == nil) != (want.Value == nil) {
					t.Fatalf("record %d: null-ness not preserved", i)
				}
			}
			if int(b.LastOffsetDelta()) != len(tc.recs)-1 {
				t.Fatalf("lastOffsetDelta %d", b.LastOffsetDelta())
			}
		})
	}
}

func TestEmptyBatchOccupiesOneOffset(t *testing.T) {
	b, err := NewBatch(DefaultBatchOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Validate(1 << 20); err != nil {
		t.Fatal(err)
	}
	if b.LastOffsetDelta() != 0 || b.RecordsCount() != 0 || len(b) != BatchHeaderSize {
		t.Fatalf("empty batch: delta %d count %d len %d", b.LastOffsetDelta(), b.RecordsCount(), len(b))
	}
	recs, err := b.Records()
	if err != nil || len(recs) != 0 {
		t.Fatalf("Records = %v, %v", recs, err)
	}
}

func TestControlBatch(t *testing.T) {
	b, err := NewControlBatch(DefaultBatchOptions(), ControlLeaderChange, []byte("leader=3"))
	if err != nil {
		t.Fatal(err)
	}
	if !b.IsControl() || b.LastOffsetDelta() != 0 {
		t.Fatalf("control=%v delta=%d", b.IsControl(), b.LastOffsetDelta())
	}
	typ, err := ControlType(b)
	if err != nil || typ != ControlLeaderChange {
		t.Fatalf("ControlType = %d, %v", typ, err)
	}
	if _, err := ControlType(mkBatch(t, 1, 1, 0, 'a')); err == nil {
		t.Fatal("ControlType accepted a data batch")
	}
}

func TestNewBatchRejectsCompression(t *testing.T) {
	opts := DefaultBatchOptions()
	opts.Attributes = 1
	if _, err := NewBatch(opts, Record{}); err == nil {
		t.Fatal("NewBatch accepted compression bits")
	}
}

func TestRecordsErrors(t *testing.T) {
	good := mkBatch(t, 2, 4, 0, 'r')
	withPayload := func(count int32, payload []byte) Batch {
		h, _ := ParseHeader(good)
		h.RecordsCount = count
		h.BatchLength = int32(BatchHeaderSize - LogOverhead + len(payload))
		b := make([]byte, BatchHeaderSize, BatchHeaderSize+len(payload))
		h.Put(b)
		b = append(b, payload...)
		Batch(b).UpdateCRC()
		return b
	}
	compressed := bytes.Clone(good)
	binary.BigEndian.PutUint16(compressed[21:], 1)
	tests := []struct {
		name string
		b    Batch
		want error
	}{
		{"compressed", compressed, ErrCompressed},
		{"huge count", withPayload(math.MaxInt32, []byte{0, 0, 0, 0, 0, 0, 0}), ErrCorrupt},
		{"count > records", withPayload(2, good[BatchHeaderSize:BatchHeaderSize+10]), nil},
		{"trailing garbage", withPayload(2, append(bytes.Clone(good[BatchHeaderSize:]), 0)), ErrCorrupt},
		{"bad varint", withPayload(1, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}), nil},
		{"record length beyond end", withPayload(1, []byte{0x7e, 0, 0, 0, 0, 0, 0}), ErrCorrupt},
		{"header count huge", withPayload(1, []byte{0x0e, 0, 0, 0, 1, 1, 0xfe, 0xff, 0xff, 0xff, 0x07}), nil},
		{"short framing", good[:BatchHeaderSize+1], ErrCorrupt},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.b.Records()
			if err == nil {
				t.Fatal("Records accepted malformed input")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestSplitBatches(t *testing.T) {
	a, b := mkBatch(t, 2, 10, 0, 'a'), mkBatch(t, 3, 5, 0, 'b')
	buf := append(bytes.Clone(a), b...)
	got, err := SplitBatches(buf, 1<<20)
	if err != nil || len(got) != 2 || !bytes.Equal(got[0], a) || !bytes.Equal(got[1], b) {
		t.Fatalf("SplitBatches = %d batches, %v", len(got), err)
	}
	if _, err := SplitBatches(buf[:len(buf)-1], 1<<20); err == nil {
		t.Fatal("accepted a truncated buffer")
	}
	// The returned views must not allow appending into the next batch.
	if cap(got[0]) != len(got[0]) {
		t.Fatal("split batch has spare capacity aliasing the next batch")
	}
}

func TestParseSyncPolicy(t *testing.T) {
	tests := []struct {
		in   string
		want SyncPolicy
		err  bool
	}{
		{"always", SyncPolicy{Mode: SyncAlways}, false},
		{"never", SyncPolicy{Mode: SyncNever}, false},
		{"interval=10", SyncPolicy{Mode: SyncInterval, Interval: 10e6}, false},
		{"interval=250ms", SyncPolicy{Mode: SyncInterval, Interval: 250e6}, false},
		{"interval=0", SyncPolicy{}, true},
		{"interval=-5ms", SyncPolicy{}, true},
		{"interval=abc", SyncPolicy{}, true},
		{"sometimes", SyncPolicy{}, true},
	}
	for _, tc := range tests {
		got, err := ParseSyncPolicy(tc.in)
		if (err != nil) != tc.err || (!tc.err && got != tc.want) {
			t.Errorf("ParseSyncPolicy(%q) = %v, %v", tc.in, got, err)
		}
		if !tc.err {
			if back, err := ParseSyncPolicy(got.String()); err != nil || back != got {
				t.Errorf("round trip %q → %q → %v, %v", tc.in, got.String(), back, err)
			}
		}
	}
}
