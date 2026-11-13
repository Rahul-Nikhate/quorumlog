package log

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Rahul-Nikhate/quorumlog/internal/fs"
)

func fuzzSeedBatches(f *testing.F) []Batch {
	opts := DefaultBatchOptions()
	opts.BaseTimestamp = 1_700_000_000_000
	var seeds []Batch
	for _, recs := range [][]Record{
		nil,
		{{Key: []byte("k"), Value: []byte("v")}},
		{{Value: bytes.Repeat([]byte("x"), 300), Headers: []Header{{Key: "h", Value: []byte("1")}}}, {TimestampDelta: 7}},
	} {
		b, err := NewBatch(opts, recs...)
		if err != nil {
			f.Fatal(err)
		}
		seeds = append(seeds, b)
	}
	c, _ := NewControlBatch(opts, ControlLeaderChange, []byte("x"))
	return append(seeds, c)
}

// FuzzBatchValidate: Validate never panics or over-reads on arbitrary input,
// and anything it accepts is safe to inspect, stamp and decode.
func FuzzBatchValidate(f *testing.F) {
	for _, b := range fuzzSeedBatches(f) {
		f.Add([]byte(b))
		f.Add([]byte(b[:len(b)-1]))
		f.Add([]byte(b[:BatchHeaderSize]))
	}
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0xff}, 61))
	f.Fuzz(func(t *testing.T, data []byte) {
		b := Batch(data)
		err := b.Validate(1 << 20)
		_, perr := ParseHeader(data)
		if (perr != nil) != (len(data) < BatchHeaderSize) {
			t.Fatalf("ParseHeader error %v for %d bytes", perr, len(data))
		}
		if err != nil {
			if !errors.Is(err, ErrCorrupt) && !errors.Is(err, ErrTooLarge) && !errors.Is(err, ErrBadMagic) {
				t.Fatalf("unexpected error class: %v", err)
			}
			return
		}
		if b.Size() != len(data) || b.LastOffsetDelta() < 0 || b.RecordsCount() < 0 {
			t.Fatalf("accepted batch with inconsistent framing")
		}
		_ = b.IsControl()
		_, _ = b.Records() // may fail (compressed, malformed records), must not panic
		c := Batch(bytes.Clone(data))
		c.Stamp(12345, 6)
		if err := c.Validate(1 << 20); err != nil {
			t.Fatalf("stamping broke validation: %v", err)
		}
		bs, err := SplitBatches(data, 1<<20)
		if err != nil || len(bs) != 1 {
			t.Fatalf("SplitBatches disagrees with Validate: %d, %v", len(bs), err)
		}
	})
}

// FuzzRecords: the record decoder never panics on arbitrary payloads, bounds
// its allocations by the input size, and decoded records re-encode to records
// that decode identically.
func FuzzRecords(f *testing.F) {
	for _, b := range fuzzSeedBatches(f) {
		f.Add(int32(b.RecordsCount()), []byte(b[BatchHeaderSize:]))
	}
	f.Add(int32(math.MaxInt32), []byte{0, 0, 0, 0, 0, 0, 0})
	f.Add(int32(1), []byte{0x0e, 0, 0, 0, 1, 1, 0xfe, 0xff, 0xff, 0xff, 0x07})
	f.Fuzz(func(t *testing.T, count int32, payload []byte) {
		h := BatchHeader{
			BatchLength:   int32(BatchHeaderSize - LogOverhead + len(payload)),
			Magic:         MagicV2,
			RecordsCount:  count,
			ProducerID:    -1,
			ProducerEpoch: -1,
			BaseSequence:  -1,
		}
		b := make([]byte, BatchHeaderSize, BatchHeaderSize+len(payload))
		h.Put(b)
		b = append(b, payload...)
		recs, err := Batch(b).Records()
		if err != nil {
			return
		}
		if len(recs) != int(count) || cap(recs) > len(payload)/minRecordSize+1 {
			t.Fatalf("decoded %d records (cap %d) from a %d-byte payload claiming %d", len(recs), cap(recs), len(payload), count)
		}
		re, err := NewBatch(DefaultBatchOptions(), recs...)
		if err != nil {
			t.Fatal(err)
		}
		recs2, err := re.Records()
		if err != nil {
			t.Fatalf("re-encoded records fail to decode: %v", err)
		}
		for i := range recs {
			recs[i].OffsetDelta = int32(i) // NewBatch assigns sequential deltas
		}
		if !reflect.DeepEqual(recs, recs2) {
			t.Fatalf("round trip mismatch:\n%+v\n%+v", recs, recs2)
		}
	})
}

// FuzzOpenCorrupted builds a small multi-segment log, corrupts one of its
// files (log, index, time index or checkpoint) with fuzzer-chosen bytes at a
// fuzzer-chosen position, optionally after an unclean shutdown, and reopens
// it. Open must never panic; if it succeeds, every readable batch must be
// valid (or the read must report corruption), and the log must stay writable.
func FuzzOpenCorrupted(f *testing.F) {
	f.Add(uint8(0), uint8(0), uint32(100), []byte{0xff}, false)
	f.Add(uint8(1), uint8(1), uint32(3), []byte{1, 2, 3, 4, 5, 6, 7, 8}, true)
	f.Add(uint8(2), uint8(2), uint32(0), []byte{0x7f, 0, 0, 0, 0, 0, 0, 0}, false)
	f.Add(uint8(3), uint8(0), uint32(8), []byte{0, 0, 0, 0, 0, 0, 0, 9}, true)
	f.Add(uint8(0), uint8(3), uint32(70), []byte{}, true)
	f.Add(uint8(4), uint8(1), uint32(9), []byte{0x10}, false)
	f.Fuzz(func(t *testing.T, which, seg uint8, pos uint32, data []byte, unclean bool) {
		ff := fs.NewFaultFS(1)
		cfg := Config{Dir: "/p", FS: ff, SegmentBytes: 1500, IndexIntervalBytes: 128, MaxIndexBytes: 240}
		l, err := Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 30; i++ {
			if _, err := l.Append(mkBatch(t, 1+i%3, 40, int64(i*10), byte(i)), uint64(1+i/10)); err != nil {
				t.Fatal(err)
			}
		}
		segs := l.list()
		target := segs[int(seg)%len(segs)].base
		if unclean {
			abandon(l)
		} else if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		var name string
		switch which % 5 {
		case 0, 4:
			name = segmentPath("/p", target, logSuffix)
		case 1:
			name = segmentPath("/p", target, indexSuffix)
		case 2:
			name = segmentPath("/p", target, timeIndexSuffix)
		case 3:
			name = filepath.Join("/p", checkpointName)
		}
		fh, err := ff.OpenFile(name, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		st, _ := fh.Stat()
		p := int64(pos) % (st.Size() + 1)
		if which%5 == 4 || len(data) == 0 {
			fh.Truncate(p) // torn / shortened file
		} else {
			if len(data) > 64 {
				data = data[:64]
			}
			fh.WriteAt(data, p)
		}
		fh.Close()

		l2, err := Open(cfg)
		if err != nil {
			return // refusing to open corrupt durable data is allowed
		}
		defer l2.Close()
		bs, err := l2.ReadBatches(l2.LogStartOffset(), l2.LogEndOffset(), math.MaxInt32)
		if err != nil && !errors.Is(err, ErrCorrupt) {
			t.Fatalf("read after reopen: %v", err)
		}
		for _, b := range bs {
			if err := b.Validate(math.MaxInt32); err != nil {
				t.Fatalf("served an invalid batch: %v", err)
			}
		}
		for off := l2.LogStartOffset(); off < l2.LogEndOffset(); off += 7 {
			if r, err := l2.Read(off, 300); err == nil {
				r.Release()
			}
			_, _ = l2.TermAt(off)
		}
		_, _, _ = l2.OffsetForTimestamp(100)
		// The log stays writable. (A flipped partitionLeaderEpoch in the very
		// last batch is undetectable, since the epoch is outside the CRC, so
		// append with the recovered last term.)
		_, lastTerm := l2.LastOffsetAndTerm()
		if lastTerm > math.MaxInt32 {
			t.Fatalf("recovered term %d out of range", lastTerm)
		}
		if _, err := l2.Append(mkBatch(t, 1, 10, 0, 'z'), lastTerm); err != nil {
			t.Fatalf("append after recovery: %v", err)
		}
	})
}

// FuzzCheckpoint: the checkpoint decoder never panics and only accepts
// in-range values with a valid CRC.
func FuzzCheckpoint(f *testing.F) {
	good := make([]byte, checkpointSize)
	binary.BigEndian.PutUint32(good[0:], checkpointMagic)
	binary.BigEndian.PutUint16(good[4:], checkpointVersion)
	f.Add(good)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		ff := fs.NewFaultFS(1)
		ff.MkdirAll("/p", 0o755)
		if err := fs.WriteFileAtomic(ff, "/p/"+checkpointName, data, 0o644); err != nil {
			t.Fatal(err)
		}
		ck, err := readCheckpoint(ff, "/p")
		if err == nil && (ck.logStart < 0 || ck.recoveryPoint < 0 || !ck.valid) {
			t.Fatalf("accepted out-of-range checkpoint %+v", ck)
		}
	})
}
