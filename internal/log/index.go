package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/Rahul-Nikhate/quorumlog/internal/fs"
)

// Index entry sizes (spec §5.2.2).
const (
	offsetEntrySize = 8  // relOffset uint32 | position uint32
	timeEntrySize   = 12 // maxTimestampSoFar int64 | relOffset uint32
)

// errIndexFull is returned by index appends when the preallocated space is
// exhausted. The log rolls a new segment before this can happen.
var errIndexFull = errors.New("log: index full")

// errIndexCorrupt reports an index that failed its sanity check; the caller
// rebuilds it from the log.
var errIndexCorrupt = errors.New("log: index corrupt")

// index is the storage shared by the offset and time indexes: a fixed-size
// entry array backed either by a shared memory mapping of the file (on Unix,
// for OS files) or by a heap buffer that is written back to the file when the
// index is sealed (portable fallback, and FaultFS in tests).
//
// While writable ("active" segment) the file is preallocated to maxBytes so the
// mapping never needs to grow; sealing trims the file to the valid entries and
// remaps it read-only. The file is never truncated while mapped (truncating a
// mapped file and touching the lost pages raises SIGBUS).
//
// mu protects the mapping's lifetime and n: lookups take the read lock, so the
// writer can unmap/remap (seal, unseal, close) without a reader touching a
// dead mapping.
type index struct {
	mu        sync.RWMutex
	f         fs.File
	buf       []byte // mapping (len = mapped length) or heap buffer
	mapped    bool
	writable  bool
	n         int // valid bytes, a multiple of entrySize
	entrySize int
	maxBytes  int // capacity while writable, a multiple of entrySize
}

// init opens the index over f. When writable, the existing valid bytes are
// [0, validBytes) and (in mmap mode) the file is preallocated to maxBytes; when
// read-only (sealed), the file must contain exactly validBytes bytes of entries.
func (ix *index) init(f fs.File, entrySize, maxBytes, validBytes int, writable bool) error {
	maxBytes -= maxBytes % entrySize
	if maxBytes < 2*entrySize {
		return fmt.Errorf("log: max index size %d too small", maxBytes)
	}
	if validBytes%entrySize != 0 || validBytes < 0 || validBytes > maxBytes {
		return errIndexCorrupt
	}
	ix.f, ix.entrySize, ix.maxBytes, ix.n, ix.writable = f, entrySize, maxBytes, validBytes, writable
	return ix.attach()
}

// attach establishes buf for the current writable/n state. Caller holds mu (or
// has exclusive access).
func (ix *index) attach() error {
	if ix.writable {
		if canMmap(ix.f) {
			// Preallocate so the mapping covers the whole capacity. Only the
			// mapped mode needs this; the heap mode writes entries back on seal.
			if err := ix.f.Truncate(int64(ix.maxBytes)); err != nil {
				return err
			}
			m, err := mmapFile(ix.f, ix.maxBytes, true)
			if err != nil {
				return err
			}
			ix.buf, ix.mapped = m, true
			return nil
		}
		buf := make([]byte, ix.n, max(ix.n, 64*ix.entrySize))
		if err := readFull(ix.f, buf); err != nil {
			return err
		}
		ix.buf, ix.mapped = buf, false
		return nil
	}
	if ix.n == 0 {
		ix.buf, ix.mapped = nil, false
		return nil
	}
	if canMmap(ix.f) {
		m, err := mmapFile(ix.f, ix.n, false)
		if err != nil {
			return err
		}
		ix.buf, ix.mapped = m, true
		return nil
	}
	buf := make([]byte, ix.n)
	if err := readFull(ix.f, buf); err != nil {
		return err
	}
	ix.buf, ix.mapped = buf, false
	return nil
}

func readFull(f fs.File, buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	n, err := f.ReadAt(buf, 0)
	if n == len(buf) {
		return nil
	}
	if err == nil || errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

// detach releases the mapping. Caller holds mu.
func (ix *index) detach() error {
	var err error
	if ix.mapped && ix.buf != nil {
		err = munmap(ix.buf)
	}
	ix.buf, ix.mapped = nil, false
	return err
}

func (ix *index) entries() int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.n / ix.entrySize
}

// full reports whether the index cannot take two more entries. One slot is kept
// in reserve for the final time-index entry written when the segment is sealed.
func (ix *index) full() bool {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.n+2*ix.entrySize > ix.maxBytes
}

func (ix *index) appendEntry(e []byte) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if !ix.writable {
		return errors.New("log: append to sealed index")
	}
	if ix.n+ix.entrySize > ix.maxBytes {
		return errIndexFull
	}
	if ix.mapped {
		copy(ix.buf[ix.n:], e)
	} else {
		ix.buf = append(ix.buf[:ix.n], e...)
	}
	ix.n += ix.entrySize
	return nil
}

// truncateEntries keeps the first k entries. The index must be writable.
func (ix *index) truncateEntries(k int) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if nb := k * ix.entrySize; nb < ix.n {
		if ix.mapped {
			clear(ix.buf[nb:ix.n])
		}
		ix.n = nb
		if !ix.mapped {
			ix.buf = ix.buf[:nb]
		}
	}
}

// seal trims the file to the valid entries, optionally syncs it, and reopens
// it read-only.
func (ix *index) seal(sync bool) (err error) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	defer ix.degradeOnError(&err)
	if !ix.writable {
		return nil
	}
	if ix.mapped {
		if sync {
			if err := msync(ix.buf); err != nil {
				return err
			}
		}
		if err := ix.detach(); err != nil {
			return err
		}
	} else if ix.n > 0 {
		if _, err := ix.f.WriteAt(ix.buf[:ix.n], 0); err != nil {
			return err
		}
	}
	if err := ix.f.Truncate(int64(ix.n)); err != nil {
		return err
	}
	if sync {
		if err := ix.f.Sync(); err != nil {
			return err
		}
	}
	ix.writable = false
	if ix.buf == nil {
		// Was mapped (detached above): remap read-only.
		return ix.attach()
	}
	// Heap mode: the in-memory copy is still exactly the sealed contents.
	ix.buf = ix.buf[:ix.n:ix.n]
	return nil
}

// unseal makes a sealed index writable again (preallocating and remapping).
func (ix *index) unseal() (err error) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	defer ix.degradeOnError(&err)
	if ix.writable {
		return nil
	}
	if err := ix.detach(); err != nil {
		return err
	}
	ix.writable = true
	// The sealed file holds exactly the n valid bytes; attach preallocates and
	// remaps it (mmap mode) or re-reads it into a growable buffer (heap mode).
	return ix.attach()
}

// degradeOnError keeps the index safe to read after a failed seal/unseal that
// already released the mapping: with no buffer the index is treated as empty,
// so lookups fall back to scanning the log from the segment start. Caller
// holds mu.
func (ix *index) degradeOnError(err *error) {
	if *err != nil && ix.buf == nil {
		ix.n = 0
	}
}

// rawEntry returns entry i. Caller holds mu (read or write).
func (ix *index) rawEntry(i int) []byte {
	o := i * ix.entrySize
	return ix.buf[o : o+ix.entrySize]
}

// offsetIndex maps relative offsets to file positions.
type offsetIndex struct{ index }

func (ix *offsetIndex) entryAt(i int) (rel, pos uint32) {
	e := ix.rawEntry(i)
	return binary.BigEndian.Uint32(e), binary.BigEndian.Uint32(e[4:])
}

// lookup returns the last entry with relOffset <= rel.
func (ix *offsetIndex) lookup(rel uint32) (r, pos uint32, ok bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	n := ix.n / offsetEntrySize
	i := sort.Search(n, func(i int) bool { r, _ := ix.entryAt(i); return r > rel }) - 1
	if i < 0 {
		return 0, 0, false
	}
	r, pos = ix.entryAt(i)
	return r, pos, true
}

// lookupPos returns the last entry with position <= p.
func (ix *offsetIndex) lookupPos(p uint32) (r, pos uint32, ok bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	n := ix.n / offsetEntrySize
	i := sort.Search(n, func(i int) bool { _, q := ix.entryAt(i); return q > p }) - 1
	if i < 0 {
		return 0, 0, false
	}
	r, pos = ix.entryAt(i)
	return r, pos, true
}

func (ix *offsetIndex) last() (rel, pos uint32, ok bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	n := ix.n / offsetEntrySize
	if n == 0 {
		return 0, 0, false
	}
	rel, pos = ix.entryAt(n - 1)
	return rel, pos, true
}

func (ix *offsetIndex) add(rel, pos uint32) error {
	var e [offsetEntrySize]byte
	binary.BigEndian.PutUint32(e[0:], rel)
	binary.BigEndian.PutUint32(e[4:], pos)
	return ix.appendEntry(e[:])
}

// truncateAtPos drops entries whose position is >= p.
func (ix *offsetIndex) truncateAtPos(p uint32) {
	ix.mu.RLock()
	n := ix.n / offsetEntrySize
	k := sort.Search(n, func(i int) bool { _, q := ix.entryAt(i); return q >= p })
	ix.mu.RUnlock()
	ix.truncateEntries(k)
}

// validate checks the invariants of a sealed offset index for a segment whose
// log is logSize bytes: the first entry is (0, 0), and relative offsets and
// positions strictly increase and stay inside the log.
func (ix *offsetIndex) validate(logSize int64) error {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	n := ix.n / offsetEntrySize
	if n == 0 {
		if logSize != 0 {
			return errIndexCorrupt
		}
		return nil
	}
	var prevRel, prevPos uint32
	for i := 0; i < n; i++ {
		rel, pos := ix.entryAt(i)
		if i == 0 {
			if rel != 0 || pos != 0 {
				return errIndexCorrupt
			}
		} else if rel <= prevRel || pos <= prevPos {
			return errIndexCorrupt
		}
		if int64(pos) >= logSize {
			return errIndexCorrupt
		}
		prevRel, prevPos = rel, pos
	}
	return nil
}

// timeIndex maps max-timestamp-so-far values to relative offsets. An entry
// (ts, rel) means: the batch at rel is the first batch whose maxTimestamp is
// ts, and every earlier batch in the segment has a smaller maxTimestamp.
type timeIndex struct{ index }

func (ix *timeIndex) entryAt(i int) (ts int64, rel uint32) {
	e := ix.rawEntry(i)
	return int64(binary.BigEndian.Uint64(e)), binary.BigEndian.Uint32(e[8:])
}

// lookupBefore returns the last entry with timestamp < ts.
func (ix *timeIndex) lookupBefore(ts int64) (t int64, rel uint32, ok bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	n := ix.n / timeEntrySize
	i := sort.Search(n, func(i int) bool { t, _ := ix.entryAt(i); return t >= ts }) - 1
	if i < 0 {
		return 0, 0, false
	}
	t, rel = ix.entryAt(i)
	return t, rel, true
}

func (ix *timeIndex) last() (ts int64, rel uint32, ok bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	n := ix.n / timeEntrySize
	if n == 0 {
		return 0, 0, false
	}
	ts, rel = ix.entryAt(n - 1)
	return ts, rel, true
}

func (ix *timeIndex) add(ts int64, rel uint32) error {
	var e [timeEntrySize]byte
	binary.BigEndian.PutUint64(e[0:], uint64(ts))
	binary.BigEndian.PutUint32(e[8:], rel)
	return ix.appendEntry(e[:])
}

// truncateAtRel drops entries whose relative offset is >= rel.
func (ix *timeIndex) truncateAtRel(rel uint32) {
	ix.mu.RLock()
	n := ix.n / timeEntrySize
	k := sort.Search(n, func(i int) bool { _, r := ix.entryAt(i); return r >= rel })
	ix.mu.RUnlock()
	ix.truncateEntries(k)
}

// validate checks that timestamps and relative offsets strictly increase and
// that every relative offset is below endRel (the segment's next relative offset).
func (ix *timeIndex) validate(endRel int64) error {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	n := ix.n / timeEntrySize
	var prevTS int64
	var prevRel uint32
	for i := 0; i < n; i++ {
		ts, rel := ix.entryAt(i)
		if i > 0 && (ts <= prevTS || rel <= prevRel) {
			return errIndexCorrupt
		}
		if int64(rel) >= endRel {
			return errIndexCorrupt
		}
		prevTS, prevRel = ts, rel
	}
	return nil
}
