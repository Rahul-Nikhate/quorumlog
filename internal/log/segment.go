package log

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Rahul-Nikhate/quorumlog/internal/fs"
)

// File name suffixes (spec §5.2.1). File names are the segment's base offset
// zero-padded to 20 digits, so a lexical sort is an offset sort.
const (
	logSuffix       = ".log"
	indexSuffix     = ".index"
	timeIndexSuffix = ".timeindex"
	deletedSuffix   = ".deleted"
	baseDigits      = 20
)

// maxSegmentBytes keeps every batch position representable in the 32-bit
// position field of the offset index.
const maxSegmentBytes = math.MaxUint32

// ErrCorruptLog reports corruption in a region of the log that was known to be
// durable (below the recovery point, or in a cleanly closed segment). The log
// refuses to truncate it, because that could destroy committed data; the
// replica must be treated as faulty and re-replicated (spec §5.2.6).
var ErrCorruptLog = errors.New("log: corruption in durable region")

func segmentPath(dir string, base int64, suffix string) string {
	return filepath.Join(dir, fmt.Sprintf("%0*d%s", baseDigits, base, suffix))
}

// parseSegmentName parses "<20 digits><suffix>" and reports the base offset.
func parseSegmentName(name, suffix string) (int64, bool) {
	if !strings.HasSuffix(name, suffix) {
		return 0, false
	}
	digits := strings.TrimSuffix(name, suffix)
	if len(digits) != baseDigits {
		return 0, false
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	v, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || v < 0 || v > maxOffset {
		return 0, false
	}
	return v, true
}

// segConfig holds the segment-level settings derived from Config.
type segConfig struct {
	indexInterval int64
	maxIndexBytes int
	logger        *slog.Logger
}

// segment is one .log file plus its two indexes.
//
// Concurrency: the Log's writer lock (Log.mu) guards all writer state and all
// mutations. Readers access a segment lock-free after taking a reference
// (tryAcquire); they read only size (atomic), maxTSPub (atomic), the immutable
// fields, and the indexes (which have their own lock). The bytes below size are
// immutable except under TruncateSuffix, which only removes uncommitted data.
type segment struct {
	base  int64
	fsys  fs.FS
	dir   string
	cfg   *segConfig
	log   fs.File
	oidx  *offsetIndex
	tidx  *timeIndex
	oFile fs.File // index files, owned by oidx/tidx once those are open
	tFile fs.File

	size     atomic.Int64 // published byte length; readers never read past it
	maxTSPub atomic.Int64 // published max timestamp (-1 if none)
	refs     atomic.Int32 // 1 for the segment list + 1 per in-flight reader
	deleted  atomic.Bool  // files renamed to *.deleted; unlink when refs hit 0

	// Writer state, guarded by Log.mu.
	next            int64 // next offset to be written in this segment
	lastPos         int64 // position of the last batch, -1 if empty
	lastTerm        uint64
	bytesSinceIndex int64
	maxTS           int64
	maxTSRel        uint32
	lastTimeIndexTS int64
	rollStart       time.Time // when the first batch was appended (segment.ms)
	dirty           bool      // has unsynced data
	sealed          bool      // indexes trimmed and read-only (closed segment)
}

func (s *segment) resetState() {
	s.next = s.base
	s.lastPos = -1
	s.lastTerm = 0
	s.bytesSinceIndex = 0
	s.maxTS = -1
	s.maxTSRel = 0
	s.lastTimeIndexTS = -1
	s.maxTSPub.Store(-1)
}

// createSegment creates fresh, empty segment files at base.
func createSegment(fsys fs.FS, dir string, base int64, cfg *segConfig) (*segment, error) {
	s := &segment{base: base, fsys: fsys, dir: dir, cfg: cfg}
	s.resetState()
	s.refs.Store(1)
	var err error
	flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if s.log, err = fsys.OpenFile(segmentPath(dir, base, logSuffix), flags, 0o644); err != nil {
		return nil, err
	}
	if s.oFile, err = fsys.OpenFile(segmentPath(dir, base, indexSuffix), flags, 0o644); err != nil {
		s.closeFiles()
		return nil, err
	}
	if s.tFile, err = fsys.OpenFile(segmentPath(dir, base, timeIndexSuffix), flags, 0o644); err != nil {
		s.closeFiles()
		return nil, err
	}
	if err = s.openIndexes(0, 0, true); err != nil {
		s.closeFiles()
		return nil, err
	}
	s.dirty = true
	return s, nil
}

// openSegmentFiles opens an existing segment's files without loading indexes.
// Missing index files are created empty (and will be rebuilt).
func openSegmentFiles(fsys fs.FS, dir string, base int64, cfg *segConfig) (*segment, error) {
	s := &segment{base: base, fsys: fsys, dir: dir, cfg: cfg}
	s.resetState()
	s.refs.Store(1)
	var err error
	if s.log, err = fsys.OpenFile(segmentPath(dir, base, logSuffix), os.O_RDWR, 0o644); err != nil {
		return nil, err
	}
	st, err := s.log.Stat()
	if err != nil {
		s.closeFiles()
		return nil, err
	}
	s.size.Store(st.Size())
	if s.oFile, err = fsys.OpenFile(segmentPath(dir, base, indexSuffix), os.O_RDWR|os.O_CREATE, 0o644); err != nil {
		s.closeFiles()
		return nil, err
	}
	if s.tFile, err = fsys.OpenFile(segmentPath(dir, base, timeIndexSuffix), os.O_RDWR|os.O_CREATE, 0o644); err != nil {
		s.closeFiles()
		return nil, err
	}
	return s, nil
}

// openIndexes attaches the index objects to the already-open index files.
func (s *segment) openIndexes(oValid, tValid int, writable bool) error {
	o, t := &offsetIndex{}, &timeIndex{}
	if err := o.init(s.oFile, offsetEntrySize, s.cfg.maxIndexBytes, oValid, writable); err != nil {
		return err
	}
	if err := t.init(s.tFile, timeEntrySize, s.cfg.maxIndexBytes, tValid, writable); err != nil {
		_ = o.detach()
		return err
	}
	s.oidx, s.tidx = o, t
	return nil
}

// detachIndexes drops the index objects (unmapping) but keeps the files open.
func (s *segment) detachIndexes() {
	if s.oidx != nil {
		s.oidx.mu.Lock()
		_ = s.oidx.detach()
		s.oidx.mu.Unlock()
		s.oidx = nil
	}
	if s.tidx != nil {
		s.tidx.mu.Lock()
		_ = s.tidx.detach()
		s.tidx.mu.Unlock()
		s.tidx = nil
	}
}

func (s *segment) closeFiles() {
	s.detachIndexes()
	for _, f := range []fs.File{s.log, s.oFile, s.tFile} {
		if f != nil {
			_ = f.Close()
		}
	}
}

// tryAcquire takes a reader reference unless the segment is being destroyed.
func (s *segment) tryAcquire() bool {
	for {
		r := s.refs.Load()
		if r <= 0 {
			return false
		}
		if s.refs.CompareAndSwap(r, r+1) {
			return true
		}
	}
}

// release drops a reference. The last reference closes the files and, if the
// segment was deleted, unlinks them.
func (s *segment) release() {
	if s.refs.Add(-1) != 0 {
		return
	}
	s.closeFiles()
	if s.deleted.Load() {
		for _, suf := range []string{logSuffix, indexSuffix, timeIndexSuffix} {
			p := segmentPath(s.dir, s.base, suf+deletedSuffix)
			if err := s.fsys.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
				s.cfg.logger.Warn("removing deleted segment file", "path", p, "err", err)
			}
		}
	}
}

// markDeleted renames the segment's files to *.deleted (crash-safe intent).
// The caller removes the segment from the list, syncs the directory, and
// drops the list's reference.
func (s *segment) markDeleted() error {
	var first error
	for _, suf := range []string{logSuffix, indexSuffix, timeIndexSuffix} {
		from := segmentPath(s.dir, s.base, suf)
		if err := s.fsys.Rename(from, from+deletedSuffix); err != nil && first == nil {
			first = err
		}
	}
	s.deleted.Store(true)
	return first
}

// track updates writer state for a batch just placed at pos (n bytes long)
// and, if writeIndex, appends index entries on the index.interval.bytes cadence.
func (s *segment) track(h Batch, pos, n int64, writeIndex bool) error {
	rel := uint32(h.BaseOffset() - s.base)
	if ts := h.MaxTimestamp(); ts > s.maxTS {
		s.maxTS, s.maxTSRel = ts, rel
	}
	if writeIndex && (s.lastPos < 0 || s.bytesSinceIndex >= s.cfg.indexInterval) {
		// The append path rolls before the indexes fill up, so errIndexFull is
		// only possible while rebuilding with a smaller configured capacity;
		// the index then just becomes sparser at the tail (lookups stay
		// correct, they scan forward further).
		err := s.oidx.add(rel, uint32(pos))
		switch {
		case err == nil:
			if s.maxTS > s.lastTimeIndexTS {
				if terr := s.tidx.add(s.maxTS, s.maxTSRel); terr == nil {
					s.lastTimeIndexTS = s.maxTS
				} else if !errors.Is(terr, errIndexFull) {
					return terr
				}
			}
			s.bytesSinceIndex = 0
		case !errors.Is(err, errIndexFull):
			return err
		}
	}
	s.bytesSinceIndex += n
	s.lastPos = pos
	s.lastTerm = h.Term()
	s.next = h.LastOffset() + 1
	s.maxTSPub.Store(s.maxTS)
	return nil
}

// needsRoll reports whether b must go into a new segment.
func (s *segment) needsRoll(b Batch, segmentBytes int64, segmentAge time.Duration, now time.Time) bool {
	size := s.size.Load()
	if size == 0 {
		return false // an empty segment takes any batch, however large
	}
	switch {
	case size+int64(len(b)) > segmentBytes:
		return true
	case segmentAge > 0 && !s.rollStart.IsZero() && now.Sub(s.rollStart) >= segmentAge:
		return true
	case s.oidx.full() || s.tidx.full():
		return true
	case b.LastOffset()-s.base > math.MaxUint32:
		return true
	}
	return false
}

// append writes one validated, stamped batch whose base offset is s.next.
func (s *segment) append(b Batch, now time.Time) (int64, error) {
	pos := s.size.Load()
	if _, err := s.log.WriteAt(b, pos); err != nil {
		return 0, err
	}
	s.dirty = true
	if err := s.track(b, pos, int64(len(b)), true); err != nil {
		return 0, fmt.Errorf("%w: %w", errInternal, err)
	}
	if s.rollStart.IsZero() {
		s.rollStart = now
	}
	// Publish after the bytes are written (atomic store = release).
	s.size.Store(pos + int64(len(b)))
	return pos, nil
}

// readHeader reads the 61-byte header at pos into hdr and checks it against
// the published end of the segment.
func (s *segment) readHeader(pos, end int64, hdr []byte) error {
	if pos < 0 || pos+BatchHeaderSize > end {
		return fmt.Errorf("%w: header at %d beyond segment end %d", ErrCorrupt, pos, end)
	}
	n, err := s.log.ReadAt(hdr[:BatchHeaderSize], pos)
	if n < BatchHeaderSize {
		if err == nil || errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	if err := validateHeader(hdr, int(min(end-pos, math.MaxInt32))); err != nil {
		return fmt.Errorf("%w: batch at position %d of segment %d", ErrCorrupt, pos, s.base)
	}
	return nil
}

// findPosition returns the position of the first batch whose last offset is
// >= target (with its header in hdr), or the segment end if there is none.
// Safe for concurrent readers holding a reference.
func (s *segment) findPosition(target int64, hdr []byte) (int64, error) {
	end := s.size.Load()
	var pos int64
	expect := s.base
	if target > s.base {
		rel := min(target-s.base, math.MaxUint32)
		if r, p, ok := s.oidx.lookup(uint32(rel)); ok {
			pos, expect = int64(p), s.base+int64(r)
		}
	}
	checkExpect := true
	for pos < end {
		if err := s.readHeader(pos, end, hdr); err != nil {
			return 0, err
		}
		b := Batch(hdr)
		if checkExpect {
			checkExpect = false
			if b.BaseOffset() != expect {
				if pos == 0 {
					return 0, fmt.Errorf("%w: segment %d starts at offset %d", ErrCorrupt, s.base, b.BaseOffset())
				}
				// Inconsistent index entry: fall back to a scan from the start.
				pos, expect, checkExpect = 0, s.base, true
				continue
			}
		}
		if b.LastOffset() >= target {
			return pos, nil
		}
		pos += int64(b.Size())
	}
	return end, nil
}

// alignDown returns the largest batch boundary p with from <= p <= limit,
// where from is a batch start. Batches [from, p) fit entirely below limit.
func (s *segment) alignDown(from, limit int64, hdr []byte) (int64, error) {
	end := s.size.Load()
	limit = min(limit, end)
	pos := from
	if _, p, ok := s.oidx.lookupPos(uint32(min(limit, math.MaxUint32))); ok && int64(p) > from {
		pos = int64(p)
	}
	for pos < limit {
		if err := s.readHeader(pos, end, hdr); err != nil {
			return 0, err
		}
		next := pos + int64(Batch(hdr).Size())
		if next > limit {
			break
		}
		pos = next
	}
	return pos, nil
}

// scanner reads a file sequentially in large chunks for recovery.
type scanner struct {
	f      fs.File
	size   int64
	buf    []byte
	bufPos int64
}

const scanChunk = 1 << 20

// peek returns n bytes at pos. The slice is valid until the next call.
func (sc *scanner) peek(pos int64, n int) ([]byte, error) {
	if pos < 0 || n < 0 || pos+int64(n) > sc.size {
		return nil, io.ErrUnexpectedEOF
	}
	if pos >= sc.bufPos && pos+int64(n) <= sc.bufPos+int64(len(sc.buf)) {
		o := pos - sc.bufPos
		return sc.buf[o : o+int64(n)], nil
	}
	want := int(min(max(int64(n), scanChunk), sc.size-pos))
	if cap(sc.buf) < want {
		sc.buf = make([]byte, want)
	}
	sc.buf = sc.buf[:want]
	m, err := sc.f.ReadAt(sc.buf, pos)
	if m < n {
		sc.buf, sc.bufPos = sc.buf[:0], 0
		if err == nil || errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	sc.buf, sc.bufPos = sc.buf[:m], pos
	return sc.buf[:n], nil
}

// recover rebuilds both indexes by scanning and CRC-validating every batch
// (and checking that terms are stamped and never drop below prevTerm, the last
// term of the previous segment), and truncates the log at the first invalid
// batch (a torn tail). If the first
// invalid batch starts below durableBefore (an offset known to be durable),
// the corruption is not a crash artifact and ErrCorruptLog is returned instead
// of truncating. It reports whether the file was truncated.
func (s *segment) recover(durableBefore int64, prevTerm uint64) (truncated bool, err error) {
	s.detachIndexes()
	if err := s.oFile.Truncate(0); err != nil {
		return false, err
	}
	if err := s.tFile.Truncate(0); err != nil {
		return false, err
	}
	if err := s.openIndexes(0, 0, true); err != nil {
		return false, err
	}
	s.sealed = false
	s.resetState()
	st, err := s.log.Stat()
	if err != nil {
		return false, err
	}
	fileSize := st.Size()
	sc := &scanner{f: s.log, size: fileSize}
	var pos int64
	var reason string
	minTerm := prevTerm
	for pos < fileSize {
		hdr, err := sc.peek(pos, BatchHeaderSize)
		if err != nil {
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				return false, err
			}
			reason = "short header"
			break
		}
		if err := validateHeader(hdr, int(min(fileSize-pos, math.MaxInt32))); err != nil {
			reason = "invalid header: " + err.Error()
			break
		}
		if got := Batch(hdr).BaseOffset(); got != s.next {
			reason = fmt.Sprintf("base offset %d, expected %d", got, s.next)
			break
		}
		// partitionLeaderEpoch is outside the CRC; the log's own invariants
		// (stamped, and terms never decrease) catch most corruption of it.
		if e := Batch(hdr).PartitionLeaderEpoch(); e < 0 || uint64(e) < minTerm {
			reason = fmt.Sprintf("epoch %d violates term order (previous %d)", e, minTerm)
			break
		}
		n := Batch(hdr).Size()
		if Batch(hdr).LastOffset()-s.base > math.MaxUint32 || pos+int64(n) > maxSegmentBytes {
			reason = "segment exceeds 32-bit relative offset or position range"
			break
		}
		full, err := sc.peek(pos, n)
		if err != nil {
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				return false, err
			}
			reason = "short batch"
			break
		}
		if Batch(full).ComputeCRC() != Batch(full).CRC() {
			reason = "crc mismatch"
			break
		}
		if err := s.track(Batch(full), pos, int64(n), true); err != nil {
			return false, err
		}
		minTerm = s.lastTerm
		pos += int64(n)
	}
	if pos < fileSize {
		if s.next < durableBefore {
			return false, fmt.Errorf("%w: segment %d position %d (offset %d): %s",
				ErrCorruptLog, s.base, pos, s.next, reason)
		}
		s.cfg.logger.Warn("truncating torn log tail", "segment", s.base, "position", pos,
			"bytes", fileSize-pos, "reason", reason)
		if err := s.log.Truncate(pos); err != nil {
			return false, err
		}
		truncated = true
	}
	s.size.Store(pos)
	s.dirty = true
	return truncated, nil
}

// openTrusted loads the indexes of a segment whose data is known to be
// durable and intact (below the recovery point), sanity-checking them and
// scanning batch headers after the last index entry. On any inconsistency it
// falls back to recover, where corruption is fatal (ErrCorruptLog).
func (s *segment) openTrusted(prevTerm uint64) error {
	if err := s.loadTrusted(prevTerm); err != nil {
		s.cfg.logger.Warn("rebuilding segment indexes", "segment", s.base, "err", err)
		_, rerr := s.recover(math.MaxInt64, prevTerm)
		return rerr
	}
	return nil
}

func (s *segment) loadTrusted(prevTerm uint64) error {
	ost, err := s.oFile.Stat()
	if err != nil {
		return err
	}
	tst, err := s.tFile.Stat()
	if err != nil {
		return err
	}
	if ost.Size() > int64(s.cfg.maxIndexBytes) || tst.Size() > int64(s.cfg.maxIndexBytes) {
		return errIndexCorrupt
	}
	if err := s.openIndexes(int(ost.Size()), int(tst.Size()), false); err != nil {
		return err
	}
	s.sealed = true
	if err := s.oidx.validate(s.size.Load()); err != nil {
		return err
	}
	if err := s.loadTail(); err != nil {
		return err
	}
	if s.lastPos >= 0 && s.lastTerm < prevTerm {
		return errIndexCorrupt // term order violated: re-scan every header
	}
	return s.tidx.validate(s.next - s.base)
}

// loadTail recomputes the writer state from the indexes plus a header scan
// from the last offset-index entry to the end of the log. It verifies framing
// and offset contiguity of the scanned headers but not their CRCs.
func (s *segment) loadTail() error {
	size := s.size.Load()
	s.resetState()
	if ts, rel, ok := s.tidx.last(); ok {
		s.maxTS, s.maxTSRel, s.lastTimeIndexTS = ts, rel, ts
	}
	rel, p, ok := s.oidx.last()
	if !ok {
		if size != 0 {
			return errIndexCorrupt
		}
		s.maxTSPub.Store(s.maxTS)
		return nil
	}
	pos, indexed := int64(p), int64(p)
	s.next = s.base + int64(rel)
	sc := &scanner{f: s.log, size: size}
	for pos < size {
		hdr, err := sc.peek(pos, BatchHeaderSize)
		if err != nil {
			return err
		}
		if err := validateHeader(hdr, int(min(size-pos, math.MaxInt32))); err != nil {
			return err
		}
		b := Batch(hdr)
		if b.BaseOffset() != s.next || b.LastOffset()-s.base > math.MaxUint32 || pos+int64(b.Size()) > maxSegmentBytes ||
			b.PartitionLeaderEpoch() < 0 || (s.lastPos >= 0 && b.Term() < s.lastTerm) {
			return errIndexCorrupt
		}
		if pos == indexed {
			s.bytesSinceIndex = 0
		}
		if err := s.track(b, pos, int64(b.Size()), false); err != nil {
			return err
		}
		pos += int64(b.Size())
	}
	if pos != size {
		return errIndexCorrupt
	}
	return nil
}

// truncateTo removes the batch containing offset and every later batch. It
// returns the new next offset of the segment.
func (s *segment) truncateTo(offset int64) (int64, error) {
	hdr := make([]byte, BatchHeaderSize)
	pos, err := s.findPosition(offset, hdr)
	if err != nil {
		return 0, err
	}
	if pos >= s.size.Load() {
		return s.next, nil
	}
	if s.sealed {
		if err := s.unseal(); err != nil {
			return 0, err
		}
	}
	newNext := Batch(hdr).BaseOffset()
	s.oidx.truncateAtPos(uint32(pos))
	s.tidx.truncateAtRel(uint32(newNext - s.base))
	s.size.Store(pos) // readers stop at pos from now on
	if err := s.log.Truncate(pos); err != nil {
		return 0, err
	}
	s.dirty = true
	if err := s.loadTail(); err != nil {
		return 0, err
	}
	return s.next, nil
}

// seal finalizes a segment that is no longer active: it records a final
// time-index entry for the segment's max timestamp, trims and remaps both
// indexes read-only, and (if sync) fsyncs the log and indexes.
func (s *segment) seal(sync bool) error {
	if s.sealed {
		if sync && s.dirty {
			return s.syncAll()
		}
		return nil
	}
	if s.maxTS > s.lastTimeIndexTS {
		if err := s.tidx.add(s.maxTS, s.maxTSRel); err == nil {
			s.lastTimeIndexTS = s.maxTS
		} else if !errors.Is(err, errIndexFull) {
			return err
		}
	}
	if sync {
		if err := s.log.Sync(); err != nil {
			return err
		}
	}
	if err := s.oidx.seal(sync); err != nil {
		return err
	}
	if err := s.tidx.seal(sync); err != nil {
		return err
	}
	s.sealed = true
	if sync {
		s.dirty = false
	}
	return nil
}

// unseal makes a sealed segment writable again (it becomes the active segment
// after a suffix truncation).
func (s *segment) unseal() error {
	if err := s.oidx.unseal(); err != nil {
		return err
	}
	if err := s.tidx.unseal(); err != nil {
		return err
	}
	s.sealed = false
	s.dirty = true
	return nil
}

// syncAll fsyncs the log and, for sealed segments, the index files.
func (s *segment) syncAll() error {
	if err := s.log.Sync(); err != nil {
		return err
	}
	if s.sealed {
		if err := s.oFile.Sync(); err != nil {
			return err
		}
		if err := s.tFile.Sync(); err != nil {
			return err
		}
	}
	s.dirty = false
	return nil
}
