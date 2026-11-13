package log

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Rahul-Nikhate/quorumlog/internal/fs"
)

// Errors returned by Log.
var (
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("log: closed")
	// ErrOffsetOutOfRange is returned for offsets outside [LogStartOffset, LogEndOffset].
	ErrOffsetOutOfRange = errors.New("log: offset out of range")
	// ErrCompacted is returned by TermAt for offsets below LogStartOffset-1.
	ErrCompacted = errors.New("log: offset compacted")
	// ErrTermRegression is returned when appending a batch whose term is
	// lower than the term of the last batch (Raft terms never decrease along
	// the log).
	ErrTermRegression = errors.New("log: term lower than last term")
	// ErrTermOverflow is returned for terms that do not fit the int32
	// partitionLeaderEpoch field.
	ErrTermOverflow = errors.New("log: term exceeds int32 partitionLeaderEpoch")
	// ErrOffsetMismatch is returned by AppendStamped when the batch does not
	// start at the log end offset.
	ErrOffsetMismatch = errors.New("log: batch base offset is not the log end offset")
	// ErrLogFailed wraps the first fsync or metadata error. After it the log
	// refuses writes and never reports more data durable (fsyncgate: a retried
	// fsync may "succeed" without the data). The process should crash and
	// recover from disk.
	ErrLogFailed = errors.New("log: failed")
	// ErrTruncated is returned by WaitDurable when the awaited offset was
	// removed by TruncateSuffix or ResetTo before it became durable.
	ErrTruncated = errors.New("log: offset truncated before it became durable")
)

// errInternal marks errors that leave in-memory state inconsistent.
var errInternal = errors.New("log: internal state error")

// Defaults (spec §5.2.2).
const (
	DefaultSegmentBytes       = 128 << 20
	DefaultIndexIntervalBytes = 4096
	DefaultMaxIndexBytes      = 10 << 20
	DefaultMaxBatchBytes      = 8 << 20
)

const checkpointName = "recovery-checkpoint"

// Config configures a Log. Zero fields take defaults.
type Config struct {
	// Dir is the partition directory. Required.
	Dir string
	// FS is the file system; defaults to fs.OS.
	FS fs.FS
	// SegmentBytes rolls a segment when appending would exceed it (Kafka
	// segment.bytes). Capped at 4 GiB-1 (32-bit index positions).
	SegmentBytes int64
	// SegmentAge rolls a non-empty segment this long after its first append
	// (Kafka segment.ms, measured with the broker clock). 0 disables it.
	SegmentAge time.Duration
	// IndexIntervalBytes is the spacing of sparse index entries (Kafka
	// index.interval.bytes).
	IndexIntervalBytes int
	// MaxIndexBytes is the preallocated size of each index file (Kafka
	// segment.index.bytes). A full index rolls the segment.
	MaxIndexBytes int
	// MaxBatchBytes rejects larger batches on append.
	MaxBatchBytes int
	// Sync is the fsync policy (spec §5.3).
	Sync SyncPolicy
	// InitialOffset is the base offset of a brand-new, empty log.
	InitialOffset int64
	// OnDurable, if set, is called (from the flusher goroutine, or from the
	// appending goroutine under SyncNever) each time the durable offset
	// advances. It must not block and must not call back into the Log
	// synchronously; a Raft driver forwards it into its inbox.
	OnDurable func(durableOffset int64)
	// Now is the clock used for segment age and retention; defaults to time.Now.
	Now func() time.Time
	// Logger receives warnings (torn tails, index rebuilds); defaults to discard.
	Logger *slog.Logger
}

func (c Config) withDefaults() (Config, error) {
	if c.Dir == "" {
		return c, errors.New("log: Config.Dir is required")
	}
	if c.FS == nil {
		c.FS = fs.OS
	}
	if c.SegmentBytes <= 0 {
		c.SegmentBytes = DefaultSegmentBytes
	}
	c.SegmentBytes = min(c.SegmentBytes, maxSegmentBytes)
	if c.IndexIntervalBytes <= 0 {
		c.IndexIntervalBytes = DefaultIndexIntervalBytes
	}
	if c.MaxIndexBytes <= 0 {
		c.MaxIndexBytes = DefaultMaxIndexBytes
	}
	if c.MaxIndexBytes < 4*timeEntrySize {
		return c, fmt.Errorf("log: MaxIndexBytes %d too small", c.MaxIndexBytes)
	}
	if c.MaxBatchBytes <= 0 {
		c.MaxBatchBytes = DefaultMaxBatchBytes
	}
	if c.MaxBatchBytes < BatchHeaderSize {
		return c, fmt.Errorf("log: MaxBatchBytes %d too small", c.MaxBatchBytes)
	}
	if c.InitialOffset < 0 || c.InitialOffset > maxOffset {
		return c, fmt.Errorf("log: invalid InitialOffset %d", c.InitialOffset)
	}
	if c.Sync.Mode == SyncInterval && c.Sync.Interval <= 0 {
		return c, errors.New("log: interval sync policy needs a positive interval")
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	return c, nil
}

// Log is a segmented partition log.
//
// Concurrency: one writer at a time (Append, AppendStamped, TruncateSuffix,
// TruncatePrefix, ResetTo, Roll, ApplyRetention, Sync, Close serialize on an
// internal mutex; in QuorumLog they are all called from the partition driver
// goroutine, plus the retention task). Readers (Read, ReadUpTo, ReadBatches,
// TermAt, BatchInfoAt, OffsetForTimestamp and the offset getters) are
// lock-free: they load a copy-on-write segment list and take a reference on
// the segment they read, so retention cannot unlink a file under them.
//
// Readers must bound reads by the committed offset (high watermark): the
// uncommitted suffix may be truncated and rewritten concurrently.
type Log struct {
	cfg    Config
	fsys   fs.FS
	dir    string
	scfg   *segConfig
	logger *slog.Logger

	segs      atomic.Pointer[[]*segment]
	leo       atomic.Int64
	logStart  atomic.Int64
	startTerm atomic.Uint64
	closedR   atomic.Bool // fast-path closed flag for readers

	mu            sync.Mutex
	lastTerm      uint64
	recoveryPoint int64 // durable: every offset below it is synced
	dirDirty      bool
	closing       bool
	closed        bool
	failed        error

	fl *flusher
}

// Open opens (creating if needed) the log in cfg.Dir and runs crash recovery:
// segments wholly below the recovery point are trusted after an index sanity
// check; later segments are CRC-scanned, their indexes rebuilt, and a torn
// tail is truncated. Corruption below the recovery point fails with
// ErrCorruptLog instead of being truncated.
func Open(cfg Config) (*Log, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	l := &Log{
		cfg:    cfg,
		fsys:   cfg.FS,
		dir:    cfg.Dir,
		logger: cfg.Logger,
		scfg: &segConfig{
			indexInterval: int64(cfg.IndexIntervalBytes),
			maxIndexBytes: cfg.MaxIndexBytes,
			logger:        cfg.Logger,
		},
	}
	if err := l.fsys.MkdirAll(l.dir, 0o755); err != nil {
		return nil, err
	}
	segs, err := l.recoverAll()
	if err != nil {
		return nil, err
	}
	l.segs.Store(&segs)
	l.fl = newFlusher(l, cfg.Sync, l.leo.Load()-1)
	l.fl.start()
	return l, nil
}

// recoverAll implements Open's recovery. On success the returned segments are
// consistent, durable, and the last one is writable.
func (l *Log) recoverAll() (_ []*segment, err error) {
	ck, ckErr := readCheckpoint(l.fsys, l.dir)
	if ckErr != nil && !errors.Is(ckErr, os.ErrNotExist) {
		l.logger.Warn("ignoring unreadable recovery checkpoint", "err", ckErr)
	}
	entries, err := l.fsys.ReadDir(l.dir)
	if err != nil {
		return nil, err
	}
	var bases []int64
	logs := map[int64]bool{}
	for _, e := range entries {
		if b, ok := parseSegmentName(e.Name(), logSuffix); ok && !e.IsDir() {
			bases = append(bases, b)
			logs[b] = true
		}
	}
	for _, e := range entries {
		name := e.Name()
		_, isIdx := parseSegmentName(name, indexSuffix)
		_, isTIdx := parseSegmentName(name, timeIndexSuffix)
		var orphan bool
		if isIdx || isTIdx {
			b, _ := parseSegmentName(strings.TrimSuffix(strings.TrimSuffix(name, timeIndexSuffix), indexSuffix)+logSuffix, logSuffix)
			orphan = !logs[b]
		}
		if strings.HasSuffix(name, deletedSuffix) || name == checkpointName+".tmp" || orphan {
			if err := l.fsys.Remove(filepath.Join(l.dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
		}
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })

	logStart, startTerm, rp, clean := l.cfg.InitialOffset, uint64(0), int64(0), false
	if ck.valid {
		logStart, startTerm, rp, clean = ck.logStart, ck.startTerm, ck.recoveryPoint, ck.clean
		if ck.reset {
			// A ResetTo was interrupted: none of the old segments survive.
			l.logger.Warn("completing interrupted log reset", "logStart", logStart)
			for _, b := range bases {
				l.removeSegmentFiles(b)
			}
			bases = nil
		}
	} else if len(bases) > 0 {
		logStart = bases[0]
	}
	if len(bases) > 0 && bases[0] > logStart {
		return nil, fmt.Errorf("%w: first segment %d is above log start offset %d", ErrCorruptLog, bases[0], logStart)
	}
	// Segments wholly below the log start were deleted by retention or prefix
	// truncation (the checkpoint is written before the files are removed).
	for len(bases) > 1 && bases[1] <= logStart {
		l.removeSegmentFiles(bases[0])
		bases = bases[1:]
	}

	var segs []*segment
	defer func() {
		if err != nil {
			for _, s := range segs {
				s.closeFiles()
			}
		}
	}()
	recovered := map[*segment]bool{}
	for i, base := range bases {
		last := i == len(bases)-1
		s, err := openSegmentFiles(l.fsys, l.dir, base, l.scfg)
		if err != nil {
			return nil, err
		}
		trusted := (!last && bases[i+1] <= rp) || (last && clean)
		// Terms never decrease along the physical log. (The first segment is
		// unconstrained: batches below a mid-segment log start may have terms
		// lower than startTerm.)
		var prevTerm uint64
		if n := len(segs); n > 0 && segs[n-1].lastPos >= 0 {
			prevTerm = segs[n-1].lastTerm
		}
		if trusted {
			err = s.openTrusted(prevTerm)
			if err == nil && last && s.next != rp {
				l.logger.Warn("clean-shutdown checkpoint disagrees with log end; recovering", "segment", base,
					"end", s.next, "checkpoint", rp)
				_, err = s.recover(rp, prevTerm)
				trusted = false
			}
		} else {
			_, err = s.recover(rp, prevTerm)
		}
		if err != nil {
			s.closeFiles()
			return nil, err
		}
		if !trusted || !s.sealed {
			// Scanned, or trusted but with rebuilt indexes: make it durable.
			recovered[s] = true
		}
		if n := len(segs); n > 0 && segs[n-1].next != base {
			prev := segs[n-1]
			if trusted || prev.next < rp {
				s.closeFiles()
				return nil, fmt.Errorf("%w: segment %d ends at offset %d but segment %d follows",
					ErrCorruptLog, prev.base, prev.next, base)
			}
			// The previous segment lost its unsynced tail in a crash, so the
			// later segments are unreachable: drop them.
			l.logger.Warn("dropping segments after a torn segment", "from", base)
			s.closeFiles()
			for _, b := range bases[i:] {
				l.removeSegmentFiles(b)
			}
			break
		}
		segs = append(segs, s)
	}
	if len(segs) > 0 && segs[len(segs)-1].next < logStart {
		// Everything on disk is below the log start (an InstallSnapshot beyond
		// our log end): start empty at logStart.
		for _, s := range segs {
			s.closeFiles()
			l.removeSegmentFiles(s.base)
		}
		segs = nil
	}
	if len(segs) == 0 {
		s, err := createSegment(l.fsys, l.dir, logStart, l.scfg)
		if err != nil {
			return nil, err
		}
		segs = []*segment{s}
		recovered[s] = true
	}
	leo := segs[len(segs)-1].next
	if leo < rp {
		return nil, fmt.Errorf("%w: log ends at offset %d below recovery point %d", ErrCorruptLog, leo, rp)
	}

	// Make everything we recovered durable before trusting it: data found by
	// the scan may have come from the page cache of a crashed process.
	for i, s := range segs {
		if i < len(segs)-1 {
			if err := s.seal(recovered[s]); err != nil {
				return nil, err
			}
			continue
		}
		if s.sealed {
			if err := s.unseal(); err != nil {
				return nil, err
			}
		}
		if err := s.log.Sync(); err != nil {
			return nil, err
		}
		s.dirty = false
		if s.size.Load() > 0 && s.rollStart.IsZero() {
			s.rollStart = l.cfg.Now()
		}
	}

	l.leo.Store(leo)
	l.logStart.Store(logStart)
	l.startTerm.Store(startTerm)
	l.lastTerm = startTerm
	for i := len(segs) - 1; i >= 0; i-- {
		if segs[i].lastPos >= 0 {
			l.lastTerm = segs[i].lastTerm
			break
		}
	}
	l.recoveryPoint = leo
	// Clears the clean-shutdown flag durably before any append (and syncs the
	// directory, covering the removals and creations above).
	if err := l.writeCheckpointLocked(false, false); err != nil {
		return nil, err
	}
	return segs, nil
}

func (l *Log) removeSegmentFiles(base int64) {
	for _, suf := range []string{logSuffix, indexSuffix, timeIndexSuffix} {
		p := segmentPath(l.dir, base, suf)
		if err := l.fsys.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			l.logger.Warn("removing segment file", "path", p, "err", err)
		}
	}
	l.dirDirty = true
}

func (l *Log) list() []*segment { return *l.segs.Load() }

func (l *Log) active() *segment {
	s := l.list()
	return s[len(s)-1]
}

// writableLocked reports whether the log accepts writes.
func (l *Log) writableLocked() error {
	if l.closed {
		return ErrClosed
	}
	return l.failed
}

// failLocked records a sticky failure.
func (l *Log) failLocked(err error) error {
	if l.failed == nil {
		l.failed = fmt.Errorf("%w: %w", ErrLogFailed, err)
		l.logger.Error("log failed; refusing further writes", "dir", l.dir, "err", err)
		if l.fl != nil {
			l.fl.fail(l.failed)
		}
	}
	return l.failed
}

// AppendResult describes an appended batch.
type AppendResult struct {
	BaseOffset int64
	LastOffset int64
}

// Append validates b (framing and CRC), stamps it in place with baseOffset =
// LogEndOffset and partitionLeaderEpoch = term (no re-CRC: both fields are
// outside the CRC), and writes it to the active segment, rolling first if
// needed. The data is in the page cache on return; use WaitDurable (or
// OnDurable) for durability according to the sync policy.
//
// b is modified (stamped) and may be retained by the caller afterwards; the
// log does not keep a reference to it.
func (l *Log) Append(b Batch, term uint64) (AppendResult, error) {
	if err := b.Validate(l.cfg.MaxBatchBytes); err != nil {
		return AppendResult{}, err
	}
	if term > math.MaxInt32 {
		return AppendResult{}, ErrTermOverflow
	}
	l.mu.Lock()
	if err := l.writableLocked(); err != nil {
		l.mu.Unlock()
		return AppendResult{}, err
	}
	if term < l.lastTerm {
		l.mu.Unlock()
		return AppendResult{}, fmt.Errorf("%w: %d < %d", ErrTermRegression, term, l.lastTerm)
	}
	if l.leo.Load() > maxOffset-int64(b.LastOffsetDelta()) {
		l.mu.Unlock()
		return AppendResult{}, ErrOffsetOutOfRange
	}
	b.Stamp(l.leo.Load(), term)
	res, notify, err := l.appendLocked(b)
	l.mu.Unlock()
	l.notifyDurable(notify)
	return res, err
}

// AppendStamped appends a batch that already carries its offsets and term, as
// a Raft follower does with entries received from the leader. b.BaseOffset()
// must equal LogEndOffset and its term must not be lower than the last term.
func (l *Log) AppendStamped(b Batch) (AppendResult, error) {
	if err := b.Validate(l.cfg.MaxBatchBytes); err != nil {
		return AppendResult{}, err
	}
	l.mu.Lock()
	if err := l.writableLocked(); err != nil {
		l.mu.Unlock()
		return AppendResult{}, err
	}
	if got, want := b.BaseOffset(), l.leo.Load(); got != want {
		l.mu.Unlock()
		return AppendResult{}, fmt.Errorf("%w: got %d, want %d", ErrOffsetMismatch, got, want)
	}
	if b.PartitionLeaderEpoch() < 0 {
		l.mu.Unlock()
		return AppendResult{}, fmt.Errorf("%w: unstamped batch (epoch %d)", ErrCorrupt, b.PartitionLeaderEpoch())
	}
	if b.Term() < l.lastTerm {
		l.mu.Unlock()
		return AppendResult{}, fmt.Errorf("%w: %d < %d", ErrTermRegression, b.Term(), l.lastTerm)
	}
	if b.BaseOffset() > maxOffset-int64(b.LastOffsetDelta()) {
		l.mu.Unlock()
		return AppendResult{}, ErrOffsetOutOfRange
	}
	res, notify, err := l.appendLocked(b)
	l.mu.Unlock()
	l.notifyDurable(notify)
	return res, err
}

func (l *Log) appendLocked(b Batch) (AppendResult, int64, error) {
	seg := l.active()
	now := l.cfg.Now()
	if seg.needsRoll(b, l.cfg.SegmentBytes, l.cfg.SegmentAge, now) {
		if err := l.rollLocked(); err != nil {
			return AppendResult{}, -1, err
		}
		seg = l.active()
	}
	if _, err := seg.append(b, now); err != nil {
		if errors.Is(err, errInternal) {
			return AppendResult{}, -1, l.failLocked(err)
		}
		// A failed write (EIO, ENOSPC) changes nothing visible: size was not
		// advanced, and the next append overwrites the partial bytes.
		return AppendResult{}, -1, err
	}
	l.lastTerm = b.Term()
	last := b.LastOffset()
	l.leo.Store(last + 1)
	notify := l.fl.written(last)
	return AppendResult{BaseOffset: b.BaseOffset(), LastOffset: last}, notify, nil
}

// Roll closes the active segment and starts a new one at LogEndOffset. It is a
// no-op if the active segment is empty.
func (l *Log) Roll() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.writableLocked(); err != nil {
		return err
	}
	if l.active().size.Load() == 0 {
		return nil
	}
	return l.rollLocked()
}

// rollLocked seals the active segment (fsyncing it unless the policy is
// SyncNever), creates the next one, and advances the recovery point.
func (l *Log) rollLocked() error {
	old := l.active()
	durable := l.cfg.Sync.Mode != SyncNever
	if err := old.seal(durable); err != nil {
		return l.failLocked(err)
	}
	base := l.leo.Load()
	ns, err := createSegment(l.fsys, l.dir, base, l.scfg)
	if err != nil {
		return l.failLocked(err)
	}
	segs := append(slices.Clone(l.list()), ns)
	if durable {
		l.recoveryPoint = base
		// Also fsyncs the directory, making the new segment's entries durable.
		if err := l.writeCheckpointLocked(false, false); err != nil {
			ns.release()
			return l.failLocked(err)
		}
	} else {
		l.dirDirty = true
	}
	l.segs.Store(&segs)
	return nil
}

// Sync synchronously fsyncs everything written so far (all dirty segments and
// the directory) and advances the durable offset, regardless of policy.
func (l *Log) Sync() error {
	target, gen, err := l.syncOnce()
	if err != nil {
		return err
	}
	l.notifyDurable(l.fl.complete(target, gen))
	return nil
}

// syncOnce fsyncs all dirty segments without holding the writer lock during
// the fsyncs (so appends proceed in parallel: group commit). It returns the
// offset covered and the truncation generation observed.
func (l *Log) syncOnce() (int64, uint64, error) {
	l.mu.Lock()
	if err := l.writableLocked(); err != nil {
		l.mu.Unlock()
		return 0, 0, err
	}
	target := l.leo.Load() - 1
	gen := l.fl.generation()
	type job struct {
		s      *segment
		sealed bool
	}
	var jobs []job
	for _, s := range l.list() {
		if s.dirty && s.tryAcquire() {
			jobs = append(jobs, job{s, s.sealed})
			s.dirty = false
		}
	}
	dirDirty := l.dirDirty
	l.dirDirty = false
	l.mu.Unlock()

	var err error
	for _, j := range jobs {
		if err == nil {
			err = j.s.log.Sync()
		}
		if err == nil && j.sealed {
			if err = j.s.oFile.Sync(); err == nil {
				err = j.s.tFile.Sync()
			}
		}
		j.s.release()
	}
	if err == nil && dirDirty {
		err = l.fsys.SyncDir(l.dir)
	}
	l.fl.syncs.Add(1)
	if err != nil {
		l.mu.Lock()
		err = l.failLocked(err)
		l.mu.Unlock()
		return 0, 0, err
	}
	return target, gen, nil
}

func (l *Log) notifyDurable(off int64) {
	if off >= 0 && l.cfg.OnDurable != nil {
		l.cfg.OnDurable(off)
	}
}

// WaitDurable blocks until offset is durable according to the sync policy
// (under SyncNever: as soon as it is written), the log fails, the offset is
// truncated away (ErrTruncated), the log is closed, or ctx is done.
func (l *Log) WaitDurable(ctx context.Context, offset int64) error {
	return l.fl.wait(ctx, offset)
}

// DurableOffset is the highest offset durable according to the sync policy
// (-1 if none). Under SyncAlways/SyncInterval it is covered by an fsync; under
// SyncNever it equals LogEndOffset-1 (the policy acknowledges on write).
func (l *Log) DurableOffset() int64 { return l.fl.durableOffset() }

// LogEndOffset (LEO) is the offset the next appended batch will get.
func (l *Log) LogEndOffset() int64 { return l.leo.Load() }

// LogStartOffset is the first offset still readable.
func (l *Log) LogStartOffset() int64 { return l.logStart.Load() }

// StartTerm is the term of offset LogStartOffset-1 (the snapshot point for
// Raft): recorded by TruncatePrefix, ApplyRetention and ResetTo. It is 0 for
// a log that was never prefix-truncated.
func (l *Log) StartTerm() uint64 { return l.startTerm.Load() }

// LastOffsetAndTerm returns the last offset (LogEndOffset-1) and its term. For
// an empty log it returns (LogStartOffset-1, StartTerm).
func (l *Log) LastOffsetAndTerm() (int64, uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.leo.Load() - 1, l.lastTerm
}

// Dir returns the log directory.
func (l *Log) Dir() string { return l.dir }

// Stats is a snapshot of log metrics.
type Stats struct {
	Segments       int
	Bytes          int64
	LogStartOffset int64
	LogEndOffset   int64
	DurableOffset  int64
	Syncs          int64 // fsync rounds issued (flusher + Sync)
}

// Stats returns current metrics.
func (l *Log) Stats() Stats {
	segs := l.list()
	var n int64
	for _, s := range segs {
		n += s.size.Load()
	}
	return Stats{
		Segments:       len(segs),
		Bytes:          n,
		LogStartOffset: l.logStart.Load(),
		LogEndOffset:   l.leo.Load(),
		DurableOffset:  l.fl.durableOffset(),
		Syncs:          l.fl.syncs.Load(),
	}
}

// FileRegion is a byte range of a segment file holding one or more complete
// batches. It pins the segment: the file stays open (and is not unlinked by
// retention) until Release. A server can stream it with sendfile by
// type-asserting File to fs.OSFile.
//
// An empty region (Len == 0) has a nil File and needs no Release (calling it
// is harmless).
type FileRegion struct {
	File        fs.File
	Pos         int64 // byte position of the first batch in File
	Len         int64 // byte length (a whole number of batches)
	StartOffset int64 // base offset of the first batch

	seg      *segment
	released atomic.Bool
}

// Release unpins the segment. It is idempotent.
func (r *FileRegion) Release() {
	if r == nil || r.seg == nil {
		return
	}
	if r.released.CompareAndSwap(false, true) {
		r.seg.release()
	}
}

// Bytes reads the region into a new buffer (the copy path).
func (r *FileRegion) Bytes() ([]byte, error) {
	if r.Len == 0 {
		return nil, nil
	}
	buf := make([]byte, r.Len)
	n, err := r.File.ReadAt(buf, r.Pos)
	if n == len(buf) {
		return buf, nil
	}
	if err == nil || errors.Is(err, io.EOF) {
		err = io.ErrUnexpectedEOF
	}
	return nil, err
}

// WriteTo copies the region to w through a section reader (the copy path).
func (r *FileRegion) WriteTo(w io.Writer) (int64, error) {
	if r.Len == 0 {
		return 0, nil
	}
	return io.Copy(w, io.NewSectionReader(r.File, r.Pos, r.Len))
}

// Read returns the batches starting with the one containing offset, up to
// maxBytes (aligned down to a batch boundary, but always at least one batch
// so consumers make progress), from a single segment. It reads up to the log
// end offset; servers should use ReadUpTo with the high watermark.
func (l *Log) Read(offset int64, maxBytes int) (*FileRegion, error) {
	return l.ReadUpTo(offset, maxBytes, math.MaxInt64)
}

// ReadUpTo is Read restricted to batches whose last offset is below upTo
// (e.g. the high watermark). offset == LogEndOffset (or >= upTo) returns an
// empty region; offsets outside [LogStartOffset, LogEndOffset] return
// ErrOffsetOutOfRange. The caller must Release the region.
func (l *Log) ReadUpTo(offset int64, maxBytes int, upTo int64) (*FileRegion, error) {
	for attempt := 0; ; attempt++ {
		if l.closedR.Load() {
			return nil, ErrClosed
		}
		leo := l.leo.Load() // load before the list: every segment holding offsets < leo is in it
		segs := l.list()
		start := l.logStart.Load()
		if offset < start || offset > leo {
			return nil, fmt.Errorf("%w: %d not in [%d, %d]", ErrOffsetOutOfRange, offset, start, leo)
		}
		limit := min(upTo, leo)
		if offset >= limit {
			return &FileRegion{StartOffset: offset}, nil
		}
		i := sort.Search(len(segs), func(i int) bool { return segs[i].base > offset }) - 1
		if i < 0 {
			return nil, fmt.Errorf("%w: %d below first segment", ErrOffsetOutOfRange, offset)
		}
		seg := segs[i]
		if !seg.tryAcquire() {
			if attempt < 16 {
				continue // the segment was just deleted or truncated away; reload
			}
			return nil, fmt.Errorf("%w: %d (segment deleted)", ErrOffsetOutOfRange, offset)
		}
		r, err := readSegment(seg, offset, maxBytes, limit, limit < leo)
		if err != nil || r.Len == 0 {
			seg.release()
			if err != nil {
				return nil, err
			}
			r.seg = nil
		}
		return r, nil
	}
}

func readSegment(seg *segment, offset int64, maxBytes int, limit int64, bounded bool) (*FileRegion, error) {
	hdr := make([]byte, BatchHeaderSize)
	pos, err := seg.findPosition(offset, hdr)
	if err != nil {
		return nil, err
	}
	end := seg.size.Load()
	if pos >= end {
		return &FileRegion{StartOffset: offset}, nil
	}
	first := Batch(hdr)
	if first.LastOffset() >= limit {
		return &FileRegion{StartOffset: offset}, nil
	}
	startOffset := first.BaseOffset()
	firstEnd := pos + int64(first.Size())
	endPos := end
	if bounded {
		lp, err := seg.findPosition(limit, hdr)
		if err != nil {
			return nil, err
		}
		endPos = min(endPos, lp)
	}
	if mb := int64(max(maxBytes, 0)); mb < endPos-pos {
		ap, err := seg.alignDown(pos, pos+mb, hdr)
		if err != nil {
			return nil, err
		}
		if ap <= pos {
			ap = firstEnd // always return at least one whole batch
		}
		endPos = min(endPos, ap)
	}
	return &FileRegion{File: seg.log, Pos: pos, Len: endPos - pos, StartOffset: startOffset, seg: seg}, nil
}

// ReadBatches returns copies of the batches overlapping [lo, hi), in order,
// stopping before maxBytes would be exceeded (but returning at least one
// batch). Each batch's CRC is verified. This is the Raft LogStore "Entries"
// primitive: entry index = batch.LastOffset(), term = batch.Term().
func (l *Log) ReadBatches(lo, hi int64, maxBytes int) ([]Batch, error) {
	if lo >= hi {
		return nil, nil
	}
	upTo := hi
	if leo := l.leo.Load(); hi < leo {
		info, err := l.BatchInfoAt(hi - 1)
		if err != nil {
			return nil, err
		}
		upTo = info.LastOffset + 1
	}
	var out []Batch
	total := 0
	for cur := lo; cur < upTo; {
		budget := maxBytes - total
		if len(out) > 0 && budget <= 0 {
			break
		}
		r, err := l.ReadUpTo(cur, budget, upTo)
		if err != nil {
			return out, err
		}
		data, err := r.Bytes()
		r.Release()
		if err != nil {
			return out, err
		}
		if len(data) == 0 {
			break
		}
		bs, err := SplitBatches(data, math.MaxInt32)
		if err != nil {
			// Stored bytes that fail validation are corruption, whatever the
			// specific check (CRC, magic, framing).
			return out, fmt.Errorf("%w: segment data at offset %d: %w", ErrCorrupt, cur, err)
		}
		for _, b := range bs {
			if len(out) > 0 && total+len(b) > maxBytes {
				return out, nil
			}
			out = append(out, b)
			total += len(b)
		}
		cur = bs[len(bs)-1].LastOffset() + 1
	}
	return out, nil
}

// BatchInfo describes one stored batch.
type BatchInfo struct {
	BaseOffset   int64
	LastOffset   int64
	Term         uint64
	MaxTimestamp int64
	Position     int64 // byte position in its segment
	Size         int
}

// BatchInfoAt returns the header information of the batch containing offset,
// which must be in [LogStartOffset, LogEndOffset).
func (l *Log) BatchInfoAt(offset int64) (BatchInfo, error) {
	for attempt := 0; ; attempt++ {
		if l.closedR.Load() {
			return BatchInfo{}, ErrClosed
		}
		leo := l.leo.Load()
		segs := l.list()
		if start := l.logStart.Load(); offset < start || offset >= leo {
			return BatchInfo{}, fmt.Errorf("%w: %d not in [%d, %d)", ErrOffsetOutOfRange, offset, start, leo)
		}
		i := sort.Search(len(segs), func(i int) bool { return segs[i].base > offset }) - 1
		if i < 0 {
			return BatchInfo{}, fmt.Errorf("%w: %d below first segment", ErrOffsetOutOfRange, offset)
		}
		seg := segs[i]
		if !seg.tryAcquire() {
			if attempt < 16 {
				continue
			}
			return BatchInfo{}, fmt.Errorf("%w: %d (segment deleted)", ErrOffsetOutOfRange, offset)
		}
		hdr := make([]byte, BatchHeaderSize)
		pos, err := seg.findPosition(offset, hdr)
		end := seg.size.Load()
		seg.release()
		if err != nil {
			return BatchInfo{}, err
		}
		b := Batch(hdr)
		if pos >= end || b.BaseOffset() > offset {
			// Concurrent truncation of the uncommitted suffix.
			return BatchInfo{}, fmt.Errorf("%w: %d (truncated)", ErrOffsetOutOfRange, offset)
		}
		return BatchInfo{
			BaseOffset:   b.BaseOffset(),
			LastOffset:   b.LastOffset(),
			Term:         b.Term(),
			MaxTimestamp: b.MaxTimestamp(),
			Position:     pos,
			Size:         b.Size(),
		}, nil
	}
}

// TermAt returns the term of the batch containing offset. offset ==
// LogStartOffset-1 returns StartTerm (the snapshot point; -1 for a log that
// starts at 0, with term 0). Lower offsets return ErrCompacted; offsets at or
// beyond LogEndOffset return ErrOffsetOutOfRange.
func (l *Log) TermAt(offset int64) (uint64, error) {
	start := l.logStart.Load()
	switch {
	case offset == start-1:
		return l.startTerm.Load(), nil
	case offset < start-1:
		return 0, fmt.Errorf("%w: %d < %d", ErrCompacted, offset, start-1)
	}
	info, err := l.BatchInfoAt(offset)
	if err != nil {
		return 0, err
	}
	return info.Term, nil
}

// TermStart returns the first offset (>= LogStartOffset) whose batch has a
// term >= term, or LogEndOffset if there is none. Terms never decrease along
// the log, so this is a binary search; Raft uses it for the fast-backup
// ConflictIndex ("first index of ConflictTerm").
func (l *Log) TermStart(term uint64) (int64, error) {
	lo, hi := l.logStart.Load(), l.leo.Load()
	for lo < hi {
		mid := lo + (hi-lo)/2
		t, err := l.TermAt(mid)
		if err != nil {
			return 0, err
		}
		if t >= term {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo, nil
}

// OffsetForTimestamp returns the base offset of the first batch whose
// maxTimestamp >= ts (clamped to LogStartOffset), for ListOffsets by
// timestamp. ok is false if no batch qualifies. Kafka returns the exact record
// offset inside that batch; QuorumLog does not decode records on the broker,
// so it returns the batch's base offset (consumers skip earlier records).
func (l *Log) OffsetForTimestamp(ts int64) (offset int64, ok bool, err error) {
	if l.closedR.Load() {
		return 0, false, ErrClosed
	}
	segs := l.list()
	start := l.logStart.Load()
	for _, seg := range segs {
		if seg.maxTSPub.Load() < ts {
			continue
		}
		if !seg.tryAcquire() {
			continue
		}
		off, found, err := seg.offsetForTimestamp(ts)
		seg.release()
		if err != nil {
			return 0, false, err
		}
		if found {
			return max(off, start), true, nil
		}
	}
	return 0, false, nil
}

func (s *segment) offsetForTimestamp(ts int64) (int64, bool, error) {
	end := s.size.Load()
	var pos int64
	if _, rel, ok := s.tidx.lookupBefore(ts); ok {
		if _, p, ok := s.oidx.lookup(rel); ok {
			pos = int64(p)
		}
	}
	hdr := make([]byte, BatchHeaderSize)
	for pos < end {
		if err := s.readHeader(pos, end, hdr); err != nil {
			return 0, false, err
		}
		b := Batch(hdr)
		if b.MaxTimestamp() >= ts {
			return b.BaseOffset(), true, nil
		}
		pos += int64(b.Size())
	}
	return 0, false, nil
}

// TruncateSuffix removes the batch containing from and every later batch
// (Raft: drop a conflicting uncommitted suffix; for batch-boundary offsets
// this is exactly "delete all batches with base offset >= from"). It returns
// the new LogEndOffset. Offsets >= LogEndOffset are a no-op. Truncating into
// or below LogStartOffset is refused. The truncation is made durable (unless
// the policy is SyncNever) before it returns, and waiters on removed offsets
// get ErrTruncated.
func (l *Log) TruncateSuffix(from int64) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.writableLocked(); err != nil {
		return 0, err
	}
	leo := l.leo.Load()
	if from >= leo {
		return leo, nil
	}
	start := l.logStart.Load()
	if from < start {
		return 0, fmt.Errorf("%w: truncate at %d below log start %d", ErrOffsetOutOfRange, from, start)
	}
	info, err := l.BatchInfoAt(from)
	if err != nil {
		return 0, err
	}
	cut := info.BaseOffset
	if cut < start {
		return 0, fmt.Errorf("%w: batch containing %d starts below log start %d", ErrOffsetOutOfRange, from, start)
	}
	l.fl.beginTruncate()
	// 1. Lower the recovery point durably first, so a crash after the
	//    truncation never mistakes rewritten unsynced data for corruption.
	if l.recoveryPoint > cut {
		l.recoveryPoint = cut
		if err := l.writeCheckpointLocked(false, false); err != nil {
			return 0, l.failLocked(err)
		}
	}
	// 2. Drop whole later segments (unlinked once readers release them).
	segs := l.list()
	i := sort.Search(len(segs), func(i int) bool { return segs[i].base > cut }) - 1
	if i < 0 {
		i = 0
	}
	if i+1 < len(segs) {
		if err := l.removeSegmentsLocked(segs[i+1:], slices.Clone(segs[:i+1])); err != nil {
			return 0, l.failLocked(err)
		}
	}
	// 3. Truncate inside the (now last) segment.
	seg := segs[i]
	next, err := seg.truncateTo(cut)
	if err != nil {
		return 0, l.failLocked(err)
	}
	if l.cfg.Sync.Mode != SyncNever {
		if err := seg.log.Sync(); err != nil {
			return 0, l.failLocked(err)
		}
		seg.dirty = false
	}
	l.leo.Store(next)
	l.lastTerm = l.startTerm.Load()
	for j := i; j >= 0; j-- {
		if segs[j].lastPos >= 0 {
			l.lastTerm = segs[j].lastTerm
			break
		}
	}
	if seg.size.Load() == 0 {
		seg.rollStart = time.Time{}
	}
	l.fl.truncated(next)
	return next, nil
}

// removeSegmentsLocked publishes keep as the new segment list, renames the
// removed segments' files to *.deleted, fsyncs the directory, and drops the
// list's references (the files are unlinked when the last reader releases).
func (l *Log) removeSegmentsLocked(remove, keep []*segment) error {
	l.segs.Store(&keep)
	var first error
	for _, s := range remove {
		if err := s.markDeleted(); err != nil && first == nil {
			first = err
		}
	}
	if first == nil {
		first = l.fsys.SyncDir(l.dir)
	}
	for _, s := range remove {
		s.release()
	}
	return first
}

// TruncatePrefix advances LogStartOffset to offset (<= LogEndOffset), for
// Raft snapshots and DeleteRecords-style trimming. Whole segments that end at
// or below offset are deleted (the active segment is always kept); data below
// offset inside a kept segment becomes unreadable. The new start is recorded
// durably, with the term of offset-1, before any file is removed.
func (l *Log) TruncatePrefix(offset int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.writableLocked(); err != nil {
		return err
	}
	start := l.logStart.Load()
	if offset <= start {
		return nil
	}
	if offset > l.leo.Load() {
		return fmt.Errorf("%w: %d beyond log end %d (use ResetTo)", ErrOffsetOutOfRange, offset, l.leo.Load())
	}
	info, err := l.BatchInfoAt(offset - 1)
	if err != nil {
		return err
	}
	return l.advanceStartLocked(offset, info.Term)
}

// advanceStartLocked records the new log start and deletes whole segments
// below it.
func (l *Log) advanceStartLocked(offset int64, term uint64) error {
	l.logStart.Store(offset)
	l.startTerm.Store(term)
	l.recoveryPoint = max(l.recoveryPoint, offset)
	if err := l.writeCheckpointLocked(false, false); err != nil {
		return l.failLocked(err)
	}
	segs := l.list()
	n := 0
	for n+1 < len(segs) && segs[n+1].base <= offset {
		n++
	}
	if n == 0 {
		return nil
	}
	if err := l.removeSegmentsLocked(segs[:n], slices.Clone(segs[n:])); err != nil {
		return l.failLocked(err)
	}
	return nil
}

// ResetTo discards the whole log and restarts it empty at offset, recording
// term as the term of offset-1. This is what a Raft follower does when it
// installs a snapshot beyond its log (spec §5.12). Crash-safe: a reset
// interrupted by a crash is completed by the next Open.
func (l *Log) ResetTo(offset int64, term uint64) error {
	if offset < 0 || offset > maxOffset {
		return fmt.Errorf("%w: %d", ErrOffsetOutOfRange, offset)
	}
	if term > math.MaxInt32 {
		return ErrTermOverflow
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.writableLocked(); err != nil {
		return err
	}
	l.fl.beginTruncate()
	// Readers see the log as empty from here on (offset == LogEndOffset).
	l.leo.Store(offset)
	l.logStart.Store(offset)
	l.startTerm.Store(term)
	l.recoveryPoint = offset
	// 1. Record the intent durably: if we crash before step 4, Open deletes
	//    every segment and starts empty at offset.
	if err := l.writeCheckpointLocked(false, true); err != nil {
		return l.failLocked(err)
	}
	// 2. Rename the old segments out of the way (a new segment may reuse a
	//    base offset, i.e. a file name). Open readers keep their descriptors.
	old := l.list()
	for _, s := range old {
		if err := s.markDeleted(); err != nil {
			return l.failLocked(err)
		}
	}
	// 3. Create the new, empty active segment and publish it.
	ns, err := createSegment(l.fsys, l.dir, offset, l.scfg)
	if err != nil {
		return l.failLocked(err)
	}
	segs := []*segment{ns}
	l.segs.Store(&segs)
	l.lastTerm = term
	for _, s := range old {
		s.release()
	}
	// 4. Clear the reset flag (this also fsyncs the directory, making the
	//    renames and the new segment durable).
	if err := l.writeCheckpointLocked(false, false); err != nil {
		return l.failLocked(err)
	}
	l.fl.truncated(offset)
	return nil
}

// Close stops the flusher, fsyncs everything, seals the active segment
// (trimming its indexes) and records a clean shutdown, so the next Open skips
// the CRC scan. Outstanding FileRegions stay valid until released. If the log
// had failed, no clean-shutdown record is written.
func (l *Log) Close() error {
	l.mu.Lock()
	if l.closing {
		l.mu.Unlock()
		return nil
	}
	l.closing = true
	l.mu.Unlock()

	l.fl.stop()

	l.mu.Lock()
	var errs []error
	notify := int64(-1)
	segs := l.list()
	if l.failed == nil {
		for i, s := range segs {
			var err error
			if i == len(segs)-1 {
				err = s.seal(true)
			} else if s.dirty {
				err = s.syncAll()
			}
			if err != nil {
				errs = append(errs, err)
				break
			}
		}
		if len(errs) == 0 {
			l.recoveryPoint = l.leo.Load()
			if err := l.writeCheckpointLocked(true, false); err != nil {
				errs = append(errs, err)
			}
		}
		if len(errs) == 0 {
			// Everything is durable now: release waiters that were satisfied
			// by the final sync before failing the rest with ErrClosed.
			notify = l.fl.complete(l.leo.Load()-1, l.fl.generation())
		} else {
			l.failLocked(errors.Join(errs...))
		}
	}
	l.fl.closeWaiters()
	l.closed = true
	l.closedR.Store(true)
	empty := []*segment{}
	l.segs.Store(&empty)
	for _, s := range segs {
		s.release()
	}
	l.mu.Unlock()
	l.notifyDurable(notify)
	return errors.Join(errs...)
}

// Recovery checkpoint file: magic u32 | version u16 | flags u16 | logStart i64
// | startTerm u64 | recoveryPoint i64 | crc32c u32 (over the preceding bytes).
const (
	checkpointMagic   = 0x514c4350 // "QLCP"
	checkpointVersion = 1
	checkpointSize    = 36
	flagClean         = 1 << 0
	flagReset         = 1 << 1
)

type checkpoint struct {
	valid         bool
	clean, reset  bool
	logStart      int64
	startTerm     uint64
	recoveryPoint int64
}

func (l *Log) writeCheckpointLocked(clean, reset bool) error {
	var p [checkpointSize]byte
	binary.BigEndian.PutUint32(p[0:], checkpointMagic)
	binary.BigEndian.PutUint16(p[4:], checkpointVersion)
	var flags uint16
	if clean {
		flags |= flagClean
	}
	if reset {
		flags |= flagReset
	}
	binary.BigEndian.PutUint16(p[6:], flags)
	binary.BigEndian.PutUint64(p[8:], uint64(l.logStart.Load()))
	binary.BigEndian.PutUint64(p[16:], l.startTerm.Load())
	binary.BigEndian.PutUint64(p[24:], uint64(l.recoveryPoint))
	binary.BigEndian.PutUint32(p[32:], crc32.Checksum(p[:32], castagnoli))
	if err := fs.WriteFileAtomic(l.fsys, filepath.Join(l.dir, checkpointName), p[:], 0o644); err != nil {
		return err
	}
	l.dirDirty = false
	return nil
}

func readCheckpoint(fsys fs.FS, dir string) (checkpoint, error) {
	p, err := fs.ReadFile(fsys, filepath.Join(dir, checkpointName), 4096)
	if err != nil {
		return checkpoint{}, err
	}
	if len(p) != checkpointSize {
		return checkpoint{}, fmt.Errorf("log: checkpoint has %d bytes", len(p))
	}
	if binary.BigEndian.Uint32(p[0:]) != checkpointMagic || binary.BigEndian.Uint16(p[4:]) != checkpointVersion {
		return checkpoint{}, errors.New("log: checkpoint has bad magic or version")
	}
	if crc32.Checksum(p[:32], castagnoli) != binary.BigEndian.Uint32(p[32:]) {
		return checkpoint{}, errors.New("log: checkpoint CRC mismatch")
	}
	flags := binary.BigEndian.Uint16(p[6:])
	ck := checkpoint{
		valid:         true,
		clean:         flags&flagClean != 0,
		reset:         flags&flagReset != 0,
		logStart:      int64(binary.BigEndian.Uint64(p[8:])),
		startTerm:     binary.BigEndian.Uint64(p[16:]),
		recoveryPoint: int64(binary.BigEndian.Uint64(p[24:])),
	}
	if ck.logStart < 0 || ck.logStart > maxOffset || ck.recoveryPoint < 0 || ck.recoveryPoint > maxOffset ||
		ck.startTerm > math.MaxInt32 {
		return checkpoint{}, errors.New("log: checkpoint values out of range")
	}
	return ck, nil
}
