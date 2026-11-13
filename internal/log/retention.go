package log

import (
	"time"
)

// RetentionPolicy configures time- and size-based deletion of closed segments
// (spec §5.12). Zero or negative fields disable that rule.
type RetentionPolicy struct {
	// MaxAge deletes a closed segment once its max record timestamp (or, if
	// it has no timestamps, its file modification time) is older than
	// now - MaxAge (Kafka retention.ms).
	MaxAge time.Duration
	// MaxBytes deletes the oldest closed segments while the log would still
	// be at least MaxBytes without them (Kafka retention.bytes semantics).
	MaxBytes int64
}

// ApplyRetention deletes whole closed segments, oldest first, while the
// policy says so and the segment lies entirely below highWatermark (never
// deleting uncommitted data). The active segment is never deleted. It returns
// the number of segments deleted.
//
// Deletion order is crash-safe: the new LogStartOffset (and the term of the
// offset before it, the Raft snapshot point) is recorded durably first; then
// the segments are removed from the copy-on-write list, renamed to *.deleted,
// and unlinked once in-flight readers (FileRegions) release them.
func (l *Log) ApplyRetention(p RetentionPolicy, highWatermark int64) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.writableLocked(); err != nil {
		return 0, err
	}
	segs := l.list()
	var total int64
	for _, s := range segs {
		total += s.size.Load()
	}
	now := l.cfg.Now()
	n := 0
	for n < len(segs)-1 {
		s, next := segs[n], segs[n+1]
		if next.base > highWatermark {
			break // contains offsets at or above the high watermark
		}
		size := s.size.Load()
		bySize := p.MaxBytes > 0 && total-size >= p.MaxBytes
		byTime := p.MaxAge > 0 && l.segmentTime(s).Before(now.Add(-p.MaxAge))
		if !bySize && !byTime {
			break
		}
		total -= size
		n++
	}
	if n == 0 {
		return 0, nil
	}
	newStart := segs[n].base
	if newStart <= l.logStart.Load() {
		// Already logically trimmed (e.g. by TruncatePrefix); just delete.
		if err := l.removeSegmentsLocked(segs[:n], append([]*segment(nil), segs[n:]...)); err != nil {
			return 0, l.failLocked(err)
		}
		return n, nil
	}
	// The last deleted batch is offset newStart-1; its term is the new
	// snapshot term.
	if err := l.advanceStartLocked(newStart, segs[n-1].lastTerm); err != nil {
		return 0, err
	}
	return n, nil
}

// segmentTime is the timestamp retention compares against: the segment's max
// record timestamp, or its file mtime when it has no record timestamps.
func (l *Log) segmentTime(s *segment) time.Time {
	if ts := s.maxTSPub.Load(); ts >= 0 {
		return time.UnixMilli(ts)
	}
	if st, err := s.log.Stat(); err == nil {
		return st.ModTime()
	}
	return l.cfg.Now() // unknown: treat as new (never delete on error)
}
