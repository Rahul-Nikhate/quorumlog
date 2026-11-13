package cluster

import (
	"sync"
	"time"
)

// Event is one entry of the dashboard's event feed (role changes, elections,
// demo kills/restarts).
type Event struct {
	TS    time.Time `json:"ts"`
	Node  uint32    `json:"node"`
	Group string    `json:"group,omitempty"`
	Msg   string    `json:"msg"`
}

// Events is a fixed-size ring buffer of recent events, safe for concurrent
// use. Several nodes may share one (demo mode: one feed for the whole
// in-process cluster).
type Events struct {
	mu   sync.Mutex
	buf  []Event
	next int
	full bool
}

// NewEvents returns a feed keeping the last n events.
func NewEvents(n int) *Events { return &Events{buf: make([]Event, max(n, 1))} }

// Add records an event (TS defaults to now).
func (e *Events) Add(ev Event) {
	if ev.TS.IsZero() {
		ev.TS = time.Now()
	}
	ev.TS = ev.TS.UTC()
	e.mu.Lock()
	e.buf[e.next] = ev
	e.next = (e.next + 1) % len(e.buf)
	if e.next == 0 {
		e.full = true
	}
	e.mu.Unlock()
}

// List returns the events, oldest first.
func (e *Events) List() []Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.full {
		return append([]Event(nil), e.buf[:e.next]...)
	}
	return append(append([]Event(nil), e.buf[e.next:]...), e.buf[:e.next]...)
}
