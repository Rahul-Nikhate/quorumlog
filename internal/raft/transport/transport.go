// Package transport moves raft.Messages between brokers.
//
// The production transport (tcp.go, Week 4) keeps one TCP connection per
// (local, remote) broker pair carrying the messages of every Raft group,
// tagged with Message.Group, with one writer goroutine and a bounded queue
// per peer that drops on overflow (spec §5.4.3). mem.go is the in-memory,
// fault-injectable network used by tests.
package transport

import "github.com/Rahul-Nikhate/quorumlog/internal/raft"

// Handler receives inbound messages. It is called for every delivered
// message, for all groups; the receiver demultiplexes on m.Group (e.g. into
// per-partition inboxes). A Handler must not block: a full inbox should drop.
type Handler func(m raft.Message)

// Transport sends Raft messages to peers. Implementations must be safe for
// concurrent use (many partition drivers share one transport).
type Transport interface {
	// Send enqueues msgs for delivery to msg.To. It never blocks; messages may
	// be dropped (Raft tolerates loss), reordered across groups, or delayed.
	// The transport takes ownership of msgs (callers must not modify them).
	Send(msgs []raft.Message)
	// Close stops the transport and releases resources.
	Close() error
}
