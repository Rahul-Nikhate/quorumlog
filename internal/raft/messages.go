package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// MsgType is the kind of a Raft message.
type MsgType uint8

// Message types. Local types (MsgHup, MsgProp, MsgDurable) are only ever
// passed to Step by the local driver and are never put on the wire.
const (
	MsgHup           MsgType = iota // local: start an election (pre-vote first if enabled)
	MsgPreVote                      // PreVote request (thesis §9.6); Term = candidate term+1, not adopted
	MsgPreVoteResp                  // PreVote response
	MsgVote                         // RequestVote
	MsgVoteResp                     // RequestVote response
	MsgApp                          // AppendEntries
	MsgAppResp                      // AppendEntries response (with fast-backup hints on reject)
	MsgHeartbeat                    // heartbeat: Commit capped at the follower's Match; Context = heartbeat seq
	MsgHeartbeatResp                // heartbeat response; echoes Context
	MsgSnap                         // InstallSnapshot (single message: snapshots are metadata-sized, §5.12)
	MsgSnapResp                     // InstallSnapshot response
	MsgTimeoutNow                   // leadership transfer: campaign immediately, bypassing pre-vote/lease
	MsgReadIndex                    // ReadIndex request (local, or follower → leader)
	MsgReadIndexResp                // ReadIndex response (leader → follower)
	MsgProp                         // local: proposal (Entries carry Data; span = LastIndex-FirstIndex+1)
	MsgDurable                      // local: entries up to (Index, LogTerm) are durable (async durability)
	numMsgTypes
)

var msgTypeNames = [...]string{
	MsgHup: "MsgHup", MsgPreVote: "MsgPreVote", MsgPreVoteResp: "MsgPreVoteResp",
	MsgVote: "MsgVote", MsgVoteResp: "MsgVoteResp", MsgApp: "MsgApp", MsgAppResp: "MsgAppResp",
	MsgHeartbeat: "MsgHeartbeat", MsgHeartbeatResp: "MsgHeartbeatResp", MsgSnap: "MsgSnap",
	MsgSnapResp: "MsgSnapResp", MsgTimeoutNow: "MsgTimeoutNow", MsgReadIndex: "MsgReadIndex",
	MsgReadIndexResp: "MsgReadIndexResp", MsgProp: "MsgProp", MsgDurable: "MsgDurable",
}

func (t MsgType) String() string {
	if int(t) < len(msgTypeNames) {
		return msgTypeNames[t]
	}
	return fmt.Sprintf("MsgType(%d)", uint8(t))
}

// IsLocal reports whether t is a local-only message type.
func (t MsgType) IsLocal() bool { return t == MsgHup || t == MsgProp || t == MsgDurable }

// IsResponse reports whether t is a response type.
func (t MsgType) IsResponse() bool {
	switch t {
	case MsgPreVoteResp, MsgVoteResp, MsgAppResp, MsgHeartbeatResp, MsgSnapResp, MsgReadIndexResp:
		return true
	}
	return false
}

// Message is the single message type exchanged between Raft nodes. Only the
// fields relevant to Type are encoded on the wire.
type Message struct {
	Type  MsgType
	Group uint64 // Raft group (partition) id, used by a multiplexed transport
	From  NodeID
	To    NodeID
	Term  uint64

	// MsgVote / MsgPreVote.
	LastLogIndex uint64
	LastLogTerm  uint64
	Force        bool // vote requested by a leadership transfer: bypass the leader lease
	// MsgVoteResp / MsgPreVoteResp.
	Granted bool

	// MsgApp (and MsgAppResp rejections echo the rejected PrevLogIndex).
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Commit       uint64 // leaderCommit (MsgApp, MsgHeartbeat)
	Entries      []Entry

	// MsgAppResp / MsgSnapResp.
	Success       bool
	MatchIndex    uint64 // on success: follower's last index known to match the leader
	ConflictIndex uint64 // on reject: first offset of ConflictTerm on the follower, or its LastIndex+1
	ConflictTerm  uint64 // on reject: term at PrevLogIndex on the follower (0 if its log is too short)

	// MsgSnap.
	Snapshot *Snapshot

	// MsgReadIndex / MsgReadIndexResp: Context is the caller's request id;
	// MsgHeartbeat / MsgHeartbeatResp: Context is the leader's heartbeat seq.
	Context uint64
	// MsgReadIndexResp: the read index. MsgDurable: the durable index.
	Index uint64
	// MsgDurable: term of the entry at Index.
	LogTerm uint64
}

func (m Message) String() string {
	switch m.Type {
	case MsgVote, MsgPreVote:
		return fmt.Sprintf("%v %d→%d t%d last=(%d,%d) force=%v", m.Type, m.From, m.To, m.Term, m.LastLogIndex, m.LastLogTerm, m.Force)
	case MsgVoteResp, MsgPreVoteResp:
		return fmt.Sprintf("%v %d→%d t%d granted=%v", m.Type, m.From, m.To, m.Term, m.Granted)
	case MsgApp:
		return fmt.Sprintf("%v %d→%d t%d prev=(%d,%d) commit=%d n=%d", m.Type, m.From, m.To, m.Term, m.PrevLogIndex, m.PrevLogTerm, m.Commit, len(m.Entries))
	case MsgAppResp, MsgSnapResp:
		if m.Success {
			return fmt.Sprintf("%v %d→%d t%d ok match=%d", m.Type, m.From, m.To, m.Term, m.MatchIndex)
		}
		return fmt.Sprintf("%v %d→%d t%d reject prev=%d hint=(%d,t%d)", m.Type, m.From, m.To, m.Term, m.PrevLogIndex, m.ConflictIndex, m.ConflictTerm)
	case MsgHeartbeat:
		return fmt.Sprintf("%v %d→%d t%d commit=%d seq=%d", m.Type, m.From, m.To, m.Term, m.Commit, m.Context)
	case MsgSnap:
		if m.Snapshot != nil {
			return fmt.Sprintf("%v %d→%d t%d snap=(%d,%d)", m.Type, m.From, m.To, m.Term, m.Snapshot.Index, m.Snapshot.Term)
		}
	}
	return fmt.Sprintf("%v %d→%d t%d ctx=%d idx=%d", m.Type, m.From, m.To, m.Term, m.Context, m.Index)
}

// Wire codec.
//
// A message is encoded as (the transport adds its own u32 length prefix):
//
//	header: [u8 version=1][u8 type][u64 group][u32 from][u32 to][u64 term]
//	MsgVote/MsgPreVote:       [u64 lastLogIndex][u64 lastLogTerm][u8 force]
//	MsgVoteResp/PreVoteResp:  [u8 granted]
//	MsgApp:                   [u64 prevIndex][u64 prevTerm][u64 commit][u32 n]
//	                          { [u64 term][u64 firstIndex][u64 lastIndex][u32 len][len bytes] } × n
//	MsgAppResp:               [u8 success][u64 matchIndex][u64 conflictIndex][u64 conflictTerm][u64 prevIndex]
//	MsgHeartbeat:             [u64 commit][u64 context]
//	MsgHeartbeatResp:         [u64 context]
//	MsgSnap:                  [u64 index][u64 term][u32 nPeers]{[u32 peer]}×nPeers[u32 len][len bytes]
//	MsgSnapResp:              [u8 success][u64 matchIndex]
//	MsgTimeoutNow:            (empty)
//	MsgReadIndex:             [u64 context]
//	MsgReadIndexResp:         [u64 context][u64 index]
//
// Integers are big-endian. Booleans are exactly 0 or 1. Decoding is strict
// (no trailing bytes, canonical booleans), so Encode(Decode(b)) == b for every
// b that decodes successfully. Local message types are rejected.

const (
	wireVersion    = 1
	headerSize     = 1 + 1 + 8 + 4 + 4 + 8
	entryHeaderLen = 8 + 8 + 8 + 4
)

// Codec errors.
var (
	ErrShortMessage   = errors.New("raft: message truncated")
	ErrBadMessage     = errors.New("raft: malformed message")
	ErrLocalMessage   = errors.New("raft: local message type cannot be encoded")
	ErrBadWireVersion = errors.New("raft: unsupported wire version")
)

// EncodedSize returns the number of bytes AppendMessage will append.
func EncodedSize(m *Message) int {
	n := headerSize
	switch m.Type {
	case MsgVote, MsgPreVote:
		n += 17
	case MsgVoteResp, MsgPreVoteResp:
		n++
	case MsgApp:
		n += 28
		for i := range m.Entries {
			n += entryHeaderLen + len(m.Entries[i].Data)
		}
	case MsgAppResp:
		n += 33
	case MsgHeartbeat:
		n += 16
	case MsgHeartbeatResp, MsgReadIndex:
		n += 8
	case MsgSnap:
		n += 24
		if m.Snapshot != nil {
			n += 4*len(m.Snapshot.Peers) + len(m.Snapshot.Data)
		}
	case MsgSnapResp:
		n += 9
	case MsgReadIndexResp:
		n += 16
	}
	return n
}

// AppendMessage appends the wire encoding of m to dst.
func AppendMessage(dst []byte, m *Message) ([]byte, error) {
	if m.Type >= numMsgTypes {
		return dst, fmt.Errorf("%w: unknown type %d", ErrBadMessage, m.Type)
	}
	if m.Type.IsLocal() {
		return dst, ErrLocalMessage
	}
	if m.Type == MsgSnap && m.Snapshot == nil {
		return dst, fmt.Errorf("%w: MsgSnap without snapshot", ErrBadMessage)
	}
	be := binary.BigEndian
	dst = append(dst, wireVersion, byte(m.Type))
	dst = be.AppendUint64(dst, m.Group)
	dst = be.AppendUint32(dst, uint32(m.From))
	dst = be.AppendUint32(dst, uint32(m.To))
	dst = be.AppendUint64(dst, m.Term)
	switch m.Type {
	case MsgVote, MsgPreVote:
		dst = be.AppendUint64(dst, m.LastLogIndex)
		dst = be.AppendUint64(dst, m.LastLogTerm)
		dst = appendBool(dst, m.Force)
	case MsgVoteResp, MsgPreVoteResp:
		dst = appendBool(dst, m.Granted)
	case MsgApp:
		dst = be.AppendUint64(dst, m.PrevLogIndex)
		dst = be.AppendUint64(dst, m.PrevLogTerm)
		dst = be.AppendUint64(dst, m.Commit)
		dst = be.AppendUint32(dst, uint32(len(m.Entries)))
		for i := range m.Entries {
			e := &m.Entries[i]
			dst = be.AppendUint64(dst, e.Term)
			dst = be.AppendUint64(dst, e.FirstIndex)
			dst = be.AppendUint64(dst, e.LastIndex)
			dst = be.AppendUint32(dst, uint32(len(e.Data)))
			dst = append(dst, e.Data...)
		}
	case MsgAppResp:
		dst = appendBool(dst, m.Success)
		dst = be.AppendUint64(dst, m.MatchIndex)
		dst = be.AppendUint64(dst, m.ConflictIndex)
		dst = be.AppendUint64(dst, m.ConflictTerm)
		dst = be.AppendUint64(dst, m.PrevLogIndex)
	case MsgHeartbeat:
		dst = be.AppendUint64(dst, m.Commit)
		dst = be.AppendUint64(dst, m.Context)
	case MsgHeartbeatResp, MsgReadIndex:
		dst = be.AppendUint64(dst, m.Context)
	case MsgSnap:
		s := m.Snapshot
		dst = be.AppendUint64(dst, s.Index)
		dst = be.AppendUint64(dst, s.Term)
		dst = be.AppendUint32(dst, uint32(len(s.Peers)))
		for _, p := range s.Peers {
			dst = be.AppendUint32(dst, uint32(p))
		}
		dst = be.AppendUint32(dst, uint32(len(s.Data)))
		dst = append(dst, s.Data...)
	case MsgSnapResp:
		dst = appendBool(dst, m.Success)
		dst = be.AppendUint64(dst, m.MatchIndex)
	case MsgTimeoutNow:
	case MsgReadIndexResp:
		dst = be.AppendUint64(dst, m.Context)
		dst = be.AppendUint64(dst, m.Index)
	}
	return dst, nil
}

// EncodeMessage returns the wire encoding of m.
func EncodeMessage(m *Message) ([]byte, error) {
	return AppendMessage(make([]byte, 0, EncodedSize(m)), m)
}

func appendBool(dst []byte, b bool) []byte {
	if b {
		return append(dst, 1)
	}
	return append(dst, 0)
}

type decoder struct {
	b   []byte
	err error
}

func (d *decoder) need(n int) bool {
	if d.err != nil {
		return false
	}
	if len(d.b) < n {
		d.err = ErrShortMessage
		return false
	}
	return true
}

func (d *decoder) u8() uint8 {
	if !d.need(1) {
		return 0
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v
}

func (d *decoder) u32() uint32 {
	if !d.need(4) {
		return 0
	}
	v := binary.BigEndian.Uint32(d.b)
	d.b = d.b[4:]
	return v
}

func (d *decoder) u64() uint64 {
	if !d.need(8) {
		return 0
	}
	v := binary.BigEndian.Uint64(d.b)
	d.b = d.b[8:]
	return v
}

func (d *decoder) boolean() bool {
	v := d.u8()
	if v > 1 && d.err == nil {
		d.err = fmt.Errorf("%w: non-canonical bool %d", ErrBadMessage, v)
	}
	return v == 1
}

// bytes returns a copy of the next n bytes (nil for n == 0), so decoded
// messages never alias the (possibly pooled) frame buffer (spec §13.3).
func (d *decoder) bytes(n uint32) []byte {
	if !d.need(int(n)) || n == 0 {
		return nil
	}
	v := make([]byte, n)
	copy(v, d.b)
	d.b = d.b[n:]
	return v
}

// count reads a u32 element count and rejects counts that cannot possibly fit
// in the remaining bytes, bounding allocation by the input size.
func (d *decoder) count(minElem int) int {
	n := d.u32()
	if d.err == nil && uint64(n)*uint64(minElem) > uint64(len(d.b)) {
		d.err = fmt.Errorf("%w: count %d exceeds remaining %d bytes", ErrShortMessage, n, len(d.b))
		return 0
	}
	return int(n)
}

// DecodeMessage parses one message produced by AppendMessage. The returned
// message does not alias b.
func DecodeMessage(b []byte) (Message, error) {
	d := decoder{b: b}
	var m Message
	if v := d.u8(); d.err == nil && v != wireVersion {
		return Message{}, fmt.Errorf("%w: %d", ErrBadWireVersion, v)
	}
	m.Type = MsgType(d.u8())
	if d.err == nil && (m.Type >= numMsgTypes || m.Type.IsLocal()) {
		return Message{}, fmt.Errorf("%w: type %d", ErrBadMessage, m.Type)
	}
	m.Group = d.u64()
	m.From = NodeID(d.u32())
	m.To = NodeID(d.u32())
	m.Term = d.u64()
	switch m.Type {
	case MsgVote, MsgPreVote:
		m.LastLogIndex = d.u64()
		m.LastLogTerm = d.u64()
		m.Force = d.boolean()
	case MsgVoteResp, MsgPreVoteResp:
		m.Granted = d.boolean()
	case MsgApp:
		m.PrevLogIndex = d.u64()
		m.PrevLogTerm = d.u64()
		m.Commit = d.u64()
		if n := d.count(entryHeaderLen); n > 0 {
			m.Entries = make([]Entry, n)
			for i := range m.Entries {
				e := &m.Entries[i]
				e.Term = d.u64()
				e.FirstIndex = d.u64()
				e.LastIndex = d.u64()
				e.Data = d.bytes(d.u32())
			}
		}
	case MsgAppResp:
		m.Success = d.boolean()
		m.MatchIndex = d.u64()
		m.ConflictIndex = d.u64()
		m.ConflictTerm = d.u64()
		m.PrevLogIndex = d.u64()
	case MsgHeartbeat:
		m.Commit = d.u64()
		m.Context = d.u64()
	case MsgHeartbeatResp, MsgReadIndex:
		m.Context = d.u64()
	case MsgSnap:
		s := &Snapshot{}
		s.Index = d.u64()
		s.Term = d.u64()
		if n := d.count(4); n > 0 {
			s.Peers = make([]NodeID, n)
			for i := range s.Peers {
				s.Peers[i] = NodeID(d.u32())
			}
		}
		s.Data = d.bytes(d.u32())
		m.Snapshot = s
	case MsgSnapResp:
		m.Success = d.boolean()
		m.MatchIndex = d.u64()
	case MsgTimeoutNow:
	case MsgReadIndexResp:
		m.Context = d.u64()
		m.Index = d.u64()
	}
	if d.err != nil {
		return Message{}, d.err
	}
	if len(d.b) != 0 {
		return Message{}, fmt.Errorf("%w: %d trailing bytes", ErrBadMessage, len(d.b))
	}
	return m, nil
}
