package raft

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"reflect"
	"testing"
)

func randomMessage(rng *rand.Rand) Message {
	wire := []MsgType{MsgPreVote, MsgPreVoteResp, MsgVote, MsgVoteResp, MsgApp, MsgAppResp, MsgHeartbeat,
		MsgHeartbeatResp, MsgSnap, MsgSnapResp, MsgTimeoutNow, MsgReadIndex, MsgReadIndexResp}
	u := func() uint64 { return rng.Uint64() >> rng.UintN(64) }
	b := func() bool { return rng.IntN(2) == 1 }
	data := func() []byte {
		if rng.IntN(3) == 0 {
			return nil
		}
		d := make([]byte, 1+rng.IntN(40))
		for i := range d {
			d[i] = byte(rng.Uint32())
		}
		return d
	}
	m := Message{Type: wire[rng.IntN(len(wire))], Group: u(), From: NodeID(rng.Uint32()), To: NodeID(rng.Uint32()), Term: u()}
	switch m.Type {
	case MsgVote, MsgPreVote:
		m.LastLogIndex, m.LastLogTerm, m.Force = u(), u(), b()
	case MsgVoteResp, MsgPreVoteResp:
		m.Granted = b()
	case MsgApp:
		m.PrevLogIndex, m.PrevLogTerm, m.Commit = u(), u(), u()
		for range rng.IntN(4) {
			m.Entries = append(m.Entries, Entry{Term: u(), FirstIndex: u(), LastIndex: u(), Data: data()})
		}
	case MsgAppResp:
		m.Success, m.MatchIndex, m.ConflictIndex, m.ConflictTerm, m.PrevLogIndex = b(), u(), u(), u(), u()
	case MsgHeartbeat:
		m.Commit, m.Context = u(), u()
	case MsgHeartbeatResp, MsgReadIndex:
		m.Context = u()
	case MsgSnap:
		s := &Snapshot{Index: u(), Term: u(), Data: data()}
		for range rng.IntN(5) {
			s.Peers = append(s.Peers, NodeID(rng.Uint32()))
		}
		m.Snapshot = s
	case MsgSnapResp:
		m.Success, m.MatchIndex = b(), u()
	case MsgReadIndexResp:
		m.Context, m.Index = u(), u()
	}
	return m
}

func TestMessageCodecRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for range 20000 {
		m := randomMessage(rng)
		b, err := EncodeMessage(&m)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) != EncodedSize(&m) {
			t.Fatalf("%v: size %d, EncodedSize %d", m.Type, len(b), EncodedSize(&m))
		}
		got, err := DecodeMessage(b)
		if err != nil {
			t.Fatalf("%v: %v", m, err)
		}
		if !reflect.DeepEqual(got, m) {
			t.Fatalf("round trip:\n got %#v\nwant %#v", got, m)
		}
		// Every strict prefix fails to decode (no silent truncation).
		if n := rng.IntN(len(b)); n < len(b) {
			if _, err := DecodeMessage(b[:n]); err == nil {
				t.Fatalf("%v: prefix of %d/%d bytes decoded", m.Type, n, len(b))
			}
		}
		// Trailing bytes are rejected.
		if _, err := DecodeMessage(append(b, 0)); err == nil {
			t.Fatalf("%v: trailing byte accepted", m.Type)
		}
	}
}

func TestMessageCodecRejects(t *testing.T) {
	for _, m := range []Message{{Type: MsgHup}, {Type: MsgProp}, {Type: MsgDurable}, {Type: MsgSnap}, {Type: 200}} {
		if _, err := EncodeMessage(&m); err == nil {
			t.Errorf("%v encoded", m.Type)
		}
	}
	good, _ := EncodeMessage(&Message{Type: MsgVoteResp, Granted: true})
	bad := bytes.Clone(good)
	bad[len(bad)-1] = 2 // non-canonical bool
	if _, err := DecodeMessage(bad); !errors.Is(err, ErrBadMessage) {
		t.Errorf("non-canonical bool: %v", err)
	}
	bad = bytes.Clone(good)
	bad[0] = 9
	if _, err := DecodeMessage(bad); !errors.Is(err, ErrBadWireVersion) {
		t.Errorf("bad version: %v", err)
	}
	// A huge entry count must not allocate: it is rejected against the
	// remaining length.
	app, _ := EncodeMessage(&Message{Type: MsgApp})
	app[len(app)-4], app[len(app)-3], app[len(app)-2], app[len(app)-1] = 0xFF, 0xFF, 0xFF, 0xFF
	if _, err := DecodeMessage(app); !errors.Is(err, ErrShortMessage) {
		t.Errorf("huge count: %v", err)
	}
	// Decoded messages do not alias the input buffer.
	m := Message{Type: MsgApp, Entries: []Entry{{Term: 1, FirstIndex: 1, LastIndex: 1, Data: []byte("abc")}}}
	b, _ := EncodeMessage(&m)
	got, _ := DecodeMessage(b)
	for i := range b {
		b[i] = 0xAA
	}
	if string(got.Entries[0].Data) != "abc" {
		t.Error("decoded entry aliases the frame buffer")
	}
}

// FuzzMessageCodec: decoding arbitrary bytes never panics and never
// allocates beyond the input size; whatever decodes re-encodes to exactly
// the same bytes (the encoding is canonical).
func FuzzMessageCodec(f *testing.F) {
	rng := rand.New(rand.NewPCG(3, 4))
	for range 64 {
		m := randomMessage(rng)
		b, _ := EncodeMessage(&m)
		f.Add(b)
	}
	f.Add([]byte{})
	f.Add([]byte{1})
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := DecodeMessage(b)
		if err != nil {
			return
		}
		if EncodedSize(&m) != len(b) {
			t.Fatalf("EncodedSize %d != input %d", EncodedSize(&m), len(b))
		}
		out, err := EncodeMessage(&m)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		if !bytes.Equal(out, b) {
			t.Fatalf("non-canonical round trip:\n in %x\nout %x", b, out)
		}
	})
}

func TestMessageCodecAllocBound(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	m := randomMessage(rng)
	for m.Type != MsgApp || len(m.Entries) < 2 {
		m = randomMessage(rng)
	}
	b, _ := EncodeMessage(&m)
	allocs := testing.AllocsPerRun(100, func() { _, _ = DecodeMessage(b) })
	if max := float64(2 + len(m.Entries)); allocs > max {
		t.Fatalf("DecodeMessage allocs=%v for %d entries (want <= %v)", allocs, len(m.Entries), max)
	}
}
