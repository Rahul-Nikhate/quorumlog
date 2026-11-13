package raft

// Knob names a deliberately unsafe behaviour for negative-control tests.
type Knob int

// Knobs (see testKnobs).
const (
	KnobCommitOldTerms Knob = iota
	KnobReadIndexNoQuorum
	KnobVoteIgnoreLog
	KnobHeartbeatFullCommit
	KnobTruncateOnAppend
)

func (k Knob) String() string {
	if k < 0 {
		return "no-knob"
	}
	return [...]string{"commit-old-terms", "readindex-no-quorum", "vote-ignore-log",
		"heartbeat-full-commit", "truncate-on-append"}[k]
}

// SetKnob enables an unsafe behaviour on a Config (test binaries only).
func SetKnob(c *Config, k Knob) {
	switch k {
	case KnobCommitOldTerms:
		c.knobs.commitOldTerms = true
	case KnobReadIndexNoQuorum:
		c.knobs.readIndexNoQuorum = true
	case KnobVoteIgnoreLog:
		c.knobs.voteIgnoreLog = true
	case KnobHeartbeatFullCommit:
		c.knobs.heartbeatFullCommit = true
	case KnobTruncateOnAppend:
		c.knobs.truncateOnAppend = true
	}
}
