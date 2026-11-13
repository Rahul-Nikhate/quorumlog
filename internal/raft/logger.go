package raft

import (
	"fmt"
	"os"
)

// DebugLoggerFromEnv returns a Logger that writes one line per event to
// stderr when the RAFT_DEBUG environment variable is non-empty, and nil
// (logging disabled, zero cost) otherwise. Lines are compact and greppable:
//
//	T12 N3 FOLLOWER→CANDIDATE term=7 last=(93,6) commit=90
//
// where T is the node's local tick count and N its id. Drivers that host many
// groups should wrap it to prefix the group id.
func DebugLoggerFromEnv() Logger {
	if os.Getenv("RAFT_DEBUG") == "" {
		return nil
	}
	return func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
}
