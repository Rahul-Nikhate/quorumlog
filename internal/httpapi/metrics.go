package httpapi

import (
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/Rahul-Nikhate/quorumlog/internal/raft/transport"
)

// Metrics are plain counters, exposed at GET /metrics as "name value" lines
// (Prometheus text format without HELP/TYPE). Counts are per node and reset
// on restart.
type Metrics struct {
	ProduceRequests atomic.Uint64 // served by this node as leader
	ProduceMessages atomic.Uint64 // appended (duplicates excluded)
	ProduceBytes    atomic.Uint64 // key+value bytes appended
	ProduceErrors   atomic.Uint64
	FetchRequests   atomic.Uint64
	FetchMessages   atomic.Uint64
	FetchBytes      atomic.Uint64
	OffsetCommits   atomic.Uint64
	Forwarded       atomic.Uint64 // requests proxied to another node's leader
}

func (s *Server) serveMetrics(w http.ResponseWriter, r *http.Request) {
	m := s.metrics
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	line := func(name string, v uint64) { fmt.Fprintf(w, "quorumlog_%s{node=\"%d\"} %d\n", name, s.node.ID(), v) }
	line("produce_requests_total", m.ProduceRequests.Load())
	line("produce_messages_total", m.ProduceMessages.Load())
	line("produce_bytes_total", m.ProduceBytes.Load())
	line("produce_errors_total", m.ProduceErrors.Load())
	line("fetch_requests_total", m.FetchRequests.Load())
	line("fetch_messages_total", m.FetchMessages.Load())
	line("fetch_bytes_total", m.FetchBytes.Load())
	line("offset_commits_total", m.OffsetCommits.Load())
	line("forwarded_requests_total", m.Forwarded.Load())
	line("elections_won_total", s.node.Elections())
	if t, ok := s.node.Transport().(*transport.TCP); ok {
		st := t.Stats()
		line("raft_messages_sent_total", st.Sent)
		line("raft_messages_received_total", st.Received)
		line("raft_messages_dropped_total", st.Dropped)
	}
}
