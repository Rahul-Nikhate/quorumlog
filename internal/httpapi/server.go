// Package httpapi is QuorumLog's HTTP/JSON API (contract: docs/SCOPE.md).
//
// Every node serves the full API. Requests that need a group's leader —
// writes AND reads — are served locally if this node leads the group and
// otherwise reverse-proxied to the leader's HTTP address. Reads go to the
// leader too because only the leader can serve a linearizable read (it
// confirms its leadership with a Raft ReadIndex round first); a follower's
// copy may be stale. The proxied request carries "X-QL-Forwarded: true" and
// a node never forwards a request that already carries it (it answers
// NOT_LEADER with an X-QL-Leader hint instead), so there are no forwarding
// loops: at most one hop. Responses that went through a hop carry
// X-QL-Forwarded: true and X-QL-Leader: <id>.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"

	"github.com/Rahul-Nikhate/quorumlog/internal/cluster"
	"github.com/Rahul-Nikhate/quorumlog/internal/partition"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
)

// Config configures a Server.
type Config struct {
	Node *cluster.Node
	// HTTPPeers maps node ids to their HTTP address (host:port), used to
	// forward to leaders. Missing peers cannot be forwarded to (NOT_LEADER
	// with a hint is returned instead).
	HTTPPeers map[raft.NodeID]string
	// Dashboard, if set, is served at "/".
	Dashboard      http.Handler
	MaxBodyBytes   int64         // default 1 MiB
	ProduceTimeout time.Duration // default 10s
	Logger         *slog.Logger
}

// Server serves the API of one node.
type Server struct {
	cfg     Config
	node    *cluster.Node
	mux     *http.ServeMux
	proxies map[raft.NodeID]*httputil.ReverseProxy
	metrics *Metrics
}

// New builds the server's handler tree.
func New(cfg Config) *Server {
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = defaultMaxBodyBytes
	}
	if cfg.ProduceTimeout <= 0 {
		cfg.ProduceTimeout = defaultProduceTimeoutMs * time.Millisecond
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	s := &Server{cfg: cfg, node: cfg.Node, mux: http.NewServeMux(), proxies: map[raft.NodeID]*httputil.ReverseProxy{}, metrics: &Metrics{}}
	for id, addr := range cfg.HTTPPeers {
		if id != cfg.Node.ID() {
			s.proxies[id] = newProxy(id, addr, cfg.Logger)
		}
	}
	m := s.mux
	m.HandleFunc("GET /v1/health", s.health)
	m.HandleFunc("GET /v1/cluster", s.clusterInfo)
	m.HandleFunc("POST /v1/topics", s.createTopic)
	m.HandleFunc("GET /v1/topics", s.listTopics)
	m.HandleFunc("POST /v1/topics/{topic}/messages", s.produce)
	m.HandleFunc("GET /v1/topics/{topic}/messages", s.fetch)
	m.HandleFunc("POST /v1/groups/{group}/offsets", s.commitOffset)
	m.HandleFunc("GET /v1/groups/{group}/offsets", s.fetchOffset)
	m.HandleFunc("GET /metrics", s.serveMetrics)
	m.HandleFunc("/v1/", func(w http.ResponseWriter, r *http.Request) {
		writeCode(w, http.StatusNotFound, CodeNotFound, "no such endpoint: "+r.Method+" "+r.URL.Path)
	})
	if cfg.Dashboard != nil {
		m.Handle("/", cfg.Dashboard)
	}
	return s
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// Close releases the forwarding connections (idle keep-alives to peers).
func (s *Server) Close() {
	for _, p := range s.proxies {
		p.Transport.(*http.Transport).CloseIdleConnections()
	}
}

// Metrics returns the server's counters.
func (s *Server) Metrics() *Metrics { return s.metrics }

// NewHTTPServer wraps h in an http.Server with conservative timeouts. The
// write timeout leaves room for the longest long poll (30s) and the produce
// timeout.
func NewHTTPServer(h http.Handler) *http.Server {
	return &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      45 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}
}

func newProxy(id raft.NodeID, addr string, logger *slog.Logger) *httputil.ReverseProxy {
	target := &url.URL{Scheme: "http", Host: addr}
	idStr := strconv.FormatUint(uint64(id), 10)
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.Header.Set(HeaderForwarded, "true")
		},
		Transport: &http.Transport{
			DialContext:           (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
			ResponseHeaderTimeout: 45 * time.Second,
			MaxIdleConnsPerHost:   64,
			IdleConnTimeout:       60 * time.Second,
		},
		// The upstream sets X-QL-Leader itself: its own id when it served
		// the request, or its leader hint when it answers NOT_LEADER (it may
		// have just stepped down). Overwriting it here would hide that hint.
		ModifyResponse: func(resp *http.Response) error {
			resp.Header.Set(HeaderForwarded, "true")
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if r.Context().Err() != nil {
				return // the client went away
			}
			logger.Warn("httpapi: forward failed", "leader", id, "err", err)
			w.Header().Set(HeaderLeader, idStr)
			writeCode(w, http.StatusServiceUnavailable, CodeUnavailable, fmt.Sprintf("forwarding to leader %d failed: %v", id, err))
		},
	}
}

// atLeader returns this node's replica of group if it leads it. Otherwise it
// handles the request — forwards it to the leader, or answers with an error —
// and returns nil.
func (s *Server) atLeader(w http.ResponseWriter, r *http.Request, group string) *partition.Partition {
	p, err := s.node.Group(group)
	if errors.Is(err, cluster.ErrNotFound) {
		// The topic may be newer than this node's copy of __meta. Only the
		// __meta leader can say for sure (linearizable read).
		if s.node.Leader(cluster.MetaGroup) != s.node.ID() {
			s.forward(w, r, s.node.Leader(cluster.MetaGroup), cluster.MetaGroup)
			return nil
		}
		if _, err := s.node.Topics(r.Context()); err != nil {
			writeError(w, err)
			return nil
		}
		if p, err = s.node.Group(group); err != nil {
			writeError(w, err)
			return nil
		}
	} else if err != nil {
		writeError(w, err)
		return nil
	}
	st := p.Status()
	if st.Role == raft.Leader {
		w.Header().Set(HeaderLeader, strconv.FormatUint(uint64(s.node.ID()), 10))
		return p
	}
	s.forward(w, r, st.Leader, group)
	return nil
}

// forward proxies r to node leader, at most one hop.
func (s *Server) forward(w http.ResponseWriter, r *http.Request, leader raft.NodeID, group string) {
	if leader == raft.None || leader == s.node.ID() {
		writeCode(w, http.StatusServiceUnavailable, CodeUnavailable, fmt.Sprintf("no leader for %s yet (election in progress); retry", group))
		return
	}
	proxy := s.proxies[leader]
	if r.Header.Get(HeaderForwarded) != "" || proxy == nil {
		w.Header().Set(HeaderLeader, strconv.FormatUint(uint64(leader), 10))
		writeCode(w, http.StatusServiceUnavailable, CodeNotLeader, fmt.Sprintf("node %d is not the leader of %s; leader is %d", s.node.ID(), group, leader))
		return
	}
	// The proxy copies the leader's response headers (including the
	// leader's X-QL-Leader; ModifyResponse adds X-QL-Forwarded), so nothing
	// is set here: each header appears exactly once.
	s.metrics.Forwarded.Add(1)
	proxy.ServeHTTP(w, r)
}

func (s *Server) decode(w http.ResponseWriter, r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes))
	if err := dec.Decode(v); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			return err
		}
		return errBad("invalid JSON body: " + err.Error())
	}
	if _, err := dec.Token(); err != io.EOF {
		return errBad("invalid JSON body: trailing data")
	}
	return nil
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"node_id": s.node.ID(), "ok": true})
}

// ClusterView is the /v1/cluster response.
type ClusterView struct {
	NodeID raft.NodeID     `json:"node_id"`
	Demo   bool            `json:"demo,omitempty"`
	Nodes  []NodeView      `json:"nodes"`
	Groups []GroupView     `json:"groups"`
	Events []cluster.Event `json:"events"`
}

// NodeView is one node in /v1/cluster.
type NodeView struct {
	ID   raft.NodeID `json:"id"`
	Addr string      `json:"addr"`
	Up   bool        `json:"up"`
}

// GroupView is one group row; Node is set only in demo mode, where the view
// has one row per (node, group).
type GroupView struct {
	Node raft.NodeID `json:"node,omitempty"`
	cluster.GroupStatus
}

func (s *Server) clusterInfo(w http.ResponseWriter, r *http.Request) {
	v := ClusterView{NodeID: s.node.ID(), Nodes: []NodeView{}, Groups: []GroupView{}, Events: s.node.Events().List()}
	for _, id := range s.node.Peers() {
		v.Nodes = append(v.Nodes, NodeView{ID: id, Addr: s.cfg.HTTPPeers[id], Up: s.node.PeerUp(id)})
	}
	for _, g := range s.node.Groups() {
		v.Groups = append(v.Groups, GroupView{GroupStatus: g})
	}
	writeJSON(w, http.StatusOK, v)
}

func (s *Server) createTopic(w http.ResponseWriter, r *http.Request) {
	if s.atLeader(w, r, cluster.MetaGroup) == nil {
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := s.decode(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.ProduceTimeout)
	defer cancel()
	created, err := s.node.CreateTopic(ctx, req.Name)
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]string{"name": req.Name})
}

func (s *Server) listTopics(w http.ResponseWriter, r *http.Request) {
	if s.atLeader(w, r, cluster.MetaGroup) == nil {
		return
	}
	topics, err := s.node.Topics(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string][]string{"topics": topics})
}

type produceRequest struct {
	Messages []struct {
		Key   *string `json:"key"`
		Value string  `json:"value"`
	} `json:"messages"`
	Acks       string `json:"acks"`
	ProducerID string `json:"producer_id"`
	Sequence   *int32 `json:"sequence"`
}

// ProduceResponse is the produce response body.
type ProduceResponse struct {
	BaseOffset int64 `json:"base_offset"`
	LastOffset int64 `json:"last_offset"`
	Duplicate  bool  `json:"duplicate"`
}

func (s *Server) produce(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if err := cluster.ValidTopicName(topic); err != nil {
		writeError(w, err)
		return
	}
	p := s.atLeader(w, r, topic)
	if p == nil {
		return
	}
	var req produceRequest
	if err := s.decode(w, r, &req); err != nil {
		s.metrics.ProduceErrors.Add(1)
		writeError(w, err)
		return
	}
	pr := partition.ProduceRequest{ProducerID: req.ProducerID}
	switch req.Acks {
	case "", "all":
		pr.Acks = partition.AcksAll
	case "leader":
		pr.Acks = partition.AcksLeader
	default:
		writeError(w, errBad(`acks must be "all" or "leader"`))
		return
	}
	if len(req.Messages) == 0 || len(req.Messages) > maxMessagesPerRequest {
		writeError(w, errBad(fmt.Sprintf("need 1..%d messages", maxMessagesPerRequest)))
		return
	}
	if req.ProducerID != "" {
		if req.Sequence == nil {
			writeError(w, errBad("sequence is required with producer_id"))
			return
		}
		pr.Sequence = *req.Sequence
	}
	var nbytes int
	for _, m := range req.Messages {
		pm := partition.Message{Value: []byte(m.Value)}
		if m.Key != nil {
			pm.Key = []byte(*m.Key)
		}
		nbytes += len(pm.Key) + len(pm.Value)
		pr.Messages = append(pr.Messages, pm)
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.ProduceTimeout)
	defer cancel()
	s.metrics.ProduceRequests.Add(1)
	res, err := p.ProduceMessages(ctx, pr)
	if err != nil {
		s.metrics.ProduceErrors.Add(1)
		writeError(w, err)
		return
	}
	if !res.Duplicate {
		s.metrics.ProduceMessages.Add(uint64(len(pr.Messages)))
		s.metrics.ProduceBytes.Add(uint64(nbytes))
	}
	writeJSON(w, http.StatusOK, ProduceResponse{BaseOffset: res.BaseOffset, LastOffset: res.LastOffset, Duplicate: res.Duplicate})
}

// MessageView is one message in a fetch response.
type MessageView struct {
	Offset    int64     `json:"offset"`
	Key       *string   `json:"key"`
	Value     string    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

// FetchResponse is the fetch response body.
type FetchResponse struct {
	Messages      []MessageView `json:"messages"`
	NextOffset    int64         `json:"next_offset"`
	HighWatermark int64         `json:"high_watermark"`
}

func queryInt(r *http.Request, name string, def, lo, hi int64) (int64, error) {
	s := r.URL.Query().Get(name)
	if s == "" {
		return def, nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v < lo || v > hi {
		return 0, errBad(fmt.Sprintf("%s must be an integer in [%d, %d]", name, lo, hi))
	}
	return v, nil
}

func (s *Server) fetch(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if err := cluster.ValidTopicName(topic); err != nil {
		writeError(w, err)
		return
	}
	offset, err := queryInt(r, "offset", 0, 0, 1<<62)
	if err != nil {
		writeError(w, err)
		return
	}
	limit, err := queryInt(r, "max", defaultFetchMessages, 1, maxFetchMessages)
	if err != nil {
		writeError(w, err)
		return
	}
	waitMs, err := queryInt(r, "wait_ms", 0, 0, maxWaitMillis)
	if err != nil {
		writeError(w, err)
		return
	}
	p := s.atLeader(w, r, topic)
	if p == nil {
		return
	}
	s.metrics.FetchRequests.Add(1)
	// r.Context() is cancelled when the client disconnects: the long poll
	// stops with it.
	rr, err := p.ReadMessages(r.Context(), partition.ReadRequest{
		Offset: offset, Max: int(limit), Wait: time.Duration(waitMs) * time.Millisecond, Consistency: partition.ConsistencyReadIndex,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	resp := FetchResponse{Messages: make([]MessageView, 0, len(rr.Messages)), NextOffset: rr.NextOffset, HighWatermark: rr.HighWatermark}
	var nbytes int
	for _, m := range rr.Messages {
		mv := MessageView{Offset: m.Offset, Value: string(m.Value), Timestamp: m.Timestamp}
		if m.Key != nil {
			k := string(m.Key)
			mv.Key = &k
		}
		nbytes += len(m.Key) + len(m.Value)
		resp.Messages = append(resp.Messages, mv)
	}
	s.metrics.FetchMessages.Add(uint64(len(rr.Messages)))
	s.metrics.FetchBytes.Add(uint64(nbytes))
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) commitOffset(w http.ResponseWriter, r *http.Request) {
	group := r.PathValue("group")
	if err := cluster.ValidGroupName(group); err != nil {
		writeError(w, err)
		return
	}
	if s.atLeader(w, r, cluster.OffsetsGroup) == nil {
		return
	}
	var req struct {
		Topic  string `json:"topic"`
		Offset *int64 `json:"offset"`
	}
	if err := s.decode(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	if req.Offset == nil {
		writeError(w, errBad("offset is required"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.ProduceTimeout)
	defer cancel()
	if err := s.node.CommitOffset(ctx, group, req.Topic, *req.Offset); err != nil {
		writeError(w, err)
		return
	}
	s.metrics.OffsetCommits.Add(1)
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) fetchOffset(w http.ResponseWriter, r *http.Request) {
	group, topic := r.PathValue("group"), r.URL.Query().Get("topic")
	if err := cluster.ValidGroupName(group); err != nil {
		writeError(w, err)
		return
	}
	if err := cluster.ValidTopicName(topic); err != nil {
		writeError(w, err)
		return
	}
	if s.atLeader(w, r, cluster.OffsetsGroup) == nil {
		return
	}
	off, err := s.node.FetchOffset(r.Context(), group, topic)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"group": group, "topic": topic, "offset": off})
}
