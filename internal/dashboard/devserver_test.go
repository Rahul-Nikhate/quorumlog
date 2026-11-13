package dashboard

// A fake, in-memory implementation of the docs/SCOPE.md HTTP API, used to
// develop and test the dashboard without the real backend.
//
// Manual use (serves the dashboard + fake API until interrupted):
//
//	QL_DASHBOARD_DEV=127.0.0.1:8099 go test -run TestDevServer -timeout 0 ./internal/dashboard/
//
// Optional: QL_FAKE_NO_PERNODE=1 omits the per-row "node" field from
// /v1/cluster groups (the plain SCOPE.md shape: one row per group, as seen by
// the answering node). QL_FAKE_NO_DEMO=1 omits "demo": true.
//
// The fake is deliberately simple: elections happen ~1.5 s after a leader is
// killed (if 2 of 3 nodes are up), followers catch up a few entries per tick.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeMsg struct {
	Offset    int64  `json:"offset"`
	Key       string `json:"key,omitempty"`
	Value     string `json:"value"`
	Timestamp string `json:"timestamp"`
}

type fakeGroup struct {
	name           string
	msgs           []fakeMsg
	leader, term   int
	leo            [4]int64 // per node id 1..3
	leaderlessFrom time.Time
	producers      map[string]fakeProducer
}

type fakeProducer struct {
	next       int64
	base, last int64
}

type fakeEvent struct {
	TS    string `json:"ts"`
	Node  int    `json:"node"`
	Group string `json:"group"`
	Msg   string `json:"msg"`
}

type fakeAPI struct {
	electionDelay time.Duration
	perNode, demo bool

	mu      sync.Mutex
	changed chan struct{} // closed and replaced on every data change
	up      [4]bool
	groups  map[string]*fakeGroup
	events  []fakeEvent
	offsets map[string]int64
	stop    chan struct{}
	done    chan struct{}
}

func newFakeAPI(electionDelay time.Duration) *fakeAPI {
	f := &fakeAPI{
		electionDelay: electionDelay, perNode: true, demo: true,
		changed: make(chan struct{}), groups: map[string]*fakeGroup{}, offsets: map[string]int64{},
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	f.up = [4]bool{false, true, true, true}
	f.addGroupLocked("__meta")
	f.addGroupLocked("__offsets")
	go f.loop()
	return f
}

func (f *fakeAPI) Close() { close(f.stop); <-f.done }

func (f *fakeAPI) notifyLocked() { close(f.changed); f.changed = make(chan struct{}) }

func (f *fakeAPI) eventLocked(node int, group, msg string) {
	f.events = append(f.events, fakeEvent{TS: time.Now().UTC().Format(time.RFC3339Nano), Node: node, Group: group, Msg: msg})
	if len(f.events) > 50 {
		f.events = f.events[len(f.events)-50:]
	}
}

func (f *fakeAPI) upCount() int {
	n := 0
	for id := 1; id <= 3; id++ {
		if f.up[id] {
			n++
		}
	}
	return n
}

func (f *fakeAPI) addGroupLocked(name string) *fakeGroup {
	g := &fakeGroup{name: name, producers: map[string]fakeProducer{}}
	for id := 1; id <= 3; id++ {
		if f.up[id] {
			g.leader, g.term = id, 1
			break
		}
	}
	f.groups[name] = g
	f.eventLocked(g.leader, name, fmt.Sprintf("became leader term=%d", g.term))
	return g
}

// loop simulates elections and follower catch-up.
func (f *fakeAPI) loop() {
	defer close(f.done)
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-f.stop:
			return
		case <-t.C:
		}
		f.mu.Lock()
		changed := false
		for _, g := range f.groups {
			if g.leader != 0 && !f.up[g.leader] {
				f.eventLocked(g.leader, g.name, fmt.Sprintf("leader lost (node %d down), election timeout pending", g.leader))
				g.leader, g.leaderlessFrom, changed = 0, time.Now(), true
			}
			if g.leader == 0 && f.upCount() >= 2 && time.Since(g.leaderlessFrom) >= f.electionDelay {
				best := 0
				for id := 1; id <= 3; id++ { // most up-to-date log wins (Raft's election restriction)
					if f.up[id] && (best == 0 || g.leo[id] > g.leo[best]) {
						best = id
					}
				}
				g.term++
				g.leader = best
				f.eventLocked(best, g.name, fmt.Sprintf("became leader term=%d", g.term))
				changed = true
			}
			if g.leader != 0 {
				for id := 1; id <= 3; id++ {
					if f.up[id] && g.leo[id] < g.leo[g.leader] {
						g.leo[id] = min(g.leo[g.leader], g.leo[id]+3)
						changed = true
					}
				}
			}
		}
		if changed {
			f.notifyLocked()
		}
		f.mu.Unlock()
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]string{"error": msg, "code": code})
}

var topicRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,200}$`)

func (f *fakeAPI) routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, map[string]any{"node_id": 1, "ok": true})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		fmt.Fprintf(w, "qlog_fake_groups %d\nqlog_fake_events %d\n", len(f.groups), len(f.events))
	})
	mux.HandleFunc("GET /v1/cluster", f.cluster)
	mux.HandleFunc("GET /v1/topics", func(w http.ResponseWriter, _ *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		topics := []string{}
		for n := range f.groups {
			if !strings.HasPrefix(n, "__") {
				topics = append(topics, n)
			}
		}
		sort.Strings(topics)
		writeJSON(w, 200, map[string]any{"topics": topics})
	})
	mux.HandleFunc("POST /v1/topics", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Name string }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || !topicRe.MatchString(req.Name) || strings.HasPrefix(req.Name, "__") {
			writeErr(w, 400, "BAD_REQUEST", "invalid topic name")
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if _, ok := f.groups[req.Name]; ok {
			writeJSON(w, 200, map[string]string{"name": req.Name})
			return
		}
		if f.groups["__meta"].leader == 0 {
			writeErr(w, 503, "UNAVAILABLE", "no __meta leader")
			return
		}
		f.addGroupLocked(req.Name)
		f.notifyLocked()
		writeJSON(w, 201, map[string]string{"name": req.Name})
	})
	mux.HandleFunc("POST /v1/topics/{topic}/messages", f.produce)
	mux.HandleFunc("GET /v1/topics/{topic}/messages", f.fetch)
	mux.HandleFunc("POST /v1/groups/{group}/offsets", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Topic  string
			Offset int64
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Topic == "" || req.Offset < 0 {
			writeErr(w, 400, "BAD_REQUEST", "need topic and offset >= 0")
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if _, ok := f.groups[req.Topic]; !ok {
			writeErr(w, 404, "NOT_FOUND", "unknown topic")
			return
		}
		f.offsets[r.PathValue("group")+"|"+req.Topic] = req.Offset
		writeJSON(w, 200, map[string]bool{"ok": true})
	})
	mux.HandleFunc("GET /v1/groups/{group}/offsets", func(w http.ResponseWriter, r *http.Request) {
		topic := r.URL.Query().Get("topic")
		f.mu.Lock()
		defer f.mu.Unlock()
		off, ok := f.offsets[r.PathValue("group")+"|"+topic]
		if !ok {
			off = -1
		}
		writeJSON(w, 200, map[string]any{"group": r.PathValue("group"), "topic": topic, "offset": off})
	})
	mux.HandleFunc("POST /v1/demo/nodes/{id}/{action}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.Atoi(r.PathValue("id"))
		act := r.PathValue("action")
		if err != nil || id < 1 || id > 3 || (act != "kill" && act != "restart") {
			writeErr(w, 404, "NOT_FOUND", "unknown node or action")
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		f.up[id] = act == "restart"
		f.eventLocked(id, "", "node "+map[string]string{"kill": "killed", "restart": "restarted"}[act])
		f.notifyLocked()
		writeJSON(w, 200, map[string]bool{"ok": true})
	})
}

func (f *fakeAPI) cluster(w http.ResponseWriter, _ *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	self := 1
	for id := 1; id <= 3; id++ {
		if f.up[id] {
			self = id
			break
		}
	}
	nodes := []map[string]any{}
	for id := 1; id <= 3; id++ {
		nodes = append(nodes, map[string]any{"id": id, "addr": fmt.Sprintf("127.0.0.1:%d", 7000+id), "up": f.up[id]})
	}
	names := make([]string, 0, len(f.groups))
	for n := range f.groups {
		names = append(names, n)
	}
	sort.Strings(names)
	groups := []map[string]any{}
	for _, n := range names {
		g := f.groups[n]
		hw := int64(len(g.msgs)) // every acknowledged message is committed in the fake
		row := func(id int) map[string]any {
			role := "follower"
			if id == g.leader {
				role = "leader"
			}
			m := map[string]any{"name": n, "leader": g.leader, "term": g.term, "role": role,
				"commit": min(hw, g.leo[id]), "high_watermark": min(hw, g.leo[id]), "log_end_offset": g.leo[id]}
			if f.perNode {
				m["node"] = id
			}
			return m
		}
		if f.perNode {
			for id := 1; id <= 3; id++ {
				groups = append(groups, row(id))
			}
		} else {
			groups = append(groups, row(self))
		}
	}
	resp := map[string]any{"node_id": self, "nodes": nodes, "groups": groups, "events": f.events}
	if f.demo {
		resp["demo"] = true
	}
	writeJSON(w, 200, resp)
}

func (f *fakeAPI) produce(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Messages []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"messages"`
		Acks       string `json:"acks"`
		ProducerID string `json:"producer_id"`
		Sequence   *int64 `json:"sequence"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Messages) == 0 {
		writeErr(w, 400, "BAD_REQUEST", "need at least one message")
		return
	}
	if req.Acks == "" {
		req.Acks = "all"
	}
	if req.Acks != "all" && req.Acks != "leader" {
		writeErr(w, 400, "BAD_REQUEST", `acks must be "all" or "leader"`)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	g, ok := f.groups[r.PathValue("topic")]
	if !ok {
		writeErr(w, 404, "NOT_FOUND", "unknown topic")
		return
	}
	if g.leader == 0 || f.upCount() < 2 {
		writeErr(w, 503, "UNAVAILABLE", "no leader with a quorum (election in progress?)")
		return
	}
	w.Header().Set("X-QL-Leader", strconv.Itoa(g.leader))
	if g.leader != 1 {
		w.Header().Set("X-QL-Forwarded", "true")
	}
	if req.ProducerID != "" && req.Sequence != nil {
		p := g.producers[req.ProducerID]
		switch seq := *req.Sequence; {
		case seq < p.next:
			writeJSON(w, 200, map[string]any{"base_offset": p.base, "last_offset": p.last, "duplicate": true})
			return
		case seq > p.next:
			writeErr(w, 409, "OUT_OF_ORDER_SEQUENCE", fmt.Sprintf("expected sequence %d, got %d", p.next, seq))
			return
		}
	}
	base := int64(len(g.msgs))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, m := range req.Messages {
		g.msgs = append(g.msgs, fakeMsg{Offset: int64(len(g.msgs)), Key: m.Key, Value: m.Value, Timestamp: now})
	}
	last := int64(len(g.msgs)) - 1
	g.leo[g.leader] = int64(len(g.msgs))
	for id := 1; id <= 3; id++ { // the up followers replicate synchronously
		if f.up[id] {
			g.leo[id] = int64(len(g.msgs))
		}
	}
	if req.ProducerID != "" && req.Sequence != nil {
		g.producers[req.ProducerID] = fakeProducer{next: *req.Sequence + int64(len(req.Messages)), base: base, last: last}
	}
	f.notifyLocked()
	writeJSON(w, 200, map[string]any{"base_offset": base, "last_offset": last, "duplicate": false})
}

func (f *fakeAPI) fetch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	offset, _ := strconv.ParseInt(q.Get("offset"), 10, 64)
	maxN, _ := strconv.Atoi(q.Get("max"))
	waitMS, _ := strconv.Atoi(q.Get("wait_ms"))
	if maxN <= 0 || maxN > 1000 {
		maxN = 100
	}
	if offset < 0 {
		writeErr(w, 400, "BAD_REQUEST", "negative offset")
		return
	}
	deadline := time.Now().Add(time.Duration(min(waitMS, 30000)) * time.Millisecond)
	for {
		f.mu.Lock()
		g, ok := f.groups[r.PathValue("topic")]
		if !ok {
			f.mu.Unlock()
			writeErr(w, 404, "NOT_FOUND", "unknown topic")
			return
		}
		hw := int64(len(g.msgs)) // every acknowledged message is committed in the fake
		if offset < hw || !time.Now().Before(deadline) {
			end := min(hw, offset+int64(maxN))
			msgs := []fakeMsg{}
			if offset < end {
				msgs = append(msgs, g.msgs[offset:end]...)
			}
			f.mu.Unlock()
			writeJSON(w, 200, map[string]any{"messages": msgs, "next_offset": max(offset, end), "high_watermark": hw})
			return
		}
		ch := f.changed
		f.mu.Unlock()
		select {
		case <-ch:
		case <-time.After(time.Until(deadline)):
		case <-r.Context().Done():
			return
		}
	}
}

func newFakeServerMux(f *fakeAPI) *http.ServeMux {
	mux := http.NewServeMux()
	f.routes(mux)
	mux.Handle("/", Handler())
	return mux
}

// TestDevServer serves the dashboard with the fake API for manual testing.
func TestDevServer(t *testing.T) {
	addr := os.Getenv("QL_DASHBOARD_DEV")
	if addr == "" {
		t.Skip("set QL_DASHBOARD_DEV=127.0.0.1:8099 (and -timeout 0) to run the dashboard dev server")
	}
	f := newFakeAPI(1500 * time.Millisecond)
	defer f.Close()
	f.perNode = os.Getenv("QL_FAKE_NO_PERNODE") == ""
	f.demo = os.Getenv("QL_FAKE_NO_DEMO") == ""
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("dashboard dev server on http://%s/ (fake API, perNode=%v demo=%v)", ln.Addr(), f.perNode, f.demo)
	t.Fatal(http.Serve(ln, newFakeServerMux(f)))
}

// TestFakeAPIFailoverFlow runs the guided-demo flow against the fake over
// real HTTP, which keeps the fake honest with respect to the contract the
// dashboard relies on.
func TestFakeAPIFailoverFlow(t *testing.T) {
	f := newFakeAPI(50 * time.Millisecond)
	defer f.Close()
	srv := httptest.NewServer(newFakeServerMux(f))
	defer srv.Close()

	call := func(method, path string, body any, out any) int {
		t.Helper()
		var rd *strings.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			rd = strings.NewReader(string(b))
		} else {
			rd = strings.NewReader("")
		}
		req, _ := http.NewRequestWithContext(context.Background(), method, srv.URL+path, rd)
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if out != nil {
			if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
				t.Fatalf("%s %s: %v", method, path, err)
			}
		}
		return resp.StatusCode
	}

	if code := call("POST", "/v1/topics", map[string]string{"name": "orders"}, nil); code != 201 {
		t.Fatalf("create topic: %d", code)
	}
	if code := call("POST", "/v1/topics", map[string]string{"name": "orders"}, nil); code != 200 {
		t.Fatalf("create topic again: %d", code)
	}
	produce := func(v string, seq int64) map[string]any {
		var out map[string]any
		code := call("POST", "/v1/topics/orders/messages", map[string]any{
			"messages": []map[string]string{{"value": v}}, "acks": "all", "producer_id": "p", "sequence": seq,
		}, &out)
		if code != 200 {
			t.Fatalf("produce %s: %d %v", v, code, out)
		}
		return out
	}
	produce("a", 0)
	if d := produce("a", 0); d["duplicate"] != true {
		t.Fatalf("retry not deduplicated: %v", d)
	}

	type cluster struct {
		Demo   bool
		Groups []struct {
			Name   string
			Node   int
			Leader int
			Term   int
			Role   string
		}
	}
	leader := func() (int, int) {
		var c cluster
		call("GET", "/v1/cluster", nil, &c)
		if !c.Demo {
			t.Fatal("demo flag missing")
		}
		for _, g := range c.Groups {
			if g.Name == "orders" && g.Role == "leader" {
				return g.Node, g.Term
			}
		}
		return 0, 0
	}
	old, oldTerm := leader()
	if old == 0 {
		t.Fatal("no leader")
	}
	call("POST", fmt.Sprintf("/v1/demo/nodes/%d/kill", old), nil, nil)
	deadline := time.Now().Add(5 * time.Second)
	for {
		l, term := leader()
		if l != 0 && l != old && term > oldTerm {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no new leader")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Long-poll: a fetch at the HW returns as soon as a message is produced.
	got := make(chan map[string]any, 1)
	go func() {
		var out map[string]any
		call("GET", "/v1/topics/orders/messages?offset=1&wait_ms=5000", nil, &out)
		got <- out
	}()
	time.Sleep(50 * time.Millisecond)
	produce("b", 1)
	select {
	case out := <-got:
		if msgs, _ := out["messages"].([]any); len(msgs) != 1 || out["next_offset"].(float64) != 2 {
			t.Fatalf("long-poll result %v", out)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("long-poll did not wake up")
	}

	call("POST", fmt.Sprintf("/v1/demo/nodes/%d/restart", old), nil, nil)
	var all struct{ Messages []fakeMsg }
	call("GET", "/v1/topics/orders/messages?offset=0&max=1000&wait_ms=0", nil, &all)
	if len(all.Messages) != 2 || all.Messages[0].Value != "a" || all.Messages[1].Value != "b" {
		t.Fatalf("messages %v", all.Messages)
	}

	var e map[string]any
	if code := call("POST", "/v1/topics/nope/messages", map[string]any{"messages": []map[string]string{{"value": "x"}}}, &e); code != 404 || e["code"] != "NOT_FOUND" {
		t.Fatalf("unknown topic: %d %v", code, e)
	}
	var off map[string]any
	call("GET", "/v1/groups/g/offsets?topic=orders", nil, &off)
	if off["offset"].(float64) != -1 {
		t.Fatalf("offset %v", off)
	}
	call("POST", "/v1/groups/g/offsets", map[string]any{"topic": "orders", "offset": 2}, nil)
	call("GET", "/v1/groups/g/offsets?topic=orders", nil, &off)
	if off["offset"].(float64) != 2 {
		t.Fatalf("offset %v", off)
	}
}
