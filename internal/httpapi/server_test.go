package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/Rahul-Nikhate/quorumlog/internal/cluster"
	"github.com/Rahul-Nikhate/quorumlog/internal/httpapi"
	"github.com/Rahul-Nikhate/quorumlog/internal/log"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
)

func TestMain(m *testing.M) {
	code := m.Run()
	client.CloseIdleConnections()
	http.DefaultClient.CloseIdleConnections()
	http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	if code == 0 {
		if err := goleak.Find(); err != nil {
			fmt.Fprintln(os.Stderr, "goleak:", err)
			code = 1
		}
	}
	os.Exit(code)
}

func startCluster(t *testing.T, events *cluster.Events) []*httpapi.LocalNode {
	t.Helper()
	nodes, err := httpapi.NewLocalCluster(3, t.TempDir(), cluster.Config{
		Sync: log.SyncPolicy{Mode: log.SyncNever}, TickInterval: 10 * time.Millisecond, Events: events,
	}, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range nodes {
		if err := l.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = l.Stop() })
	}
	return nodes
}

type resp struct {
	status int
	hdr    http.Header
	body   []byte
}

func (r resp) decode(t *testing.T, v any) {
	t.Helper()
	if err := json.Unmarshal(r.body, v); err != nil {
		t.Fatalf("decode %s: %v", r.body, err)
	}
}

func (r resp) code(t *testing.T) string {
	t.Helper()
	var e httpapi.ErrorBody
	r.decode(t, &e)
	return e.Code
}

var client = &http.Client{Timeout: 30 * time.Second}

func do(t *testing.T, method, url string, body any, hdr ...string) resp {
	t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	default:
		buf, _ := json.Marshal(b)
		rd = bytes.NewReader(buf)
	}
	req, _ := http.NewRequest(method, url, rd)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return resp{res.StatusCode, res.Header, b}
}

// until retries a request while it returns a retriable 503/504.
func until(t *testing.T, method, url string, body any, hdr ...string) resp {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		r := do(t, method, url, body, hdr...)
		if (r.status != http.StatusServiceUnavailable && r.status != http.StatusGatewayTimeout) || time.Now().After(deadline) {
			return r
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func base(l *httpapi.LocalNode) string { return "http://" + l.HTTPAddr() }

// leaderOf waits until node 1 knows the leader of group and returns it.
func leaderOf(t *testing.T, nodes []*httpapi.LocalNode, group string) *httpapi.LocalNode {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, l := range nodes {
			if n := l.Node(); n != nil && n.Leader(group) == n.ID() {
				return l
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no leader for %s", group)
	return nil
}

func TestAPIContract(t *testing.T) {
	nodes := startCluster(t, nil)
	n1 := base(nodes[0])

	var h map[string]any
	do(t, "GET", n1+"/v1/health", nil).decode(t, &h)
	if h["ok"] != true || h["node_id"] != float64(1) {
		t.Fatalf("health: %v", h)
	}

	// Topics: 201 then 200; validation.
	if r := until(t, "POST", n1+"/v1/topics", map[string]string{"name": "orders"}); r.status != http.StatusCreated {
		t.Fatalf("create: %d %s", r.status, r.body)
	}
	if r := until(t, "POST", base(nodes[2])+"/v1/topics", map[string]string{"name": "orders"}); r.status != http.StatusOK {
		t.Fatalf("create again: %d %s", r.status, r.body)
	}
	if r := do(t, "POST", n1+"/v1/topics", map[string]string{"name": "__x"}); r.status != 400 && r.status != 503 {
		t.Fatalf("bad topic name: %d %s", r.status, r.body)
	}
	var tl struct{ Topics []string }
	until(t, "GET", base(nodes[1])+"/v1/topics", nil).decode(t, &tl)
	if len(tl.Topics) != 1 || tl.Topics[0] != "orders" {
		t.Fatalf("topics: %+v", tl)
	}

	// Produce through a follower: forwarded once, headers set.
	leader := leaderOf(t, nodes, "orders")
	var follower *httpapi.LocalNode
	for _, l := range nodes {
		if l != leader {
			follower = l
			break
		}
	}
	key := "k1"
	body := map[string]any{"messages": []map[string]any{{"key": key, "value": "v1"}, {"value": "v2"}}, "acks": "all"}
	r := until(t, "POST", base(follower)+"/v1/topics/orders/messages", body)
	if r.status != 200 || r.hdr.Get("X-QL-Forwarded") != "true" || r.hdr.Get("X-QL-Leader") != strconv.Itoa(int(leader.ID())) {
		t.Fatalf("forwarded produce: %d %v %s", r.status, r.hdr, r.body)
	}
	if v := r.hdr.Values("X-QL-Leader"); len(v) != 1 {
		t.Fatalf("X-QL-Leader appears %d times: %q", len(v), v)
	}
	if v := r.hdr.Values("X-QL-Forwarded"); len(v) != 1 {
		t.Fatalf("X-QL-Forwarded appears %d times: %q", len(v), v)
	}
	var pr httpapi.ProduceResponse
	r.decode(t, &pr)
	if pr.LastOffset != pr.BaseOffset+1 || pr.Duplicate {
		t.Fatalf("produce result %+v", pr)
	}
	// A request that was already forwarded is never forwarded again.
	r = do(t, "POST", base(follower)+"/v1/topics/orders/messages", body, "X-QL-Forwarded", "true")
	if r.status != 503 || r.code(t) != "NOT_LEADER" || r.hdr.Get("X-QL-Leader") != strconv.Itoa(int(leader.ID())) {
		t.Fatalf("double forward: %d %v %s", r.status, r.hdr, r.body)
	}

	// Fetch from offset 0: the election marker is skipped.
	var fr httpapi.FetchResponse
	r = until(t, "GET", base(follower)+"/v1/topics/orders/messages?offset=0", nil)
	if v := r.hdr.Values("X-QL-Leader"); len(v) != 1 || r.hdr.Get("X-QL-Forwarded") != "true" {
		t.Fatalf("forwarded fetch headers: %v", r.hdr)
	}
	r.decode(t, &fr)
	if len(fr.Messages) != 2 || fr.Messages[0].Value != "v1" || fr.Messages[0].Key == nil || *fr.Messages[0].Key != key ||
		fr.Messages[1].Key != nil || fr.Messages[0].Offset != pr.BaseOffset || fr.NextOffset != pr.LastOffset+1 ||
		fr.HighWatermark != pr.LastOffset+1 || fr.Messages[0].Timestamp.IsZero() {
		t.Fatalf("fetch: %+v (produce %+v)", fr, pr)
	}

	// Long poll at the HW is woken by a produce.
	got := make(chan httpapi.FetchResponse, 1)
	go func() {
		var fr httpapi.FetchResponse
		res, err := client.Get(fmt.Sprintf("%s/v1/topics/orders/messages?offset=%d&wait_ms=5000", base(leader), fr2(pr)))
		if err == nil {
			_ = json.NewDecoder(res.Body).Decode(&fr)
			res.Body.Close()
		}
		got <- fr
	}()
	time.Sleep(50 * time.Millisecond)
	until(t, "POST", base(leader)+"/v1/topics/orders/messages", map[string]any{"messages": []map[string]string{{"value": "v3"}}, "acks": "leader"})
	select {
	case fr := <-got:
		if len(fr.Messages) != 1 || fr.Messages[0].Value != "v3" {
			t.Fatalf("long poll: %+v", fr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("long poll not woken")
	}

	// Idempotent producer.
	idem := map[string]any{"messages": []map[string]string{{"value": "i0"}}, "producer_id": "p-1", "sequence": 0}
	var first, again httpapi.ProduceResponse
	until(t, "POST", n1+"/v1/topics/orders/messages", idem).decode(t, &first)
	until(t, "POST", n1+"/v1/topics/orders/messages", idem).decode(t, &again)
	if !again.Duplicate || again.BaseOffset != first.BaseOffset {
		t.Fatalf("duplicate: %+v vs %+v", again, first)
	}
	idem["sequence"] = 5
	if r := until(t, "POST", n1+"/v1/topics/orders/messages", idem); r.status != 409 || r.code(t) != "OUT_OF_ORDER_SEQUENCE" {
		t.Fatalf("out of order: %d %s", r.status, r.body)
	}

	// Errors.
	for _, c := range []struct {
		method, path string
		body         any
		status       int
		code         string
	}{
		{"POST", "/v1/topics/orders/messages", "{not json", 400, "BAD_REQUEST"},
		{"POST", "/v1/topics/orders/messages", map[string]any{"messages": []any{}}, 400, "BAD_REQUEST"},
		{"POST", "/v1/topics/orders/messages", map[string]any{"messages": []map[string]string{{"value": "x"}}, "acks": "none"}, 400, "BAD_REQUEST"},
		{"POST", "/v1/topics/orders/messages", map[string]any{"messages": []map[string]string{{"value": "x"}}, "producer_id": "p"}, 400, "BAD_REQUEST"},
		{"POST", "/v1/topics/orders/messages", `{"messages":[{"value":"` + strings.Repeat("x", 2<<20) + `"}]}`, 413, "BAD_REQUEST"},
		{"POST", "/v1/topics/nope/messages", map[string]any{"messages": []map[string]string{{"value": "x"}}}, 404, "NOT_FOUND"},
		{"GET", "/v1/topics/nope/messages", nil, 404, "NOT_FOUND"},
		{"GET", "/v1/topics/orders/messages?offset=999999", nil, 400, "BAD_REQUEST"},
		{"GET", "/v1/topics/orders/messages?offset=-1", nil, 400, "BAD_REQUEST"},
		{"GET", "/v1/topics/orders/messages?max=0", nil, 400, "BAD_REQUEST"},
		{"GET", "/v1/topics/orders/messages?wait_ms=99999999", nil, 400, "BAD_REQUEST"},
		{"GET", "/v1/groups/g/offsets", nil, 400, "BAD_REQUEST"},
		{"POST", "/v1/groups/g/offsets", map[string]any{"topic": "orders"}, 400, "BAD_REQUEST"},
		{"POST", "/v1/demo/nodes/1/kill", nil, 404, "NOT_FOUND"},
		{"GET", "/v1/nothing", nil, 404, "NOT_FOUND"},
	} {
		for _, l := range nodes { // same answer from leader and followers
			r := until(t, c.method, base(l)+c.path, c.body)
			if r.status != c.status || r.code(t) != c.code {
				t.Fatalf("%s %s via node %d: %d %s, want %d %s", c.method, c.path, l.ID(), r.status, r.body, c.status, c.code)
			}
		}
	}

	// Consumer group offsets.
	var off struct {
		Group, Topic string
		Offset       int64
	}
	until(t, "GET", base(follower)+"/v1/groups/g1/offsets?topic=orders", nil).decode(t, &off)
	if off.Offset != -1 || off.Group != "g1" || off.Topic != "orders" {
		t.Fatalf("offset before commit: %+v", off)
	}
	if r := until(t, "POST", base(follower)+"/v1/groups/g1/offsets", map[string]any{"topic": "orders", "offset": 3}); r.status != 200 {
		t.Fatalf("commit: %d %s", r.status, r.body)
	}
	until(t, "GET", n1+"/v1/groups/g1/offsets?topic=orders", nil).decode(t, &off)
	if off.Offset != 3 {
		t.Fatalf("offset after commit: %+v", off)
	}

	// Cluster view and metrics.
	var cv httpapi.ClusterView
	do(t, "GET", n1+"/v1/cluster", nil).decode(t, &cv)
	if cv.NodeID != 1 || len(cv.Nodes) != 3 || len(cv.Groups) != 3 || cv.Groups[2].Name != "orders" || cv.Demo {
		t.Fatalf("cluster: %+v", cv)
	}
	for _, g := range cv.Groups {
		if g.HighWatermark > g.Commit+1 || g.Node != 0 {
			t.Fatalf("group row %+v", g)
		}
	}
	m := do(t, "GET", base(leader)+"/metrics", nil)
	if !strings.Contains(string(m.body), "quorumlog_produce_messages_total") || !strings.Contains(string(m.body), "quorumlog_elections_won_total") {
		t.Fatalf("metrics: %s", m.body)
	}
	m = do(t, "GET", base(follower)+"/metrics", nil)
	if strings.Contains(string(m.body), "quorumlog_forwarded_requests_total{node=\""+strconv.Itoa(int(follower.ID()))+"\"} 0\n") {
		t.Fatalf("follower forwarded nothing? %s", m.body)
	}
}

func fr2(pr httpapi.ProduceResponse) int64 { return pr.LastOffset + 1 }

// A client that gives up on a long poll ends it on the server (no stuck
// handler; goleak at exit checks for leftovers).
func TestLongPollClientCancel(t *testing.T) {
	nodes := startCluster(t, nil)
	until(t, "POST", base(nodes[0])+"/v1/topics", map[string]string{"name": "t"})
	l := leaderOf(t, nodes, "t")
	var fr httpapi.FetchResponse
	until(t, "GET", base(l)+"/v1/topics/t/messages", nil).decode(t, &fr)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/v1/topics/t/messages?offset=%d&wait_ms=30000", base(nodes[0]), fr.NextOffset), nil)
	start := time.Now()
	if _, err := http.DefaultClient.Do(req); err == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("cancelled long poll: %v after %v", err, time.Since(start))
	}
}

// Demo front: aggregated cluster view, kill and restart through the API,
// requests keep working through the surviving nodes.
func TestDemoKillRestart(t *testing.T) {
	events := cluster.NewEvents(500)
	nodes := startCluster(t, events)
	front := httptest.NewServer(httpapi.NewDemo(nodes, events, http.NotFoundHandler()))
	defer front.Close()
	u := front.URL

	if r := until(t, "POST", u+"/v1/topics", map[string]string{"name": "demo"}); r.status != 201 {
		t.Fatalf("create: %d %s", r.status, r.body)
	}
	l := leaderOf(t, nodes, "demo")
	until(t, "POST", u+"/v1/topics/demo/messages", map[string]any{"messages": []map[string]string{{"value": "before"}}})

	var cv httpapi.ClusterView
	do(t, "GET", u+"/v1/cluster", nil).decode(t, &cv)
	if !cv.Demo || len(cv.Nodes) != 3 || len(cv.Groups) != 9 || cv.Groups[0].Node == 0 {
		t.Fatalf("demo cluster view: %+v", cv)
	}
	if r := do(t, "POST", fmt.Sprintf("%s/v1/demo/nodes/%d/kill", u, l.ID()), nil); r.status != 200 {
		t.Fatalf("kill: %d %s", r.status, r.body)
	}
	if r := do(t, "POST", u+"/v1/demo/nodes/9/kill", nil); r.status != 404 {
		t.Fatalf("kill unknown: %d", r.status)
	}
	do(t, "GET", u+"/v1/cluster", nil).decode(t, &cv)
	for _, n := range cv.Nodes {
		if n.Up != (n.ID != l.ID()) {
			t.Fatalf("node %d up=%v after killing %d", n.ID, n.Up, l.ID())
		}
	}
	if r := until(t, "POST", u+"/v1/topics/demo/messages", map[string]any{"messages": []map[string]string{{"value": "after"}}}); r.status != 200 {
		t.Fatalf("produce after kill: %d %s", r.status, r.body)
	}
	if r := do(t, "POST", fmt.Sprintf("%s/v1/demo/nodes/%d/restart", u, l.ID()), nil); r.status != 200 {
		t.Fatalf("restart: %d %s", r.status, r.body)
	}
	var fr httpapi.FetchResponse
	until(t, "GET", u+"/v1/topics/demo/messages?offset=0", nil).decode(t, &fr)
	if len(fr.Messages) != 2 || fr.Messages[0].Value != "before" || fr.Messages[1].Value != "after" {
		t.Fatalf("messages: %+v", fr)
	}
	var sawKill, sawRestart bool
	for _, ev := range events.List() {
		sawKill = sawKill || strings.HasPrefix(ev.Msg, "killed")
		sawRestart = sawRestart || strings.HasPrefix(ev.Msg, "restarted")
	}
	if !sawKill || !sawRestart {
		t.Fatalf("events: %+v", events.List())
	}
	// The restarted node catches up.
	deadline := time.Now().Add(10 * time.Second)
	for {
		n := l.Node()
		p, err := n.Group("demo")
		if err == nil && p.Status().HighWatermark >= fr.HighWatermark+1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted node did not catch up: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A forwarded request can reach a node that has just lost leadership. Its
// NOT_LEADER answer names the new leader in X-QL-Leader; the forwarding node
// must pass that hint through, not replace it with the node it forwarded to.
func TestForwardKeepsUpstreamLeaderHint(t *testing.T) {
	nodes := startCluster(t, nil)
	if r := until(t, "POST", base(nodes[0])+"/v1/topics", map[string]string{"name": "orders"}); r.status/100 != 2 {
		t.Fatalf("create: %d %s", r.status, r.body)
	}
	leader := leaderOf(t, nodes, "orders")
	var follower, other *httpapi.LocalNode
	for _, l := range nodes {
		switch {
		case l == leader:
		case follower == nil:
			follower = l
		default:
			other = l
		}
	}
	for deadline := time.Now().Add(10 * time.Second); follower.Node().Leader("orders") != leader.ID(); {
		if time.Now().After(deadline) {
			t.Fatalf("node %d does not know leader %d", follower.ID(), leader.ID())
		}
		time.Sleep(5 * time.Millisecond)
	}
	hint := strconv.Itoa(int(other.ID()))
	// A stand-in for the old leader that has just stepped down.
	stale := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-QL-Leader", hint)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"error":"not the leader","code":"NOT_LEADER"}`)
	}))
	defer stale.Close()
	api := httpapi.New(httpapi.Config{Node: follower.Node(), HTTPPeers: map[raft.NodeID]string{
		leader.ID(): strings.TrimPrefix(stale.URL, "http://"),
	}})
	defer api.Close()
	req := httptest.NewRequest("POST", "/v1/topics/orders/messages", strings.NewReader(`{"messages":[{"value":"v"}]}`))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("X-QL-Forwarded") != "true" {
		t.Fatalf("forwarded to a stale leader: %d %v %s", rec.Code, rec.Header(), rec.Body)
	}
	if v := rec.Header().Values("X-QL-Leader"); len(v) != 1 || v[0] != hint {
		t.Fatalf("X-QL-Leader = %q, want the upstream hint %q", v, hint)
	}
}
