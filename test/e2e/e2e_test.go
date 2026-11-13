// Package e2e runs three real nodes in-process — real TCP between them, real
// HTTP to them, real files in temp dirs — and checks the promises QuorumLog
// makes across a leader failure: no acknowledged message is lost, offsets
// are the same on every node, idempotent producers never duplicate, and
// consumer-group offsets survive failover.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/Rahul-Nikhate/quorumlog/internal/cluster"
	"github.com/Rahul-Nikhate/quorumlog/internal/httpapi"
	"github.com/Rahul-Nikhate/quorumlog/internal/log"
	"github.com/Rahul-Nikhate/quorumlog/internal/partition"
)

var httpClient = &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 32}}

func TestMain(m *testing.M) {
	code := m.Run()
	httpClient.CloseIdleConnections()
	if code == 0 {
		if err := goleak.Find(); err != nil {
			fmt.Fprintln(os.Stderr, "goleak:", err)
			code = 1
		}
	}
	os.Exit(code)
}

type env struct {
	t     *testing.T
	nodes []*httpapi.LocalNode
	rr    atomic.Uint64
}

func start(t *testing.T) *env {
	t.Helper()
	nodes, err := httpapi.NewLocalCluster(3, t.TempDir(), cluster.Config{
		Sync: log.SyncPolicy{Mode: log.SyncAlways}, TickInterval: 20 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		if err := n.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = n.Stop() })
	}
	return &env{t: t, nodes: nodes}
}

type apiErr struct {
	status int
	code   string
	msg    string
}

func (e *apiErr) Error() string { return fmt.Sprintf("%d %s %s", e.status, e.code, e.msg) }

// call sends one request to node i.
func (e *env) call(ctx context.Context, i int, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, _ := json.Marshal(in)
		body = bytes.NewReader(b)
	}
	req, _ := http.NewRequestWithContext(ctx, method, "http://"+e.nodes[i].HTTPAddr()+path, body)
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		var eb httpapi.ErrorBody
		_ = json.Unmarshal(data, &eb)
		return &apiErr{resp.StatusCode, eb.Code, eb.Error}
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

// retry sends the request to any node (round robin) until it succeeds or
// fails with a non-retriable API error. Network errors (node down), 503 and
// 504 are retried — exactly what a client does during a failover.
func (e *env) retry(ctx context.Context, method, path string, in, out any) error {
	for {
		err := e.call(ctx, int(e.rr.Add(1)%3), method, path, in, out)
		var ae *apiErr
		if err == nil || (errors.As(err, &ae) && ae.status != 503 && ae.status != 504) {
			return err
		}
		if ctx.Err() != nil {
			return fmt.Errorf("gave up: %w (last: %v)", ctx.Err(), err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// leaderNode returns the index of the running node that leads group.
func (e *env) leaderNode(group string) int {
	e.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for i, l := range e.nodes {
			if n := l.Node(); n != nil && n.Leader(group) == n.ID() {
				return i
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	e.t.Fatalf("no leader for %s", group)
	return -1
}

type produceBody struct {
	Messages   []map[string]string `json:"messages"`
	Acks       string              `json:"acks"`
	ProducerID string              `json:"producer_id"`
	Sequence   int32               `json:"sequence"`
}

func TestFailoverNoLossNoDuplicates(t *testing.T) {
	e := start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := e.retry(ctx, "POST", "/v1/topics", map[string]string{"name": "orders"}, nil); err != nil {
		t.Fatal(err)
	}

	const producers, perProducer = 4, 60
	var mu sync.Mutex
	acked := map[string]int64{} // value → acknowledged offset
	var nacked, dups atomic.Int64
	var wg sync.WaitGroup
	errc := make(chan error, producers)
	for p := range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pid := fmt.Sprintf("producer-%d", p)
			for seq := range perProducer {
				v := fmt.Sprintf("%s/%d", pid, seq)
				body := produceBody{Messages: []map[string]string{{"value": v}}, Acks: "all", ProducerID: pid, Sequence: int32(seq)}
				var res httpapi.ProduceResponse
				if err := e.retry(ctx, "POST", "/v1/topics/orders/messages", body, &res); err != nil {
					errc <- fmt.Errorf("%s: %w", v, err)
					return
				}
				mu.Lock()
				if old, ok := acked[v]; ok && old != res.BaseOffset {
					errc <- fmt.Errorf("%s acked twice at different offsets %d and %d", v, old, res.BaseOffset)
				}
				acked[v] = res.BaseOffset
				mu.Unlock()
				nacked.Add(1)
				if res.Duplicate {
					dups.Add(1) // a retry whose first attempt had been written
				}
			}
		}()
	}
	waitAcked := func(n int64) {
		for nacked.Load() < n && ctx.Err() == nil {
			time.Sleep(5 * time.Millisecond)
		}
	}

	// Kill the topic's leader mid-stream, keep producing, restart it.
	waitAcked(40)
	victim := e.leaderNode("orders")
	t.Logf("killing node %d (leader of orders) after %d acks", e.nodes[victim].ID(), nacked.Load())
	if err := e.nodes[victim].Stop(); err != nil {
		t.Fatal(err)
	}
	before := nacked.Load()
	waitAcked(before + 40)
	t.Logf("restarting node %d after %d acks", e.nodes[victim].ID(), nacked.Load())
	if err := e.nodes[victim].Start(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}
	t.Logf("%d messages acknowledged; %d retries were answered as duplicates (deduplicated, not re-appended)", nacked.Load(), dups.Load())

	// Consume everything (through any node; reads go to the leader).
	var hw int64
	seen := map[string]int64{}
	var log []partition.Message
	for off := int64(0); ; {
		var fr httpapi.FetchResponse
		if err := e.retry(ctx, "GET", fmt.Sprintf("/v1/topics/orders/messages?offset=%d&max=50", off), nil, &fr); err != nil {
			t.Fatal(err)
		}
		for _, m := range fr.Messages {
			if prev, dup := seen[m.Value]; dup {
				t.Fatalf("duplicate message %q at offsets %d and %d", m.Value, prev, m.Offset)
			}
			seen[m.Value] = m.Offset
			log = append(log, partition.Message{Offset: m.Offset, Value: []byte(m.Value)})
		}
		off, hw = fr.NextOffset, fr.HighWatermark
		if off >= hw {
			break
		}
	}
	if len(acked) != producers*perProducer {
		t.Fatalf("%d acked values, want %d", len(acked), producers*perProducer)
	}
	for v, off := range acked {
		got, ok := seen[v]
		if !ok {
			t.Fatalf("acknowledged message %q (offset %d) lost", v, off)
		}
		if got != off {
			t.Fatalf("message %q acknowledged at offset %d but stored at %d", v, off, got)
		}
	}
	if len(seen) != len(acked) {
		t.Fatalf("log has %d messages, %d acknowledged (unexpected extra messages)", len(seen), len(acked))
	}
	// Per producer, sequence order = offset order.
	for p := range producers {
		last := int64(-1)
		for seq := range perProducer {
			off := seen[fmt.Sprintf("producer-%d/%d", p, seq)]
			if off <= last {
				t.Fatalf("producer %d seq %d at offset %d, not after %d", p, seq, off, last)
			}
			last = off
		}
	}

	// Every node ends up with the identical log (same offsets, same values).
	for _, l := range e.nodes {
		n := l.Node()
		deadline := time.Now().Add(15 * time.Second)
		for {
			p, err := n.Group("orders")
			if err == nil && partition.APIOffset(p.HighWatermark()) >= hw {
				var local []partition.Message
				for off := int64(0); off < hw; {
					rr, err := p.ReadMessages(ctx, partition.ReadRequest{Offset: off, Max: 1000, Consistency: partition.ConsistencyLocal})
					if err != nil {
						t.Fatal(err)
					}
					for _, m := range rr.Messages {
						if m.Offset < hw {
							local = append(local, partition.Message{Offset: m.Offset, Value: m.Value})
						}
					}
					off = rr.NextOffset
				}
				if len(local) != len(log) {
					t.Fatalf("node %d has %d messages below HW %d, leader view %d", n.ID(), len(local), hw, len(log))
				}
				for i := range local {
					if local[i].Offset != log[i].Offset || !bytes.Equal(local[i].Value, log[i].Value) {
						t.Fatalf("node %d differs at %d: %d/%s vs %d/%s", n.ID(), i, local[i].Offset, local[i].Value, log[i].Offset, log[i].Value)
					}
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d did not catch up to HW %d: %v", n.ID(), hw, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// Consumer-group offsets are committed through Raft (__offsets) and survive
// the failure of that group's leader.
func TestConsumerOffsetsSurviveFailover(t *testing.T) {
	e := start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	commit := func(off int64) {
		if err := e.retry(ctx, "POST", "/v1/groups/billing/offsets", map[string]any{"topic": "orders", "offset": off}, nil); err != nil {
			t.Fatal(err)
		}
	}
	fetch := func() int64 {
		var r struct{ Offset int64 }
		if err := e.retry(ctx, "GET", "/v1/groups/billing/offsets?topic=orders", nil, &r); err != nil {
			t.Fatal(err)
		}
		return r.Offset
	}
	if got := fetch(); got != -1 {
		t.Fatalf("initial offset %d, want -1", got)
	}
	commit(17)
	victim := e.leaderNode(cluster.OffsetsGroup)
	if err := e.nodes[victim].Stop(); err != nil {
		t.Fatal(err)
	}
	if got := fetch(); got != 17 {
		t.Fatalf("after failover: %d, want 17", got)
	}
	commit(18)
	if err := e.nodes[victim].Start(); err != nil {
		t.Fatal(err)
	}
	// Kill another leader (possibly the restarted node's group peer) once
	// more; the value must still be the last committed one.
	victim2 := e.leaderNode(cluster.OffsetsGroup)
	if err := e.nodes[victim2].Stop(); err != nil {
		t.Fatal(err)
	}
	if got := fetch(); got != 18 {
		t.Fatalf("after second failover: %d, want 18", got)
	}
}
