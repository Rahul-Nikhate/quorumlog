// Command qlog is a small command-line client for the QuorumLog HTTP API.
//
//	qlog [--addr http://localhost:8080] topics create NAME
//	qlog topics list
//	qlog produce TOPIC [--value V | stdin lines] [--key K] [--acks all|leader] [--producer-id P --sequence N]
//	qlog consume TOPIC [--from N] [--follow] [--group G] [--max N]
//	qlog status
//	qlog bench TOPIC [-n 10000] [-c 16] [--size 100] [--acks all|leader] [--idempotent]
//
// Requests that fail with a retriable error (NOT_LEADER, UNAVAILABLE,
// TIMEOUT, or a network error) are retried with backoff; produce retries
// reuse the same producer id and sequence, so with --producer-id a retry can
// never duplicate a message.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "qlog:", err)
		os.Exit(1)
	}
}

const usage = `usage: qlog [--addr URL] <command> [args]

commands:
  topics create NAME
  topics list
  produce TOPIC [--value V] [--key K] [--acks all|leader] [--producer-id P] [--sequence N]
                (without --value: one message per stdin line)
  consume TOPIC [--from N] [--follow] [--group G] [--max N]
  status
  bench TOPIC [-n N] [-c CONCURRENCY] [--size BYTES] [--acks all|leader] [--idempotent]
`

func run(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer) error {
	gf := flag.NewFlagSet("qlog", flag.ContinueOnError)
	addr := gf.String("addr", envOr("QLOG_ADDR", "http://localhost:8080"), "server URL (env QLOG_ADDR)")
	gf.Usage = func() { fmt.Fprint(gf.Output(), usage); gf.PrintDefaults() }
	if err := gf.Parse(args); err != nil {
		return err
	}
	c := &client{base: strings.TrimRight(*addr, "/"), http: &http.Client{Timeout: 60 * time.Second}}
	rest := gf.Args()
	if len(rest) == 0 {
		gf.Usage()
		return errors.New("missing command")
	}
	switch cmd, rest := rest[0], rest[1:]; cmd {
	case "topics":
		return cmdTopics(ctx, c, rest, stdout)
	case "produce":
		return cmdProduce(ctx, c, rest, stdin, stdout)
	case "consume":
		return cmdConsume(ctx, c, rest, stdout)
	case "status":
		return cmdStatus(ctx, c, stdout)
	case "bench":
		return cmdBench(ctx, c, rest, stdout)
	default:
		gf.Usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// parse parses flags that may be interleaved with positional arguments.
func parse(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if fs.NArg() == 0 {
			return pos, nil
		}
		pos = append(pos, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

// ---------------------------------------------------------------- client

type client struct {
	base string
	http *http.Client
}

// apiError is a non-2xx response.
type apiError struct {
	Status int
	Code   string `json:"code"`
	Msg    string `json:"error"`
}

func (e *apiError) Error() string { return fmt.Sprintf("%d %s: %s", e.Status, e.Code, e.Msg) }

// retriable: transient cluster states (leader election, failover) and
// network errors.
func retriable(err error) bool {
	var ae *apiError
	if errors.As(err, &ae) {
		return ae.Status == http.StatusServiceUnavailable || ae.Status == http.StatusGatewayTimeout || ae.Status == http.StatusBadGateway
	}
	return err != nil && !errors.Is(err, context.Canceled)
}

// do sends one request; out (if non-nil) receives the JSON response.
func (c *client) do(ctx context.Context, method, path string, in, out any) (int, error) {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return 0, err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return resp.StatusCode, err
	}
	if resp.StatusCode/100 != 2 {
		ae := &apiError{Status: resp.StatusCode}
		if json.Unmarshal(data, ae) != nil || ae.Code == "" {
			ae.Msg = strings.TrimSpace(string(data))
		}
		return resp.StatusCode, ae
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return resp.StatusCode, fmt.Errorf("decode response: %w", err)
		}
	}
	return resp.StatusCode, nil
}

// retry calls do until success, a non-retriable error, or ~30s of retries.
func (c *client) retry(ctx context.Context, method, path string, in, out any) (int, error) {
	backoff := 50 * time.Millisecond
	deadline := time.Now().Add(30 * time.Second)
	for {
		status, err := c.do(ctx, method, path, in, out)
		if err == nil || !retriable(err) || time.Now().After(deadline) || ctx.Err() != nil {
			return status, err
		}
		select {
		case <-ctx.Done():
			return status, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, time.Second)
	}
}

// ---------------------------------------------------------------- commands

func cmdTopics(ctx context.Context, c *client, args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: qlog topics create NAME | topics list")
	}
	switch args[0] {
	case "create":
		if len(args) != 2 {
			return errors.New("usage: qlog topics create NAME")
		}
		status, err := c.retry(ctx, "POST", "/v1/topics", map[string]string{"name": args[1]}, nil)
		if err != nil {
			return err
		}
		if status == http.StatusCreated {
			fmt.Fprintf(out, "created topic %s\n", args[1])
		} else {
			fmt.Fprintf(out, "topic %s already exists\n", args[1])
		}
		return nil
	case "list":
		var r struct{ Topics []string }
		if _, err := c.retry(ctx, "GET", "/v1/topics", nil, &r); err != nil {
			return err
		}
		for _, t := range r.Topics {
			fmt.Fprintln(out, t)
		}
		return nil
	}
	return fmt.Errorf("unknown topics subcommand %q", args[0])
}

type wireMessage struct {
	Key   *string `json:"key,omitempty"`
	Value string  `json:"value"`
}

type produceReq struct {
	Messages   []wireMessage `json:"messages"`
	Acks       string        `json:"acks,omitempty"`
	ProducerID string        `json:"producer_id,omitempty"`
	Sequence   *int32        `json:"sequence,omitempty"`
}

type produceResp struct {
	BaseOffset int64 `json:"base_offset"`
	LastOffset int64 `json:"last_offset"`
	Duplicate  bool  `json:"duplicate"`
}

func cmdProduce(ctx context.Context, c *client, args []string, stdin io.Reader, out io.Writer) error {
	fs := flag.NewFlagSet("produce", flag.ContinueOnError)
	value := fs.String("value", "", "message value (default: one message per stdin line)")
	key := fs.String("key", "", "message key")
	acks := fs.String("acks", "all", "all | leader")
	pid := fs.String("producer-id", "", "idempotent producer id (dedups retries)")
	seq := fs.Int("sequence", 0, "first sequence number (with --producer-id; +1 per message)")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return errors.New("usage: qlog produce TOPIC [--value V] ...")
	}
	path := "/v1/topics/" + url.PathEscape(pos[0]) + "/messages"
	next := int32(*seq)
	send := func(v string) error {
		req := produceReq{Messages: []wireMessage{{Value: v}}, Acks: *acks, ProducerID: *pid}
		if *key != "" {
			req.Messages[0].Key = key
		}
		if *pid != "" {
			s := next
			req.Sequence = &s
		}
		var r produceResp
		if _, err := c.retry(ctx, "POST", path, req, &r); err != nil {
			return err
		}
		next++
		dup := ""
		if r.Duplicate {
			dup = " (duplicate: already written)"
		}
		fmt.Fprintf(out, "offset %d%s\n", r.BaseOffset, dup)
		return nil
	}
	if flagSet(fs, "value") {
		return send(*value)
	}
	sc := bufio.NewScanner(stdin)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		if err := send(sc.Text()); err != nil {
			return err
		}
	}
	return sc.Err()
}

func flagSet(fs *flag.FlagSet, name string) bool {
	set := false
	fs.Visit(func(f *flag.Flag) { set = set || f.Name == name })
	return set
}

type fetchResp struct {
	Messages []struct {
		Offset    int64     `json:"offset"`
		Key       *string   `json:"key"`
		Value     string    `json:"value"`
		Timestamp time.Time `json:"timestamp"`
	} `json:"messages"`
	NextOffset    int64 `json:"next_offset"`
	HighWatermark int64 `json:"high_watermark"`
}

func cmdConsume(ctx context.Context, c *client, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("consume", flag.ContinueOnError)
	from := fs.Int64("from", -1, "start offset (default: the group's committed offset, else 0)")
	follow := fs.Bool("follow", false, "keep waiting for new messages (long poll)")
	group := fs.String("group", "", "consumer group: start from and commit its offset")
	maxN := fs.Int("max", 100, "messages per request")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return errors.New("usage: qlog consume TOPIC [--from N] [--follow] [--group G]")
	}
	topic := pos[0]
	offset := *from
	if offset < 0 {
		offset = 0
		if *group != "" {
			var r struct{ Offset int64 }
			if _, err := c.retry(ctx, "GET", "/v1/groups/"+url.PathEscape(*group)+"/offsets?topic="+url.QueryEscape(topic), nil, &r); err != nil {
				return err
			}
			offset = max(r.Offset, 0)
		}
	}
	for {
		wait := 0
		if *follow {
			wait = 20000
		}
		var r fetchResp
		path := fmt.Sprintf("/v1/topics/%s/messages?offset=%d&max=%d&wait_ms=%d", url.PathEscape(topic), offset, *maxN, wait)
		if _, err := c.retry(ctx, "GET", path, nil, &r); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		for _, m := range r.Messages {
			k := ""
			if m.Key != nil {
				k = *m.Key
			}
			fmt.Fprintf(out, "%d\t%s\t%s\n", m.Offset, k, m.Value)
		}
		if *group != "" && r.NextOffset != offset {
			body := map[string]any{"topic": topic, "offset": r.NextOffset}
			if _, err := c.retry(ctx, "POST", "/v1/groups/"+url.PathEscape(*group)+"/offsets", body, nil); err != nil {
				return err
			}
		}
		offset = r.NextOffset
		if !*follow && offset >= r.HighWatermark {
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

func cmdStatus(ctx context.Context, c *client, out io.Writer) error {
	var v struct {
		NodeID uint32 `json:"node_id"`
		Nodes  []struct {
			ID   uint32
			Addr string
			Up   bool
		}
		Groups []struct {
			Node          uint32 `json:"node"`
			Name          string
			Leader        uint32
			Term          uint64
			Role          string
			Commit        int64
			HighWatermark int64 `json:"high_watermark"`
			LogEndOffset  int64 `json:"log_end_offset"`
		}
		Events []struct {
			TS    time.Time
			Node  uint32
			Group string
			Msg   string
		}
	}
	if _, err := c.retry(ctx, "GET", "/v1/cluster", nil, &v); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "answered by node %d\n\nNODE\tADDR\tUP\n", v.NodeID)
	for _, n := range v.Nodes {
		fmt.Fprintf(tw, "%d\t%s\t%v\n", n.ID, n.Addr, n.Up)
	}
	fmt.Fprintf(tw, "\nGROUP\tVIEW OF\tROLE\tLEADER\tTERM\tCOMMIT\tHW\tLEO\n")
	for _, g := range v.Groups {
		node := g.Node
		if node == 0 {
			node = v.NodeID
		}
		fmt.Fprintf(tw, "%s\t%d\t%s\t%d\t%d\t%d\t%d\t%d\n", g.Name, node, g.Role, g.Leader, g.Term, g.Commit, g.HighWatermark, g.LogEndOffset)
	}
	_ = tw.Flush()
	if n := len(v.Events); n > 0 {
		fmt.Fprintln(out, "\nrecent events:")
		for _, e := range v.Events[max(0, n-10):] {
			fmt.Fprintf(out, "  %s node %d %s %s\n", e.TS.Local().Format("15:04:05.000"), e.Node, e.Group, e.Msg)
		}
	}
	return nil
}
