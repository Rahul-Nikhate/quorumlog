package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// cmdBench measures produce throughput and per-request latency: N messages
// sent by C concurrent workers, each request waits for its acks before the
// worker sends the next one (closed loop). Latency includes retries.
func cmdBench(ctx context.Context, c *client, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	n := fs.Int("n", 10000, "total messages")
	conc := fs.Int("c", 16, "concurrent producers")
	size := fs.Int("size", 100, "value size in bytes")
	batch := fs.Int("batch", 1, "messages per request")
	acks := fs.String("acks", "all", "all | leader")
	idem := fs.Bool("idempotent", false, "use an idempotent producer id per worker")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 || *n <= 0 || *conc <= 0 || *batch <= 0 || *size < 0 {
		return errors.New("usage: qlog bench TOPIC [-n N] [-c C] [--size B] [--batch K] [--acks all|leader] [--idempotent]")
	}
	topic := pos[0]
	if _, err := c.retry(ctx, "POST", "/v1/topics", map[string]string{"name": topic}, nil); err != nil {
		return fmt.Errorf("create topic: %w", err)
	}
	// Plenty of keep-alive connections for the workers.
	c.http.Transport = &http.Transport{MaxIdleConnsPerHost: *conc * 2}
	path := "/v1/topics/" + url.PathEscape(topic) + "/messages"
	value := strings.Repeat("x", *size)
	requests := (*n + *batch - 1) / *batch
	var next, retries, failed atomic.Int64
	lat := make([][]time.Duration, *conc)
	run := time.Now().UnixNano()
	start := time.Now()
	var wg sync.WaitGroup
	for w := range *conc {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var seq int32
			pid := ""
			if *idem {
				pid = fmt.Sprintf("bench-%d-%d", run, w)
			}
			for {
				i := next.Add(1) - 1
				if i >= int64(requests) || ctx.Err() != nil {
					return
				}
				k := min(*batch, *n-int(i)**batch)
				req := produceReq{Acks: *acks, ProducerID: pid, Messages: make([]wireMessage, k)}
				for j := range req.Messages {
					req.Messages[j].Value = value
				}
				if pid != "" {
					s := seq
					req.Sequence = &s
				}
				t0 := time.Now()
				var err error
				for attempt := 0; ; attempt++ {
					_, err = c.do(ctx, "POST", path, req, nil)
					if err == nil || !retriable(err) || attempt == 50 || ctx.Err() != nil {
						break
					}
					retries.Add(1)
					time.Sleep(20 * time.Millisecond)
				}
				if err != nil {
					// Stop this worker: its next sequence number would be
					// ambiguous (the failed request may have been written).
					failed.Add(1)
					return
				}
				seq += int32(k)
				lat[w] = append(lat[w], time.Since(t0))
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	all := slices.Concat(lat...)
	slices.Sort(all)
	ok := len(all)
	msgs := min(*n, ok**batch)
	pct := func(p float64) time.Duration {
		if ok == 0 {
			return 0
		}
		return all[min(ok-1, int(p*float64(ok)))]
	}
	fmt.Fprintf(out, "bench: %d messages (%d requests ok, %d failed) in %.2fs  [c=%d batch=%d size=%dB acks=%s idempotent=%v]\n",
		msgs, ok, failed.Load(), elapsed.Seconds(), *conc, *batch, *size, *acks, *idem)
	fmt.Fprintf(out, "throughput: %.0f msg/s, %.2f MB/s (values only)\n",
		float64(msgs)/elapsed.Seconds(), float64(msgs**size)/elapsed.Seconds()/1e6)
	fmt.Fprintf(out, "request latency: p50 %v  p99 %v  max %v  (retries %d)\n",
		pct(0.50).Round(10*time.Microsecond), pct(0.99).Round(10*time.Microsecond), pct(1).Round(10*time.Microsecond), retries.Load())
	if failed.Load() > 0 {
		return fmt.Errorf("%d requests failed", failed.Load())
	}
	return nil
}
