# Results

Every number in this file is measured, never estimated. Each entry states the
hardware, OS, and the exact command, and any caveat that limits what the
number means.

## Microbenchmarks (single goroutine, laptop: Apple M-series, macOS)

**What these are:** `go test -bench` microbenchmarks of `internal/log`. They
measure the storage engine in-process: no network, no Raft, no client. They are
**not** system throughput numbers; the end-to-end numbers are the `qlog bench`
section below (laptop, demo mode).

**Environment (measured 2026-11-13):**

| Item | Value |
|---|---|
| CPU | Apple M1 Pro, 10 cores (`GOMAXPROCS=10`) |
| RAM | 16 GiB |
| OS | macOS 15.7.7, APFS on the internal SSD |
| Go | go1.26.5 darwin/arm64 |
| Command | `go test -count=1 -run '^$' -bench . -benchmem ./internal/log/` (one run, default `-benchtime=1s`) |
| Batches | uncompressed RecordBatch v2 made of ~1 KiB records (1, 16 or 64 records); MB/s = encoded batch bytes / time |

### Append, real file system (`fs.OS`, 128 MiB segments)

Single goroutine, one batch per `Append`. The log validates the batch's CRC-32C,
stamps the offset and epoch, then writes it.

| Batch | `sync=never` (write(2) only) | `sync=always`, `WaitDurable` after every batch |
|---|---|---|
| 1 KiB | 2,948 ns/op · **366 MB/s** · 0 allocs | 4.76 ms/op · 0.23 MB/s |
| 16 KiB | 8,964 ns/op · **1,822 MB/s** · 0 allocs | 4.65 ms/op · 3.5 MB/s |
| 64 KiB | 21,447 ns/op · **3,038 MB/s** · 0 allocs | 4.68 ms/op · 13.9 MB/s |

Caveats:
- `sync=never` writes into the page cache. The numbers are the CPU and syscall
  ceiling of the append path, not disk bandwidth.
- `sync=always` with a single producer pays one full fsync per batch, because
  group commit has nothing to batch. On macOS Go's `File.Sync` is
  **`F_FULLFSYNC`**, which flushes the drive cache (~4.7 ms here). **Do not
  compare it with Linux `fdatasync`**.

### Append, in-memory `FaultFS` (log CPU cost without the OS)

| Batch | ns/op | MB/s | allocs/op |
|---|---|---|---|
| 1 KiB | 823 | 1,310 | 1 (FaultFS keeps a copy of each unsynced write for crash simulation) |
| 16 KiB | 7,732 | 2,112 | 1 |
| 64 KiB | 34,639 | 1,881 | 1 |

### Other microbenchmarks (same run)

| Benchmark | Result | Notes |
|---|---|---|
| `BenchmarkGroupCommit` (**60 concurrent producers**, not single-goroutine), 1 KiB batches, `sync=always`, `fs.OS` | 122,981 ns/batch (≈8.1k acked batches/s), **0.032 fsyncs per batch** (≈31 batches per `F_FULLFSYNC`) | Shows group commit amortising the macOS full fsync. It is not a throughput claim. |
| `BenchmarkRead`: `Read(randomOffset, 1 MiB)` + copy out, 64 MiB log of 16 KiB batches, `fs.OS`, warm page cache | 365 µs/op · 2,870 MB/s · 3 allocs (the 1 MiB copy buffer dominates) | Copy path (`FileRegion.Bytes`), the only read path the server uses (no zero-copy `sendfile`, see SCOPE.md). |
| `BenchmarkValidate64KiB` (CRC-32C + framing) | 16,148 ns/op · 4,034 MB/s · 0 allocs | Hardware CRC32C (arm64). One earlier smoke run measured 8.2 GB/s with `-benchtime=2000x`, so the number varies between runs. Treat it as order-of-magnitude. |

Raw output: the `go test -bench` lines are reproduced in
`docs/IMPLEMENTATION_NOTES.md` (Phase 1A, verification).

## Crash tests: 10,000 iterations each (laptop)

**What this is:** the power-loss tests of `internal/log/crash_test.go` with their iteration count raised from the
default (300/300/200/100) to 10,000. Each iteration appends random batches through `FaultFS`, simulates power loss
(unsynced bytes dropped, or a partial page torn), reopens the log and compares it with a model. Apple M1 Pro, macOS
15.7.7, go1.26.5, 2026-11-13 (re-run during the Phase 3 review; the earlier Phase 1A run, recorded in
IMPLEMENTATION_NOTES.md, also passed, with identical counts for the first two tests and counts within 0.3 % for the
third, whose timing varies).

```text
$ go test -count=1 -timeout 60m -run TestCrash -v ./internal/log/ -args -crash.iters=10000
=== RUN   TestCrashRecoversExactlySyncedPrefix
    crash_test.go:79: 10000 iterations, 7793 lost unsynced batches
--- PASS: TestCrashRecoversExactlySyncedPrefix (2.30s)
=== RUN   TestCrashTornWrites
    crash_test.go:194: 10000 iterations, 3453 recovered a partial unsynced tail
--- PASS: TestCrashTornWrites (1.77s)
=== RUN   TestCrashDurableWatermark
    crash_test.go:210: 10000 iterations: 5236 lost a non-durable tail, 8330 had durable data
--- PASS: TestCrashDurableWatermark (30.62s)
=== RUN   TestCrashAfterPrefixTruncationAndRetention
--- PASS: TestCrashAfterPrefixTruncationAndRetention (3.46s)
PASS
ok  	github.com/Rahul-Nikhate/quorumlog/internal/log	38.675s
```

The counts after each test name show that the faults actually fired: in 7,793 of 10,000 iterations the crash really
dropped unsynced batches, and recovery still produced exactly the synced prefix. No iteration lost a durable batch.

## Manual demo smoke test (laptop)

**Setup (2026-11-13):** Apple M1 Pro, macOS 15.7.7, go1.26.5. Binaries built with
`go build -o /tmp/qlbin/qlogd ./cmd/qlogd` and `go build -o /tmp/qlbin/qlog ./cmd/qlog`, then
`PORT=18080 qlogd --demo --data-dir $(mktemp -d)` (default `--fsync always`) in the background. The commands below were
run by a small shell script (each command is echoed with `$`, followed by its real output, unedited). `QLOG_ADDR` was
`http://localhost:18080`. The leader of `orders` was looked up from `/v1/cluster` before the kill.

Notes for reading it: offset 0 of a new topic is the first leader's election marker, so the first message is at
offset 1; the kill made the new leader write another marker (offset 5), which is why `after-kill` is at offset 6.
`COMMIT` is the last committed offset and `HW` the next offset after it (0-based API offsets). `/metrics` lists only
non-zero counters of the node the demo front happened to route to.

```text
$ curl -s localhost:18080/v1/health
{"node_id":2,"ok":true}

$ curl -s -XPOST localhost:18080/v1/topics -d '{"name":"orders"}' -w ' HTTP %{http_code}\n'
{"name":"orders"}
 HTTP 201

$ qlog topics create orders
topic orders already exists

$ qlog topics list
orders

$ curl -s -XPOST localhost:18080/v1/topics/orders/messages -d '{"messages":[{"key":"k1","value":"hello"}],"acks":"all","producer_id":"p-1","sequence":0}'
{"base_offset":1,"last_offset":1,"duplicate":false}

$ curl -s -XPOST localhost:18080/v1/topics/orders/messages -d '{"messages":[{"key":"k1","value":"hello"}],"acks":"all","producer_id":"p-1","sequence":0}'   # retry: duplicate
{"base_offset":1,"last_offset":1,"duplicate":true}

$ curl -s -XPOST localhost:18080/v1/topics/orders/messages -d '{"messages":[{"value":"x"}],"producer_id":"p-1","sequence":5}' -w ' HTTP %{http_code}\n'
{"error":"partition: out of order sequence number: producer 8681573497451636757 epoch 0 expected sequence 1, got 5","code":"OUT_OF_ORDER_SEQUENCE"}
 HTTP 409

$ printf 'one\ntwo\nthree\n' | qlog produce orders --producer-id cli-1
offset 2
offset 3
offset 4

$ qlog consume orders
1	k1	hello
2		one
3		two
4		three

$ qlog status
answered by node 1

NODE  ADDR             UP
1     127.0.0.1:52275  true
2     127.0.0.1:52277  true
3     127.0.0.1:52279  true

GROUP      VIEW OF  ROLE      LEADER  TERM  COMMIT  HW  LEO
__meta     1        follower  3       1     1       2   2
__offsets  1        leader    1       1     0       1   1
orders     1        leader    1       1     4       5   5
__meta     2        follower  3       1     1       2   2
__offsets  2        follower  1       1     0       1   1
orders     2        follower  1       1     4       5   5
__meta     3        leader    3       1     1       2   2
__offsets  3        follower  1       1     0       1   1
orders     3        follower  1       1     4       5   5

recent events:
  19:14:00.515 node 3 __offsets follower term=1, no leader known
  19:14:00.516 node 3 __offsets follower term=1 leader=1
  19:14:00.516 node 2 __offsets follower term=1 leader=1
  19:14:01.599 node 1 orders no leader heard: pre-vote, term=0
  19:14:01.611 node 1 orders started election term=1
  19:14:01.623 node 3 orders follower term=1, no leader known
  19:14:01.626 node 2 orders follower term=1, no leader known
  19:14:01.627 node 1 orders became leader term=1
  19:14:01.628 node 3 orders follower term=1 leader=1
  19:14:01.633 node 2 orders follower term=1 leader=1

$ curl -s -XPOST localhost:18080/v1/demo/nodes/1/kill   # kill the leader of orders (node 1)
{"ok":true}

$ sleep 1.5; qlog status
answered by node 2

NODE  ADDR             UP
1     127.0.0.1:52275  false
2     127.0.0.1:52277  true
3     127.0.0.1:52279  true

GROUP      VIEW OF  ROLE      LEADER  TERM  COMMIT  HW  LEO
__meta     2        follower  3       1     1       2   2
__offsets  2        leader    2       2     1       2   2
orders     2        leader    2       2     5       6   6
__meta     3        leader    3       1     1       2   2
__offsets  3        follower  2       2     1       2   2
orders     3        follower  2       2     5       6   6

recent events:
  19:14:02.454 node 2 orders no leader heard: pre-vote, term=1
  19:14:02.469 node 2 orders started election term=2
  19:14:02.476 node 3 orders follower term=2, no leader known
  19:14:02.476 node 2 orders became leader term=2
  19:14:02.477 node 3 orders follower term=2 leader=2
  19:14:02.734 node 2 __offsets no leader heard: pre-vote, term=1
  19:14:02.747 node 2 __offsets started election term=2
  19:14:02.760 node 3 __offsets follower term=2, no leader known
  19:14:02.760 node 2 __offsets became leader term=2
  19:14:02.761 node 3 __offsets follower term=2 leader=2

$ qlog produce orders --value after-kill --producer-id cli-1 --sequence 3
offset 6

$ qlog consume orders --group billing
1	k1	hello
2		one
3		two
4		three
6		after-kill

$ curl -s 'localhost:18080/v1/groups/billing/offsets?topic=orders'
{"group":"billing","offset":7,"topic":"orders"}

$ curl -s -XPOST localhost:18080/v1/demo/nodes/1/restart
{"ok":true}

$ sleep 1.5; qlog status
answered by node 1

NODE  ADDR             UP
1     127.0.0.1:52275  true
2     127.0.0.1:52277  true
3     127.0.0.1:52279  true

GROUP      VIEW OF  ROLE      LEADER  TERM  COMMIT  HW  LEO
__meta     1        follower  3       1     1       2   2
__offsets  1        follower  2       2     2       3   3
orders     1        follower  2       2     6       7   7
__meta     2        follower  3       1     1       2   2
__offsets  2        leader    2       2     2       3   3
orders     2        leader    2       2     6       7   7
__meta     3        leader    3       1     1       2   2
__offsets  3        follower  2       2     2       3   3
orders     3        follower  2       2     6       7   7

recent events:
  19:14:02.760 node 3 __offsets follower term=2, no leader known
  19:14:02.760 node 2 __offsets became leader term=2
  19:14:02.761 node 3 __offsets follower term=2 leader=2
  19:14:03.590 node 1 __meta follower term=1, no leader known
  19:14:03.604 node 1 __meta follower term=1 leader=3
  19:14:03.605 node 1 __offsets follower term=1, no leader known
  19:14:03.605 node 1  restarted (demo): recovered from its data dir
  19:14:03.620 node 1 orders follower term=1, no leader known
  19:14:03.644 node 1 __offsets follower term=2 leader=2
  19:14:03.663 node 1 orders follower term=2 leader=2

$ qlog produce orders --value after-restart
offset 7

$ qlog consume orders --from 0
1	k1	hello
2		one
3		two
4		three
6		after-kill
7		after-restart

$ curl -s 'localhost:18080/v1/topics/orders/messages?offset=8&wait_ms=1000' -w ' HTTP %{http_code} in %{time_total}s\n'   # long poll at the HW: nothing new, returns after wait_ms
{"messages":[],"next_offset":8,"high_watermark":8}
 HTTP 200 in 1.002048s

$ curl -s localhost:18080/metrics | grep -v ' 0$'
quorumlog_forwarded_requests_total{node="1"} 1
quorumlog_raft_messages_sent_total{node="1"} 164
quorumlog_raft_messages_received_total{node="1"} 245
```

Then the whole process was stopped and started again on the same data directory (process-level crash safety of the
clean path; the hard power-loss cases are covered by `internal/log` crash tests and the Porcupine nemesis). Note that
the listing below also shows a topic `bench`, which the transcript above never creates: some command against this
data directory (most likely a `qlog bench` run) happened between the two transcripts and is not shown. The Phase 3
review could not reconstruct it; the rest of the transcript is consistent with the commands shown.

```text
$ kill -INT <qlogd pid>   # graceful shutdown
time=2026-11-13T19:15:24.935+05:30 level=INFO msg="shutting down"
(process exited)

$ PORT=18080 qlogd --demo --data-dir $D &   # start again on the same data dir
$ qlog topics list
bench
orders

$ qlog consume orders --from 0
1	k1	hello
2		one
3		two
4		three
6		after-kill
7		after-restart

$ curl -s 'localhost:18080/v1/groups/billing/offsets?topic=orders'
{"group":"billing","offset":7,"topic":"orders"}

$ kill -TERM <qlogd pid>
time=2026-11-13T19:15:28.588+05:30 level=INFO msg="shutting down"
(process exited)
```

Outcome: topic create (201, then "already exists"), produce with `acks=all`, idempotent retry → `duplicate: true` with
the original offset, a sequence gap → `409 OUT_OF_ORDER_SEQUENCE`, consume, leader kill → node 2 became leader of `orders` in
term 2 (pre-vote at 19:14:02.454, leader at 19:14:02.476, i.e. ~22 ms once its election timer fired; the timer is
10 ticks × 50 ms, randomized in [500, 1000) ms after the last heartbeat), produce after the kill, consumer-group commit/fetch, restart → the node rejoins as a
follower and catches up (same COMMIT/HW/LEO on all three), long poll at the HW returns empty after `wait_ms`, and all
data, topics and group offsets survive a full process restart. No acknowledged message was lost and none was
duplicated.

## `qlog bench` (demo mode, single process, laptop)

**What this is:** end-to-end produce throughput/latency of `qlogd --demo` (3 Raft nodes in **one process**, sharing one
SSD and 10 CPU cores) measured by `qlog bench` over HTTP/JSON on localhost. Closed loop: each of `c` workers sends one
request, waits for its acknowledgement, then sends the next. Every request goes through the demo front, which routes it
round-robin to a node; 2 of 3 requests are then forwarded once more to the leader (so most requests take two local HTTP
hops). Values are 100 bytes. Latency is per request (including any retries; there were none). Apple M1 Pro, macOS
15.7.7, go1.26.5, 2026-11-13. One run each, so treat differences under ~10 % as noise.

**`--fsync always` (the default)**: every acknowledged `acks=all` message is fsynced on the leader and a follower
before the reply. On macOS Go's `File.Sync` is `F_FULLFSYNC` (≈4.7 ms each, see the microbenchmarks above), and the
three nodes' logs share one disk, so this is fsync-bound:

| c | batch | acks | idempotent | messages | time | msg/s | p50 | p99 | max | failed / retries |
|---|---|---|---|---|---|---|---|---|---|---|
| 16 | 1 | all | false | 5000 | 11.84 s | **422** | 37.13ms | 69.64ms | 86.46ms | 0 / 0 |
| 16 | 1 | all | true | 5000 | 13.17 s | **380** | 41.62ms | 72.21ms | 80.07ms | 0 / 0 |
| 16 | 1 | leader | false | 5000 | 10.84 s | **461** | 34.33ms | 59.58ms | 73.29ms | 0 / 0 |
| 16 | 10 | all | false | 20000 | 5.28 s | **3786** | 41.73ms | 68.4ms | 98.31ms | 0 / 0 |
| 1 | 1 | all | false | 1000 | 12.71 s | **79** | 11.97ms | 27.03ms | 33.25ms | 0 / 0 |

**`--fsync never`** (fresh demo, same commands): durability comes only from replication to a majority's page cache.
This shows the cost of the full fsync on this laptop, not a recommended setting:

| c | batch | acks | idempotent | messages | time | msg/s | p50 | p99 | max | failed / retries |
|---|---|---|---|---|---|---|---|---|---|---|
| 16 | 1 | all | false | 5000 | 0.57 s | **8722** | 1.63ms | 6.32ms | 10.94ms | 0 / 0 |
| 16 | 1 | all | true | 5000 | 0.53 s | **9397** | 1.58ms | 5.01ms | 10.71ms | 0 / 0 |
| 16 | 1 | leader | false | 5000 | 0.56 s | **8873** | 1.61ms | 4.21ms | 6.15ms | 0 / 0 |
| 16 | 10 | all | false | 20000 | 0.28 s | **71463** | 2.09ms | 5.75ms | 9.18ms | 0 / 0 |
| 1 | 1 | all | false | 1000 | 0.52 s | **1908** | 480µs | 950µs | 8.41ms | 0 / 0 |

Commands: `qlog bench bench -n N -c C --size 100 [--batch K] [--acks all|leader] [--idempotent]` against
`PORT=18080 qlogd --demo --data-dir $(mktemp -d) [--fsync never]`. Raw output:

```text
$ qlog bench bench -n 5000 -c 16 --acks all --size 100
bench: 5000 messages (5000 requests ok, 0 failed) in 11.84s  [c=16 batch=1 size=100B acks=all idempotent=false]
throughput: 422 msg/s, 0.04 MB/s (values only)
request latency: p50 37.13ms  p99 69.64ms  max 86.46ms  (retries 0)

$ qlog bench bench -n 5000 -c 16 --acks all --idempotent --size 100
bench: 5000 messages (5000 requests ok, 0 failed) in 13.17s  [c=16 batch=1 size=100B acks=all idempotent=true]
throughput: 380 msg/s, 0.04 MB/s (values only)
request latency: p50 41.62ms  p99 72.21ms  max 80.07ms  (retries 0)

$ qlog bench bench -n 5000 -c 16 --acks leader --size 100
bench: 5000 messages (5000 requests ok, 0 failed) in 10.84s  [c=16 batch=1 size=100B acks=leader idempotent=false]
throughput: 461 msg/s, 0.05 MB/s (values only)
request latency: p50 34.33ms  p99 59.58ms  max 73.29ms  (retries 0)

$ qlog bench bench -n 20000 -c 16 --batch 10 --acks all --size 100
bench: 20000 messages (2000 requests ok, 0 failed) in 5.28s  [c=16 batch=10 size=100B acks=all idempotent=false]
throughput: 3786 msg/s, 0.38 MB/s (values only)
request latency: p50 41.73ms  p99 68.4ms  max 98.31ms  (retries 0)

$ qlog bench bench -n 1000 -c 1 --acks all --size 100
bench: 1000 messages (1000 requests ok, 0 failed) in 12.71s  [c=1 batch=1 size=100B acks=all idempotent=false]
throughput: 79 msg/s, 0.01 MB/s (values only)
request latency: p50 11.97ms  p99 27.03ms  max 33.25ms  (retries 0)
```

```text
$ qlog bench bench -n 5000 -c 16 --acks all --size 100
bench: 5000 messages (5000 requests ok, 0 failed) in 0.57s  [c=16 batch=1 size=100B acks=all idempotent=false]
throughput: 8722 msg/s, 0.87 MB/s (values only)
request latency: p50 1.63ms  p99 6.32ms  max 10.94ms  (retries 0)

$ qlog bench bench -n 5000 -c 16 --acks all --idempotent --size 100
bench: 5000 messages (5000 requests ok, 0 failed) in 0.53s  [c=16 batch=1 size=100B acks=all idempotent=true]
throughput: 9397 msg/s, 0.94 MB/s (values only)
request latency: p50 1.58ms  p99 5.01ms  max 10.71ms  (retries 0)

$ qlog bench bench -n 5000 -c 16 --acks leader --size 100
bench: 5000 messages (5000 requests ok, 0 failed) in 0.56s  [c=16 batch=1 size=100B acks=leader idempotent=false]
throughput: 8873 msg/s, 0.89 MB/s (values only)
request latency: p50 1.61ms  p99 4.21ms  max 6.15ms  (retries 0)

$ qlog bench bench -n 20000 -c 16 --batch 10 --acks all --size 100
bench: 20000 messages (2000 requests ok, 0 failed) in 0.28s  [c=16 batch=10 size=100B acks=all idempotent=false]
throughput: 71463 msg/s, 7.15 MB/s (values only)
request latency: p50 2.09ms  p99 5.75ms  max 9.18ms  (retries 0)

$ qlog bench bench -n 1000 -c 1 --acks all --size 100
bench: 1000 messages (1000 requests ok, 0 failed) in 0.52s  [c=1 batch=1 size=100B acks=all idempotent=false]
throughput: 1908 msg/s, 0.19 MB/s (values only)
request latency: p50 480µs  p99 950µs  max 8.41ms  (retries 0)
```

Caveats: this is a laptop, one process, loopback networking and a single disk for all three "nodes", so it says
nothing about a real 3-machine deployment (where each node fsyncs its own disk in parallel and network RTT adds to
latency). With `fsync always`, single-message requests are limited by full-flush fsyncs (≈80 msg/s for one client,
≈400 msg/s for 16); batching 10 messages per request raises it ~9×. Under `fsync never` the HTTP/JSON path and the two
hops dominate (~1.6 ms p50 at c=16).
