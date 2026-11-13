# QuorumLog — Simplified Scope (authoritative)

> This file replaces the "full Kafka-compatible" plan in `resume-projects/01-distributed-log-raft-kafka.md`.
> Goal: a **simple, end-to-end, deployed** mini message queue that shows system design (replication,
> leader election, failover, idempotency, consumer offsets) and CS fundamentals (OS: fsync/page cache/mmap;
> DBMS: WAL, indexes, crash recovery; CN: TCP framing, HTTP, long-polling, timeouts), and that can be
> explained in 2 minutes.

## One-line pitch
A 3-node, Raft-replicated message queue in Go: topics, durable produce/consume, consumer-group offsets,
idempotent producers, automatic leader failover, and a live dashboard where you can kill the leader and
watch the cluster recover without losing acknowledged messages.

## In scope
| Area | What |
|---|---|
| Storage (done) | `internal/log`: segmented append-only log, sparse offset/time index (mmap), CRC-32C, fsync policies (always = group commit / interval / never), torn-tail crash recovery, retention. `internal/fs`: real + fault-injecting FS. |
| Consensus (done) | `internal/raft`: election (randomized timeouts, pre-vote, check-quorum), log replication with fast backup, Figure-8 commit rule, no-op on election, snapshots, ReadIndex, leadership transfer. `rafttest` harness. |
| Raft ↔ disk (done) | `internal/raftlog`: the segmented log implements `raft.LogStore` (one Raft entry = one record batch). |
| Partition driver | `internal/partition`: one goroutine per Raft group; produce with `acks=leader|all`, high watermark (only committed data is readable), long-poll fetch, idempotent producer (producer id + sequence dedup). |
| Node-to-node transport | `internal/raft/transport/tcp.go`: one TCP connection per peer pair, length-prefixed frames, all Raft groups multiplexed by group id, reconnect with backoff+jitter. (Mem transport stays for tests.) |
| Cluster metadata | Raft group `__meta` replicated on all nodes: the topic list. Every node watches it and creates/opens one Raft group per topic. Replication factor = cluster size (3). One partition per topic. |
| Consumer offsets | Raft group `__offsets`: records `{group, topic, offset}`; each node rebuilds an in-memory map by applying committed entries. No rebalance protocol: a group name + topic → last committed offset. |
| Client API | HTTP/JSON on every node (see contract below). Writes sent to a follower are **forwarded to the leader** (response header `X-QL-Leader`, `X-QL-Forwarded: true`). |
| Dashboard | Static HTML/JS (embedded with `embed`) served at `/`: nodes, per-topic leader/term/commit/HW/LEO, produce form, live tail (long-poll), election event feed, and in demo mode **Kill node / Restart node** buttons. |
| Demo mode | `qlogd --demo` runs **3 nodes in one process** (localhost TCP between them, separate data dirs) and serves the dashboard + API on `$PORT` (default 8080). Used for the free Render deployment. |
| CLI | `cmd/qlog`: `topics create|list`, `produce`, `consume`, `status`, `bench` (simple throughput/latency) over HTTP. |
| Tests | Existing unit/property/crash/fuzz/raft-seed suites; partition tests; **e2e test**: 3 in-process nodes over real HTTP+TCP, produce acks=all, kill leader, keep producing, restart, consume → no acknowledged message lost, no duplicates with idempotent producer; **one Porcupine linearizability test** (+ negative control). |
| Deploy | Multi-stage `Dockerfile` (static binary, distroless), `deploy/docker-compose.yml` (3 containers + volumes), `render.yaml` (free web service, demo mode), `docs/DEPLOY.md` (Render steps + Oracle Cloud Always Free VM steps). GitHub Actions CI: vet, gofmt, `go test -race`, build image. |

## Out of scope (removed to keep it simple)
Kafka wire protocol & franz-go/kcat interop, consumer-group rebalancing (Join/Sync/Heartbeat), separate controller
& placement/balancer, multiple partitions per topic, zero-copy `sendfile` path, Docker chaos harness (toxiproxy/iptables),
Kafka/Redpanda benchmark comparison, Prometheus/Grafana stack (a `/metrics` text endpoint with a few counters is fine).

## HTTP API contract (all JSON; errors: `{"error": "...", "code": "NOT_LEADER|NOT_FOUND|BAD_REQUEST|TIMEOUT|UNAVAILABLE|OUT_OF_ORDER_SEQUENCE"}`)
| Method & path | Request | Response |
|---|---|---|
| `GET /v1/health` | – | `{"node_id":1,"ok":true}` |
| `GET /v1/cluster` | – | `{"node_id":1,"nodes":[{"id":1,"addr":"…","up":true}],"groups":[{"name":"orders","leader":2,"term":5,"role":"follower","commit":41,"high_watermark":42,"log_end_offset":42}], "events":[{"ts":"…","node":2,"group":"orders","msg":"became leader term=5"}]}` |
| `POST /v1/topics` | `{"name":"orders"}` | `201 {"name":"orders"}` (idempotent: 200 if exists) |
| `GET /v1/topics` | – | `{"topics":["orders"]}` |
| `POST /v1/topics/{topic}/messages` | `{"messages":[{"key":"k","value":"v"}], "acks":"all"|"leader", "producer_id":"p-1", "sequence":0}` (producer fields optional) | `{"base_offset":42,"last_offset":42,"duplicate":false}` |
| `GET /v1/topics/{topic}/messages?offset=0&max=100&wait_ms=0` | – | `{"messages":[{"offset":42,"key":"k","value":"v","timestamp":"…"}],"next_offset":43,"high_watermark":43}` (long-polls up to `wait_ms` when `offset == high_watermark`) |
| `POST /v1/groups/{group}/offsets` | `{"topic":"orders","offset":43}` | `{"ok":true}` |
| `GET /v1/groups/{group}/offsets?topic=orders` | – | `{"group":"g","topic":"orders","offset":43}` (offset `-1` if none) |
| `POST /v1/demo/nodes/{id}/kill` · `POST /v1/demo/nodes/{id}/restart` | – | `{"ok":true}` (demo mode only; kill = stop node process-state, keep its data dir) |
| `GET /metrics` | – | plain text counters (produce/fetch counts, elections, bytes) |

Offsets are Kafka-style: first message offset 0 is exposed to clients (internally Raft index = offset+1 is fine;
the API always shows 0-based offsets). `high_watermark` = next offset after the last committed message.

## Package layout (target)
```
cmd/qlogd   cmd/qlog
internal/{fs,log,raft,raft/transport,raft/rafttest,raftlog,partition,cluster,httpapi,dashboard}
test/{e2e,porcupine}
deploy/docker-compose.yml   Dockerfile   render.yaml   .github/workflows/ci.yml
docs/{SCOPE,DESIGN,DEPLOY,RESULTS,IMPLEMENTATION_NOTES}.md   README.md
```
