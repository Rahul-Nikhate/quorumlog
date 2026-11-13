# QuorumLog design

QuorumLog is a small, replicated message queue that is easy to explain. This document covers what it does, how a
write and a read move through the system, what happens when the leader dies, and the trade-offs behind each choice.
The API contract and scope are in [SCOPE.md](SCOPE.md). Measured numbers are in [RESULTS.md](RESULTS.md).

## Goals

1. **No acknowledged write is lost** (`acks=all`) while a majority of nodes (2 of 3) survives, including power loss on
   a minority.
2. **Automatic failover.** If the leader dies, the survivors elect a new one and keep accepting writes without any
   operator action.
3. **Consumers only see committed data.** A message that a consumer has read can never disappear later.
4. **Exactly-once appends under retry.** An idempotent producer can retry after a timeout or a failover without
   creating duplicates.
5. **Explainable in two minutes,** and deployable for free with a dashboard that demonstrates goals 1–4 live.

## Non-goals

Kafka wire compatibility, multiple partitions per topic, a consumer-group rebalance protocol, dynamic membership,
cross-node TLS/auth, and peak throughput. Each one would add a lot of code without teaching anything new about
replication and durability (see "Limitations" in the [README](../README.md#limitations-and-non-goals)).

## Components

```
cmd/qlogd ─▶ cluster.Node ─┬─▶ httpapi        (HTTP/JSON, leader forwarding, long-poll)
                           ├─▶ partition × N  (one per Raft group: __meta, __offsets, one per topic)
                           │      ├─ raft.Node       pure state machine: Tick/Step/Ready/Advance
                           │      ├─ raftlog         segmented log as raft.LogStore
                           │      └─ producer state  (producer id → last sequence, offsets)
                           └─▶ transport.TCP  (one connection per peer, frames tagged with group id)
```

| Component | Responsibility | Key idea |
|---|---|---|
| `internal/log` | Append-only segmented log with a sparse mmap'd index | Kafka-style record batches, CRC-32C, fsync policy, torn-tail recovery |
| `internal/fs` | File system interface, real and fault-injecting | Crash tests can drop unsynced writes and tear pages |
| `internal/raft` | Consensus as a pure state machine | No goroutines, clocks or I/O inside, so the logic is deterministic and testable |
| `internal/raftlog` | Glue: one Raft entry = one record batch | The message log *is* the Raft log, with no second copy |
| `internal/partition` | One goroutine per group: drives Raft, persists, applies, answers produce/fetch | High watermark = commit index mapped to offsets |
| `internal/cluster` | Wires groups to a node: `__meta` (topics), `__offsets`, topic groups, event feed | Metadata is just another replicated log |
| `internal/raft/transport` | TCP between nodes, multiplexing all groups | Length-prefixed frames, reconnect with backoff + jitter |
| `internal/httpapi`, `internal/dashboard` | Client API and embedded UI | Any node accepts any request |

## Write path (`POST /v1/topics/orders/messages`, `acks=all`)

1. **Any node receives the request.** If this node is not the leader of the `orders` group, it forwards the request to
   the leader. The response carries `X-QL-Leader` and `X-QL-Forwarded: true`, so clients never have to locate the
   leader themselves.
2. The leader's HTTP handler turns the JSON messages into one **record batch** and hands it to the partition driver.
3. **Idempotence check.** If the request has `producer_id`/`sequence`, the producer state decides:
   - a sequence that was already appended gets `duplicate: true` and the original offsets;
   - a gap in the sequence gets `OUT_OF_ORDER_SEQUENCE`;
   - the expected next sequence is appended.
4. The driver **proposes** the batch to Raft. The leader appends it to its own segmented log, and the fsync policy
   decides when the data is durable. `always` uses group commit: one fsync covers every batch that arrived while the
   previous fsync was running.
5. Raft sends `AppendEntries` to both followers over TCP. Each follower appends, makes the entry durable, and replies.
6. Once a **majority** (the leader plus one follower) has the entry durably, and the entry is from the leader's
   current term (the Figure-8 rule), the commit index advances. The **high watermark** moves past the batch.
7. The waiting request completes with `{"base_offset", "last_offset"}`. With `acks=leader`, step 7 happens right after
   step 4. That is faster, but a batch acknowledged this way can be lost if the leader dies before replicating it.

## Read path (`GET /v1/topics/orders/messages?offset=42&wait_ms=5000`)

1. Like a write, the read is forwarded to the group's **leader** if it arrives at a follower (one hop). The leader first
   confirms it is still the leader with Raft **ReadIndex** (a quorum heartbeat round), then serves from its local log,
   but **never past the high watermark**. Uncommitted entries can still be truncated by a future leader, so they are
   invisible to consumers. (A follower's copy may be stale, which is why followers don't serve reads.)
2. If `offset == high_watermark`, the request **long-polls**. It parks on the partition's "HW advanced" signal until new
   data commits or `wait_ms` expires, then returns what it has (possibly an empty list) and `next_offset`.
3. Lookup: a binary search over segment base offsets finds the segment, and the sparse index (one entry every N bytes,
   mmap'd) gives a nearby file position. A short forward scan over batch headers finds the exact batch.
4. ReadIndex makes reads linearizable even when an old leader is cut off and does not know it yet. The Porcupine test
   checks this, and its negative controls (follower-local reads; an `acks=leader` write lost by a failover) must fail.

## Consumer offsets and topics

Both are small replicated state machines on top of the same partition machinery:

- `POST /v1/topics` proposes `create orders` to the `__meta` group. When the entry commits, **every** node applies it
  and opens (or creates) a local Raft group for `orders`.
- `POST /v1/groups/g/offsets` proposes `{group, topic, offset}` to `__offsets`. Each node rebuilds an in-memory map
  `(group, topic) → offset` by applying committed entries (a "tailer" goroutine per group reads the group's own
  committed log from offset 0, so a restart rebuilds the state). Offset fetches and the topic list are served by the
  group's leader after a ReadIndex barrier and after its tailer has applied up to that point: linearizable.
- A topic's Raft group id is derived from the `__meta` offset of its create record, so every node computes the same id.

## Failover walkthrough

Start: node 2 leads `orders` in term 5, and every node has offsets 0..41 committed (HW = 42).

1. **Node 2 dies** (in demo mode, *Kill* stops it but keeps its data dir). In-flight `acks=all` produces on node 2 fail
   with an unknown outcome. The client cannot tell whether those batches committed.
2. Nodes 1 and 3 stop hearing heartbeats. After a **randomized election timeout**, one of them (say node 3) runs a
   **pre-vote**. Pre-vote asks "would you vote for me?" without bumping the term, so a node that was only partitioned
   cannot disrupt a healthy cluster when it comes back.
3. Node 3 wins the pre-vote, becomes a candidate in **term 6**, and asks for votes. Node 1 grants its vote only if node
   3's log is at least as up-to-date as its own (Raft's election restriction). This is why no committed entry can be
   lost: every committed entry is on a majority, and any winning candidate needs a vote from that majority.
4. Node 3 becomes leader and appends a **no-op** entry for term 6. Committing it also commits everything before it from
   earlier terms, so the HW catches up without waiting for client traffic.
5. **Writes resume.** Clients that retried with the same `producer_id`/`sequence` get `duplicate: true` if their
   earlier attempt had in fact committed, and a fresh append if it had not. Either way there is exactly one copy.
6. **Node 2 restarts.** It recovers its log from disk: CRC-checked, with any torn tail cut off. Then it rejoins as a
   follower in term 6. Node 3 finds the point where the two logs diverge using fast backup (the follower replies with
   a conflict term and index instead of stepping back one entry at a time), and overwrites node 2's uncommitted
   suffix. It then streams the missing entries, or a snapshot if they were compacted. The dashboard shows node 2's LEO
   catching up to the leader's.
7. **Verification** (guided demo, step 7): read the topic from offset 0, and every message that was acknowledged with
   `acks=all` is present exactly once.

A node that is partitioned while it is leader steps down by itself after one election timeout without hearing from a
majority (**check-quorum**). That bounds how long it keeps accepting requests it can no longer complete: its writes
cannot commit, and its ReadIndex rounds cannot get a quorum, so both fail and the client retries elsewhere.

## Key trade-offs

| Decision | Alternative | Why this way |
|---|---|---|
| **Own Raft, written as a pure state machine** | etcd/raft or hashicorp/raft | The point of the project is to show the algorithm. A side-effect-free core lets `rafttest` run deterministic, seed-replayable simulations with partitions, drops and reordering. |
| **The message log is the Raft log** (`raftlog`) | Separate Raft WAL + message store | Data is written once instead of twice, and one recovery path handles both. The cost: the log format must carry Raft terms (stored in the batch's leader-epoch field). |
| **One Raft group per topic, plus `__meta` and `__offsets`** | One global Raft log | Topics fail over and catch up independently, and metadata uses the same machinery. Groups share one TCP connection per peer, so the number of connections doesn't grow with the number of topics. |
| **Replication factor = cluster size (3)** | Placement and replica sets | No placement or balancer code. Every node has every topic, so any node can take over as leader. |
| **HTTP/JSON API** | Kafka protocol, gRPC | It can be used with `curl` and a browser, and the dashboard needs no client library. Throughput is not a goal. |
| **Follower forwards writes and reads to the leader** | Redirect (`307`), client-side routing, follower reads | Clients and the dashboard stay simple, and reads stay linearizable. The cost is one extra hop, which is visible in `X-QL-Forwarded`; a forwarded request is never forwarded again (no loops). |
| **`acks=all` default, `acks=leader` available** | Only `all` | Shows the latency-vs-durability trade-off concretely. The dashboard's verify step reports losses of `acks=leader` messages separately. |
| **fsync `always` with group commit** | fsync per write, or `never` | Durability per acknowledgement at a throughput cost of about one fsync per round of batches, not per batch. `interval`/`never` exist for benchmarking. Under `never`, durability comes only from replication to a majority. |
| **Sparse mmap'd index** | Dense index or a B-tree | Kafka's approach: small, fits in the page cache, and rebuildable from the log. A lookup costs a short scan. |
| **High watermark = commit index** | Serve uncommitted data | Consumers never see data that could be truncated. Reading through the HW costs at most one replication round trip of latency. |
| **Producer id + sequence dedup in the replicated state** | Client-side dedup, transactions | Retries across failover stay exactly-once per append. The state is rebuilt from the log after truncation, so a new leader has the same dedup state. |
| **Demo mode: 3 nodes in one process** | 3 services | Fits a single free web service (Render) while still running real TCP between nodes and separate data directories. It is honest about being a demo: one machine, one disk. |
| **Long-poll instead of WebSockets/SSE** | Push streams | Plain HTTP that works through any proxy. The server keeps no per-consumer state, and the consumer's offset is the cursor. |

## Failure model (what is and isn't tolerated)

- **Tolerated:** crash or power loss of any minority (1 of 3); a leader partitioned from the rest; lost, duplicated or
  reordered messages between nodes; torn writes at the tail of a log file; a client retrying any request.
- **Not tolerated:** losing a majority at the same time. Writes then stop with `UNAVAILABLE`/`TIMEOUT` instead of
  risking divergence, and they resume once a majority is back. Byzantine faults, and disks that lie about fsync, are
  also outside the model.

## Testing strategy

The tests are layered so that each layer can trust the one below it:

1. **Storage** (`internal/log`, `internal/fs`): unit tests; property tests that compare against an in-memory model;
   fuzzing of the batch decoder and of recovery from corrupted files; and crash tests on `FaultFS`. The crash tests
   simulate power loss at random points and assert that recovery yields exactly the synced prefix.
2. **Consensus** (`internal/raft`, `rafttest`): a deterministic simulator drives many seeded runs with partitions,
   drops, reordering and restarts, and checks election safety, log matching and leader completeness after every step.
   The *negative controls* turn on known Raft bugs and require the checkers to catch them, which proves that the
   checkers can fail.
3. **Partition driver** (`internal/partition`): real goroutines over the in-memory transport. These tests cover
   produce/acks, the HW, leadership loss with pending waiters, idempotent retries across failover, snapshot install
   and fail-stop on storage errors.
4. **System** (`test/e2e`, `test/porcupine`). The e2e test runs three nodes over real HTTP + TCP with real files and
   the same kill-the-leader scenario as the dashboard's guided demo. The Porcupine test drives a 3-replica partition
   directly (real Raft and segmented log, on the in-memory network and `FaultFS`, so it can partition and crash
   replicas), records concurrent client operations and checks them for linearizability, with two negative controls.
   It does not go through the HTTP forwarding layer, which the e2e and `httpapi` tests cover.

Everything runs under `go test -race` in CI. Long-running variants (more seeds, more crash iterations) use flags and
are recorded in [RESULTS.md](RESULTS.md) and [IMPLEMENTATION_NOTES.md](IMPLEMENTATION_NOTES.md).
