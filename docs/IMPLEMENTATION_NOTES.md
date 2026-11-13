# Implementation Notes (handoff log between build phases)

Spec: /Users/rahulwork/Documents/Personal/resume-projects/01-distributed-log-raft-kafka.md

Each phase appends a section: packages written, exported API summary, deviations from spec (with reason),
known gaps/TODOs, and the exact commands run to verify (build/vet/test -race, GOOS=linux cross-build).

---

## Phase 1B: raft core (M2a, Week 3 + pure parts of M2b)

Packages: `internal/raft` (pure state machine, no I/O / goroutines / clocks inside `Node`),
`internal/raft/transport` (Transport interface + in-memory fault-injecting network),
`internal/raft/rafttest` (deterministic cluster harness + 6.5840-style tests). Own Raft, written from
Figure 2 — no hashicorp/etcd raft. No new module dependencies (stdlib only).

| File | Contents |
|---|---|
| `raft/node.go` | `Config`, `Node` (Tick, Step, Campaign, Propose/ProposeBatch, ReadIndex, TransferLeadership, ReportDurable, HasReady/Ready/Advance, Status/BasicStatus, LogTerm), elections (randomized timeout, pre-vote, check-quorum, leader stickiness), AppendEntries with fast backup, Figure-8 commit rule, election no-op, leadership transfer, snapshots, `NotLeaderError`/`ErrNotLeader`, package doc with the Ready contract |
| `raft/log.go` | `raftLog`: stable `LogStore` + unstable tail (pending snapshot / `truncateFrom` / entries), term lookup, gapped-index binary searches, conflict hints, commit/apply |
| `raft/progress.go` | `Progress` (Match/Next, probe/replicate/snapshot), inflight window (msgs + bytes) |
| `raft/messages.go` | `Message`, `MsgType`, strict canonical binary codec (`AppendMessage`/`EncodeMessage`/`DecodeMessage`/`EncodedSize`) |
| `raft/storage.go` | `Entry`, `Snapshot`, `LogStore` interface, `MemoryStore` (multi-offset batches, `Compact`, `Clone`) |
| `raft/hardstate.go` | `HardState{Term,Vote,Commit}`, `HardStateStore`, `MemHardStateStore`, `FileHardStateStore` (tmp → fsync → rename → fsync dir, CRC32C, injectable `FileOps`) |
| `raft/readindex.go` | ReadIndex with batched heartbeat-quorum confirmation; optional `UnsafeLeaseReads` |
| `raft/logger.go` | `DebugLoggerFromEnv()` (`RAFT_DEBUG=1`) |
| `raft/transport/transport.go` | `Transport{Send([]raft.Message); Close()}`, `Handler` — shaped for the Week-4 multiplexed TCP transport (messages carry `Group`) |
| `raft/transport/mem.go` | `Network`: per-link drop/dup/delay/reorder, arbitrary partitions, disconnect, crash/restart, interposer hook, seeded RNG, deterministic delivery order, codec round-trip in transit, `RunRealtime` for goroutine-driven tests |
| `raft/rafttest/` | `Cluster` harness (`cluster.go`), `MemDisk` (page cache vs durable image), tests |

### Exported API summary

```go
type NodeID uint32
type Entry struct{ Term, FirstIndex, LastIndex uint64; Data []byte } // one RecordBatch; Raft index = LastIndex
type Snapshot struct{ Index, Term uint64; Peers []NodeID; Data []byte }
type HardState struct{ Term uint64; Vote NodeID; Commit uint64 }

func NewNode(Config) (*Node, error)
func (*Node) Tick()
func (*Node) Step(Message) error                 // ErrWrongGroup/ErrWrongRecipient/ErrUnknownPeer/ErrInvalidEntries on bad input
func (*Node) Campaign()
func (*Node) Propose(data []byte) (Entry, error)  // span 1
func (*Node) ProposeBatch(span uint64, data []byte, stamp func(*Entry)) (Entry, error) // *NotLeaderError{Leader,Term} | ErrProposalDropped
func (*Node) ReadIndex(ctx uint64) error          // result later in Ready.ReadStates
func (*Node) TransferLeadership(to NodeID) error
func (*Node) ReportDurable(index, term uint64)    // AsyncDurability only
func (*Node) HasReady() bool; Ready() Ready; Advance(Ready)
func (*Node) Status() Status; BasicStatus() Status; LogTerm(i) (uint64, error)

type Ready struct {
    SoftState *SoftState; HardState *HardState; MustSync bool
    Snapshot *Snapshot; TruncateFrom uint64; Entries []Entry
    CommittedEntries []Entry; Messages []Message; ReadStates []ReadState
}
```

Config knobs: `ElectionTick` (10; randomized in [T,2T)), `HeartbeatTick` (1), `PreVote`, `CheckQuorum`,
`MaxSizePerMsg` (1 MiB), `MaxInflightMsgs` (64), `MaxInflightBytes` (4 MiB), `MaxCommittedSizePerReady`
(16 MiB), `AsyncDurability`, `UnsafeLeaseReads` (+`LeaseMarginTicks`, requires CheckQuorum),
`NoopData func(index, term) []byte` (lets the driver emit a Kafka control batch as the election no-op),
`Group` (stamped on every message), `Seed`, `Logger`, `DebugChecks` (extra batch-boundary assertions).

### The `LogStore` contract a segmented-log adapter (`internal/log`) must implement

```go
type LogStore interface {
    FirstIndex() uint64                               // first offset present (snapshot.Index+1)
    LastIndex() uint64                                // LEO-1 (snapshot.Index if empty)
    Term(i uint64) (uint64, error)                    // term of the batch CONTAINING offset i
    Entries(lo, hi, maxBytes uint64) ([]Entry, error) // batches overlapping [lo,hi)
    Snapshot() (Snapshot, error)
    Append(entries []Entry) error
    TruncateSuffix(from uint64) error                 // drop batches with FirstIndex >= from
    ApplySnapshot(snap Snapshot) error                // drop everything; log restarts at snap.Index+1
}
```

Semantics the adapter must honour (spec §5.4.2, followed exactly):
1. One entry = one RecordBatch: `FirstIndex = baseOffset`, `LastIndex = lastOffset` (= the Raft index),
   `Term = partitionLeaderEpoch`. Offsets are contiguous across batches; Raft indices are gapped.
2. `Term(i)` must work for **any** offset in `[FirstIndex()-1, LastIndex()]`, including mid-batch offsets
   (the Node binary-searches offsets for fast-backup hints and `lastIndexOfTerm`: O(log n) `Term` calls
   per rejection, never a batch-by-batch walk). `Term(FirstIndex()-1)` is the snapshot term; if the log
   starts at a non-zero logStartOffset without a snapshot, synthesize snapshot metadata
   (`Index = logStart-1`, its term). `ErrCompacted` below, `ErrUnavailable` above.
3. `Entries(lo,hi,maxBytes)`: batches overlapping `[lo,hi)`, at least one, then stop before the running
   size (`len(Data)+24`, or the on-disk batch size) exceeds `maxBytes`; `maxBytes == 0` ⇒ exactly one.
   The Node also calls `Entries(i, i+1, 0)` with an arbitrary offset `i` to find the containing batch.
   The returned slice may be retained by the Node (it never writes into it; it appends to a clipped copy).
4. `Snapshot().Index` must always equal `FirstIndex()-1` (retention must update `snapshot.meta` together
   with logStartOffset), otherwise the leader would ship a snapshot that leaves a gap.
5. Retention/compaction may advance `FirstIndex` concurrently but **only up to the applied index**
   (< HW); the Node tolerates `ErrCompacted` anywhere (switches the follower to snapshot state).
6. On `NewNode` every entry in the store is assumed durable — run log recovery + fsync first.
7. The adapter should validate `partitionLeaderEpoch == Entry.Term` (cheap corruption check, §5.4.3);
   the Node treats `Data` as opaque.

### The Ready/Advance contract the partition driver must follow

No other Node method may be called between `Ready()` and `Advance()` (enforced: panics). Per Ready, in order:
1. `rd.Snapshot != nil` → `LogStore.ApplySnapshot` durably + restore app state (producer state).
2. `rd.HardState != nil` → persist. If `rd.MustSync` (Term or Vote changed) it MUST be durable before any
   of `rd.Messages` leaves (`FileHardStateStore.Save`). Commit-only changes may be written lazily or
   skipped: the emitted `Commit` is capped at the durable log (`min(commit, durable)`, and lowered on
   truncation/snapshot) so a persisted Commit is always safe to restore in any write order.
3. `rd.TruncateFrom != 0` → `TruncateSuffix(rd.TruncateFrom)` (never ≤ commit; rebuild producer state).
4. `Append(rd.Entries)`.
5. Sync mode (default): make 3–4 durable, then send. `AsyncDurability`: send right away; later call
   `ReportDurable(lastIndex, lastTerm)` from the driver goroutine with the last (index, term) written
   **before** the fsync started (stale reports are ignored by term check). The Node withholds every
   success ack (`MsgAppResp`/`MsgSnapResp`) above the durable index and the leader counts itself
   toward commit only up to its durable index (thesis §10.2.1).
6. Send `rd.Messages` (non-blocking; drop on full queue is fine).
7. Apply `rd.CommittedEntries` in order; HW = last `LastIndex` + 1; complete acks=all waiters.
8. Serve `rd.ReadStates` once applied ≥ `ReadState.Index` (driver needs a timeout: requests are dropped
   on leadership change/message loss; `ctx` must be unique per outstanding request).
9. `rd.SoftState` (leader/state changed): fail pending produce waiters with a retriable error (§5.5).
10. `Advance(rd)`.

Proposals: `ProposeBatch(recordCount, batchBytes, stamp)` — the leader assigns `[FirstIndex,LastIndex]`
and `Term`; `stamp` lets the driver write baseOffset/partitionLeaderEpoch into the batch header before
the entry enters the log. Non-leaders return `*NotLeaderError{Leader: hint}` (`errors.Is(err, ErrNotLeader)`);
proposals are never forwarded (Kafka semantics: client retries against the leader).

### Deviations from the spec (with reasons)

1. **Separate `MsgHeartbeat`/`MsgHeartbeatResp`** instead of "heartbeat = empty MsgApp". Commit in a
   heartbeat is capped at the follower's acknowledged `Match` (etcd's design), which is a prefix proven
   to match, so it is safe; using empty MsgApps with an optimistic `Next` would cause spurious rejections
   that collapse the replication pipeline. Every MsgApp (including empty ones) still does the
   prevLogIndex/prevLogTerm check. Heartbeats carry the ReadIndex sequence (`Context`).
2. **Message additions**: `Force` (transfer vote, bypasses leader stickiness), `Index` (ReadIndexResp),
   `LogTerm` + local `MsgDurable`; rejections echo the rejected `PrevLogIndex` for stale-reject detection.
   Wire layout extends §5.4.3 accordingly (documented in `messages.go`; e.g. MsgAppResp adds `u64 prevIndex`,
   votes add `u8 force`). The transport adds the `u32` length prefix.
3. **Ready additions**: `SoftState`, `MustSync`; `HardState.Vote` (spec: `VotedFor`). HardState file is
   `[magic][u8 version][u64 term][u32 vote][u64 commit][crc32c]` (29 bytes) — version + commit added.
4. **`LogStore.ApplySnapshot`** added (the spec interface had no way to install InstallSnapshot).
   Compaction hook: `MemoryStore.Compact(index, peers, data)`; for `internal/log` it is retention
   (delete closed segments ≤ HW-1, update logStartOffset + snapshot.meta atomically).
5. **InstallSnapshot is a single message** (not chunked): data-partition snapshots are metadata-sized (§5.12).
6. **Leader stickiness**: with CheckQuorum, a follower that heard from the leader within ElectionTick
   ignores higher-term votes (pre-votes always), unless `Force`. Rejoining partitioned nodes without
   pre-vote can still depose the leader via a higher-term response (same as etcd; the pre-vote test's
   negative control demonstrates it).
7. **Election timer** is not reset when a newer term is learned from a vote request/response (Students'
   Guide); only valid leader messages, granted votes and own campaigns reset it.
8. **Pipeline stall recovery**: a Replicate follower with in-flight MsgApps and no progress for
   ElectionTick/2 ticks falls back to Probe (otherwise a lost tail is never resent without new proposals).
9. **Lease reads**: lease = quorum-acked heartbeat send tick + ElectionTick − LeaseMarginTicks(2);
   disabled for the rest of a term once MsgTimeoutNow was sent; a freshly started voter refuses votes for
   one ElectionTick (it may have acked a lease just before crashing). Still **unsafe under clock-rate
   skew / pauses** — a negative-control test demonstrates the stale read.
10. Cluster tests live in `internal/raft/rafttest` rather than `raft/raft_test.go` (an in-package test
    cannot import `transport`, which imports `raft`); the harness is exported for Week 4 reuse.
11. **Static membership** only (membership change/learners skipped as allowed; `Snapshot.Peers` is
    informational). Proposal flow control (max uncommitted bytes) not implemented.

### Tests

- `internal/raft` unit tests (election restriction, vote persisted before reply, one vote per term,
  Figure-8 commit rule, leader self-counts only durable, async ack withholding, stale/reordered responses,
  timer-reset rules, pre-vote term rules, leader stickiness/forced votes, check-quorum, delayed duplicate
  MsgApp does not truncate, commit = min(leaderCommit, lastNew), fast backup over a 50-batch divergence
  with *different batch boundaries* (≤3 rejections, every prevLogIndex a batch end), propose/stamp/
  NotLeaderError, Ready-contract panic, ReadIndex waits for current-term commit, follower ReadIndex,
  restart clamps commit, snapshot install/ignore, leader sends snapshot when compacted, transfer,
  config validation, misrouted messages; `raftLog` gapped lookups/hints/truncation/size limits;
  `MemoryStore` property test vs a 1-offset reference (200 seeds × 200 ops); inflights; HardState codec
  bit-flip detection, file store atomicity + injected rename/fsync failures; codec round trip (20k random
  messages, prefix/trailing rejection, no aliasing, alloc bound) + `FuzzMessageCodec`.
- **Negative controls** (`negative_test.go`): test-only knobs (settable only via `export_test.go`) re-enable
  classic bugs; each must be caught by the harness: ReadIndex without quorum → LINEARIZABILITY;
  heartbeat full commit → STATE MACHINE SAFETY; votes ignoring logs → LEADER COMPLETENESS; committing
  old-term entries by counting (Figure 8) → LEADER COMPLETENESS (seed 109); truncate on any AppendEntries
  → LEADER COMPLETENESS; lease reads under clock skew → LINEARIZABILITY (and the same scenario with
  ReadIndex passes). (A "commit = min(leaderCommit, lastIndex)" knob was tried and dropped: with
  fast backup the first matching MsgApp always truncates the stale tail, so the bug is unreachable
  through the cluster; `TestFollowerCommitUsesLastNewEntry` covers the rule directly.)
- `internal/raft/rafttest` (seeded, parallel subtests `seed=N`; options vary per seed over
  pre-vote × check-quorum × async-durability × 1-offset vs 1..4-offset batches): TestInitialElection,
  TestReElection (incl. no quorum → no leader), TestManyElections, TestPreVotePreventsTermInflation
  (+control), TestCheckQuorumStepDown (+control), TestLeadershipTransfer (up-to-date, lagging, unreachable),
  TestBasicAgree, TestProposeNotLeader, TestFollowerFailure, TestLeaderFailure, TestFailNoAgree,
  TestConcurrentProposals, TestRejoin, TestBackup, TestMessageCount, TestUnreliableAgree, TestPersist1/2/3,
  TestFigure8, TestFigure8Unreliable (drop/dup/delay/heavy reorder + partitions + crashes + compaction +
  tick skips), TestChurn/{reliable,unreliable}, TestSnapshotInstall, TestReadIndex (incl. stale leader),
  TestReadIndexChaos, TestLeaseReads, TestLeaseReadsChaos.
- Harness invariants after **every** Step/Tick/fsync/Ready: ≤1 leader per term; leader completeness at
  election; log matching (cumulative prefix hash per (index, term)); state machine safety (same entry and
  same state hash at every index, contiguous offsets, snapshot state matches); persist-before-send (no
  outgoing message carries an unpersisted term, no granted vote leaves before it is durable); commit monotonic per
  incarnation; applied ≤ commit; term never decreases; vote never changes within a term; persisted
  Commit covered by the durable log; ReadIndex ≥ max commit at issue time; raft's own panics
  (leader truncation, overwrite of committed entry, batch-boundary DebugChecks).
- Async-durability mode models the page cache: `MemDisk` keeps a durable image; a crash discards every
  write (including truncations) not covered by a completed simulated fsync.
- Seeds: `RAFT_SEEDS=N` (per test), `RAFT_SEED=S` (reproduce one), `RAFT_SEED_BASE`, `RAFT_DEBUG=1`
  (compact per-node log, e.g. `U0000088 T17 N1 FOLLOWER→CANDIDATE term=2 last=(3,1) commit=3`).
  Every failure prints the seed, a rerun command and a state dump of all nodes.

### Verification (run on darwin/arm64, go1.26.5)

| Command | Result |
|---|---|
| `go build ./...` | OK |
| `go vet ./internal/raft/...` | clean |
| `gofmt -l internal/raft` | clean |
| `GOOS=linux GOARCH=amd64 go build ./internal/raft/...` (and `go vet`) | OK |
| `go test -race -count=1 ./internal/raft/...` ×3 | all `ok` (raft ≈8–10 s, rafttest ≈8–10 s, transport ≈1–2 s; wall ≈9–12 s) |
| `go test -count=1 -v ./internal/raft/rafttest/` (defaults) | 1,380 seeded cluster runs, 0 failures (60/30/15 seeds per test) |
| `RAFT_SEEDS=1000 go test -race -count=1 ./internal/raft/rafttest/` | 30 test configs × 1,000 seeds = 30,000 runs, 0 failures, 98 s (final code) |
| `RAFT_SEEDS=30000 go test -count=1 ./internal/raft/rafttest/` | 30 × 30,000 = 900,000 runs, 0 failures, 478 s (run just before the harness gained the persist-before-send message check, raft code identical) |
| `go test -run '^$' -fuzz '^FuzzMessageCodec$' -fuzztime 30s ./internal/raft/` | PASS, 9.66 M execs, 123 corpus entries, 0 crashers |
| `go test -coverpkg=./internal/raft/... ./internal/raft/...` | 91.1 % of statements (raft alone 88.7 %, transport 85.0 %) |

Earlier during development: `RAFT_SEEDS=10000` (290,000 runs) passed in 124 s.

### Known gaps / TODO (Week 4+)

- `transport/tcp.go` (multiplexed, per-peer writer goroutine, bounded queue, drop on full, backoff).
- The harness's crash model needs `*raft.MemoryStore` (`Clone`, `Compact`, `AllEntries`); to re-run the
  suite on `internal/log`, add a `MemDisk`-like adapter over the real log + fault FS.
- No membership changes / learners; no snapshot chunking; no proposal flow control (max uncommitted).
- `staticcheck`/`golangci-lint` not run (not installed; `go run` of staticcheck was not permitted in this
  session). Fuzz corpus lives in the Go build cache, not committed under `testdata/fuzz`.
- Leader sends are emitted in the same Ready as its own appends (sync mode); parallel leader fsync is
  available through `AsyncDurability`.

---

## Phase 1A: storage (`internal/fs`, `internal/log`) — Milestone M1a, Week 1

Status: **complete**. Everything listed under "Verification" was run on 2026-11-13 and passed.

### Files

```
internal/fs/fs.go            FS + File interfaces, OSFile (raw *os.File for mmap/sendfile), ReadFile, WriteFileAtomic
internal/fs/osfs.go          fs.OS (real FS); SyncDir = open dir + fsync
internal/fs/osfs_linux.go    File.Sync = fdatasync(2) via SyscallConn().Control (EINTR-retried)
internal/fs/osfs_other.go    File.Sync = (*os.File).Sync (F_FULLFSYNC on darwin)
internal/fs/faultfs.go       FaultFS: deterministic in-memory FS with crash simulation + fault injection
internal/log/batch.go        RecordBatch v2 view: header accessors, Validate, Stamp, ParseHeader/Put, SplitBatches
internal/log/record.go       record codec (zigzag varints), NewBatch, NewControlBatch, ControlType, Batch.Records
internal/log/index.go        sparse offset index (8 B) + time index (12 B): mmap or heap mode, seal/unseal, validation
internal/log/index_unix.go   mmap/munmap/msync via golang.org/x/sys/unix (//go:build unix)
internal/log/index_other.go  portable fallback (heap mode only)
internal/log/segment.go      segment files, append/track, findPosition/alignDown, recover (CRC scan), loadTrusted, truncateTo, seal
internal/log/log.go          Log: Config, Open/recovery, Append*, Read*, terms, Truncate*, ResetTo, Close, checkpoint
internal/log/flusher.go      SyncPolicy/ParseSyncPolicy, group-commit flusher, durable watermark + waiters
internal/log/retention.go    RetentionPolicy, ApplyRetention
tests: fs_test.go; batch_test.go log_test.go property_test.go crash_test.go flusher_test.go retention_test.go
       fuzz_test.go bench_test.go helpers_test.go; testdata/fuzz/FuzzOpenCorrupted/* (2 regression inputs found by the fuzzer)
```

Dependencies added (all allowed by §4.8): `golang.org/x/sys v0.49.0` (mmap), `pgregory.net/rapid v1.3.0` and
`go.uber.org/goleak v1.3.0` (tests only). `go get` rewrote the go directive `go 1.26` → `go 1.26.0`
(equivalent); the module line is untouched.

### Exported API summary

**`internal/fs`**
- `type FS interface { OpenFile; Create; Open; Rename; Remove; MkdirAll; ReadDir; SyncDir; Stat }`.
  `type File interface { Read; ReadAt; Write; WriteAt; Close; Name; Stat; Sync; Truncate }`.
  `type OSFile interface { OSFile() *os.File }` is implemented by `fs.OS` files only (mmap here; sendfile in M1b).
- `var OS FS`. `ReadFile(fsys, name, maxBytes)`. `WriteFileAtomic(fsys, name, data, perm)` does tmp → fsync →
  rename → fsync dir. The Raft `hardstate`/`snapshot.meta` code can reuse it.
- `NewFaultFS(seed) *FaultFS`. `(*FaultFS).Crash(mode) *FaultFS` returns the "rebooted" FS, and the old instance
  and its handles then fail with `ErrCrashed`. The modes are `CrashDropUnsynced` (power loss: files revert to
  their last successful `Sync`, directories to their last `SyncDir`), `CrashTearUnsynced` (also persists a
  random in-order prefix of the unsynced writes, tearing the next one at a random byte) and `CrashProcess`
  (everything survives). Fault injection: `SetFaults(Faults{SyncErrorRate, WriteErrorRate, SyncLatency,
  WriteLatency, DropDirtyOnSyncError /*fsyncgate*/, Match})`, `FailNextSyncs(n)`, `FailNextWrites(n)`. Also
  `Stats()` and `SetClock`. Injected errors wrap `syscall.EIO` (`ErrInjected`). All randomness comes from the seed.

**`internal/log`: batches**
- `type Batch []byte` (zero-copy view). Accessors: `BaseOffset`, `LastOffset`, `LastOffsetDelta`, `BatchLength`,
  `PartitionLeaderEpoch`, `Term` (epoch as uint64; −1 → 0), `Magic`, `CRC`, `Attributes`, `Compression`,
  `IsControl`, `IsTransactional`, `BaseTimestamp`, `MaxTimestamp`, `ProducerID`, `ProducerEpoch`,
  `BaseSequence`, `RecordsCount`, `Size`.
- `Validate(maxBytes)` checks length bounds, magic == 2, codec ≤ 4, `lastOffsetDelta`/`recordsCount` ≥ 0, exact
  framing and CRC-32C over `[21,end)`. It never panics and makes 0 allocations (tested). `Stamp(baseOffset, term)`
  writes bytes 0–7 and 12–15 only, so it needs **no re-CRC** (tested: CRC unchanged, still valid).
  `ComputeCRC`/`UpdateCRC` exist for builders.
- `NewBatch(BatchOptions, ...Record)`, `DefaultBatchOptions()`, `NewControlBatch(opts, ControlLeaderChange, v)`,
  `ControlType(b)`, `b.Records()` (uncompressed only, otherwise `ErrCompressed`; allocation bounded by
  `len(b)/7`; keys and values alias the batch), `ParseHeader`/`(*BatchHeader).Put`, and `SplitBatches(buf, max)`.
  `SplitBatches` splits and validates a region of concatenated batches.

**`internal/log`: Log**
- `Open(Config) (*Log, error)` runs crash recovery (below). The `Config` fields are `Dir`, `FS`,
  `SegmentBytes` (128 MiB), `SegmentAge` (segment.ms), `IndexIntervalBytes` (4096), `MaxIndexBytes` (10 MiB),
  `MaxBatchBytes` (8 MiB), `Sync SyncPolicy`, `InitialOffset`, `OnDurable func(int64)`, `Now` and `Logger`.
- Writes go through one internal mutex. QuorumLog calls them from the driver goroutine plus the retention task.
  - `Append(b, term)` validates, stamps `baseOffset = LEO` and `epoch = term` in place, and rolls if needed
    (size, age, index full, or 32-bit relative offset). It returns `{BaseOffset, LastOffset}`. Errors:
    `ErrTermRegression`, `ErrTermOverflow` (term > int32), `ErrTooLarge`, `ErrCorrupt`.
  - `AppendStamped(b)` is the follower/replay path: `b.BaseOffset()` must equal LEO (`ErrOffsetMismatch`), and the
    batch must be stamped with a non-decreasing term.
  - `TruncateSuffix(from) (newLEO, error)` removes the batch containing `from` and everything after it. It is
    durable on return (unless the policy is `never`). Waiters on removed offsets get `ErrTruncated`.
  - `TruncatePrefix(offset)` advances `LogStartOffset` (≤ LEO), records `StartTerm` = term(offset−1) durably, and
    deletes whole segments below it.
  - `ResetTo(offset, term)` discards the whole log and restarts empty at `offset` (InstallSnapshot). It is
    crash-safe through a reset flag in the checkpoint.
  - `Roll()`, `Sync()` (synchronous fsync of everything, regardless of policy), `ApplyRetention(policy, hw)`, and
    `Close()`. `Close` does a final fsync, trims the indexes and writes the clean-shutdown record.
- Reads are lock-free and run concurrently with writes.
  - `Read(offset, maxBytes)` and `ReadUpTo(offset, maxBytes, upTo /*HW*/)` return `*FileRegion{File, Pos, Len,
    StartOffset}`: whole batches from **one** segment, starting with the batch containing `offset`, aligned down
    to a batch boundary ≤ `maxBytes`, but always ≥ 1 batch. Batches with `lastOffset ≥ upTo` are excluded.
    `offset == LEO` gives an empty region; offsets outside `[LogStartOffset, LEO]` give `ErrOffsetOutOfRange`.
    The region pins its segment (refcount) until `Release()`, so retention renames the files to `*.deleted` but
    unlinks them only after the last release. `Bytes()` and `WriteTo(w)` are the copy path. M1b's sendfile
    will type-assert `File.(fs.OSFile)` and use `Pos`/`Len`.
  - `ReadBatches(lo, hi, maxBytes) ([]Batch, error)` returns copies of the batches overlapping `[lo,hi)`, with
    every CRC verified, ≥ 1 batch, stopping before `maxBytes`. Corrupt stored bytes return a wrapped `ErrCorrupt`.
  - `BatchInfoAt(off)` returns `{BaseOffset, LastOffset, Term, MaxTimestamp, Position, Size}`.
    `TermAt(off)` returns the term of the batch containing `off`; `LogStartOffset-1` gives `StartTerm`, lower
    offsets give `ErrCompacted`, and `≥ LEO` gives `ErrOffsetOutOfRange`.
  - `TermStart(term)` returns the first offset whose term is ≥ `term` (binary search, for Raft fast backup).
    `OffsetForTimestamp(ts)` uses the time index.
  - `LogEndOffset()`, `LogStartOffset()`, `StartTerm()`, `LastOffsetAndTerm()` (empty log →
    `(LogStartOffset-1, StartTerm)`), `DurableOffset()`, `WaitDurable(ctx, off)`, `Stats()` and `Dir()`.
- `ParseSyncPolicy("always" | "never" | "interval=10" | "interval=10ms")`.

### API for the Raft `LogStore` adapter (spec §5.4.2; `internal/raft/storage.go`)

An adapter (planned for `internal/partition` or a small `raft`↔`log` shim) maps one-to-one:

| `raft.LogStore` | `internal/log` |
|---|---|
| index space | Open the log with **`Config{InitialOffset: 1}`** so that Kafka offset == Raft index and index 0 stays the "empty log" sentinel. Kafka clients accept a partition whose earliest offset is 1. The alternative is an `index = offset+1` shim. |
| `FirstIndex()` | `LogStartOffset()` |
| `LastIndex()` | `LogEndOffset()-1`. For an empty log this is `LogStartOffset()-1`, the snapshot index. |
| `Term(i)` | `TermAt(i)`. It is defined for every offset in `[FirstIndex-1, LastIndex]`, mid-batch too, and `FirstIndex-1` gives `StartTerm()`. Error mapping: `ErrCompacted` stays `raft.ErrCompacted`; `ErrOffsetOutOfRange` for `i ≥ LEO` becomes `raft.ErrUnavailable`. |
| `Entries(lo, hi, maxBytes)` | `ReadBatches(lo, hi, maxBytes)` → `Entry{Term: b.Term(), FirstIndex: b.BaseOffset(), LastIndex: b.LastOffset(), Data: b}`. `maxBytes == 0` returns exactly one batch. Check `lo < LogStartOffset` → `ErrCompacted` and `hi > LEO` → `ErrUnavailable` before calling. |
| `Append(entries)` | `AppendStamped(Batch(e.Data))` per entry. The leader's driver already stamped them with `Append(b, term)`, or with `Stamp` before proposing. **A no-op entry (`Data == nil`) must be materialised as `NewControlBatch(opts, ControlLeaderChange, nil)` stamped with `(FirstIndex, Term)`** so that it occupies its offset on disk. |
| `TruncateSuffix(from)` | `TruncateSuffix(from)`. Raft only passes batch boundaries, where "drop batches with FirstIndex ≥ from" and the Kafka "drop the batch containing from" semantics coincide. |
| `ApplySnapshot(snap)` | `ResetTo(snap.Index+1, snap.Term)`. Persist `snap.Peers`/`snap.Data` in `raft/snapshot.meta` with `fs.WriteFileAtomic`. |
| `Snapshot()` | `{Index: LogStartOffset()-1, Term: StartTerm()}` plus the stored peers/data. After `TruncatePrefix`/`ApplyRetention` the log advances these itself. |
| durability for acks | `Config.OnDurable(off)` → driver inbox ("durable up to X"), or `WaitDurable(ctx, off)`. `DurableOffset()` never runs ahead of an fsync under `always`/`interval`. |
| conflict hint (XTerm/XIndex) | `TermStart(term)` and `BatchInfoAt(off)` |

### On-disk format (per partition directory)

```
<dir>/00000000000000000000.log        concatenated RecordBatch v2, byte-identical to what was appended (stamped)
<dir>/00000000000000000000.index      sparse offset index: [relOffset u32 BE | position u32 BE] per entry
<dir>/00000000000000000000.timeindex  sparse time index:   [maxTimestampSoFar i64 BE | relOffset u32 BE]
<dir>/recovery-checkpoint             36 B: magic "QLCP" u32 | version u16 (1) | flags u16 (bit0 clean, bit1 reset)
                                      | logStart i64 | startTerm u64 | recoveryPoint i64 | crc32c u32 (over bytes 0..31)
<dir>/*.deleted, recovery-checkpoint.tmp   transient; removed by Open
```
- File names are the base offset zero-padded to 20 digits. Other names in the directory (`raft/`,
  `producer-state.snapshot`) are ignored.
- **Offset index:** one entry for the first batch, then one for each batch that starts ≥ `IndexIntervalBytes`
  after the last indexed batch. The first entry is always (0,0); relOffsets and positions strictly increase.
  A segment rolls before the 32-bit relOffset or position overflows, or when an index has only one free slot.
  That slot is reserved for the final time-index entry.
- **Time index:** entry `(ts, rel)` means "the batch at rel is the first batch whose maxTimestamp is ts, and every
  earlier batch has a smaller maxTimestamp". It is appended on the offset-index cadence when the max timestamp
  increased, plus one final entry when the segment is sealed. Lookup takes the last entry with
  `ts < target`, then scans forward.
- **Active segment index:** in mmap mode the file is preallocated (sparse ftruncate) to `MaxIndexBytes` and
  mapped `MAP_SHARED` read/write. Sealing (roll/Close) does msync (when syncing) → munmap → ftruncate to the
  valid entries → fsync (when syncing) → mmap read-only. The file is never truncated while mapped. In heap mode
  (non-Unix, FaultFS) the entries live in memory and are written back on seal.
- **Recovery point R:** every offset < R is known fsynced. R is set to LEO at Open (after syncing what was
  recovered) and to the new base at each roll (policies `always`/`interval`). It is lowered durably by
  `TruncateSuffix` before any file changes, and `Close` sets it to LEO together with the clean flag.

### Recovery (`Open`)

1. Read the checkpoint. If it is unreadable, ignore it with a warning (see gaps). List `*.log`. Delete `*.deleted`,
   `recovery-checkpoint.tmp` and orphan index files. If the reset flag is set, delete every segment. Delete
   segments wholly below logStart. If the first segment starts above logStart, return `ErrCorruptLog`.
2. **Trusted** segments are those wholly below R, or the last segment after a clean shutdown. The index files
   must have sane sizes and pass validation (entry 0 = (0,0), strictly increasing, inside the log). Then the
   headers from the last index entry to EOF must frame exactly, with contiguous offsets, epoch ≥ 0 and
   non-decreasing terms. If anything fails, the indexes are rebuilt by a full CRC scan in which **any**
   invalid batch is `ErrCorruptLog` (never truncate durable data, spec §5.2.6).
3. **Untrusted** segments get a full CRC scan with 1 MiB chunked reads and an index rebuild. A batch is invalid
   if it is a short header or batch, has a bad length or magic or codec, has a CRC mismatch, has
   `baseOffset != expected`, has an epoch < 0, or has a term below the previous batch's term (the epoch is
   outside the CRC, so these invariants catch most corruption of it). If the invalid batch starts **below R**,
   Open returns `ErrCorruptLog`. Otherwise the file is truncated there (torn tail), and later segments, now
   unreachable, are deleted.
4. Segments must be contiguous. A gap after an untrusted torn segment drops the rest; any other gap is
   `ErrCorruptLog`. If LEO < logStart, the log restarts empty at logStart. If LEO < R, Open returns
   `ErrCorruptLog`.
5. Recovered closed segments are sealed with fsync, and the active segment's log is fsynced. Data found by the
   scan may only have been in a crashed process's page cache. Then the checkpoint is written with clean=0 and
   R=LEO (fsync + rename + dir fsync) before any append.

### Durability (what is fsynced where)

| Event | `always` | `interval=N` | `never` |
|---|---|---|---|
| Append | flusher goroutine: one fsync round covers everything written so far (group commit). The flusher fsyncs without holding the writer lock, so appends continue during the fsync. | same, at most one round per N (an idle-then-write is synced immediately) | none; durable offset = written offset |
| Roll | fsync old .log + both indexes; create new files; write checkpoint (R = new base) + dir fsync | same | no fsync (dir marked dirty for `Sync`) |
| TruncateSuffix | lower R durably first if needed, rename later segments `*.deleted` + dir fsync, ftruncate, fsync the file | same | same except the final file fsync |
| TruncatePrefix / ApplyRetention / ResetTo | checkpoint (logStart, startTerm) written durably **before** files are renamed or removed; dir fsync | same | same |
| `Sync()` / `Close()` / `Open()` | fsync every dirty segment (+ indexes of closed ones) and the dir; Close also writes the clean record | same | same |

- An fsync error is **sticky** (fsyncgate). Every waiter fails with `ErrLogFailed`, all later writes fail, the
  durable offset never advances again, and `Close` does not write the clean record.
- A failed `write(2)` (EIO/ENOSPC) is **not** sticky: nothing was published (size not advanced), and the next
  append overwrites the partial bytes.
- An fsync that was in flight when `TruncateSuffix`/`ResetTo` started is discarded. A generation counter is
  bumped *before* any file is touched (bug found by `TestWaitDurableTruncated`, fixed).

### Concurrency model

- Writers take `Log.mu`.
- Readers load `leo` (atomic), then the COW `atomic.Pointer[[]*segment]` list, then `tryAcquire` the segment.
  `tryAcquire` is a CAS refcount that fails once the list's reference has been dropped, and the reader then
  reloads.
- Published `size` (atomic) is stored after the bytes are written, so readers never read past it.
- Index mappings are protected by a per-index `sync.RWMutex`. Lookups take `RLock`; seal, unseal and unmap take
  `Lock`.
- The flusher lock is always taken after `Log.mu` (lock order).
- `goleak.VerifyTestMain` runs in both packages, and `-race` is clean.

### Deviations from the spec (with reasons)

1. **fsync failure does not `panic`** (the §5.3.2 sketch panics). The library makes the error sticky
   (`ErrLogFailed`) and reports it to every waiter. The broker (`cmd/qlogd`) should exit on it to stay
   fail-stop. A panic inside a library goroutine cannot be handled or tested cleanly.
2. **Index lookups take an RWMutex** rather than relying only on an atomic entry count (§5.2.4). The mutex makes
   unmap/remap on seal, unseal (truncation into a closed segment) and close safe against concurrent readers
   (no SIGSEGV on a dead mapping). The cost is one uncontended `RLock` per lookup.
3. **Portable heap-mode index** for non-Unix builds and for non-OS files (FaultFS). mmap is used only when
   the file implements `fs.OSFile` on a `unix` build.
4. **`TruncateSuffix(from)` removes the batch containing `from`** (Kafka `truncateTo`), not only batches with
   `base ≥ from`. For Raft's boundary offsets the two are identical, and this way LEO can never stay above
   `from`.
5. **The recovery checkpoint also stores `logStart`, `startTerm` and a reset flag.** Prefix truncation inside a
   segment needs a durable log start, Raft needs the term at `logStart-1` (the snapshot point), and `ResetTo`
   needs crash atomicity. `raft/snapshot.meta` (peers, opaque data) stays the Raft layer's file. The checkpoint
   is written at open, roll, truncation, retention and close, not on a periodic timer.
6. **Policy `never` does not fsync at roll** and does not advance R there. "Never" means never on the write
   path. Recovery then scans every segment written since the last Open/Close, which is the honest cost of that
   policy.
7. **`segment.ms` is measured with the broker clock from the segment's first append.** Kafka uses the first
   batch's timestamp. Producer clocks must not control rolling (§5.12 clock skew).
8. **`OffsetForTimestamp` returns the base offset of the first batch** with `maxTimestamp ≥ ts`, not the exact
   record offset. The broker never decodes records; consumers skip the earlier records in that batch.
9. **`Append` always verifies the CRC** (≈4–8 GB/s with hardware CRC32C; see RESULTS.md), even though the produce
   handler also validates. This is defence in depth, and the cost is small next to the write.
10. **`DurableOffset` under `never` equals the written offset.** The policy acknowledges on write; use `Sync`
    for a real fsync.

### Known gaps / TODOs

- **Linux execution is not verified on this machine.** The code is cross-compiled and vetted for linux/amd64
  and linux/arm64 (the fdatasync and mmap build tags compile), but the tests ran only on darwin/arm64. The
  Docker daemon (colima) was not running and was not started. Run
  `docker run --rm -v "$PWD":/src -w /src golang:1.26 go test -race ./internal/fs/... ./internal/log/...` in CI.
- **Torn tail vs. corruption above R:** in the untrusted region (data written since the last Open/roll) a
  corrupted batch is indistinguishable from a torn write, so it is truncated, as Kafka does. Below R it is
  fatal.
- **Trusted segments are not CRC-checked at open**, only their tail headers. Bit rot in a closed segment is
  detected at read time by `ReadBatches`. The zero-copy `Read`/`FileRegion` path cannot check it, and
  consumers verify the CRC themselves. A background scrubber is future work.
- **A partitionLeaderEpoch flip in the very last batch is undetectable**: it is outside the CRC, and no later
  batch exists to check term order against. Kafka's format has the same blind spot.
- **A lost or corrupt checkpoint** makes Open fall back to a full scan with truncation allowed, logStart = the
  first segment's base and startTerm = 0. The Raft layer must re-assert its snapshot (`TruncatePrefix`).
  Atomic replace makes a torn checkpoint impossible, so this only happens through bit rot or deletion.
- Every segment's fd and index mapping is opened eagerly. There is no lazy open for cold segments (EMFILE risk
  at thousands of segments, §13.2).
- Not implemented (later milestones or stretch): producer-state snapshot, the Kafka "warm tail" index search,
  `fallocate` of `.log` files, Prometheus metrics, and a retention scheduler (`ApplyRetention` is a method for
  the broker's background task).
- FaultFS simplifications: `MkdirAll` is durable immediately; tearing persists an **in-order** prefix of a
  file's unsynced writes (no reordering of pages within a file); there is no disk-full quota (use
  `FailNextWrites`).
- Readers must bound reads by the HW. A read of the uncommitted suffix racing `TruncateSuffix` can return a
  short read or `ErrCorrupt` (never a panic, never another batch's bytes past the published size).

### Verification (exact commands, darwin/arm64, go1.26.5, 2026-11-13)

All commands were run from the repo root.

| Command | Result |
|---|---|
| `go build ./...` | OK |
| `go vet ./...` | OK |
| `go test -race -count=1 ./internal/fs/... ./internal/log/...` | `ok internal/fs`, `ok internal/log` (≈14 s). 78 top-level tests pass (15 in fs, 59 in log, 4 fuzz seed corpora), plus 161 subtests (most tests run on both `fs.OS`/mmap and FaultFS/heap). 0 failures, 0 data races, goleak clean. |
| `go test -cover ./internal/fs/... ./internal/log/...` | fs 82.3 %, log 85.3 % of statements |
| `go test -count=1 -timeout 60m -run TestCrash -v ./internal/log/ -args -crash.iters=10000` | PASS in 60.7 s. **10,000 iterations per crash test.** Exact synced prefix after power loss: 7,793 of 10,000 iterations actually lost unsynced batches. Torn writes: 3,453 recovered a partial unsynced tail. Durable watermark under `always`/`interval` with truncations: 5,224 lost a non-durable tail, 8,340 had durable data, and none lost a durable batch. Prefix truncation/retention crash: PASS. The default `go test` uses 300/300/200/100 iterations. |
| `go test -run TestPropertyFaultFS ./internal/log/ -args -rapid.checks=5000` | `[rapid] OK, passed 5000 tests` (3.2 s) |
| `go test -run TestPropertyFaultFS ./internal/log/ -args -rapid.checks=2000 -rapid.steps=200` | `[rapid] OK, passed 2000 tests` (7.3 s) |
| `go test -run TestPropertyOSFS ./internal/log/ -args -rapid.checks=1000` | `[rapid] OK, passed 1000 tests` (14.7 s). `-rapid.checks=300 -rapid.steps=200`: passed 300 (31.6 s). |
| `go test -run='^$' -fuzz='^FuzzBatchValidate$' -fuzztime=30s ./internal/log/` | PASS, 3,589,711 execs |
| `go test -run='^$' -fuzz='^FuzzRecords$' -fuzztime=30s ./internal/log/` | PASS, 3,832,000 execs |
| `go test -run='^$' -fuzz='^FuzzOpenCorrupted$' -fuzztime=30s ./internal/log/` | PASS, 1,002,618 execs. Earlier runs found two real issues, fixed and kept as regression inputs in `testdata/fuzz/FuzzOpenCorrupted/`: (a) a corrupted header in a trusted segment surfaced as a bare `ErrBadMagic`, now a wrapped `ErrCorrupt`; (b) a flipped `partitionLeaderEpoch` (outside the CRC) was accepted, now recovery enforces epoch ≥ 0 and non-decreasing terms. |
| `go test -run='^$' -fuzz='^FuzzCheckpoint$' -fuzztime=30s ./internal/log/` | PASS, 683,867 execs |
| `go test -count=1 -run '^$' -bench . -benchmem ./internal/log/` | numbers in `docs/RESULTS.md`; raw lines below |
| `GOOS=linux GOARCH=amd64 go build ./...` | OK |
| `GOOS=linux GOARCH=arm64 go vet ./internal/...` | OK. `GOOS=linux GOARCH=arm64 go test -c` for both packages compiles. `GOOS=windows go vet` (heap-index fallback) OK. |

Mutation checks (each bug injected by hand, the tests run, the bug reverted):
- An off-by-one in `findPosition` and the "≥ 1 batch" guard in `readSegment` are caught by both property tests.
- Each of these is caught by the crash tests: no dir-fsync after a roll under `never`, not fsyncing closed
  segments in `Sync`, no fsync after `TruncateSuffix`, and not lowering R before a truncation.

Raw benchmark output (Apple M1 Pro, macOS 15.7.7, one run):
```
BenchmarkAppend/sync=never/batch=1KiB-10         	  379054	      2948 ns/op	 365.69 MB/s	       0 B/op	       0 allocs/op
BenchmarkAppend/sync=never/batch=16KiB-10        	  129788	      8964 ns/op	1821.98 MB/s	       0 B/op	       0 allocs/op
BenchmarkAppend/sync=never/batch=64KiB-10        	   56979	     21447 ns/op	3037.68 MB/s	       1 B/op	       0 allocs/op
BenchmarkAppend/sync=always/batch=1KiB-10        	     262	   4762266 ns/op	   0.23 MB/s	     145 B/op	       3 allocs/op
BenchmarkAppend/sync=always/batch=16KiB-10       	     246	   4649654 ns/op	   3.51 MB/s	     144 B/op	       3 allocs/op
BenchmarkAppend/sync=always/batch=64KiB-10       	     244	   4678738 ns/op	  13.92 MB/s	     144 B/op	       3 allocs/op
BenchmarkAppendFaultFS/batch=1KiB-10             	 1889638	       823.1 ns/op	1309.76 MB/s	    1380 B/op	       1 allocs/op
BenchmarkAppendFaultFS/batch=16KiB-10            	  215871	      7732 ns/op	2112.42 MB/s	   17209 B/op	       1 allocs/op
BenchmarkAppendFaultFS/batch=64KiB-10            	   61732	     34639 ns/op	1880.82 MB/s	  143500 B/op	       1 allocs/op
BenchmarkGroupCommit-10                          	    9472	    122981 ns/op	   8.77 MB/s	         0.03209 fsyncs/batch	     155 B/op	       3 allocs/op
BenchmarkRead-10                                 	    3006	    365309 ns/op	2870.38 MB/s	 1016155 B/op	       3 allocs/op
BenchmarkValidate64KiB-10                        	  148158	     16148 ns/op	4034.45 MB/s	       0 B/op	       0 allocs/op
```

---

## Phase 2 (simplified): dashboard/deploy/docs

Scope: the dashboard, deploy files, CI, and docs from `docs/SCOPE.md`. Backend packages (`partition`, `cluster`,
`httpapi`, `raft/transport/tcp.go`, `cmd/*`, `test/*`) were not touched.

| File | Contents |
|---|---|
| `internal/dashboard/dashboard.go` | `func Handler() http.Handler`. It serves `//go:embed static`, maps `/` to `index.html`, and sets explicit Content-Types (independent of the host's mime.types). Every asset gets a sha256 ETag with `Cache-Control: no-cache` (a redeploy shows up at once, and an unchanged asset costs a 304). Also: `nosniff`, a strict same-origin CSP on HTML, 404 for unknown paths (including directories), 405 + `Allow` for methods other than GET/HEAD. |
| `internal/dashboard/static/{index.html,app.css,app.js}` | Single page in vanilla JS with no build step and no CDN. Panels: cluster (polls `/v1/cluster` every 1 s), events, topics, produce (acks, optional producer id/sequence, "Retry last" to show `duplicate:true`), live tail (long-poll `wait_ms=5000`), consumer-group commit/fetch, a 7-step guided demo, and Kill/Restart in demo mode. Light/dark via `prefers-color-scheme`, works down to 390 px wide, every input has a label, aria-live result lines. Errors show the JSON `code` + `error` + HTTP status. All server data goes through `textContent`. |
| `internal/dashboard/dashboard_test.go` | Handler tests: assets and content types, CSP, no external URLs, 404s, 405/HEAD, ETag/304, mounting under a mux next to `/v1/` routes. |
| `internal/dashboard/devserver_test.go` | A **fake in-memory implementation of the SCOPE.md API**: elections about 1.5 s after a leader is killed, follower catch-up, long-poll, idempotent sequences, offsets, demo kill/restart. `TestFakeAPIFailoverFlow` exercises it over real HTTP. `TestDevServer` serves dashboard + fake for manual work: `QL_DASHBOARD_DEV=127.0.0.1:8099 go test -run TestDevServer -timeout 0 ./internal/dashboard/`. `QL_FAKE_NO_PERNODE=1` / `QL_FAKE_NO_DEMO=1` switch to the plain contract shape (one row per group, no demo flag). |
| `Dockerfile`, `.dockerignore` | `golang:1.26` builder, `CGO_ENABLED=0 go build -trimpath -ldflags "-s -w"` for `qlogd` and `qlog`. Final image `gcr.io/distroless/static-debian13:nonroot`, `/data` owned by 65532, `ENV PORT=8080`, `ENTRYPOINT qlogd`, `CMD --demo --data-dir /data`. Uses only plain Dockerfile syntax (no BuildKit cache mounts), so it builds the same everywhere. |
| `deploy/docker-compose.yml` | 3 non-demo nodes (`qlogd1..3`), Raft on `:7001` and forwarding via `--http-peers` on `:8080` inside the compose network, HTTP published on 8081–8083, named volumes `qlog1..3`, `restart: unless-stopped`. |
| `render.yaml` | Blueprint: `type: web`, `runtime: docker`, `plan: free`, `healthCheckPath: /v1/health`, `autoDeployTrigger: commit`, `PORT=8080`. |
| `.github/workflows/ci.yml` | `actions/checkout@v7`, `actions/setup-go@v7` (Go 1.26). Steps: gofmt check, `go vet`, `go test -race`, `go build`. A second job runs `docker build` (no push) plus a smoke test: run the image and wait for `/v1/health`. |
| `README.md`, `LICENSE` (MIT, Rahul Nikhate, 2026), `docs/DESIGN.md`, `docs/DEPLOY.md` | Portfolio README, design doc, deploy guide (local / Render free / Oracle Always Free + Caddy). |

### Dashboard ↔ API contract assumptions (for the backend)

These are all read defensively, and the dashboard works without them:

- **`"demo": true` in `GET /v1/cluster`** turns on the Kill/Restart buttons (SCOPE.md has no explicit flag).
  `httpapi.ClusterView.Demo` already provides it. `?demo=1` / `?demo=0` in the page URL override it.
- **An optional `"node"` per group row** (one row per (node, group)) lets the dashboard show every replica, a
  "Behind" column, and step 6's measured catch-up. `httpapi.Demo.clusterInfo` provides it. Without it, each row is the
  answering node's view, and step 6 only waits for the node to come back up.
- **Leader:** a row with `role == "leader"` on a node that is up. Otherwise the highest-term row's `leader` hint.
  `leader: 0` means none.
- **Producer sequence** follows Kafka base-sequence semantics: the next request's `sequence` = previous + number of
  messages. This matches `partition/producerstate.go`. A new `producer_id` starts at 0. The guided demo sends
  single-message requests through an idempotent producer and retries any error except BAD_REQUEST / NOT_FOUND /
  OUT_OF_ORDER_SEQUENCE for up to 20 s. That makes retries during an election safe. If it gives up, it switches to a
  fresh producer id.
- **Outcome-unknown codes** (`TIMEOUT`, and client-side timeouts or network errors) are counted separately and left
  out of step 7's loss check. Step 7 reads the topic from offset 0 and reports acks=all loss, acks=leader loss
  (allowed), extra copies, and messages from other producers.
- **Tail:** follows `next_offset`. On `BAD_REQUEST` (offset out of range after a data reset) it re-reads
  `high_watermark` and resumes there. It never busy-loops, even if a server ignores `wait_ms`.
- **Mounting:** `httpapi.Config.Dashboard = dashboard.Handler()` (or `mux.Handle("/", dashboard.Handler())`). More
  specific `/v1/` and `/metrics` patterns win.

### Deviations (with reason)

- **The distroless base is `static-debian13`, not `static-debian12`.** The distroless README (checked 2026-11-13) lists
  only Debian 13 images. Tags not listed there "are considered deprecated and are no longer updated". To go back,
  change one line.
- **Compose was re-checked against `cmd/qlogd/main.go`** once that file existed. It uses `--id`, `--peers`,
  `--http-peers` (needed so followers can forward writes; it also fills `nodes[].addr` in `/v1/cluster`),
  `--raft-addr 0.0.0.0:7001`, `--http-addr 0.0.0.0:8080`, `--data-dir /data` and `--fsync always`. The image's
  `--demo --data-dir /data` relies on demo mode listening on `:$PORT` (default 8080), which `runDemo` does.

### Verification (darwin/arm64, go1.26.5, 2026-11-13)

| Command | Result |
|---|---|
| `gofmt -l internal/dashboard` | empty |
| `go vet ./internal/dashboard/...` | OK |
| `go test -race -count=1 ./internal/dashboard/...` (×3) | ok ×3 (7 tests, ~1.5 s each run) |
| `node --check internal/dashboard/static/app.js` | OK (node from Homebrew) |
| Python `yaml.safe_load` on compose, render.yaml, ci.yml; `docker compose -f deploy/docker-compose.yml config -q` | OK (compose config needs no daemon) |
| HTML check (Python `html.parser`): duplicate ids, inputs without labels, JS `$("id")` references missing from HTML | none |
| `curl` against the dev server: `/` → 200 `text/html` + CSP; `/app.js` → `text/javascript`; `/nope` → 404 | OK |
| Headless Chromium (Playwright) against the dev server: all 7 guided steps in demo mode (all *Done*, step 7 "40 acknowledged, 40 found, 0 lost"); the same with `QL_FAKE_NO_PERNODE=1 QL_FAKE_NO_DEMO=1` (non-demo instructions shown, election detected); manual produce, idempotent retry → `duplicate`, `OUT_OF_ORDER_SEQUENCE` and `UNAVAILABLE` rendered with their codes; offsets commit/fetch (−1 before commit); API-down banner; dark mode at 390 px with no horizontal scroll | OK. The only console errors were the browser's own logs of the intentional 409/503 responses. |
| Real backend: `go build ./cmd/qlogd && PORT=8098 qlogd --demo --data-dir /tmp/qlog-demo`, then the dashboard in headless Chromium | All 7 guided steps *Done*. The new leader showed up ~1.0 s after the kill, the restarted node matched the leader's LEO, and step 7 reported "40 acknowledged, 40 found, 0 lost" (HW 42: the offsets include the two election no-ops). Also checked: idempotent retry → `duplicate`, `OUT_OF_ORDER_SEQUENCE` 409, offsets commit/fetch, `NOT_FOUND` 404. |
| `docker build` | **not run**: the Docker daemon (colima) is stopped and was left stopped. CI's `docker` job builds and smoke-tests the image. |

External facts were checked on 2026-11-13 and are cited in DEPLOY.md:
- Render: Blueprint fields (`runtime` replaces the deprecated `env`; `autoDeployTrigger`), the free tier (spin-down
  after 15 min, ~1 min cold start, no persistent disk, ephemeral FS, 750 h/month), and `PORT` (default 10000, bind
  0.0.0.0).
- Oracle Always Free: A1 limits (2 OCPU / 12 GB equivalent, 200 GB block storage), and idle reclamation (7 days with
  p95 CPU < 20%, network < 20%, memory < 20% on A1).
- GitHub Actions: latest majors are `actions/checkout` v7 and `actions/setup-go` v7.

Backend observation: a forwarded produce response carries `X-QL-Leader` twice (`"2, 2"`). The follower sets it
before proxying, and the leader sets it again. The dashboard shows only the first value. Deleting the header before
proxying, or not setting it on the forwarding path, would fix it.

---

## Phase 2 (simplified): backend

Scope: `internal/partition` (fixed + finished), `internal/raft/transport/tcp.go`, `internal/cluster`,
`internal/httpapi`, `cmd/qlogd`, `cmd/qlog`, `test/e2e`, `test/porcupine`, following `docs/SCOPE.md`. New module
dependency: `github.com/anishathalye/porcupine v1.3.1` (test only).

### What each package does

- **`internal/partition`** — one replica of one Raft group, driven by one goroutine (`Partition.run`: tick, inbound
  messages, proposals, ReadIndex, Ready/Advance in the documented order). Produce with `acks=leader|all` and waiter
  queues (failed with an outcome-unknown + NotLeader error on leadership loss), HW = applied+1 (never above commit+1),
  long-poll Fetch, idempotent producer state rebuilt from the log. New `messages.go`: the message-level API used above
  it — `ProduceMessages`/`ReadMessages` with **0-based API offsets** (= log offset − 1), string producer ids
  (`ProducerIDFor`, FNV-1a → 63-bit id in the batch header), `ReadBarrier` (ReadIndex → HW), `IsRetriable`.
- **`internal/raft/transport/tcp.go`** — one TCP connection per node pair (smaller id dials, the other accepts; a
  12-byte handshake names both ends), `[u32 len][raft wire message]` frames, all groups multiplexed via
  `Message.Group`, a bounded per-peer queue drained by one writer goroutine (Send never blocks, drops when full),
  redial with exponential backoff + jitter, 1 s keepalive frames + 5 s read idle timeout, `Close` waits for every
  goroutine and the handler is never called after it.
- **`internal/cluster`** — a `Node` = data dir + transport + `partition.Manager`. `__meta` (topic list) and
  `__offsets` (group → offset) are groups whose state machines are *tailers*: goroutines that read the group's own
  committed log from offset 0 and apply it, so a restart rebuilds state from disk. Applying a `create_topic` opens the
  topic's group on every node (group id = 1000 + the create record's `__meta` offset, identical everywhere). Linearizable
  reads of that state = ReadIndex barrier on the leader + wait for the tailer to reach it. Event feed (`Events` ring
  buffer of role/term/leader changes, shareable across nodes), leader lookup, per-group status in API offsets.
- **`internal/httpapi`** — the SCOPE.md contract on Go 1.22 `METHOD /path/{x}` patterns, JSON errors with the codes,
  1 MiB body limit (413), request validation, server timeouts (`ReadHeaderTimeout` 5 s, `ReadTimeout` 15 s,
  `WriteTimeout` 45 s > max long poll 30 s, `IdleTimeout` 120 s), produce timeout 10 s, long polls bound to
  `r.Context()`, `/metrics` counters. `LocalNode` = cluster.Node + HTTP server that can be stopped/started on the same
  dir and ports; `NewLocalCluster` (n nodes on free localhost ports); `Demo` = the public front of demo mode.
- **`cmd/qlogd`** — flags/env (`--id --peers --http-peers --http-addr --raft-addr --data-dir --fsync --demo`,
  `QLOG_*`), dashboard at `/`, SIGINT/SIGTERM graceful shutdown. `--demo`: three `LocalNode`s in one process on
  127.0.0.1 (own Raft and HTTP ports, `data-dir/node-N`), front on `:$PORT` (default 8080).
- **`cmd/qlog`** — `topics create|list`, `produce` (stdin lines or `--value`, `--key`, `--acks`, `--producer-id`,
  `--sequence`), `consume` (`--from`, `--follow`, `--group` starts at and commits the group offset), `status`,
  `bench` (closed loop, N messages, C workers, prints msg/s, MB/s, p50/p99/max). Retriable errors (503/504/network) are
  retried with the same sequence.

### Key decisions (and why)

1. **Reads are forwarded to the leader too, and the leader runs ReadIndex before serving.** Only the leader can tell
   that a committed prefix is current; a follower (or a deposed leader that hasn't noticed) may be stale. The
   Porcupine negative control shows the follower-read anomaly. Cost: one heartbeat round per read (sub-ms locally).
2. **No forwarding loops:** a proxied request carries `X-QL-Forwarded: true`; a node never forwards such a request
   (answers `503 NOT_LEADER` + `X-QL-Leader` instead) → at most one hop. The follower does not set `X-QL-Leader` itself
   when proxying; the proxy's `ModifyResponse` sets `X-QL-Leader`/`X-QL-Forwarded` once (bug report from the
   dashboard engineer: the header was duplicated — fixed, `TestAPIContract` asserts a single value).
3. **Unknown topic on a node:** forwarded to the `__meta` leader, which does a linearizable topic check (404 only
   there), because a lagging node's copy of `__meta` may not have the topic yet.
4. **State machines as tailers of their own log** (instead of an apply callback inside the driver): uses only the
   public read API, cannot block the Raft driver, restart = re-read from offset 0.
5. **API offsets = log offset − 1** in exactly one place (`partition/messages.go`); everything above it uses API
   offsets. `/v1/cluster` `commit` = last committed API offset (raft commit − 1), `high_watermark` = applied API HW,
   `log_end_offset` = API LEO, so `high_watermark ≤ commit + 1` always.
6. **Demo `/v1/cluster` aggregates all in-process nodes** (one row per (node, group) with a `node` field, `"demo": true`,
   one shared event feed) — the dashboard relies on both fields. Kill = `LocalNode.Stop` (HTTP server closed,
   groups closed with final fsync, transport closed, data dir kept); restart = `Start` (recovery from disk, same ports).
7. **Producer id strings are hashed**, not registered: no extra replicated state; collisions (~n²/2⁶⁴) are documented.
8. **Removed as unneeded** from the previous engineer's partition code: `AcksNone`, `ConsistencyLeader` (not
   linearizable, unused), `ListOffsets` (Kafka-only). Kept: ReadIndex/Local consistency, Compact + snapshot install,
   TransferLeadership (all tested, small).

### Fixes to existing code

- **partition tests failed (goleak):** `TestIdempotentRetryAcrossFailover` restarts a broker from a goroutine that
  could run after the cluster's cleanup, leaking a partition driver (`partition.go:195` was the `go p.run()` in the
  goleak report). `partitiontest.Cluster` now refuses `Restart` after `Close`.
- **partitiontest crash model could panic Raft:** `Crash` crashed the FaultFS while the driver kept running; a read of
  the crashed FS inside Raft (`lastTerm`) panics by design (storage failure is fatal). New `Partition.Halt` /
  `Manager.Crash(crashFS)`: stop the drivers with no further disk writes, then crash the FS, then release the logs —
  the same "disk frozen at the crash instant" semantics, without the panic (found by the Porcupine nemesis).
- **Flaky `TestSnapshotInstallToLaggingFollower`:** a control request (e.g. `Compact`) replied before the status was
  republished; the driver now publishes before replying.
- **raft (investigated MsgSnapResp leadership-transfer path):** `internal/raft` matched its documented behaviour except
  one liveness bug: when the transfer target caught up via InstallSnapshot, the `MsgSnapResp` path never checked the
  transferee, so the transfer stalled until its election-timeout abort. Fixed with the same check as the
  `MsgAppResp` path (3 lines in `stepLeader`); regression test `TestLeadershipTransferAfterSnapshot` (fails before,
  passes after). All raft/rafttest suites still pass.

### Deviations from SCOPE.md (with reasons)

1. **`--http-peers`** flag (not in the task's flag list): forwarding needs each leader's HTTP address; `--peers` only
   has Raft addresses. It also fills `nodes[].addr` in `/v1/cluster`.
2. **First message offset:** offsets are 0-based as specified, but each new leader's election no-op occupies an offset
   (Raft index), so a fresh topic's first message is at offset 1 and offsets have gaps after failovers (like Kafka
   control records). `next_offset` always skips them. Documented in README.
3. **Extra fields:** `"demo": true` and per-row `"node"` in demo mode's `/v1/cluster`. Error code for a body over 1 MiB
   is `BAD_REQUEST` with HTTP 413; `OUT_OF_ORDER_SEQUENCE` uses 409; `NOT_LEADER`/`UNAVAILABLE` 503; `TIMEOUT` 504.
4. **Offset above the HW** in a fetch is `400 BAD_REQUEST` (no OUT_OF_RANGE code in the contract); long poll happens
   only at `offset == high_watermark`.
5. **Consumer offsets don't check that the topic exists** (opaque per (group, topic) name; avoids stale `__meta`
   checks on the `__offsets` leader).

### Tests added

- `internal/partition/messages_test.go`: API offsets, control batches skipped, max cuts mid-batch, mid-batch start,
  long poll (timeout, wakeup, client cancel), out of range; string producer ids, duplicate → original offsets,
  `OUT_OF_ORDER_SEQUENCE` (gap, stale, negative, new producer ≠ 0), dedup after failover, followers refuse
  linearizable reads.
- `internal/raft/transport/tcp_test.go` (goleak `TestMain` for the package): all pairs × 3 groups, per-link order,
  2 MiB message, reconnect in both dial directions after a peer restart on the same address, queue-full drops /
  non-blocking Send, bad handshakes rejected, Close stops the handler, silent peer dropped by idle timeout + idle link
  kept by keepalives.
- `internal/cluster/node_test.go`: create on follower → NotLeader, create/duplicate/bad name, topic group elected and
  replicated, offsets commit/fetch, full stop + restart of all nodes rebuilds topics/messages/offsets, status
  invariants, leader events; offsets survive `__offsets` leader failover and restart; ring buffer.
- `internal/httpapi/server_test.go`: the whole contract over real HTTP on 3 nodes (every error case asked of leader and
  followers, forwarding headers exactly once, double-forward refused, long poll, idempotence, offsets, metrics);
  client-cancelled long poll; demo front kill/restart/aggregated view.
- `test/e2e`: 3 `LocalNode`s (real HTTP + TCP + files, `fsync always`): 4 idempotent producers × 60 messages with
  `acks=all` while the `orders` leader is killed (after 40 acks) and restarted (after 40 more); clients retry on any
  node. Asserts: all 240 acknowledged messages present at their acknowledged offsets, no duplicates, per-producer
  order, identical logs on all three nodes. Typical run: 4 retries answered `duplicate: true` (the dedup path is
  exercised). Second test: group offset survives two `__offsets` leader kills.
- `test/porcupine`: 20 seeded histories (`QLOG_PORCUPINE_HISTORIES=N`), 3 clients, ~80 ops each over 1.2 s, nemesis
  (isolate / partition / crash with `CrashDropUnsynced` + restart, one fault at a time, leader targeted ⅓ of the
  time), produce(`acks=all`) + full-log read via ReadIndex; unknown outcomes get Return = +∞. Negative controls (must
  be `Illegal`, and are): follower-local read after a partition; `acks=leader` write on an isolated leader lost after
  failover. `QLOG_PORCUPINE_VIZ=/tmp` writes a visualization of any failing history.

### Verification (darwin/arm64, go1.26.5, 2026-11-13; all from the repo root)

| Command | Result |
|---|---|
| `go build ./...` | OK |
| `go vet ./...` | OK |
| `gofmt -l .` | empty |
| `GOOS=linux GOARCH=amd64 go build ./...` and `go vet ./...` | OK |
| `go test -race -count=1 ./...` × 3 | 3 × 12 packages `ok`, 0 failures (wall ≈ 25–30 s per run; porcupine 10–26 s, rafttest ≈ 20 s, log ≈ 16 s) |
| `go test -race -count=10 -v ./test/e2e/` | 20/20 PASS (10 × 2 tests), 40.9 s; `TestFailoverNoLossNoDuplicates` ≈ 2.6 s each |
| `go test -race -count=1 -v ./test/porcupine/` | 20 histories, 1,621 operations (54 with unknown outcome), all linearizable; both negative controls flagged `Illegal` |
| Test counts | partition 18 tests (2 new), transport 12 (7 TCP), cluster 3, httpapi 3, e2e 2, porcupine 3 (+20 seeded subtests), raft 46 (+1) |
| Manual smoke test of `qlogd --demo` + `qlog`/curl (create, produce, dup, 409, consume, kill leader, produce, group commit, restart, long poll, process restart on the same dir) | all as expected, transcript in `docs/RESULTS.md` |
| 3-process mode (`qlogd --id N --peers … --http-peers … --raft-addr 0.0.0.0:700N --http-addr …`) | create/produce via node 1, consume via node 3, forwarded produce via node 2 → single `X-Ql-Leader: 3`, `X-Ql-Forwarded: true`; dashboard served at `/` |
| `qlog bench` (demo, one process, laptop) | `fsync always`: 422 msg/s c=16 (p50 37 ms, p99 70 ms), 3,786 msg/s with batch 10, 79 msg/s c=1 (p50 12 ms); `fsync never`: 8,722 msg/s c=16 (p50 1.6 ms). Full tables + caveats in `docs/RESULTS.md` |

### Known gaps / TODO

- Linux execution not run here (cross-build + vet only); CI runs the race tests on Linux.
- No retention scheduler (the log's `ApplyRetention` is not called by the server); `__meta`/`__offsets` are never
  compacted (their tailers re-read from offset 0 on restart — fine at demo scale).
- The Porcupine test drives the partition layer directly (mem network + FaultFS); the HTTP layer's forwarding is
  covered by httpapi/e2e tests, not by the linearizability checker.
- A node that is the only one knowing a topic group failed to open (disk error) reports `UNAVAILABLE` for it; there is
  no automatic retry of the open.
- README/DESIGN/DEPLOY were corrected where they disagreed with the implementation (reads are forwarded too, offsets
  start at the election marker, retention not scheduled, `raft/hardstate` path, test descriptions).

---

## Phase 3: independent review

Scope: a fresh-eyes review of the Phase 2 backend (`internal/partition`, `internal/raft/transport/tcp.go`,
`internal/cluster`, `internal/httpapi`, `cmd/*`, `test/*`), the deploy files and the docs, for real defects only.
Each code fix is minimal and has a regression test that fails before the fix and passes after it (checked by running
the new test against an unmodified copy of the tree). No new features and no new dependencies.

### Findings and fixes

| # | Severity | Where | Defect | Fix | Regression test |
|---|---|---|---|---|---|
| 1 | **High** on case-insensitive file systems (macOS default APFS, Windows); none on Linux | `internal/cluster/node.go` `applyMeta` / `openGroup` (topic dir = `DataDir/<topic name>`) | Topics `orders` and `Orders` were both accepted. Their Raft groups opened **the same directory**, wrote into each other's segments, and the driver **panicked** (`raft: term of last index 2: log: corrupt batch`), killing the whole process (in demo mode: all three nodes). Reproduced on this Mac. | Topic names are unique ignoring case. `CreateTopic` refuses a name that differs only in case from an existing topic (`ErrBadName` → `400 BAD_REQUEST`), and `applyMeta` ignores such a `__meta` record (deterministic on every node, so concurrent creates are safe too). Helpers `checkNewTopic`, `topicFoldLocked` (`node.go:398`, `:410`). | `TestTopicNamesDifferingOnlyInCaseAreRefused` (`internal/cluster/node_test.go:260`): create-time refusal and a raw `__meta` record that must be ignored. |
| 2 | Medium (correctness of the produce reply; no data loss) | `internal/partition/partition.go` `waiterQueue.push` (`:83`, was `:81`) + `await` | The waiter queues are assumed sorted by offset, but a retry of a batch that is **still in flight** (`dup` of an uncommitted entry) was appended *behind* waiters of newer batches. When the newer entry committed, `completeAll` popped the retry's waiter as "entry replaced" and failed it with an outcome-unknown NotLeader error, although its entry committed (spurious error log, needless client retry, and on a single-node cluster a misleading "not leader"). | `push` inserts by `LastOffset` (stable, so equal offsets stay FIFO). | `TestDuplicateOfInFlightBatchWaitsForIt` (`internal/partition/messages_test.go:193`): slow fsyncs (FaultFS `SyncLatency`) keep P and Q in flight, the retry of P must get `duplicate:true` with P's offsets. Failed 3/3 before the fix. |
| 3 | Medium (HTTP resource exhaustion, public demo) | `internal/partition/messages.go` `ReadMessages` (`:55`, `:161`) | A fetch was bounded only by message count (`max` ≤ 1000), not bytes: `max=100` over ~1 MiB messages built ~100 MiB in memory (plus the JSON copy) per request, so a few concurrent fetches could OOM a small instance. | `ReadRequest.MaxBytes` (default 1 MiB of key+value bytes; at least one message is always returned, so a larger message still makes progress). The HTTP API uses the default; clients already page by `next_offset`. The Porcupine whole-log read passes `MaxBytes: 1<<30` explicitly. Documented in README (API notes). | `TestReadMessagesIsBoundedInBytes` (`messages_test.go:234`): 8 × 512 KiB messages; a default read returns ≤ ~1 MiB (was 4 MiB), `MaxBytes=1` still returns one message, paging by `next_offset` reads all 8. |
| 4 | Low (wrong leader hint) | `internal/httpapi/server.go` proxy `ModifyResponse` (`:137`) | The forwarding node overwrote `X-QL-Leader` with the id it forwarded to. When that node had just stepped down and answered `NOT_LEADER` with the new leader's id, the client got the stale node's id instead. (Also: on a no-leader 503 it claimed the stale node was leader.) | `ModifyResponse` only sets `X-QL-Forwarded`; the upstream already sets `X-QL-Leader` on every leader-served response and on NOT_LEADER with a hint, so it still appears exactly once (`TestAPIContract` unchanged and passing). | `TestForwardKeepsUpstreamLeaderHint` (`internal/httpapi/server_test.go:396`): a stand-in "stale leader" answers NOT_LEADER with hint 2; the forwarded response must carry `X-QL-Leader: 2`. Failed before the fix (`["3"]`). |
| 5 | Docs (unsupported claim) | `README.md` "Why it's correct" | "10,000 iterations per test in RESULTS.md": RESULTS.md had no crash-test numbers (they were only in Phase 1A notes). | Re-ran `go test -count=1 -timeout 60m -run TestCrash -v ./internal/log/ -args -crash.iters=10000` (PASS, 38.7 s; 7,793 / 3,453 / 5,236 + 8,330) and added it to RESULTS.md with environment, command and raw output. | – |
| 6 | Docs (stale) | `docs/RESULTS.md` microbenchmark intro and table | Referred to the old spec (§3.2 N4/N5, §8.1, §13.2, "Milestone M1a"), a non-existent `cmd/bench`, and "the zero-copy sendfile path is measured in M1b" (sendfile is out of scope). | Reworded to point at `qlog bench` and state there is no sendfile path. | – |
| 7 | Docs (transcript gap) | `docs/RESULTS.md` restart transcript | `qlog topics list` after the process restart shows a `bench` topic that the transcript never creates, so the transcript is not the complete command sequence for that data dir. | Added a note saying so (could not be reconstructed; most likely a `qlog bench` run in between). Numbers were not changed. | – |
| 8 | Docs (wrong) | `docs/DESIGN.md` testing strategy, failover section | Said the Porcupine test runs "three nodes over real HTTP + TCP" with "a negative control". It drives the partition layer over the in-memory network and FaultFS, with two negative controls, and does not cover HTTP forwarding. The check-quorum paragraph spoke of "reads that are not ReadIndex-checked" (all reads are). | Corrected both. | – |
| 9 | Docs (limitations) | `README.md` Limitations | Undocumented: demo kill/restart endpoints are unauthenticated (anyone can stop a public demo; with all three down `/v1/health` is 503 and Render may restart the instance); idempotence window of 5 batches and hashed producer ids; a produce without `producer_id` retried after TIMEOUT/UNAVAILABLE can be written twice. | Documented (design limitations, no code change). | – |

Correction to the Phase 2 notes: the Porcupine histories have ~80 operations **per history** (3 clients together;
this run: 1,651 operations in 20 histories), not "~80 ops each" per client. Phase 2 decision 2 ("ModifyResponse sets
X-QL-Leader") is superseded by finding 4.

### Reviewed and found correct (no change)

- **Partition driver:** Ready/Advance order; HW = applied+1 never above commit+1; HardState commit hint is clamped to
  the durable index by raft (`min(committed, durable)`), so restart-time `applied = min(hs.Commit, LastIndex)` cannot
  expose a non-durable or truncated entry; `failOutstanding` answers every queued and in-flight request (definite vs
  outcome-unknown split is right, including the `Produce`/`donec` race); `Halt` + `Manager.Crash` freeze the disk with
  no further writes; producer-state speculative/committed split and rebuild after truncation or a vanished proposal.
- **TCP transport:** single connection per pair (no dial race), handshake validation, replacement of a stale accepted
  connection, writer/ping/idle-timeout interplay, `Close` waits for every goroutine and the handler is never called
  after it. No authentication on the Raft port is a documented non-goal.
- **Cluster:** `__meta`/`__offsets` tailers rebuild from offset 0 on restart; group id = 1000 + create offset is
  identical everywhere; ReadIndex barrier + `waitApplied` gives linearizable topic list and offset fetch; `Close`
  waits for a tailer that is inside `openGroup`.
- **HTTP:** forwarding is at most one hop (`X-QL-Forwarded`), the body is proxied unread, the 1 MiB body limit and
  validation run on the leader, long polls follow `r.Context()` through the proxy hop, server timeouts leave room for
  the 30 s long poll. Error codes match SCOPE.md (plus the documented 413/409 choices).
- **Porcupine test power** (mutation checks, not committed): with the main test's reads switched to
  `ConsistencyLocal` (no ReadIndex), most of the 20 histories were flagged Illegal; with produces switched to
  `acks=leader`, 17 of 20 were flagged. So the main test can fail, and both negative controls use real unsafe API
  paths (a follower-local read; a real `acks=leader` loss), not synthetic histories.
- **Deploy files vs `cmd/qlogd`:** every flag used in `deploy/docker-compose.yml` (`--id --peers --http-peers
  --raft-addr --http-addr --data-dir --fsync always`), the Dockerfile `CMD --demo --data-dir /data`, `render.yaml`
  `PORT=8080` + `/v1/health`, and the CI smoke test exist and mean what the docs say. `--fsync interval=10ms` matches
  `log.ParseSyncPolicy`.

### Verification (darwin/arm64, go1.26.5, Apple M1 Pro, 2026-11-13)

| Command | Result |
|---|---|
| `gofmt -l .` | empty |
| `go vet ./...` | OK |
| `GOOS=linux GOARCH=amd64 go vet ./...` (and `go build ./...`) | OK |
| `go test -race -count=1 ./...` × 3 | 3 × 12 packages `ok`, 0 failures. (A first attempt had one failure in the new `TestForwardKeepsUpstreamLeaderHint`: the follower had not learned the leader yet. The test now waits for that; then `-count=30` of it passed, and all three full runs above came after the change.) |
| `go test -race -count=5 ./test/e2e/... ./test/porcupine/...` | ok (e2e 19.9 s, porcupine 36.5 s) |
| New tests under stress: `-race -count=20` (partition, cluster), `-count=30` (httpapi) | all pass |
| `go test -count=1 -run TestCrash -v ./internal/log/ -args -crash.iters=10000` | PASS (in RESULTS.md) |
| Demo smoke test: `PORT=18095 qlogd --demo --data-dir $(mktemp -d)`, driven by a Python script over HTTP: create `orders`; 30 messages with idempotent producer `smoke-1` (acks=all, retry on 503/504/network); kill the `orders` leader (node 1) via `/v1/demo/nodes/1/kill`; 20 more (new leader node 2); restart node 1; 10 more; wait until all three replicas show the same HW (62); consume everything with `max=7` pages | **acked=60, consumed=60, lost=0, duplicates=0, moved=0, extra=0**. Also: `POST /v1/topics {"name":"Orders"}` → `400 BAD_REQUEST` (finding 1); forwarded fetch carries `X-QL-Leader: 2`, `X-QL-Forwarded: true`. Process stopped with SIGINT; a second run confirmed qlogd exits 0 on SIGINT. |
| `docker build` | not run (no Docker daemon used); CI's `docker` job covers it. |

### Known gaps left as they are

- On an already-existing data dir that (on Linux) holds two topics differing only in case, the second one is now
  ignored on replay; there is no migration. Fresh data dirs are unaffected.
- Demo `/v1/cluster` blocks while a node is being stopped (`LocalNode.Stop` holds the node's mutex across the final
  fsyncs): a short stall of the dashboard poll, not a correctness issue.
