# Cairn — a sharded, Raft-replicated, LSM-backed distributed KV store

**Status:** design approved, planning
**Date:** 2026-07-18
**Type:** flagship portfolio project

## Purpose

A from-scratch distributed key-value database that demonstrates the ability to
architect a large, complicated system. The complexity is intrinsic to the
problem — consensus, storage-engine internals, transaction isolation, and shard
placement — not a function of traffic volume. The bar is a working, benchmarked,
adversarially-tested system with an architecture writeup that explains *why*
each tradeoff was made, not just *what* was built.

Success is measured by correctness under failure (a chaos/Jepsen-style suite
proving the promised consistency guarantees hold under partitions, crashes, and
clock skew), quantified performance (before/after benchmarks on the storage
engine), and legible design docs (ADRs).

## Non-goals

- Serving third-party / production traffic. This is a portfolio system, run
  locally or on the homelab.
- SQL. No parser, planner, or relational layer — the interface is a typed KV +
  transaction API. (Explicitly rejected during design to avoid a second hard
  domain.)
- Multi-region / geo-replication, authentication/authorization, and a
  wire-compatible clone of any existing database. All out of scope.

## Guarantees (the properties the test suite must prove)

- **Linearizable** reads and writes within a single key's Raft group.
- **Snapshot isolation** for multi-key transactions via MVCC.
- **Durability**: an acknowledged write survives a crash of the acking node
  (WAL fsync + Raft commit on a majority).
- **Availability** under a minority of node failures per group (Raft quorum).
- Cross-shard transactions are **out of scope for the guarantee set** in the
  first cut; the initial transaction guarantee is single-group. (Revisited in
  Phase 4 — see Open Questions.)

## Architecture

Layered, built strictly bottom-up. Each layer has one purpose and a narrow
interface to the layer above, so it can be built and tested in isolation.

```
        ┌───────────────────────────────────────────────┐
        │  Control plane / shard router  (TS/Bun)        │  Phase 4
        │  placement · split/rebalance · cluster dashboard│
        └───────────────┬───────────────────────────────┘
                        │ routes keys → groups
   ┌────────────────────┼────────────────────┐
   ▼                    ▼                     ▼
┌─────────┐        ┌─────────┐          ┌─────────┐
│ Raft grp│        │ Raft grp│   ...    │ Raft grp│         Phase 3 (multi-Raft)
└────┬────┘        └────┬────┘          └────┬────┘
     ▼  each replica hosts, per group:
┌──────────────────────────────────────────────────┐
│  MVCC transaction layer  (Rust)                   │        Phase 2
├──────────────────────────────────────────────────┤
│  Raft consensus  (Rust)                           │        Phase 1
│  election · replication · read-index · snapshot · membership
├──────────────────────────────────────────────────┤
│  Custom LSM storage engine  (Rust)                │        Phase 1
│  WAL · memtable · SSTables · leveled compaction · bloom
└──────────────────────────────────────────────────┘
```

### Sequencing principle

Every phase ends at a **finished, demoable, benchmarked, chaos-tested** system.
If work stops after any phase, the result is a complete smaller system, never a
broken half of a larger one. Phase 1 is the load-bearing correctness work;
sharding (Phases 3–4) is the ambitious capstone that is only *safe* to attempt
because it sits on a proven single-group base.

## Components and interfaces

Interfaces are described by responsibility and boundary, not final signatures.

### 1. LSM storage engine (Rust) — Phase 1

- **Does:** durable, ordered local key→value storage with range scans and
  point lookups.
- **Interface up:** `put(key, value, seqno)`, `get(key) -> Option<value>`,
  `scan(range) -> iterator`, `flush()`, `snapshot() / restore()`.
- **Internals:** append-only WAL (fsync on commit) → in-memory memtable
  (ordered map) → immutable SSTables on disk → leveled compaction → per-SSTable
  bloom filters + block index. Sequence numbers thread through so the MVCC layer
  can store multiple versions per key.
- **Depends on:** filesystem only.

### 2. Raft consensus (Rust) — Phase 1

- **Does:** replicate a log of commands across a group so every replica applies
  the same commands in the same order.
- **Interface up:** `propose(command) -> committed`, `read_index()` for
  linearizable reads, `add_node / remove_node` for membership, applied-command
  callback into the state machine.
- **Internals:** leader election with pre-vote, log replication + commit index,
  read-index for linearizable reads without a log write, log compaction via
  storage-engine snapshots, joint-consensus membership changes.
- **Depends on:** the storage engine (for the Raft log + snapshots) and a
  transport (see below).
- **State machine:** the applied log drives writes into the MVCC/LSM layer.

### 3. Transport / RPC (Rust) — Phase 1

- **Does:** node-to-node messaging for Raft (AppendEntries, RequestVote,
  snapshots) and client request routing.
- **Interface:** async send/receive of typed messages between nodes; pluggable
  so the chaos harness can inject partitions, drops, delays, and reordering.
- **Depends on:** async runtime (tokio).

### 4. MVCC transaction layer (Rust) — Phase 2

- **Does:** multi-key transactions at snapshot isolation.
- **Interface up:** `begin() -> txn`, `txn.get/put`, `txn.commit() -> ok | conflict`.
- **Internals:** multi-version keys keyed by `(key, version)`, a timestamp/
  version source ordered through the Raft log, snapshot-isolation conflict
  checks at commit, garbage collection of obsolete versions during compaction.
- **Depends on:** LSM engine (versioned keys) + Raft (to order commit
  timestamps).

### 5. Multi-Raft (Rust) — Phase 3

- **Does:** run many independent Raft groups on the same node set, each owning a
  contiguous key range (a shard).
- **Interface up:** `group_for(key) -> group_id`, per-group lifecycle
  (create/split/move), a shared transport multiplexed across groups.
- **Internals:** one Raft instance per range, shared heartbeat/transport,
  per-group storage namespaced within the engine.
- **Depends on:** Raft + engine + transport, generalized to N groups.

### 6. Control plane + shard router (TS/Bun) — Phase 4

- **Does:** decide which node hosts which shard, route client keys to the right
  group, trigger splits/rebalances, and expose cluster state.
- **Interface:** a client-facing KV/txn API (HTTP or gRPC), an admin API, and a
  live dashboard visualizing the shard map and per-group Raft leadership.
- **Internals:** a placement/range map, split-on-size or split-on-load,
  rebalancing by moving a group's replica set. The language boundary sits here:
  everything below is Rust, the router/control plane/dashboard is TS/Bun.
- **Depends on:** the Rust node cluster over its RPC/admin surface.

## Data flow — a linearizable write (single group)

1. Client sends `put(k, v)` to the router; router maps `k` to its Raft group and
   forwards to that group's leader.
2. Leader appends the command to its Raft log and replicates via AppendEntries.
3. A majority persists the entry (WAL fsync in the storage engine) and acks.
4. Leader advances commit index, applies the command to the state machine →
   MVCC/LSM `put(k, v, version)`.
5. Leader acks the client. A subsequent linearizable read uses read-index to
   confirm leadership before serving from the applied state.

## Testing strategy — the centerpiece

The differentiator between "senior architect" and "ambitious junior" is proving
the guarantees, not asserting them.

- **Unit + property tests** per layer: the LSM engine gets property tests
  (random op sequences vs. a reference `BTreeMap` model); Raft gets targeted
  tests for election, replication, and membership edge cases.
- **Deterministic simulation** of a cluster: a single-threaded, seeded scheduler
  drives multiple Raft nodes over the pluggable transport so runs are
  reproducible. This is where most consensus bugs are caught.
- **Chaos / Jepsen-style suite (the flagship deliverable):** inject network
  partitions, message drop/delay/reorder, node crashes and restarts, and clock
  skew while a workload runs; record a history and check it with a linearizability
  checker (Elle/Knossos-style, or an embedded checker) to prove linearizability
  (single-key) and snapshot isolation (transactions) actually hold.
- **Benchmarks:** storage-engine throughput/latency (and a before/after when a
  compaction or memory optimization lands), plus cluster write/read throughput.
  Quantified before/after numbers go in the writeup.

## "Done and impressive" bar

- All promised guarantees hold under the chaos suite, with the checker output
  committed as evidence.
- ADRs for the core tradeoffs: Raft-over-Paxos, LSM-over-B-tree, leveled-vs-
  tiered compaction, snapshot-isolation choice, range-vs-hash sharding.
- An architecture doc with diagrams, failure-mode analysis, and quantified
  benchmarks.
- A live demo: bring up a cluster, drive load, kill/partition nodes, watch the
  dashboard show re-election and shard state, and show zero guarantee violations
  in the history checker.

## Risks and rabbit-holes (and how each is fenced)

- **Custom LSM eating the schedule** → build the minimal correct engine first
  (WAL + memtable + single-level SSTables + naive compaction); optimize
  (leveling, bloom tuning, block cache) only after Phase 1 is chaos-tested.
- **Multi-Raft scope creep** → Phases 1–2 are single-group and fully finished
  before any sharding exists. Sharding cannot destabilize a proven base.
- **Cross-shard transactions** → deliberately deferred; the guarantee set is
  single-group first. Distributed (2PC-over-Raft) transactions are an explicit
  later stretch, not a Phase-1 assumption.
- **Consensus bugs being non-reproducible** → deterministic seeded simulation
  from the start, not printf debugging on a live cluster.
- **Networking/transport depth** → keep transport dumb (length-prefixed
  messages over TCP); the pluggable seam exists for the chaos harness, not for
  production networking features.

## Tech stack

- **Rust** (tokio) for the storage engine, Raft, transport, MVCC, and
  multi-Raft — the performance-critical, correctness-critical core.
- **TypeScript / Bun** for the Phase-4 control plane, client/admin API, and the
  cluster dashboard.
- **Biome** for the TS side; standard Rust tooling (`cargo`, `clippy`) for Rust.
- Language boundary is exactly the shard router — a clean, defensible seam.

## Open questions (to resolve during planning, not now)

- Cross-shard transaction guarantee: ship single-group SI first; decide whether
  2PC-over-Raft is an in-scope capstone or a documented "future work."
- Sharding key strategy: range-based (enables ordered scans, needs split logic)
  vs hash-based (simpler placement, no ordered cross-shard scan). Leaning range;
  confirm in the plan.
- Dashboard transport: server-sent events vs websocket for live cluster state.

## Phase summary

| Phase | Deliverable | Finished-state demo |
| --- | --- | --- |
| 1 | Custom LSM + single Raft group | Chaos-tested replicated linearizable KV |
| 2 | MVCC transactions | Snapshot-isolation transactions, checker-proven |
| 3 | Multi-Raft (many groups, one node set) | Many key ranges, per-group consensus |
| 4 | Shard router + control plane + dashboard | Live sharded cluster, visualized, fault-tested |
