# Cairn — single-group Raft consensus

**Status:** design approved (forks resolved), planning
**Date:** 2026-07-18
**Type:** Phase-1 subsystem (sits on the LSM engine; below MVCC)

## Purpose

Replicate a log of commands across a fixed set of nodes so every replica applies
the same commands in the same order, tolerating a minority of node failures.
This is the consensus core of cairn and the layer that turns the local LSM
engine into a linearizable, fault-tolerant key-value store. Full single-group
Raft in one cycle: leader election (with pre-vote), log replication, a commit
index, linearizable reads via read-index, log persistence, snapshotting / log
compaction, and joint-consensus membership changes.

Correctness is the deliverable. The differentiator is proving — under adversarial
scheduling — that the replicated state machine stays linearizable through
elections, partitions, crashes, and message reordering.

## Resolved design decisions (forks)

- **Scope:** full single-group Raft in one cycle (all features above), not a
  minimal-core-first split.
- **Transport:** real TCP is the shipped transport. It sits behind a `Transport`
  trait so a deterministic in-memory implementation can drive Raft in tests
  (seeded, single-threaded, reproducible). The trait is the seam the later
  chaos/Jepsen harness injects partitions/drops/delays through.
- **Log storage:** a dedicated append-oriented Raft log store (this cycle's first
  buildable unit), NOT the LSM KV engine. Raft-log semantics — index-keyed
  entries, suffix truncation on conflict, snapshot-driven prefix compaction —
  differ enough from a KV store that reusing the LSM would bend it out of shape.
  The log store reuses the WAL/CRC ideas, not the LSM's structure.

## Non-goals (this cycle)

- Multi-Raft / sharding (Phase 3), MVCC transactions (Phase 2), the client-facing
  KV API and dashboard (Phase 4).
- The chaos/Jepsen harness itself is a *separate* follow-up plan; this cycle
  builds the `Transport` seam and deterministic in-memory transport it needs,
  and ships unit/integration tests, but the full linearizability-checking harness
  is its own subsystem.
- Disk `fsync`-of-directory durability polish and the LSM MANIFEST work tracked
  from Phase 1 remain tracked, not in scope here.

## Guarantees the tests must pin

- **Election safety:** at most one leader per term.
- **Log matching:** if two logs contain an entry with the same index and term,
  the logs are identical up through that index.
- **Leader completeness:** a committed entry is present in the log of every
  future leader.
- **State-machine safety:** no two nodes apply different commands at the same log
  index.
- **Linearizable reads:** a read-index read reflects all entries committed before
  the read began.
- **Durability:** a node restarts with its persisted term, vote, and log intact;
  a committed entry survives a crash of a majority-minus... (survives as long as a
  majority persisted it).

## Component structure (new crate: `crates/raft`, package `cairn-raft`)

Built bottom-up; each unit has one job and a narrow interface, testable alone.

### 1. Raft log store (`log.rs`) — FIRST PLAN, foundation

- **Does:** durable, index-addressed storage of `LogEntry { term, index, command }`
  plus the persisted hard state (`current_term`, `voted_for`) and the latest
  snapshot metadata.
- **Interface:** `append(entries)`, `entries_from(index)`, `entry(index)`,
  `last_index()`, `last_term()`, `truncate_suffix(from_index)` (conflict
  resolution), `compact_prefix(up_to_index, snapshot_meta)` (log compaction),
  `save_hard_state(term, voted_for)` / `load_hard_state()`.
- **Durability:** CRC-checksummed append log with crash-tolerant replay (reuse the
  Phase-1 WAL record/replay pattern); hard state persisted with fsync on change.
- **Depends on:** filesystem only. No Raft logic here — pure storage.

### 2. Raft types + roles (`types.rs`)

- `NodeId`, `Term`, `LogIndex`, `LogEntry`, `HardState`, `Role`
  (Follower/Candidate/Leader), and the RPC message enums (RequestVote / AppendEntries
  / InstallSnapshot, each with its response).

### 3. Transport trait + implementations (`transport.rs`, `tcp.rs`)

- **`Transport` trait:** async send of a typed message to a `NodeId`, and a
  receive stream of inbound messages. Pluggable.
- **TCP impl (shipped):** length-prefixed framed messages over tokio TCP.
- **In-memory impl (tests):** a deterministic, seeded message bus for reproducible
  multi-node simulation; the seam the chaos harness later extends.

### 4. Raft core state machine (`raft.rs`)

- **Does:** the consensus algorithm as a deterministic step function over inputs
  (tick, inbound message, client proposal) producing outputs (messages to send,
  entries to persist, entries to apply). Keeping the core a pure `step`-style
  machine — I/O pushed to the edges — is what makes deterministic simulation
  testing possible.
- Covers: pre-vote + election, log replication with the consistency check,
  commit-index advancement by majority match, read-index linearizable reads,
  snapshot install, and joint-consensus (C-old,new → C-new) membership changes.
- **Depends on:** log store (persistence), types, and a clock/tick source.

### 5. Node driver (`node.rs`)

- **Does:** wires the core to a real `Transport`, a real clock, and the log store;
  owns the async event loop (ticks + inbound messages + client proposals) and
  applies committed entries to a state machine callback.
- **State machine callback:** for this cycle, a simple in-memory KV applied-state
  (the LSM-engine integration is the *next* plan). Keeps Raft testable without
  dragging the whole engine in yet.

## Data flow — a replicated write

1. Client proposal reaches the leader's node driver → core `step`.
2. Core appends the entry to its log store (persisted), emits AppendEntries to
   followers via the transport.
3. Followers persist and ack; core advances commit index when a majority match.
4. Committed entries are handed to the apply callback in index order on every
   node; the leader responds to the client after its own apply.
5. A linearizable read uses read-index: confirm leadership via a heartbeat round,
   then serve once the apply index has caught up to the read index.

## Testing strategy

- **Unit tests** per module: log store (append/truncate/compact/replay, CRC),
  election edge cases, log-matching consistency check, commit-index math.
- **Deterministic multi-node simulation:** drive N `RaftCore`s over the in-memory
  transport with a seeded scheduler; assert the safety guarantees (one leader per
  term, log matching, applied-index agreement) across scripted partitions,
  drops, reorderings, and crashes/restarts. This is where consensus bugs die.
- **TCP integration test:** a small real-socket cluster elects a leader and
  replicates a few entries end-to-end.
- The full linearizability-checking chaos/Jepsen harness is the next plan; this
  cycle delivers the deterministic-sim substrate it builds on.

## Risks and rabbit-holes (fenced)

- **Full Raft is large** → build strictly bottom-up (log store → types/transport →
  core → node), each shipped and tested before the next; the core is a pure step
  function so it's unit-testable without the network.
- **TCP-first hides consensus bugs** → mitigated by the `Transport` trait + the
  deterministic in-memory transport used for the safety-property tests. TCP is
  the product; the sim is the microscope.
- **Membership changes (joint consensus) are the subtlest part** → land election +
  replication + read-index + snapshots first within the core, add joint-consensus
  membership last, behind its own tests.
- **Snapshot/log-compaction interplay** → the log store owns prefix compaction; the
  core only decides *when* to snapshot. Keep that boundary clean.

## Tech stack

- Rust + tokio for the node driver and TCP transport. The `RaftCore` step
  function itself is sync and I/O-free (testable without a runtime).
- `crates/raft` (`cairn-raft`), a second workspace crate alongside `crates/storage`.

## Plan sequence for this cycle

| Plan | Deliverable | Ships when |
| --- | --- | --- |
| A (this first) | Dedicated Raft **log store** + hard-state persistence | append/truncate/compact/replay all tested |
| B | Types + `Transport` trait + in-memory + TCP transports | a 3-node cluster exchanges framed messages |
| C | `RaftCore` step function: election + replication + commit + read-index | deterministic sim proves safety props |
| D | Snapshots/log-compaction + joint-consensus membership | sim proves it across config changes |
| E | Node driver + TCP integration + apply callback | real-socket cluster elects + replicates |

(Each plan is executed subagent-driven with per-task review, like the LSM engine.
This spec's first plan — the log store — is written alongside it.)
