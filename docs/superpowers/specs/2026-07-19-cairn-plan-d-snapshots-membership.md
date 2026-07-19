# Cairn — Plan D: snapshots/log-compaction + single-server membership

**Status:** design approved (user delegated the calls), planning
**Date:** 2026-07-19
**Type:** Phase-1 subsystem — Plan D of the single-group Raft cycle
**Builds on:** Plan C (`RaftCore`), merged to `main`.
**Parent specs:** `2026-07-18-cairn-raftcore-plan-c-design.md`, `2026-07-18-cairn-raft-design.md`

## Purpose

Two capabilities a long-lived Raft cluster needs, added to `RaftCore` without
disturbing the safety it already proves:

1. **Snapshots / log-compaction** — a node whose log has grown unboundedly can
   compact a committed prefix into a snapshot of the applied state, and a leader
   can bring a follower that has fallen *behind the compacted prefix* back up to
   date by shipping that snapshot (`InstallSnapshot`).
2. **Single-server membership changes** — add or remove one node at a time from
   the cluster's configuration, safely, while it keeps serving.

Correctness is still the deliverable: the deterministic simulation must prove the
four safety invariants continue to hold *across* compactions, snapshot installs,
and configuration changes.

## Resolved design decisions (this cycle)

- **Membership = single-server changes, NOT joint consensus** (amends the
  Plan-C-era raft-design spec's "joint consensus" choice). Adding or removing one
  node at a time guarantees the old and new configurations share a majority, so no
  transitional joint state is needed — the modern default (etcd; Ongaro's later
  recommendation), simpler and with fewer edge cases at equal safety.
- **Snapshot bytes live in `RaftStorage`;** the state-machine *content* crosses
  the pure core boundary the same way `apply` does in Plan C — the driver produces
  it (via a `compact()` input) and restores it (via a `Ready.restore` output). The
  core stays I/O-free and state-machine-agnostic.
- **`LogEntry` gains an `EntryType { Normal, ConfigChange }`.** A config change
  must be a replicated, committed log entry the core can *recognize*; the frozen
  `LogEntry { term, index, command }` has no type tag. Extending the type is the
  clean, standard choice (same "extend the contract deliberately when a later phase
  needs a distinction the original couldn't express" precedent as Plan C's
  `RequestVoteResp.pre_vote`); a magic-prefix encoding in `command` would collide
  with user data.

## Scope

**In scope**
- `RaftStorage` snapshot persistence (`save_snapshot` / `read_snapshot`) + `MemStorage` impl.
- Core `compact(index, data)` input: record snapshot meta + `compact_prefix` the log.
- `InstallSnapshot` send (leader, when a follower lags past the compacted prefix)
  and receive (follower resets to the snapshot boundary + emits `Ready.restore`).
- `LogEntry.entry_type`; core recognizes `ConfigChange` entries.
- Single-server membership: `propose_conf_change`, config-takes-effect-on-append,
  quorum over the live config, one-change-in-flight, leader step-down when removed.
- Unit + simulation tests proving safety across snapshots + membership.

**Out of scope (deferred)**
- Joint consensus / multi-node atomic config changes (explicitly replaced by
  single-server changes this cycle).
- Learner / non-voting catch-up phase before promoting an added node (a liveness
  optimization; note as a future enhancement, not built here).
- The `RaftLog`-backed `RaftStorage` adapter + async node driver + real TCP →
  **Plan E** (this cycle stays on `MemStorage` + the deterministic sim).
- Snapshot *chunking* (streaming a large snapshot in pieces) — `InstallSnapshot`
  ships the whole snapshot in one message this cycle; chunking is a later refinement.

## Architecture

### Snapshots

**`RaftStorage` additions (sync, like the rest of the trait):**
```rust
fn save_snapshot(&mut self, meta: SnapshotMeta, data: &[u8]) -> Result<()>;
fn read_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>>;
```
`MemStorage` holds `Option<(SnapshotMeta, Vec<u8>)>`. `compact_prefix` on the log
store already exists; `RaftStorage` gains a `compact(up_to, meta)` passthrough (or
the core calls the existing `truncate`/compaction path) so the in-memory log drops
the compacted prefix.

**Compaction (core input, driver-triggered):**
```rust
pub fn compact(&mut self, index: LogIndex, data: Vec<u8>) -> Result<()>;
```
- Precondition: `snapshot.last_index < index <= last_applied` (never compact
  uncommitted/unapplied entries; never go backwards). Violation → `Err(Corruption)`
  (never panic).
- Effect: build `SnapshotMeta { last_index: index, last_term: term(index) }`,
  `storage.save_snapshot(meta, &data)`, then compact the log prefix through
  `index`. Policy (when to snapshot, how to serialize state) stays in the driver.

**Install (leader side):** in `send_append_to(peer)`, if `next_index[peer] <=
snapshot.last_index` (the entries the follower needs are compacted away), send
`InstallSnapshot { term, leader_id, last_index, last_term, data }` (data from
`storage.read_snapshot()`) instead of `AppendEntries`. On `InstallSnapshotResp`
(same-term), set `match_index[peer] = last_index`, `next_index[peer] = last_index +
1`, and count it as contact (same `ack_count` gate as Plan C).

**Install (follower side):** `step(InstallSnapshot)`:
- Reject if `req.term < current_term`. Else adopt leader / step down (as
  AppendEntries does), reset election timer.
- If `req.last_index <= snapshot.last_index` (stale snapshot) → ack and ignore.
- Else: `storage.save_snapshot(meta, &req.data)`; reset the log so its base is the
  snapshot (`compact_prefix` through / discard entries `<= last_index`; if the
  follower has a longer log that conflicts, discard it entirely and adopt the
  snapshot base); set `commit_index = last_index`, `last_applied = last_index`;
  emit `Ready.restore = Some((meta, data))` so the driver reloads its state machine
  from the snapshot *before* applying any later entry. Reply `InstallSnapshotResp { term }`.

**`Ready` gains** `restore: Option<(SnapshotMeta, Vec<u8>)>` — drained like
`apply`/`messages`/`reads`; the driver, on a non-`None` restore, reloads its state
machine, then applies any `apply` entries that follow.

### Single-server membership

**`LogEntry`:** add `entry_type: EntryType` where `EntryType { Normal,
ConfigChange }` (serde/bincode + the hand-rolled TCP codec updated; log-store
op-log format updated with a version bump or additive field — must stay
crash-recoverable, torn-tail-safe as before).

**Config in the core:** the fixed `config.peers` becomes a mutable current voter
set the core derives from the **latest `ConfigChange` entry present in its log**
(config takes effect on *append*, reverts on truncation — the standard Raft rule).
`RaftCore::new` seeds it from the initial `Config.peers` (bootstrap config).

**`propose_conf_change`:**
```rust
pub fn propose_conf_change(&mut self, node: NodeId, change: ConfChange) -> Result<Option<LogIndex>>;
// ConfChange { AddVoter(NodeId), RemoveVoter(NodeId) }
```
- Leader only (`Ok(None)` if not leader).
- Reject (`Ok(None)` or a typed refusal) if a config change is already in flight
  (an uncommitted `ConfigChange` entry exists at index > `commit_index`) — **one
  change at a time**.
- Encode the resulting new voter set into a `ConfigChange` `LogEntry`, append it
  (the config takes effect immediately for this node's quorum math), and replicate
  as usual. Initialize `next_index`/`match_index` for a newly-added node.

**Rules the tests pin:**
- Quorum (elections *and* commit) always uses the **current (post-append) config**.
- A single-node add/remove keeps old/new majority overlap → no split-brain.
- When a `ConfigChange` that **removes the leader itself** commits, the leader
  steps down (and stops counting itself once the change commits).
- An added node starts empty and catches up via AppendEntries / InstallSnapshot.
- Config reverts correctly if the `ConfigChange` entry is truncated by a new leader.

## Guarantees the tests must pin (in addition to Plan C's four invariants)

- **Compaction never loses committed state:** after `compact(i, …)`, entries
  `<= i` are recoverable from the snapshot; `last_index`/`last_term`/reads still correct.
- **Snapshot install converges a lagging follower** whose needed entries were
  compacted, with no divergence, and the four safety invariants still hold.
- **Membership safety:** no configuration change produces two leaders in one term
  or a committed entry that a later leader lacks; quorum tracks the live config.

## Testing strategy

- **Unit** (`MemStorage` + core): snapshot save/read roundtrip; `compact`
  precondition errors + prefix drop + meta; `send_append_to` switches to
  `InstallSnapshot` when the follower lags past the prefix; follower install resets
  state + emits `restore`; stale-snapshot ignore; `EntryType` roundtrip through
  storage + codec; `propose_conf_change` appends + takes effect on append; quorum
  uses live config; one-change-in-flight refusal; leader-removed step-down; config
  revert on truncation.
- **Simulation** (extend `tests/raft_sim.rs` for dynamic membership + snapshots):
  - *Snapshot catch-up*: partition a follower long enough that the leader compacts
    past its `next_index`; heal; leader `InstallSnapshot`s it; it converges; the
    four invariants hold. (Harness: a `compact_leader` control + a `restore` sink.)
  - *Grow 3→5* one node at a time; *shrink 5→3* one node at a time; assert
    invariants + continued progress after each change.
  - *Kill + replace*: remove a crashed node, add a fresh one; it catches up
    (via snapshot if compaction happened); invariants hold.
- Determinism preserved (logical time, seeded, single-task, `BTree*` ordering).

## Risks and rabbit-holes (fenced)

- **Config-takes-effect-on-append vs. commit** is the classic membership subtlety.
  Single-server changes make it safe (majority overlap), but the core must apply
  the config on *append* and revert on *truncation* — pin both with tests.
- **Leader removed from the new config** must keep replicating the removal entry
  until it commits, *then* step down — stepping down early can stall the commit.
- **Snapshot ↔ log boundary** off-by-ones (`term_at` at the snapshot boundary;
  `next_index <= snapshot.last_index` trigger; a follower with a conflicting longer
  log receiving an older-base snapshot) — the exact edge cases Plan C's
  `term_at`/consistency-check already tiptoe around; extend those carefully.
- **`LogEntry`/op-log format change** must remain crash-recoverable and
  torn-tail-safe (the log store's existing guarantees) — additive + versioned, and
  round-tripped by a test.

## Plan sequence (bite-sized, TDD, subagent-driven — same loop as Plan C)

| # | Task | Done when |
| --- | --- | --- |
| 1 | `RaftStorage` snapshot persistence + `MemStorage` + `Ready.restore` field | save/read snapshot unit tests green |
| 2 | Core `compact(index, data)` — record meta + drop log prefix (+ precondition errors) | compaction unit tests green |
| 3 | `InstallSnapshot` send (leader lag trigger) + receive (follower reset + restore) + resp handling | install unit tests green |
| 4 | `LogEntry.entry_type` (types + op-log format + TCP codec) — additive, crash-safe | entry-type roundtrip (storage + codec) tests green |
| 5 | Membership core: live config from log, `propose_conf_change`, effect-on-append, one-in-flight, leader-removed step-down | membership unit tests green |
| 6 | Sim: snapshot catch-up + grow/shrink/replace scenarios; safety invariants across all | scripted scenarios pass from fixed seed |
| 7 | Whole-branch opus review + fixes + HANDOFF update + PR | workspace green, review clean, merged |
