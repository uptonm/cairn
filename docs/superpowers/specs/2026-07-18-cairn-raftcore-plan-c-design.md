# Cairn — RaftCore (Plan C): the consensus step function

**Status:** design approved, planning
**Date:** 2026-07-18
**Type:** Phase-1 subsystem — Plan C of the single-group Raft cycle
**Parent spec:** `2026-07-18-cairn-raft-design.md`

## Purpose

Build the consensus core of cairn as a **pure, synchronous, I/O-free step
function**: it decides *what consensus requires* (start an election, replicate an
entry, advance the commit index, release a linearizable read) but never itself
touches a socket or the wall clock. That purity is the whole point — it is what
makes deterministic, in-memory simulation of an N-node cluster possible, which is
where consensus bugs die.

Plan C covers full single-group Raft *behaviour*: pre-vote + election, log
replication with the consistency check, commit-index advancement by majority, and
linearizable reads via read-index. Snapshots / log compaction and joint-consensus
membership are **Plan D**; wiring the core to a real `Transport`, real clock, and
the disk-backed `RaftLog` is **Plan E**.

## Scope

**In scope**

- `RaftCore<S: RaftStorage>` — the synchronous step function.
- The `RaftStorage` trait — the persistence + log seam.
- `MemStorage` — an in-memory `RaftStorage` for the sim and unit tests.
- A deterministic multi-node simulation harness with seeded fault injection.
- Unit + simulation + restart tests pinning the safety guarantees.

**Out of scope (deferred, do not build here)**

- Snapshots / `InstallSnapshot` handling / log compaction → **Plan D**.
- Joint-consensus membership changes (config is fixed at construction) → **Plan D**.
- A `RaftLog`-backed `RaftStorage` adapter, the async node driver, the real
  `Transport`, TCP integration → **Plan E**.
- The linearizability-checking chaos/Jepsen harness → its own later plan.

## Locked decisions (from brainstorming — do not re-litigate)

- **Persistence + log reads go through a synchronous `RaftStorage` trait** the core
  calls inline. This lets the core enforce Raft's persist-before-act ordering
  itself (append then send; persist vote then respond) and keeps the sim
  deterministic (the fake storage is synchronous). This is the raft-rs model.
- **Network + apply are outputs, not trait calls.** The network is genuinely async
  and lives outside the core, so outbound messages, apply-ready entries, and
  ready reads are *buffered* in the core and *drained* by the driver via `ready()`.
- **The core is I/O-free and holds no filesystem handle.** Plan C never touches
  disk; the disk-backed adapter is Plan E.
- **The Plan-C sim uses a synchronous in-harness message router**, not the async
  in-memory `Transport`. This gives a single controlling task and fully
  reproducible interleavings, sidestepping the `Instant::now()` concurrent-sender
  determinism caveat in `TRANSPORT_NOTES.md`. The async transport is exercised in
  Plan E + the chaos harness.

## Architecture — the core/driver seam

`RaftCore<S: RaftStorage>` is a synchronous struct. No async, no wall clock, no
filesystem.

**Inputs (mutating methods):**

- `tick(&mut self)` — advance logical time by one unit (drives election +
  heartbeat timeouts).
- `step(&mut self, from: NodeId, msg: Message) -> Result<()>` — process one inbound
  RPC.
- `propose(&mut self, command: Vec<u8>) -> Result<LogIndex>` — leader appends a new
  command; errors if not leader.
- `read_index(&mut self, token: ReadToken)` — register a linearizable read request.

**Persistence** happens inside these methods via `self.storage` at the correct
algorithmic point. `RaftStorage` methods return `Result`; a storage error
propagates out of `step`/`propose`/`tick` as `Err` — the core never panics on I/O
(guardrail: no `unwrap`/`expect` in library I/O paths).

**Outputs** are buffered in the core and drained by the driver:

```rust
struct Ready {
    messages: Vec<(NodeId, Message)>, // hand to the transport
    apply: Vec<LogEntry>,             // apply to the state machine, in index order
    reads: Vec<ReadToken>,            // read-index reads now safe to serve
}

fn ready(&mut self) -> Ready; // drains the buffers
```

`ReadToken` is an opaque caller-supplied handle (e.g. a `u64` request id) the core
echoes back once the read is linearizably safe; the core assigns it no meaning.

## The `RaftStorage` trait

The seam that lets the same core run against real disk (Plan E) or an in-memory
fake (Plan C). Synchronous; mirrors the existing `RaftLog` + hardstate surface.

```rust
trait RaftStorage {
    // hard state
    fn hard_state(&self) -> HardState;
    fn save_hard_state(&mut self, hs: &HardState) -> Result<()>;
    // log reads
    fn last_index(&self) -> LogIndex;
    fn last_term(&self) -> Term;
    fn term(&self, index: LogIndex) -> Result<Option<Term>>; // None if compacted/absent
    fn entries_from(&self, index: LogIndex) -> Vec<LogEntry>;
    fn snapshot_meta(&self) -> SnapshotMeta;
    // log writes
    fn append(&mut self, entries: &[LogEntry]) -> Result<()>;
    fn truncate_suffix(&mut self, from_index: LogIndex) -> Result<()>;
}
```

- `term(index)` returns `Ok(None)` for an index at/below the snapshot boundary or
  past the log end — the consistency check needs to distinguish "compacted away"
  from "present with term T". (`snapshot_meta().last_term` covers the boundary
  index itself.)
- `compact_prefix` / snapshot install stay **out of the trait** until Plan D.
- Plan C ships **`MemStorage`**, a `Vec<LogEntry>` + `HardState` implementation.
  The `RaftLog`-backed adapter is Plan E.

## State machine internals

- **Role:** `Follower`, `PreCandidate`, `Candidate`, `Leader`.
- **Persistent (via `RaftStorage`):** `current_term`, `voted_for`, the log.
- **Volatile (in the core):** `commit_index`, `last_applied`, `role`, `leader_id`,
  `election_elapsed`, `heartbeat_elapsed`, the current-term vote tally; and —
  leader only — `next_index`/`match_index` per peer plus pending read-index state.
- **Config:** a fixed `Vec<NodeId>` peer set (including self) passed at
  construction. Membership changes are Plan D.
- **Randomized election timeout:** drawn from a per-node seeded PRNG held in the
  core, reset on each timeout. Reproducible from the construction seed.

## Algorithm coverage (what the tests pin)

- **Election + pre-vote.** On election timeout a follower runs a *pre-vote* round
  (`RequestVote { pre_vote: true }`) without bumping its term; only on a pre-vote
  majority does it become a real `Candidate`, increment term, vote for itself,
  persist hard state, and broadcast real `RequestVote`. This stops a partitioned
  node from inflating terms and forcing needless leader step-downs. A higher term
  observed in any message → step down to `Follower` and persist.
- **Vote granting.** Grant iff the candidate's log is at-least-as-up-to-date
  (compare `last_log_term`, then `last_log_index`) **and** we have not already
  voted for a different candidate this term. Persist `voted_for` before replying.
  Pre-vote replies never persist and never mutate `voted_for`.
- **Replication + consistency check.** `AppendEntries` carries
  `prev_log_index`/`prev_log_term`; the follower rejects on mismatch and returns a
  `conflict_index` hint for fast back-up (avoids one-decrement-per-round). On match
  it truncates any conflicting suffix (`truncate_suffix`) then appends the new
  entries. Heartbeats are empty `AppendEntries`. The leader updates
  `next_index`/`match_index` from the ack (or backs up on rejection using
  `conflict_index`).
- **Commit advancement.** The leader advances `commit_index` to the highest index
  replicated on a majority of `match_index` values **whose entry belongs to the
  current term** (Raft §5.4.2 — never commit a prior-term entry by count alone).
  Followers adopt `min(leader_commit, last_index)`.
- **Read-index linearizable reads.** On `read_index`, the leader snapshots its
  current `commit_index` as the read's floor, confirms it is still leader via a
  heartbeat quorum round, then releases the read (token into `Ready.reads`) once
  `last_applied >= floor`. A freshly elected leader will not serve reads until it
  has committed an entry in its own term (it appends a no-op on election to close
  that gap).

## Determinism

- Logical time only: `tick()` drives all timeouts; no `Instant`/`SystemTime` in the
  core.
- Randomness is a per-node seeded PRNG inside the core → same seed, same election
  timing.
- The sim harness owns the clock and the network and advances all nodes from **one
  task**, so every interleaving is reproducible from a scenario seed.

## Testing strategy

- **Unit tests** (core in isolation, hand-built `MemStorage`): up-to-date vote
  check; term step-down + persist; pre-vote is non-disruptive; consistency-check
  rejection + `conflict_index` back-up; append-with-conflict truncation;
  commit-index majority math including the current-term rule; read-index release
  conditions (floor, quorum confirm, apply catch-up, no-op gap).
- **Deterministic multi-node simulation.** N `RaftCore`s over `MemStorage`, driven
  by a synchronous in-harness message router with seeded fault injection —
  partition, drop, reorder, crash-restart. After each scripted scenario assert the
  safety invariants:
  - **Election safety** — at most one leader per term.
  - **Log matching** — equal (index, term) ⇒ identical logs up to that index.
  - **State-machine safety** — no two nodes apply different commands at one index.
  - **Leader completeness** — a committed entry is present in every later leader.
  Scenarios: clean election; leader crash → re-election; partition + heal; dropped
  AppendEntries → back-up + catch-up; reordered delivery.
- **Restart test.** Discard a node's volatile state, rebuild the core from its
  `MemStorage`, and verify persisted term/vote/log recover and no committed entry
  is lost.

## Risks and rabbit-holes (fenced)

- **Commit-safety off-by-one (§5.4.2).** The classic Raft bug: committing a
  prior-term entry by replica count. Pinned by a dedicated sim scenario
  (leader appends, partitions before commit, new leader overwrites).
- **Pre-vote / vote persistence ordering.** Persisting `voted_for` *after* replying
  would allow a double-vote across a crash. Pinned by the restart test.
- **Read-index staleness.** Serving a read before quorum-confirming leadership (or
  before apply catches up) is a linearizability violation. Pinned by unit tests on
  the release conditions.
- **Sim non-determinism.** Any `Instant::now()` / `HashMap`-iteration ordering
  leaking into the harness breaks reproducibility. The sim uses logical time, a
  single task, and ordered collections; forbid wall-clock and unordered iteration
  in the router.

## Deliverable — ships when

- `RaftCore` + `RaftStorage` + `MemStorage` compile clean (clippy `-D warnings`,
  fmt) with no `unwrap`/`expect` in the core's `Result` paths.
- The deterministic sim proves the four safety invariants across the scripted
  partition / crash / drop / reorder scenarios from a fixed seed.
- The restart test passes. `cargo test` green workspace-wide.
- Merged to `main` via PR after the whole-branch opus review.

## Plan sequence within Plan C (bite-sized, TDD, subagent-driven)

| # | Task | Done when |
| --- | --- | --- |
| 1 | `RaftStorage` trait + `MemStorage` + `Ready`/`ReadToken` types | trait impl'd, storage unit tests green |
| 2 | Core skeleton: roles, volatile/persistent state, `tick`, `ready()` drain | constructs; tick advances logical time |
| 3 | Election + pre-vote + vote granting (persist ordering) | election unit tests green |
| 4 | Replication: AppendEntries send/recv, consistency check, conflict back-up | log-matching + back-up unit tests green |
| 5 | Commit advancement (majority + current-term rule) + follower commit | commit-math unit tests green |
| 6 | Read-index linearizable reads (no-op-on-election, quorum confirm, release) | read-index unit tests green |
| 7 | Deterministic sim harness + fault injection + safety-invariant asserts | scripted scenarios pass from fixed seed |
| 8 | Restart test + whole-branch opus review + fixes | workspace green, review clean |
