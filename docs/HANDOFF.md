# Cairn — Session Handoff (2026-07-19)

Cold-start briefing for the next session. Read this first, then
`docs/superpowers/specs/`.

> **Latest:** Plan D (snapshots + single-server membership) is **DONE and merged
> to a green `main`** — the whole-branch opus review caught a Critical
> (split-brain) + an Important, both fixed. **Plan E (async node driver + real-TCP
> integration) is the immediate next cycle.** See "Plan D — DONE" below.

## What cairn is

A from-scratch **sharded, Raft-replicated, LSM-backed distributed key-value
store** in Rust — a flagship portfolio project to demonstrate architecting
large, hard systems. Complexity comes from the problem (consensus, storage
internals, MVCC, sharding), not traffic volume. The differentiator is
*provable correctness under failure* (a chaos/Jepsen-style suite).
Public repo: https://github.com/uptonm/cairn.

## Current state — all merged to `main`, green

- **`crates/storage`** — Phase-1 LSM engine: WAL (CRC, crash-tolerant replay) →
  memtable → SSTables (footer index + bloom) → atomic flush (temp+rename) →
  leveled compaction. Differential proptest vs `BTreeMap`, crash-recovery tests,
  criterion baselines. **34 unit + 1 proptest + 2 recovery.**
- **`crates/raft`** — Raft-cycle Plans A + B + **C**:
  - Log store: `types`, `error`, `hardstate` (atomic CRC'd), `oplog` (CRC records,
    torn-tail replay + **truncate-on-open**), `log` (`RaftLog`: index-addressed,
    truncate/compact, invariants return `Error::Corruption`).
  - **`rpc.rs`** — the RPC contract (`Message` enum + RequestVote/AppendEntries/
    InstallSnapshot req/resp). *Was "frozen"; Plan C added `pre_vote: bool` to
    `RequestVoteResp` — see the design-corrections note below.*
  - **`transport`** — `Transport` trait + `in_memory` (deterministic, seeded,
    fault-injectable: partition/drop/delay) + `tcp` (length-prefixed framed, with a
    hand-rolled `codec.rs`). See `crates/raft/TRANSPORT_NOTES.md`.
  - **`lincheck.rs`** — standalone per-key linearizability checker for the future
    chaos harness.
  - **`core/`** (Plan C) — `RaftCore<S: RaftStorage>`: pure, sync, I/O-free step
    function. `storage.rs` (`RaftStorage` trait + in-memory `MemStorage`),
    `core/mod.rs` (state, `tick`, `step`, `ready()` drain), `core/election.rs`
    (pre-vote + election + vote granting), `core/replication.rs` (AppendEntries,
    consistency check, conflict back-up, commit-by-majority §5.4.2, apply),
    `core/read_index.rs` (read-index linearizable reads). `tests/raft_sim.rs` —
    deterministic N-node sim proving the 4 safety invariants (election safety, log
    matching, state-machine safety, leader-completeness *containment*) under
    partition/drop/reorder/crash-restart.
  - **Plan D (snapshots + single-server membership)** — `RaftStorage` snapshot
    persistence (`save_snapshot(meta,data,config)`/`read_snapshot`) + `Ready.restore`;
    `core::compact(index,data)` (snapshot a committed prefix, storing the config
    **as-of `index`**); `InstallSnapshot` send/receive/restore; `LogEntry.entry_type
    { Normal, ConfigChange }` (additive, torn-tail-safe op-log + TCP codec);
    **single-server membership** (`ConfChange{AddVoter,RemoveVoter}`,
    `propose_conf_change`, a live `voters` set replacing bootstrap `config.peers`
    for ALL quorum/peer iteration, effect-on-append + revert-on-truncation,
    one-change-in-flight, leader step-down on committing its own removal, config
    persisted as snapshot state). Sim extended with snapshot catch-up + grow/shrink/
    replace membership scenarios. **148 raft lib + 13 sim tests, green.**
- **`apps/site`** — Next.js + Tailwind + shadcn + Fumadocs marketing/docs site.
  Domain `cairn.uptonm.dev`. **NOT deployed** (design/SEO delegated to the user's
  designer; deploy needs explicit user go — Vercel root dir = `apps/site`).
- **Monorepo**: `apps/*` + `packages/*` (Bun workspace) for TS; `crates/*` for
  Rust (Cargo workspace). `packages/*` empty, reserved.

## Build / test / run

```bash
cargo test                                   # both crates
cargo clippy --all-targets -- -D warnings    # must be clean
cargo fmt --check
cd apps/site && bun install && bun run build  # site (run bun install from repo root for the workspace)
```

## Driving cairn to 100% (roadmap · process · guardrails · gotchas)

**GOAL:** drive cairn to 100% done — the full distributed KV in
`docs/superpowers/specs/2026-07-18-cairn-distributed-kv-design.md` — one shippable
subsystem at a time, each merged to a green `main`.

**Immediate next step:** **Plan E** — the async node driver wiring `RaftCore` +
`Transport` + a `RaftLog`-backed `RaftStorage` adapter + apply/restore callback,
with a real-TCP integration test. See "Plan D — DONE" below for the exact
Plan-E-owned carryover items (the adapter must adopt `LogEntry.entry_type` +
`save_snapshot(meta,data,config)`; the driver must apply `Ready.restore` before
any `Ready.apply` in one drained batch).

**Remaining roadmap** (each its own spec→plan→build→review→merge cycle, in order):
- **Plan E** — async node driver wiring `RaftCore` + `Transport` + a
  **`RaftLog`-backed `RaftStorage` adapter** (must adopt `LogEntry.entry_type` +
  `save_snapshot(meta,data,config)`/`read_snapshot`) + an apply/restore callback.
  The driver **must apply `Ready.restore` before any `Ready.apply` in one drained
  batch.** Ship a real-TCP integration test (cluster elects a leader + replicates).
- **Chaos/Jepsen harness** — drive N cores over the in-memory transport's fault
  injection, record histories, verify with `lincheck`. FIRST extend lincheck's
  `Event` type for **crashed ops** (invoked, no response). Also drive **reads**
  through it (the Plan C/D sim doesn't observe reads).
- **Crash-hardening finale** — storage MANIFEST of live SSTables (crash-atomic
  multi-file compaction); resolve the transport HOL-blocking + seed-determinism
  caveats (`crates/raft/TRANSPORT_NOTES.md`).
- **Phase 2** — MVCC transactions (snapshot isolation) over the replicated store.
- **Phase 3** — multi-Raft (many groups, one node set).
- **Phase 4** — shard router + control plane + dashboard (TS/Bun, `apps/*`).

**Build process:** superpowers **brainstorming → writing-plans →
subagent-driven-development**. Each subsystem: spec (surface only genuine design
forks — the user delegates most, "you choose") → bite-sized TDD plan → execute
subagent-driven. Per task: fresh implementer subagent (**sonnet** for judgment,
**haiku** only for pure code-complete transcription) → adversarial reviewer → fix
loop until clean → after all tasks, a **whole-branch review on OPUS**. **The
whole-branch opus review has caught a Critical safety bug in EVERY subsystem it has
reviewed — never skip it.** Use **OPUS reviewers on consensus-critical tasks**
(election/replication/commit/read-index/membership/snapshot), sonnet elsewhere.
Use the skill's `scripts/task-brief PLAN N` and `scripts/review-package BASE HEAD`;
hand subagents FILE PATHS, not pasted text; record the BASE commit before each
implementer (never `HEAD~1`). Track progress in `.superpowers/sdd/<name>-progress.md`.

**Guardrails:**
- Never work on `main` directly — branch in a worktree, PR, merge. Verify
  `cargo test --workspace` + `cargo clippy --all-targets -- -D warnings` +
  `cargo fmt --check` green before every merge.
- Rust 2021, no `unsafe`, no `unwrap`/`expect` in library I/O paths, corrupt/torn
  input recoverable (never panic), `BTreeMap`/`BTreeSet` for behavior-affecting
  order, logical time only in the core. Rust in `crates/`; TS in `apps/*`.
- **`rpc.rs` is no longer frozen** — deliberately extended twice for correctness
  (`RequestVoteResp.pre_vote`, `InstallSnapshotReq.config`). Extend it deliberately
  when a phase needs a distinction the wire can't express, not gratuitously.
- No public deploys without explicit user approval (site PR #13 is unmerged).
- Pause only for genuine decisions that are the user's (scope forks, deploys) or an
  unresolvable blocker. The user delegates most design calls.

**Environment gotchas (learned the hard way):**
- Multiple concurrent sessions share this repo; the PRIMARY checkout
  `~/Projects/cairn` gets its branch switched out from under you mid-task. **Always
  work in a dedicated git worktree** (`~/Projects/cairn-<subsystem>`), never the
  primary checkout, never `main`.
- `.superpowers/sdd/*.md` ledgers are **gitignored** — surface durable state into
  this file before finishing any cycle.

## Plan D — DONE (merged to `main`, green)

Membership uses **single-server changes** (not joint consensus — simpler, safer,
majority-overlap guaranteed). All 7 tasks built + adversarially reviewed (opus on
the consensus-critical ones), whole-branch opus review clean after fixes, merged.
**Green: 148 raft lib + 13 sim + 34 storage tests, clippy `-D warnings` + fmt clean,
sim 3× deterministic.**

- **T1** `RaftStorage` snapshot persistence (`save_snapshot(meta,data,config)` /
  `read_snapshot() -> Option<(meta,data,config)>`) + `Ready.restore`.
- **T2** core `compact(index, data)` — snapshot a committed prefix, storing the
  config **as-of `index`** (a whole-branch-review fix: it used to store the live
  effect-on-append `voters`, which could include an uncommitted change past the
  compaction point → phantom voter after truncation).
- **T3** `InstallSnapshot` send/receive/restore. `handle_install_snapshot` clears
  `apply_buf` on install (stale sub-base applies would otherwise ride the same
  `ready()` batch as `restore`); `handle_install_snapshot_resp` has a
  `resp.term < current_term` stale-drop.
- **T4** `LogEntry.entry_type: EntryType { Normal, ConfigChange }` — additive,
  torn-tail-safe op-log (tested torn *exactly* at the trailing type byte) + TCP codec.
- **T5** single-server membership (`ConfChange`, live `voters` set replaces
  `config.peers` for ALL quorum/peer iteration, effect-on-append + revert-on-
  truncation, one-change-in-flight, leader step-down-on-commit) **+ config as
  snapshot state**. Extended `InstallSnapshotReq` with `config: Vec<u8>` (a
  deliberate frozen-message extension, like Plan C's `RequestVoteResp.pre_vote`).
- **T6** sim harness: restore sink (restore-before-apply), `compact_leader`,
  `add_voter`/`remove_voter` over a growable node set, `voters()`-aware
  containment/convergence checks, restore-event proof the InstallSnapshot path
  actually fires. Scenarios: `snapshot_catch_up`, `grow_three_to_five`,
  `shrink_five_to_three`, `kill_and_replace`. `RaftCore::voters()` is now `pub`.
- **T7** whole-branch opus review → **caught a Critical** (`MemStorage::read_snapshot`
  used `snapshot_data.is_empty()` as its has-snapshot predicate → an empty-payload
  snapshot read back as `None`, dropping the persisted config → `recompute_voters`
  reverted to bootstrap peers → wrong quorum → **split-brain**; fixed by keying the
  predicate off `snapshot.last_index == 0`, and `recompute_voters` now errors on a
  present-but-empty config instead of reverting) + the T2 as-of-index config bug.
  Continues the record: the whole-branch review has caught a Critical in *every*
  subsystem — **never skip it.**

**Deferred (tracked; NOT merge-blockers for a not-yet-live single-group core):**
- **§4.2.2 removed-leader / removed-server disruption** — neither
  `handle_request_vote` nor `handle_append_entries` checks candidate/leader
  membership, so a removed-but-live node can win a re-election among survivors
  (bounded: differs from the committed config by one member, so it's a *liveness*
  churn, not a safety/split-brain violation). The sim's `shrink_five_to_three`
  (seed 9) doesn't trip it, but another seed would (a true positive its voter-aware
  containment check would catch). **Own this in Plan E/hardening**: pre-vote
  leader-stickiness gate + drop votes from non-members. **Pre-production blocker.**
- `propose_conf_change` has no `readable_term`/current-term-commit gate (a leader
  can change membership before its own current-term no-op commits) — robustness,
  not safety.
- `save_snapshot`'s snapshot-boundary "contiguous" check is index-only, not
  term-aware (a divergent boundary entry is reconciled by later AppendEntries
  before it can apply — not a safety hole; tighten per §7 when convenient).
- `recompute_voters` rescans the log (+ a snapshot read on fallback) per call — perf.

## Original Plan D plan notes (superseded by single-server; kept for context)

RaftCore (Plan C) is done. Build **Plan D** on top of it: `InstallSnapshot`
handling + log compaction (the log store already has `compact_prefix`; wire the
core to decide *when* to snapshot and to install a leader's snapshot on a lagging
follower), and joint-consensus (C-old,new → C-new) membership changes (config is
currently fixed at `RaftCore::new`). The core's `step` already accepts-and-ignores
`InstallSnapshot`/`InstallSnapshotResp` — Plan D gives them behavior.

Then: **Plan E** (node driver wiring core + `Transport` + `RaftLog`-backed
`RaftStorage` adapter + apply callback into an async event loop; real-TCP
integration test) → **chaos harness** (drive N cores over fault injection, record
histories, check with `lincheck` — first extend lincheck's `Event` for crashed
ops).

Plan status: **A (log store) ✅ · B (transport) ✅ · C (core) ✅ · D (snapshots +
membership) ✅ · E ← NEXT · chaos harness.** Specs:
`docs/superpowers/specs/2026-07-18-cairn-raftcore-plan-c-design.md`
(+ `-cairn-raft-design.md` + the distributed-kv design). Plan:
`docs/superpowers/plans/2026-07-18-raft-core-plan-c.md`. Process: superpowers
brainstorm → writing-plans → subagent-driven-development (per-task implement →
adversarial review → fix loop → **opus whole-branch final review — this caught
two Critical cross-cutting safety bugs in Plan C that per-task reviews missed;
never skip it**). Cheap model (haiku) for code-complete transcription, sonnet for
judgment/reviews, opus for consensus-critical + whole-branch reviews.

## Plan C — design corrections & the rpc.rs contract change (read before Plan D/E)

The whole-branch opus review found (and we fixed) two Critical safety bugs whose
fixes deviate from what the Plan C spec/plan *prose* described. The CODE is the
source of truth; the spec/plan prose on these two points is superseded:

- **Pre-vote disambiguation → `RequestVoteResp` gained `pre_vote: bool`.** The
  "frozen" `RequestVoteResp { term, vote_granted }` cannot distinguish a pre-vote
  grant from a real vote when the granting peer is itself at the candidate's
  prospective term T+1 → two leaders in one term. Term-overloading provably can't
  fix it, so (with user sign-off) we minimally extended the contract: responder
  echoes `req.pre_vote`; `handle_vote_resp` counts a response toward the real-vote
  tally only when `!resp.pre_vote` (and pre-vote tally only when `resp.pre_vote`).
  The hand-rolled TCP `codec.rs` was updated for the new field (length `4+8+1+1`).
  **rpc.rs is no longer frozen-as-was — extend it deliberately if a later phase
  needs a field, don't treat it as immutable.**
- **Read-index leadership confirmation uses a per-peer send/ack barrier, not
  `last_contact_tick`.** A read snapshots `barrier[peer]=send_count[peer]`, forces
  a broadcast, and releases only when a quorum has `ack_count[peer] > barrier[peer]`
  (pigeonhole: a peer acked a *post-registration* send). `ack_count` increments
  only when a same-term success pops the shared `inflight` FIFO (duplicate-ack
  safe). This is correct under arbitrary reorder/drop; `last_contact_tick`/
  `tick_count` were removed.

## Tracked items / known limitations (surfaced here — the `.superpowers/sdd/*` ledgers are gitignored)

**From Plan C/D (RaftCore) — deferred/tracked:**
- ✅ **Plan D delivered** `InstallSnapshot` handling + snapshot install on lagging
  followers, and single-server membership (config no longer fixed at `new`).
  Re-verify: `term_at`/`term()` at the snapshot base and the conflict-backup scan
  across a compacted region were part of the Plan D whole-branch review's hunt list
  (came back clean) — but the chaos harness should re-exercise them under fault
  injection.
- **Plan E owns:** a `RaftLog`-backed `RaftStorage` adapter (Plan C ships only the
  in-memory `MemStorage`); the async node driver; real-TCP integration. Add a
  byte-level serialize-reload restart test then (Plan C's restart test reuses a
  retained in-memory `MemStorage`, not a real reload).
- **Chaos harness (Plan 4) owns:** driving *reads* through arbitrary-reorder fault
  injection (the sim currently doesn't observe reads). The read-index send/ack
  barrier is correct under arbitrary reorder given no ack *duplication* (enforced
  in-core via the inflight-pop gate); add a sim read-linearizability observer then.
- **Minor perf (not a bug):** `read_index` forces a full `broadcast_append` per
  call — O(reads × peers) heartbeats. Batch multiple pending reads into one forced
  round if read throughput ever matters.
- **Benign nuance:** in `handle_vote_resp`, the role/flag short-circuit runs before
  the higher-term step-down check, so a *wrong-flavor* higher-term vote reply no
  longer forces an immediate step-down. Verified non-harmful (real-vote replies +
  AppendEntries still step the node down on a higher term); left as-is.

**Before the chaos harness / before this backs a live cluster:**
- **`lincheck` `Event` can't express crashed ops** (invoked, no response) — the
  dominant anomaly source in real fault-injection traces. Extend the `Event` type
  before wiring lincheck into the harness. Also: no search memoization (factorial
  worst-case on highly-concurrent single-key histories) — add a cap/cache.
- **Storage MANIFEST**: multi-file compaction is not crash-atomic across the
  rename→delete window (a crash there can orphan a stale SSTable that resurrects a
  dropped tombstone on reopen). Fix = an on-disk MANIFEST of live SSTables.
  Documented in `crates/storage/src/engine.rs`.
- **Transport caveats** (`TRANSPORT_NOTES.md`): the shared 16 MiB `InboundBudget`
  can head-of-line-block all peers when one large frame is in flight; the
  in-memory transport's `seed` does NOT order *concurrent* senders (delivery uses
  `Instant::now()` + FIFO tiebreak) — reproducible only for a **single controlling
  task**. RaftCore's sim harness must drive all nodes from one task (or add
  seeded interleaving) to stay deterministic. **Verify this when building the harness.**
- **`truncate_suffix`'s snapshot-boundary check is still `debug_assert`** (only
  `append` + `compact_prefix` were promoted to real errors).

**Already done (do not redo):** LSM seqno-reuse-after-reopen fix; raft
torn-tail-truncate-on-open + append/compact invariant errors; length-field
allocation bounds across all readers; directory fsync after atomic renames.

**Product:** deploy `apps/site` to `cairn.uptonm.dev` (Vercel, root `apps/site`)
— needs user go. Consider filing the tracked items above as GitHub issues.

## Decisions locked (do not re-litigate)

- Rust stays in `crates/`; TS in `apps/*` + `packages/*`.
- Real **TCP is the product transport**; the in-memory transport is the
  deterministic test substrate (both behind the `Transport` trait).
- **Dedicated** Raft log store (not the LSM engine) — index-addressed, suffix
  truncation, prefix compaction.
- Plan C scope = full single-group Raft *core* (election/replication/commit/
  read-index); snapshots + membership are Plan D.
- Every phase ships as a finished, tested, demoable system. Adversarial review
  loop on every change; site design/SEO owned by the user's designer.

## Parallelization playbook

The transport, lincheck, and hardening were built by the user's **parallel
agents** against **frozen interface contracts**, then PR'd + reviewed + integrated.
To parallelize again: freeze the interface (types + trait) first, hand each agent
a self-contained prompt (context, contract, TDD, deliverable, report format) on
its own branch, then review + integrate. RaftCore is best kept on the main
adversarial loop (highest bug risk); peripheral pieces (a metrics layer, the
chaos-harness scaffolding, Plan D sub-pieces) parallelize well.
