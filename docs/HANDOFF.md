# Cairn — Session Handoff (2026-07-19)

Cold-start briefing for the next session. Read this first, then
`docs/superpowers/specs/`.

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
    partition/drop/reorder/crash-restart. **104 raft lib + 9 sim tests, green.**
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

## NEXT: Plan D — snapshots/log-compaction + joint-consensus membership

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

Plan status: **A (log store) ✅ · B (transport) ✅ · C (core) ✅ · D ← NEXT · E ·
chaos harness.** Specs: `docs/superpowers/specs/2026-07-18-cairn-raftcore-plan-c-design.md`
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

**From Plan C (RaftCore) — deferred/tracked:**
- **Plan D owns:** `InstallSnapshot` handling + snapshot install on lagging
  followers (core currently accepts-and-ignores it); membership changes (config
  fixed at `new`). Also: `term_at` in `replication.rs` masks a compacted index
  (`0 < idx < snapshot.last_index`) as term 0 — harmless until snapshots exist,
  but the conflict-backup scan must be revisited when Plan D adds compaction.
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
