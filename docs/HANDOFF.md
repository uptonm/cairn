# Cairn — Session Handoff (2026-07-18)

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
- **`crates/raft`** — Raft-cycle Plans A + B:
  - Log store: `types`, `error`, `hardstate` (atomic CRC'd), `oplog` (CRC records,
    torn-tail replay + **truncate-on-open**), `log` (`RaftLog`: index-addressed,
    truncate/compact, invariants return `Error::Corruption`).
  - **`rpc.rs`** — the FROZEN RPC contract (`Message` enum + RequestVote/
    AppendEntries/InstallSnapshot req/resp). *RaftCore builds against this.*
  - **`transport`** — `Transport` trait + `in_memory` (deterministic, seeded,
    fault-injectable: partition/drop/delay) + `tcp` (length-prefixed framed).
    See `crates/raft/TRANSPORT_NOTES.md`.
  - **`lincheck.rs`** — standalone per-key linearizability checker for the future
    chaos harness. **61 raft lib tests + 1 proptest total.**
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

## NEXT: RaftCore — Plan C

Build the consensus core as a **pure, I/O-free step function** (tick / inbound
message / client proposal → outputs: messages to send, entries to persist,
entries to apply) so it is unit-testable without a runtime. Cover: pre-vote +
election, log replication with the consistency check, commit-index advancement
by majority, linearizable reads via read-index. Build it **against the frozen
`rpc.rs` `Message` types + the `Transport` trait** (already on `main`).

Then: **Plan D** (snapshots/log-compaction + joint-consensus membership) →
**Plan E** (node driver wiring core + transport + log store + apply callback) →
**chaos harness** (drives N cores over the in-memory transport's fault injection,
records histories, checks them with `lincheck`).

Plan status: **A (log store) ✅ · B (transport) ✅ · C (core) ← NEXT · D · E ·
chaos harness.** Specs: `docs/superpowers/specs/2026-07-18-cairn-raft-design.md`
(+ the distributed-kv design). Process: superpowers brainstorm → writing-plans →
subagent-driven-development (per-task implement → adversarial review → fix loop →
opus final review). Cheap model (haiku) for code-complete transcription tasks,
sonnet for judgment/reviews, opus for whole-branch final review.

## Tracked items / known limitations (surfaced here — the `.superpowers/sdd/*` ledgers are gitignored)

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
