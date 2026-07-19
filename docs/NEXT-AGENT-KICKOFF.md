# Cairn — Next-Agent Kickoff Prompt (2026-07-19)

Paste the block below to launch the next session. It resumes from the Plan D
checkpoint and drives cairn to 100%. The authoritative state lives in
`docs/HANDOFF.md` (on branch `feat/raft-plan-d`); this file is just the launcher.

---

You are continuing work on **cairn** — a from-scratch, sharded, Raft-replicated,
LSM-backed distributed key-value store in Rust (github.com/uptonm/cairn, cloned at
~/Projects/cairn). It is a flagship portfolio project; correctness and a clean
process record matter as much as features.

GOAL: drive cairn to **100% done** — the full distributed KV described in
`docs/superpowers/specs/2026-07-18-cairn-distributed-kv-design.md` — one shippable
subsystem at a time, until every phase is built, tested, and merged to a green
`main`.

FIRST, read (do not skip): `docs/HANDOFF.md` (cold-start briefing + current state +
the **Plan D checkpoint** + the **Plan C design corrections / rpc.rs contract
changes** + tracked items + locked decisions), then the specs/plans under
`docs/superpowers/`. Your auto-memory ([[cairn-project]]) also has state.

ENVIRONMENT GOTCHAS (learned the hard way — heed these):
- The repo has **multiple concurrent sessions**; the PRIMARY checkout
  `~/Projects/cairn` gets its branch switched out from under you mid-task. **Work
  in a dedicated git worktree** (e.g. `~/Projects/cairn-<subsystem>` off the branch
  you need), never the primary checkout, never `main` directly.
- `.superpowers/sdd/*.md` ledgers are **gitignored** — surface anything durable
  into committed docs (`docs/HANDOFF.md`).

CURRENT STATE (2026-07-19):
- Plan A (log store), B (transport), **C (RaftCore)** ✅ merged to `main`. Plan C's
  whole-branch review caught 2 Critical safety bugs (fixed); it required extending
  `RequestVoteResp` with `pre_vote: bool`.
- **Plan D (snapshots + single-server membership): 5/7 tasks done on branch
  `feat/raft-plan-d` (pushed), green (141 raft lib + 9 sim + storage tests, clippy
  `-D warnings` + fmt clean).** A review caught a Critical (a compacted ConfigChange
  reverted voters to bootstrap → wrong quorum), fixed by making the config part of
  snapshot state — this extended `InstallSnapshotReq` with `config: Vec<u8>`.
  **START HERE:** resume `feat/raft-plan-d` in a worktree; do **T6** (extend
  `crates/raft/tests/raft_sim.rs` for snapshot catch-up + grow/shrink/replace
  membership scenarios, `voters()`-aware invariant checks, deterministic 3× run)
  then **T7** (whole-branch OPUS review + fixes + update HANDOFF + PR + merge to a
  green `main`). Exact resume steps + tracked minors are in `docs/HANDOFF.md`.
- Site docs/SEO: **PR #13 open, unmerged** — merging may trigger a Vercel
  production deploy, which needs explicit user go. Do NOT merge/deploy it without
  that go.

REMAINING ROADMAP after Plan D (each its own spec→plan→build→review→merge cycle):
- **Plan E** — node driver: an async event loop wiring `RaftCore` + `Transport` + a
  **`RaftLog`-backed `RaftStorage` adapter** (must adopt `LogEntry.entry_type` +
  snapshot persistence: `save_snapshot(meta,data,config)` / `read_snapshot`) + an
  apply/restore callback. The driver **must apply `Ready.restore` before any
  `Ready.apply` in one drained batch.** Ship a real-TCP integration test (a cluster
  elects a leader + replicates).
- **Chaos/Jepsen harness** — drive N cores over the in-memory transport's fault
  injection, record histories, verify with `lincheck`. FIRST extend lincheck's
  `Event` type to represent **crashed ops** (invoked, no response). Also drive
  **reads** through it (the Plan C/D sim does not observe reads yet).
- **Crash-hardening finale** — storage MANIFEST of live SSTables (crash-atomic
  multi-file compaction); resolve the transport HOL-blocking + seed-determinism
  caveats (`crates/raft/TRANSPORT_NOTES.md`).
- **Phase 2** — MVCC transactions (snapshot isolation) over the replicated store.
- **Phase 3** — multi-Raft (many groups, one node set).
- **Phase 4** — shard router + control plane + dashboard (TS/Bun, in `apps/*`).

PROCESS (this is how cairn is built — follow it):
- superpowers skills: **brainstorming → writing-plans → subagent-driven-development**.
  Each subsystem: write a spec (surface only genuine design forks — the user
  delegates most, "you choose"; get approval) → a bite-sized TDD plan → execute
  subagent-driven.
- Per task: fresh implementer subagent (**sonnet** for judgment, **haiku** only for
  pure code-complete transcription) → adversarial reviewer subagent → fix loop until
  clean → after all tasks, a **whole-branch review on OPUS**. THE WHOLE-BRANCH OPUS
  REVIEW HAS CAUGHT A CRITICAL SAFETY BUG IN EVERY SUBSYSTEM IT HAS REVIEWED — never
  skip it. Use **OPUS reviewers on consensus-critical tasks** (election, replication,
  commit, read-index, membership, snapshot install), sonnet elsewhere scaled to risk.
- Use the subagent-driven-development scripts (in the skill dir): `scripts/task-brief
  PLAN N` (extract a task brief to a file) and `scripts/review-package BASE HEAD`
  (build a diff package for reviewers). Hand artifacts to subagents as FILE PATHS,
  not pasted text. Record the BASE commit before each implementer; never use `HEAD~1`.
- Track progress in `.superpowers/sdd/<name>-progress.md` (gitignored — surface
  deferred/tracked items into `docs/HANDOFF.md` before finishing a cycle).

GUARDRAILS:
- Never work on `main` directly — branch in a worktree, PR, merge. Verify
  `cargo test --workspace` + `cargo clippy --all-targets -- -D warnings` +
  `cargo fmt --check` green before every merge; the whole workspace must stay green.
- Constraints: Rust 2021, no `unsafe`, no `unwrap`/`expect` in library I/O paths,
  corrupt/torn input must be recoverable (never panic), `BTreeMap`/`BTreeSet` for
  any behavior-affecting iteration order, logical time only in the core. Rust stays
  in `crates/`; TS in `apps/*`.
- **`rpc.rs` is NO LONGER frozen** — it was deliberately extended twice for
  correctness (`RequestVoteResp.pre_vote`, `InstallSnapshotReq.config`). Extend it
  deliberately when a phase needs a distinction the wire can't express, with the
  same adversarial scrutiny as any consensus-critical change — but don't churn it
  gratuitously.
- Do NOT deploy the site or anything public without explicit user approval.
- Don't re-litigate locked decisions (see HANDOFF.md): dedicated Raft log store;
  TCP-is-product / in-memory-is-test-substrate; **single-server membership (not
  joint consensus)**; every phase ships finished.
- Pause only for genuine decisions that are the user's to make (scope forks, public
  deploys) or a blocker you can't resolve. The user delegates most design calls.

When you finish a subsystem, update `docs/HANDOFF.md` so the next session stays
oriented. End each cycle with a merged PR and a green `main`.

Start by reading `docs/HANDOFF.md`, confirm the `feat/raft-plan-d` branch is green
in a worktree, then **finish Plan D (T6 sim → T7 whole-branch opus review → merge)**.
Then keep going through the roadmap until cairn is 100% done, pausing only for
genuine user decisions or an unresolvable blocker.
