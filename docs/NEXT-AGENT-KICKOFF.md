# Cairn — Next-Agent Kickoff Prompt

Paste the block below to launch the next session. All detail (state, roadmap,
process, guardrails, gotchas) lives in `docs/HANDOFF.md`.

---

You are continuing **cairn** — a from-scratch sharded, Raft-replicated, LSM-backed
distributed KV store in Rust (~/Projects/cairn, github.com/uptonm/cairn). Flagship
portfolio project; correctness and a clean process record matter as much as features.

**READ `docs/HANDOFF.md` FIRST** (on branch `feat/raft-plan-d`) — it has the full
current state, the roadmap to 100%, the build process, guardrails, environment
gotchas, and locked decisions. Your auto-memory ([[cairn-project]]) also has state.

GOAL: drive cairn to 100% done, one subsystem at a time (spec → plan →
subagent-driven build → whole-branch OPUS review → merge to a green `main`), pausing
only for genuine user decisions.

START: resume `feat/raft-plan-d` in a **dedicated git worktree** (NOT the primary
checkout — concurrent sessions thrash it; NOT `main`), confirm it's green, finish
**Plan D** (T6 sim scenarios + T7 whole-branch opus review → merge), then continue
the roadmap in `docs/HANDOFF.md` until cairn is done.
