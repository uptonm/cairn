# Plan D — Snapshots + Single-Server Membership Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `RaftCore` (Plan C, on `main`) with **snapshots/log-compaction** (compact a committed prefix; `InstallSnapshot` a follower that lagged past it) and **single-server membership changes** (add/remove one node at a time), proven safe by the deterministic simulation.

**Architecture:** Same seams as Plan C — persistence behind the `RaftStorage` trait, network + apply + now **restore** as drained `Ready` outputs, core stays pure/sync/I-O-free. Snapshot *bytes* live in `RaftStorage`; snapshot *content* crosses the boundary via a `compact(index, data)` input and a `Ready.restore` output (driver-mediated, like apply). Config changes are `LogEntry`s tagged with a new `EntryType`; the core derives its live voter set from the latest config entry in its log (effect-on-append). Membership is single-server (majority overlap guaranteed — no joint consensus).

**Tech Stack:** Rust 2021, `crates/raft` (`cairn-raft`). Builds on the merged Plan C core. No new deps. Spec: `docs/superpowers/specs/2026-07-19-cairn-plan-d-snapshots-membership.md`.

## Global Constraints

- Rust edition 2021. No new crate deps. No `unsafe`. **No `.unwrap()`/`.expect()` outside `#[cfg(test)]`** (except `.try_into().unwrap()` on already-length-checked slices). Storage errors propagate as `Err` out of every core method — never panic on storage failure or malformed input.
- `BTreeMap`/`BTreeSet` for anything whose iteration order can affect emitted messages/decisions — never `HashMap`/`HashSet`. Logical time only (no `Instant`/`SystemTime`).
- `cargo test --workspace`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check` clean at every commit.
- Types: `NodeId`/`Term`/`LogIndex` = `u64`; log indices 1-based (0 = before first entry). Quorum for `n` voters = `n/2 + 1`.
- **Compaction never discards uncommitted/unapplied state**; **config takes effect on append and reverts on truncation**; **one membership change in flight at a time**; a leader removed from the new config steps down only *after* the removal commits.
- The op-log (`oplog.rs`) format change must stay **crash-recoverable / torn-tail-safe** (a short/torn record → stop cleanly, keep the valid prefix, never panic) — the existing log-store guarantee.

## Current-state anchors (verified in the worktree — build against these)

- `RaftStorage` (storage.rs): `hard_state`/`save_hard_state`/`last_index`/`last_term`/`term`/`entries_from`/`snapshot_meta`/`append`/`truncate_suffix`. `MemStorage { hs, entries: Vec<LogEntry>, snapshot: SnapshotMeta }` (Default). Note `MemStorage.snapshot` is currently always the default `(0,0)` — nothing sets it yet.
- `RaftCore<S>` (core/mod.rs): fields incl. `config: Config{id,peers,election_timeout,heartbeat_interval,seed}`, `role`, `leader_id`, `commit_index`, `last_applied`, `next_index`/`match_index`/`inflight`/`send_count`/`ack_count: BTreeMap`, `pending_reads`, `readable_term`. `Ready { messages, apply, reads }`. `step` currently ignores `InstallSnapshot`/`InstallSnapshotResp`. `#[cfg(test)]` accessors: `stored_hard_state`, `match_index_of`, `ack_count_of`.
- Replication (core/replication.rs): `send_append_to(peer)`, `broadcast_append`, `handle_append_entries`, `handle_append_resp`, `propose`, `maybe_advance_commit`, `advance_apply`.
- Election (core/election.rs): `become_leader` (inits next/match, clears inflight/pending_reads/send_count/ack_count, resets readable_term, appends no-op), `become_follower`, `quorum()`.
- `LogEntry { term, index, command: Vec<u8> }` (types.rs, derives serde). Hand-encoded in `oplog.rs` (`Op::Append`: term8+index8+clen4+command) and `codec.rs` (`add_log_entry_len = len + 24 + command.len`; `read_log_entries`). `rpc::InstallSnapshotReq { term, leader_id, last_index, last_term, data: Vec<u8> }`, `InstallSnapshotResp { term }` (frozen — do not change these).

---

### Task 1: `RaftStorage` snapshot persistence + `Ready.restore`

**Files:** Modify `crates/raft/src/storage.rs`, `crates/raft/src/core/mod.rs`.

**Interfaces — Produces:**
- `RaftStorage` gains:
  - `fn save_snapshot(&mut self, meta: SnapshotMeta, data: &[u8]) -> Result<()>` — persist `(meta, data)` as the latest snapshot AND compact the log to that base: drop every entry with `index <= meta.last_index`; **keep** entries with `index > meta.last_index` ONLY if they remain contiguous from `meta.last_index + 1` (i.e. an entry at exactly `meta.last_index+1` exists) — otherwise discard ALL remaining entries (a snapshot from a leader supersedes a shorter/divergent follower log). Set `snapshot = meta`. Reject `meta.last_index < current snapshot.last_index` with `Err(Corruption)` (never move the snapshot backwards).
  - `fn read_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>>` — the latest saved snapshot, or `None`.
- `MemStorage` gains a `snapshot_data: Vec<u8>` field (empty = no snapshot); implement both methods. `snapshot_meta()` still returns `self.snapshot`.
- `Ready` gains `pub restore: Option<(SnapshotMeta, Vec<u8>)>` (Default `None`); `ready()` drains it via `mem::take` alongside the others. Add a private `restore_buf: Option<(SnapshotMeta, Vec<u8>)>` field to `RaftCore` (init `None` in `new`).

- [ ] **Step 1: Write failing tests** in storage.rs `tests`:
```rust
#[test]
fn save_and_read_snapshot_compacts_log() {
    let mut s = MemStorage::default();
    s.append(&[e(1,1), e(1,2), e(1,3)]).unwrap();
    s.save_snapshot(SnapshotMeta{last_index:2, last_term:1}, b"snap").unwrap();
    assert_eq!(s.snapshot_meta(), SnapshotMeta{last_index:2,last_term:1});
    assert_eq!(s.read_snapshot().unwrap(), Some((SnapshotMeta{last_index:2,last_term:1}, b"snap".to_vec())));
    // entries <= 2 dropped; entry 3 (contiguous from 3) retained
    assert_eq!(s.last_index(), 3);
    assert_eq!(s.term(2).unwrap(), Some(1)); // boundary term from snapshot
    assert_eq!(s.term(3).unwrap(), Some(1));
    assert_eq!(s.entries_from(3), vec![e(1,3)]);
}
#[test]
fn snapshot_superseding_a_shorter_log_clears_it() {
    let mut s = MemStorage::default();
    s.append(&[e(1,1)]).unwrap();
    s.save_snapshot(SnapshotMeta{last_index:5, last_term:2}, b"x").unwrap(); // base beyond log
    assert_eq!(s.last_index(), 5); // no entries; base is the snapshot
    assert_eq!(s.entries_from(1), vec![]);
    assert_eq!(s.term(5).unwrap(), Some(2));
}
#[test]
fn snapshot_cannot_move_backwards() {
    let mut s = MemStorage::default();
    s.save_snapshot(SnapshotMeta{last_index:5,last_term:1}, b"a").unwrap();
    assert!(s.save_snapshot(SnapshotMeta{last_index:3,last_term:1}, b"b").is_err());
}
```
- [ ] **Step 2:** `cargo test -p cairn-raft storage::tests` → FAIL.
- [ ] **Step 3:** Implement `save_snapshot`/`read_snapshot` per the interface; add `MemStorage.snapshot_data`; add `Ready.restore` + `restore_buf` + drain. (Existing `term`/`entries_from`/`last_index` already read from `snapshot` correctly once it's set.)
- [ ] **Step 4:** `cargo test -p cairn-raft storage::tests core::` → PASS. clippy + fmt clean.
- [ ] **Step 5:** Commit `feat(raft): RaftStorage snapshot persistence + Ready.restore`.

---

### Task 2: Core `compact(index, data)`

**Files:** Modify `crates/raft/src/core/mod.rs` (or a small `core/snapshot.rs` submodule — your call; if new, `mod snapshot;`).

**Interfaces — Produces:** `pub fn compact(&mut self, index: LogIndex, data: Vec<u8>) -> Result<()>`.
- Precondition: `self.snapshot_meta().last_index < index <= self.last_applied` else `Err(Corruption)` (never compact uncommitted/unapplied entries; never go backwards; `last_applied` reachable via `self.last_applied`; add a `snapshot_meta()`-style read via `self.storage.snapshot_meta()`).
- Look up `let last_term = self.storage.term(index)?.ok_or(Error::Corruption(...))?;` build `SnapshotMeta { last_index: index, last_term }`, call `self.storage.save_snapshot(meta, &data)?`. That single call both persists the snapshot and compacts the log (Task 1). No other state changes (commit_index/last_applied are already >= index).

- [ ] **Step 1: Failing tests** (core tests): `compact` past `last_applied` errors; `compact` below the current snapshot errors; a valid `compact` drops the prefix (`entries_from(1)` shortens, `snapshot_meta().last_index == index`, `read_snapshot` returns the bytes) while `last_index`/`last_term`/`commit_index` are unchanged and reads still resolve. Build a leader, propose+commit+apply a few entries (drive via acks like the restart test), then `compact`.
- [ ] **Step 2:** FAIL. **Step 3:** Implement. **Step 4:** PASS + clippy/fmt. **Step 5:** Commit `feat(raft): core compact() — snapshot a committed prefix`.

---

### Task 3: `InstallSnapshot` send + receive + resp

**Files:** Modify `crates/raft/src/core/replication.rs`, `crates/raft/src/core/mod.rs` (route the two InstallSnapshot messages in `step` — replace the ignore arm).

**Interfaces — Produces (private on `RaftCore<S>`):** `send_install_snapshot(&mut self, peer) -> Result<()>`, `handle_install_snapshot(&mut self, from, InstallSnapshotReq) -> Result<()>`, `handle_install_snapshot_resp(&mut self, from, InstallSnapshotResp) -> Result<()>`.

**Design:**
- In `send_append_to(peer)`: FIRST check `if self.next_index[peer] <= self.storage.snapshot_meta().last_index { return self.send_install_snapshot(peer); }` (the entries the follower needs are compacted away). Otherwise proceed as today.
- `send_install_snapshot(peer)`: `let (meta, data) = self.storage.read_snapshot()?.ok_or(Error::Corruption("no snapshot to send"))?;` emit `Message::InstallSnapshot(InstallSnapshotReq { term: current_term, leader_id: self.config.id, last_index: meta.last_index, last_term: meta.last_term, data })`. (Whole snapshot in one message — chunking deferred.) Do NOT push to `inflight`/`send_count` (leadership-contact accounting stays keyed on AppendEntries; a follower being snapshotted is by definition not confirming reads).
- `handle_install_snapshot(from, req)` (follower): reject (reply `InstallSnapshotResp{term: current_term}`) if `req.term < current_term`. Else adopt: `become_follower(req.term, Some(req.leader_id))` semantics (persist on term bump, step down, reset election timer, set leader_id). If `req.last_index <= self.storage.snapshot_meta().last_index` OR `req.last_index <= self.commit_index` → stale/redundant: reply and return without installing. Else `self.storage.save_snapshot(SnapshotMeta{last_index:req.last_index,last_term:req.last_term}, &req.data)?` (Task 1 compacts/supersedes the log); set `self.commit_index = req.last_index`, `self.last_applied = req.last_index`; set `self.restore_buf = Some((meta, req.data.clone()))` so the driver reloads its state machine before applying later entries. Reply `InstallSnapshotResp{term: current_term}`.
- `handle_install_snapshot_resp(from, resp)` (leader): `resp.term > current_term` → `become_follower`. If not leader, ignore. Else set `match_index[from] = ...` — read the follower's new base from the snapshot we sent: `let base = self.storage.snapshot_meta().last_index;` `match_index[from] = max(match_index[from], base)`, `next_index[from] = base + 1`, then `send_append_to(from)` to continue catching it up with post-snapshot entries.
- Route in `step`: `InstallSnapshot(r) => handle_install_snapshot(from,r)`, `InstallSnapshotResp(r) => handle_install_snapshot_resp(from,r)`. Remove the permanent-ignore arm. Keep `install_snapshot_is_ignored_not_panicked` semantics only insofar as a malformed/stale install must not panic — update that test to reflect the new behavior (a valid install now acts).

- [ ] **Step 1: Failing tests:** a leader whose `next_index[peer]` is at/below the snapshot base sends `InstallSnapshot` (not AppendEntries); a follower receiving a fresh `InstallSnapshot` installs it (`snapshot_meta`, `commit_index`, `last_applied` advance; `Ready.restore` set to the bytes) and replies; a stale `InstallSnapshot` (`last_index <= commit_index`) is a no-op ack; the resp advances the leader's `match_index`/`next_index` for that peer and resumes AppendEntries.
- [ ] **Step 2:** FAIL. **Step 3:** Implement. **Step 4:** PASS + clippy/fmt. **Step 5:** Commit `feat(raft): InstallSnapshot send/receive/restore`.

---

### Task 4: `LogEntry.entry_type` (types + op-log + TCP codec) — additive, crash-safe

**Files:** Modify `crates/raft/src/types.rs`, `crates/raft/src/oplog.rs`, `crates/raft/src/transport/tcp/codec.rs`, and **every `LogEntry { … }` construction site** in the crate (core, tests, rpc test).

**Interfaces — Produces:** `pub enum EntryType { Normal, ConfigChange }` (Clone, Copy, Debug, PartialEq, Serialize, Deserialize; `#[derive(Default)]` with `#[default] Normal`). `LogEntry` gains `pub entry_type: EntryType`. Provide `impl LogEntry { pub fn normal(term, index, command) -> Self` and `pub fn config_change(term, index, command) -> Self }` constructors and migrate literals to them where it reduces churn (tests may keep literals with `..Default::default()`? — NO, `LogEntry` isn't `Default`; use the constructors or full literals).

**Serialization (both hand-rolled encoders must round-trip the new field, additively + crash-safely):**
- **oplog.rs** `Op::Append` encode (currently term8+index8+clen4+command): append a 1-byte `entry_type` (0=Normal, 1=ConfigChange). Put it at the END (after command) so the fixed-offset reads of term/index/clen are unchanged and only the tail parsing gains a byte — and a torn/short record still returns `None` (truncate-and-recover), never panics. Update the reader to parse the trailing type byte; if absent/short → treat the record as torn (`None`), consistent with existing torn-tail handling. Update oplog tests.
- **codec.rs** `add_log_entry_len` (currently `+24`) → `+25` for the type byte; `write` a LogEntry’s type byte and `read_log_entries` parse it; keep encode/decode symmetric and the frame-length exact (the existing `frame.remaining == 0` trailing-byte guard will catch any drift). Update codec/tcp tests.
- **rpc.rs** bincode roundtrip test: LogEntry literals gain `entry_type` (serde auto-serializes it).

- [ ] **Step 1: Failing tests:** an `EntryType::ConfigChange` LogEntry round-trips through (a) `MemStorage` append/read, (b) the oplog write→read (in oplog tests), (c) the TCP codec encode→decode (in codec/tcp tests), (d) bincode (rpc test). Each asserts `entry_type` survives. Write these first (they won't compile until the field exists — that IS the RED).
- [ ] **Step 2:** `cargo build -p cairn-raft` → FAIL (missing field at every literal) — that's expected; the task is to make the whole crate compile + these tests pass.
- [ ] **Step 3:** Add the enum + field + constructors; migrate every construction site; update the two hand-rolled codecs + their length math + tests. The Plan-C no-op append (`become_leader`) and `propose` construct `Normal` entries.
- [ ] **Step 4:** `cargo test --workspace` PASS; clippy `-D warnings` + fmt clean. Confirm the oplog torn-tail recovery test still passes with the new byte.
- [ ] **Step 5:** Commit `feat(raft): LogEntry EntryType (Normal|ConfigChange) — additive op-log + codec`.

---

### Task 5: Single-server membership in the core

**Files:** Modify `crates/raft/src/core/mod.rs`, `crates/raft/src/core/election.rs`, `crates/raft/src/core/replication.rs` (or a `core/membership.rs` submodule for the config-change helpers; `mod membership;`).

**Interfaces — Produces:**
- `pub enum ConfChange { AddVoter(NodeId), RemoveVoter(NodeId) }` (Clone, Copy, Debug, PartialEq).
- `pub fn propose_conf_change(&mut self, change: ConfChange) -> Result<Option<LogIndex>>`.
- Internal: the core tracks a live voter set. **Do not** keep using the immutable `config.peers` for quorum; instead maintain `voters: BTreeSet<NodeId>` derived from the log, and make `quorum()` and every peer-iteration use `voters`.

**Design:**
- **Live config derivation:** `voters` = the membership encoded by the **latest `ConfigChange` entry in the log** (present, not necessarily committed — effect-on-append), or the bootstrap `config.peers` if none. A `ConfigChange` entry’s `command` bytes encode the FULL resulting voter set (simple: length-prefixed list of `NodeId` `u64`s — a tiny helper `encode_voters(&BTreeSet)`/`decode_voters(&[u8])`). Recompute `voters` whenever the log’s config-entry set changes: after `append` of a config entry, and after `truncate_suffix` (a truncation can remove a config entry → revert). Implement a single private `recompute_voters(&mut self) -> Result<()>` that scans for the highest-index `ConfigChange` entry (via `entries_from` over the in-core log or a tracked pointer) and sets `voters`; call it after any append/truncate that could touch config entries, and in `new` (seed from `config.peers`; also honor a config entry already present after restart).
- **`propose_conf_change(change)`:** leader only (`Ok(None)` if not). **One-in-flight guard:** if any `ConfigChange` entry exists at `index > commit_index` → refuse (`Ok(None)`). Compute the new voter set from `voters` + `change` (reject a no-op add/remove, e.g. removing a non-member, with `Ok(None)`). Append a `LogEntry::config_change(current_term, last_index+1, encode_voters(new_set))`; `recompute_voters()` (now the new config is live for THIS node); if the change ADDs a node, init `next_index[node]=last_index()+1`, `match_index[node]=0`, `send_count`/`ack_count` entries; `broadcast_append()`; return `Ok(Some(idx))`.
- **Quorum + peers everywhere use `voters`:** `quorum() = voters.len()/2 + 1`; `become_leader`/`broadcast_append`/vote tallies/commit-majority iterate `voters` (not `config.peers`). Self counts iff `self.config.id ∈ voters`.
- **Leader removed from new config:** when a `ConfigChange` that excludes `self.config.id` **commits** (detect in `advance_apply`/`maybe_advance_commit` when `commit_index` crosses that entry): `become_follower(current_term, None)` (step down) and stop campaigning. Until it commits, the leader keeps replicating it normally (do NOT step down on append — that could stall the commit).
- **Truncation revert:** `handle_append_entries`’ conflict truncation must `recompute_voters()` after `truncate_suffix` so a reverted config entry restores the prior voter set.

- [ ] **Step 1: Failing tests:** `propose_conf_change(AddVoter)` appends a `ConfigChange` entry and `voters` immediately includes the node (effect-on-append); quorum reflects the new size; a second `propose_conf_change` before the first commits is refused; committing a `RemoveVoter(self)` steps the leader down; a truncation that removes a config entry reverts `voters`; `encode_voters`/`decode_voters` round-trip. Add `#[cfg(test)] pub(crate) fn voters(&self) -> Vec<NodeId>` accessor.
- [ ] **Step 2:** FAIL. **Step 3:** Implement per design. **Step 4:** PASS + clippy/fmt. **Step 5:** Commit `feat(raft): single-server membership (add/remove voter, effect-on-append, one-in-flight, leader step-down)`.

---

### Task 6: Simulation — snapshot catch-up + grow/shrink/replace

**Files:** Modify `crates/raft/tests/raft_sim.rs`.

**Interfaces — Consumes:** the new `compact`, `Ready.restore`, `propose_conf_change`, `ConfChange`. Harness additions: a **restore sink** (on draining `ready()`, if `restore` is `Some`, replace that node's tracked applied-log baseline with the snapshot's represented state — since the sim's "state machine" is the applied-log, model a snapshot's content as the set of committed entries up to `last_index`; on restore, seed the node's applied prefix accordingly so state-machine-safety/containment checks stay meaningful across a snapshot); a `compact_leader(up_to, data)` control; membership controls `add_voter(id)`/`remove_voter(id)` that call `propose_conf_change` on the leader and register/unregister a node in the harness. `voters()`-aware invariant checks (quorum/leader-count over the live config, not a fixed 3).

**Scenarios (each `#[test]`, each asserting the four invariants after `settle()`):**
- `snapshot_catch_up`: 3 nodes; partition follower C; on the majority, propose several entries and `compact_leader` past C's `next_index`; heal; C receives `InstallSnapshot`, restores, and converges; invariants hold and C's applied state matches.
- `grow_three_to_five`: start 3, `add_voter(4)` then `add_voter(5)` one at a time (each committed + caught up before the next); propose across the change; assert one leader per term throughout and all 5 converge.
- `shrink_five_to_three`: reverse; `remove_voter` twice; assert progress + safety, and that removing the leader triggers a clean re-election in the new config.
- `kill_and_replace`: remove a crashed node, add a fresh one that catches up (via snapshot if a compaction happened); invariants hold.
- Keep determinism: run the file 3× (`for i in 1 2 3; do cargo test -p cairn-raft --test raft_sim || break; done`).

- [ ] **Step 1:** Build the harness extensions + `snapshot_catch_up`; iterate to green. **Step 2:** add the membership scenarios. **Step 3:** 3× determinism. **Step 4:** full gate. **Step 5:** Commit `test(raft): sim — snapshot catch-up + single-server membership scenarios`.

---

### Task 7: Whole-branch review + handoff + PR

- [ ] **Step 1:** `cargo test --workspace && cargo clippy --all-targets -- -D warnings && cargo fmt --check` — all green.
- [ ] **Step 2:** Whole-branch **opus** review over the full `feat/raft-plan-d` diff (`git merge-base main HEAD`..HEAD) with this plan + the spec as rubric. Hunt cross-cutting bugs: config effect-on-append/revert races; quorum using stale `config.peers` anywhere instead of `voters`; leader-removed step-down timing (early step-down stalling commit); snapshot↔log boundary off-by-ones (`term_at` at the base; `next_index <= snapshot.last_index` trigger; a follower with a longer conflicting log receiving an older-base snapshot); op-log torn-tail safety with the new type byte; codec frame-length drift; `Ready.restore` ordering vs. subsequent `apply`. Fix findings via the receiving-code-review loop (fix subagent + re-review).
- [ ] **Step 3:** Update `docs/HANDOFF.md` — Plan D ✅, Plan E next; note single-server-membership (not joint consensus), the `LogEntry.entry_type` + `RaftStorage` snapshot additions, and any tracked/deferred items (learner catch-up phase; snapshot chunking; the `RaftLog`-backed adapter that must adopt entry_type + snapshot persistence in Plan E).
- [ ] **Step 4:** Commit handoff; open the PR against `main`; verify green; merge (merge commit) after the review is clean.

---

## Self-review (author checklist)
- **Spec coverage:** snapshot persistence (T1) ✓; compact (T2) ✓; InstallSnapshot send/receive/restore (T3) ✓; EntryType (T4) ✓; single-server membership incl. effect-on-append, one-in-flight, leader step-down, revert (T5) ✓; sim across snapshots + membership (T6) ✓; review+handoff+PR (T7) ✓. Deferred (joint consensus, learners, chunking, Plan-E adapter/driver) explicitly out of scope. ✓
- **Placeholder scan:** concrete interfaces + tests per task; sim scenario bodies are construction recipes (implementer authors them) — acceptable. ✓
- **Type consistency:** `save_snapshot`/`read_snapshot`, `Ready.restore`, `compact`, `EntryType`, `LogEntry::{normal,config_change}`, `ConfChange`, `voters`/`quorum()` used consistently across tasks; `voters` replaces `config.peers` for all quorum/peer iteration from T5 on. ✓
