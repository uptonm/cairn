# RaftCore (Plan C) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `RaftCore` — the pure, synchronous, I/O-free consensus step function (pre-vote + election, log replication with the consistency check, commit-by-majority, read-index linearizable reads) — plus the `RaftStorage` seam and an in-memory storage, proven correct by a deterministic multi-node simulation.

**Architecture:** `RaftCore<S: RaftStorage>` is a sync struct. It reasons over inputs (`tick`, `step`, `propose`, `read_index`), persists through a *synchronous* `RaftStorage` trait it calls inline (so it enforces Raft's persist-before-act ordering itself), and emits network sends + apply-ready entries + ready reads as *buffered outputs* drained via `ready()`. No async, no wall clock, no filesystem in the core. A deterministic sim drives N cores over `MemStorage` through a synchronous message router with seeded fault injection and asserts the four safety invariants. See `docs/superpowers/specs/2026-07-18-cairn-raftcore-plan-c-design.md`.

**Tech Stack:** Rust (stable, edition 2021), crate `crates/raft` (`cairn-raft`). Builds against the frozen `rpc.rs` `Message` types, `types.rs` (`NodeId`/`Term`/`LogIndex`/`LogEntry`/`HardState`/`SnapshotMeta` — all `u64` aliases), and `error.rs` (`Error { Io, Corruption }`, `Result`). No new dependencies — a tiny deterministic PRNG (SplitMix64) is hand-rolled to avoid a `rand` dep. No `tokio` in the core.

## Global Constraints

- Rust edition 2021, resolver 2. All new code in `crates/raft/src/`. No new crate deps.
- `cargo test --workspace`, `cargo clippy --all-targets -- -D warnings`, and `cargo fmt --check` clean at every commit.
- **No `unsafe`. No `.unwrap()`/`.expect()` in the core's `Result`/logic paths** (allowed in `#[cfg(test)]` and on already-length-checked `try_into` conversions). A `RaftStorage` error must propagate out of `step`/`propose`/`tick`/`read_index` as `Err` — the core never panics on storage failure.
- Types: `NodeId = u64`, `Term = u64`, `LogIndex = u64`. Log indices are **1-based** (0 = "empty/before first entry"). `current_term` starts at 0. Commands are `Vec<u8>`; a **no-op** entry is `command: vec![]`.
- The core holds **no wall clock and no unordered iteration that affects behavior**: logical `tick()` only; use `BTreeMap`/`BTreeSet` (never `HashMap`/`HashSet`) anywhere iteration order can influence emitted messages or decisions.
- Quorum for a config of `n` nodes is `n / 2 + 1`. "Self" always counts toward its own quorums.

## Contract with the frozen RPC layer (read before starting)

From `crates/raft/src/rpc.rs` (do **not** modify — it is frozen):
- `RequestVoteReq { term, candidate_id, last_log_index, last_log_term, pre_vote: bool }`
- `RequestVoteResp { term, vote_granted }` — **note: carries no `pre_vote` flag.** Disambiguate pre-vote vs real-vote responses by the receiver's own role plus the stale-term rule (see Task 3's design note).
- `AppendEntriesReq { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit }`
- `AppendEntriesResp { term, success, conflict_index: Option<LogIndex> }` — **carries no read-index/round field.** Read-index leadership confirmation therefore uses per-peer last-contact tick, not a tagged heartbeat (see Task 6's design note).
- `InstallSnapshot*` exist in the enum but are **out of scope for Plan C** — `step` must accept and ignore them without panicking (they arrive only in Plan D+).

## File structure

- `crates/raft/src/storage.rs` — `RaftStorage` trait + `MemStorage`. (Task 1)
- `crates/raft/src/core/mod.rs` — `RaftCore<S>`, `Role`, `Ready`, `ReadToken`, `Config`, the `SplitMix64` rng, construction, `tick`, `step` dispatch, `ready()`, quorum/timeout helpers. (Task 2)
- `crates/raft/src/core/election.rs` — pre-vote, election, `RequestVote` handling, vote-response tallying. (Task 3)
- `crates/raft/src/core/replication.rs` — `propose`, `AppendEntries` build/send, receive + consistency check, response handling (match/next-index, conflict back-up), commit advancement. (Tasks 4 + 5)
- `crates/raft/src/core/read_index.rs` — `read_index`, per-peer contact tracking, read release. (Task 6)
- `crates/raft/src/lib.rs` — add `pub mod storage;`, `pub mod core;` and re-exports. (Task 1/2)
- `crates/raft/tests/raft_sim.rs` — deterministic simulation harness + scenarios + safety invariants. (Task 7)
- Restart recovery test lives as a unit test in `core/mod.rs` or `tests/raft_sim.rs`. (Task 8)

**Note on cross-file `impl`s:** `core/election.rs` etc. are submodules of `core`, so their `impl RaftCore` blocks may access `RaftCore`'s private fields (Rust privacy is visible-to-descendants). Each submodule does `use super::*;`.

**Signature refinement vs the spec:** `propose` returns `Result<Option<LogIndex>>` — `Ok(Some(idx))` accepted by the leader, `Ok(None)` = not leader (a normal control outcome, not an error), `Err` = storage failure. This avoids inventing an `Error` variant for "not leader". Recorded here as an intentional refinement of the spec's `Result<LogIndex>`.

---

### Task 1: `RaftStorage` trait + `MemStorage`

**Files:**
- Create: `crates/raft/src/storage.rs`
- Modify: `crates/raft/src/lib.rs` (add `pub mod storage;` + re-exports)

**Interfaces:**
- Consumes: `types::{NodeId, Term, LogIndex, LogEntry, HardState, SnapshotMeta}`, `error::{Error, Result}`.
- Produces:
  - `trait RaftStorage` with: `hard_state(&self) -> HardState`; `save_hard_state(&mut self, &HardState) -> Result<()>`; `last_index(&self) -> LogIndex`; `last_term(&self) -> Term`; `term(&self, LogIndex) -> Result<Option<Term>>`; `entries_from(&self, LogIndex) -> Vec<LogEntry>`; `snapshot_meta(&self) -> SnapshotMeta`; `append(&mut self, &[LogEntry]) -> Result<()>`; `truncate_suffix(&mut self, LogIndex) -> Result<()>`.
  - `struct MemStorage { hs: HardState, entries: Vec<LogEntry>, snapshot: SnapshotMeta }` implementing `RaftStorage` + `Default`.

**Semantics `MemStorage` must honor (mirrors on-disk `RaftLog`):**
- `append` must be contiguous from `last_index()+1`; a gap → `Err(Corruption(..))`.
- `term(index)`: `Ok(Some(t))` if the entry is present; `Ok(Some(snapshot.last_term))` if `index == snapshot.last_index && index != 0`; `Ok(None)` if `index == 0`, `index < snapshot.last_index`, or `index > last_index()`.
- `truncate_suffix(from)`: drop entries with `index >= from`; no-op if `from > last_index()`.
- `entries_from(index)`: entries with `index >= max(index, snapshot.last_index+1)`, in order.
- `last_index()`/`last_term()`: fall back to `snapshot.last_index`/`last_term` when `entries` is empty.

- [ ] **Step 1: Write failing tests**

Create `crates/raft/src/storage.rs` with the trait, an empty `MemStorage`, and this test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LogEntry;

    fn e(term: Term, index: LogIndex) -> LogEntry {
        LogEntry { term, index, command: vec![] }
    }

    #[test]
    fn empty_storage_is_index0_term0() {
        let s = MemStorage::default();
        assert_eq!(s.last_index(), 0);
        assert_eq!(s.last_term(), 0);
        assert_eq!(s.term(0).unwrap(), None);
        assert_eq!(s.hard_state(), HardState::default());
    }

    #[test]
    fn append_then_read_back() {
        let mut s = MemStorage::default();
        s.append(&[e(1, 1), e(1, 2), e(2, 3)]).unwrap();
        assert_eq!(s.last_index(), 3);
        assert_eq!(s.last_term(), 2);
        assert_eq!(s.term(2).unwrap(), Some(1));
        assert_eq!(s.term(3).unwrap(), Some(2));
        assert_eq!(s.term(4).unwrap(), None);
        assert_eq!(s.entries_from(2), vec![e(1, 2), e(2, 3)]);
    }

    #[test]
    fn noncontiguous_append_is_corruption() {
        let mut s = MemStorage::default();
        assert!(s.append(&[e(1, 2)]).is_err());
    }

    #[test]
    fn truncate_suffix_drops_from_index() {
        let mut s = MemStorage::default();
        s.append(&[e(1, 1), e(1, 2), e(1, 3)]).unwrap();
        s.truncate_suffix(2).unwrap();
        assert_eq!(s.last_index(), 1);
        assert_eq!(s.term(2).unwrap(), None);
        s.truncate_suffix(9).unwrap(); // no-op past end
        assert_eq!(s.last_index(), 1);
    }

    #[test]
    fn save_and_load_hard_state() {
        let mut s = MemStorage::default();
        let hs = HardState { current_term: 4, voted_for: Some(2) };
        s.save_hard_state(&hs).unwrap();
        assert_eq!(s.hard_state(), hs);
    }
}
```

- [ ] **Step 2: Run tests, verify they fail to compile / fail.**
  Run: `cargo test -p cairn-raft storage::tests` → Expected: FAIL (methods unimplemented).

- [ ] **Step 3: Implement the trait + `MemStorage`.** Follow the semantics table above. `save_hard_state` clones into `self.hs`. Keep it allocation-simple; no fsync (in-memory). Add `pub mod storage;` to `lib.rs` and `pub use storage::{MemStorage, RaftStorage};`.

- [ ] **Step 4: Run tests, verify pass.** Run: `cargo test -p cairn-raft storage::tests` → Expected: PASS. Then `cargo clippy --all-targets -- -D warnings` and `cargo fmt --check`.

- [ ] **Step 5: Commit.**
  `git add -A && git commit -m "feat(raft): RaftStorage trait + in-memory MemStorage"`

---

### Task 2: Core skeleton — state, construction, `tick`, `ready()` drain

**Files:**
- Create: `crates/raft/src/core/mod.rs`
- Modify: `crates/raft/src/lib.rs` (`pub mod core;` + re-exports)

**Interfaces:**
- Consumes: `RaftStorage` (Task 1), `rpc::Message`, `types::*`.
- Produces:
  - `pub type ReadToken = u64;`
  - `pub enum Role { Follower, PreCandidate, Candidate, Leader }` (Clone, Copy, PartialEq, Debug)
  - `pub struct Ready { pub messages: Vec<(NodeId, Message)>, pub apply: Vec<LogEntry>, pub reads: Vec<ReadToken> }` (Default, Debug)
  - `pub struct Config { pub id: NodeId, pub peers: Vec<NodeId>, pub election_timeout: u64, pub heartbeat_interval: u64, pub seed: u64 }` — `peers` includes `id`.
  - `pub struct RaftCore<S: RaftStorage> { .. }` with:
    - `pub fn new(config: Config, storage: S) -> Result<Self>` (loads `current_term`/`voted_for` from storage; sets `commit_index = snapshot.last_index`, `last_applied = snapshot.last_index`, `role = Follower`, randomized election deadline).
    - `pub fn tick(&mut self) -> Result<()>`
    - `pub fn ready(&mut self) -> Ready` (drains `outbox`, `apply_buf`, `reads_buf`)
    - `pub fn role(&self) -> Role`, `pub fn current_term(&self) -> Term`, `pub fn commit_index(&self) -> LogIndex`, `pub fn leader_id(&self) -> Option<NodeId>` (test/inspection accessors)
    - `pub fn step(&mut self, from: NodeId, msg: Message) -> Result<()>` (dispatch stub; full handling added in Tasks 3–6)

**Internal fields (private):** `config: Config`, `storage: S`, `role: Role`, `leader_id: Option<NodeId>`, `commit_index: LogIndex`, `last_applied: LogIndex`, `elapsed: u64` (ticks since last leader contact / election reset), `election_deadline: u64` (randomized target in ticks), `heartbeat_elapsed: u64`, `votes: BTreeSet<NodeId>`, `next_index: BTreeMap<NodeId, LogIndex>`, `match_index: BTreeMap<NodeId, LogIndex>`, `last_contact_tick: BTreeMap<NodeId, u64>`, `tick_count: u64`, `pending_reads: Vec<PendingRead>`, `readable_term: Option<Term>` (term whose entry has committed, enabling reads), `rng: SplitMix64`, `outbox: Vec<(NodeId, Message)>`, `apply_buf: Vec<LogEntry>`, `reads_buf: Vec<ReadToken>`.

`struct PendingRead { token: ReadToken, floor: LogIndex, registered_tick: u64 }` (private).

**The RNG (hand-rolled, deterministic):**

```rust
struct SplitMix64(u64);
impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    // election timeout in [t, 2t)
    fn election_timeout(&mut self, base: u64) -> u64 {
        base + self.next_u64() % base.max(1)
    }
}
```

- [ ] **Step 1: Write failing tests.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemStorage;

    fn cfg(id: NodeId, peers: &[NodeId]) -> Config {
        Config { id, peers: peers.to_vec(), election_timeout: 10, heartbeat_interval: 3, seed: 42 }
    }

    #[test]
    fn new_starts_as_follower_term0() {
        let c = RaftCore::new(cfg(1, &[1, 2, 3]), MemStorage::default()).unwrap();
        assert_eq!(c.role(), Role::Follower);
        assert_eq!(c.current_term(), 0);
        assert_eq!(c.leader_id(), None);
    }

    #[test]
    fn ready_drains_and_resets() {
        let mut c = RaftCore::new(cfg(1, &[1, 2, 3]), MemStorage::default()).unwrap();
        let r1 = c.ready();
        assert!(r1.messages.is_empty() && r1.apply.is_empty() && r1.reads.is_empty());
    }

    #[test]
    fn follower_starts_prevote_after_election_timeout() {
        // Single-node peer list not used here; 3-node, no leader contact.
        let mut c = RaftCore::new(cfg(1, &[1, 2, 3]), MemStorage::default()).unwrap();
        for _ in 0..40 { c.tick().unwrap(); } // exceed any randomized deadline in [10,20)
        // Pre-vote must have begun: role is PreCandidate and RequestVote{pre_vote:true} was emitted.
        assert_eq!(c.role(), Role::PreCandidate);
        let r = c.ready();
        assert!(r.messages.iter().any(|(_, m)| matches!(
            m, Message::RequestVote(rv) if rv.pre_vote && rv.term == 1)));
    }

    #[test]
    fn install_snapshot_is_ignored_not_panicked() {
        let mut c = RaftCore::new(cfg(1, &[1, 2, 3]), MemStorage::default()).unwrap();
        let msg = Message::InstallSnapshot(crate::rpc::InstallSnapshotReq {
            term: 1, leader_id: 2, last_index: 0, last_term: 0, data: vec![],
        });
        assert!(c.step(2, msg).is_ok());
    }
}
```

- [ ] **Step 2: Run, verify fail.** `cargo test -p cairn-raft core::` → FAIL.

- [ ] **Step 3: Implement the skeleton.**
  - `new`: load hard state; seed `rng = SplitMix64(config.seed ^ config.id)`; `reset_election_timer()` sets `election_deadline = rng.election_timeout(config.election_timeout)` and `elapsed = 0`.
  - `tick`: `tick_count += 1`. If `Leader`: `heartbeat_elapsed += 1`, and when it reaches `heartbeat_interval`, broadcast heartbeats (stub calls `broadcast_append()` from Task 4 — for now, in Task 2 just reset the counter; wire the real call in Task 4). Else (`Follower`/`PreCandidate`/`Candidate`): `elapsed += 1`; if `elapsed >= election_deadline`, call `start_prevote()` (Task 3; in Task 2 provide a minimal `start_prevote` that sets `role = PreCandidate` and emits pre-vote `RequestVote{ term: current_term+1, pre_vote: true }` to all peers != self — this is exercised by the test above, then extended in Task 3). Always end with `maybe_release_reads()` (Task 6 — no-op stub in Task 2).
  - `step`: `match msg { InstallSnapshot(_) | InstallSnapshotResp(_) => Ok(()), .. => Ok(()) }` — a dispatch skeleton returning `Ok(())` for the not-yet-implemented arms (filled in Tasks 3–6). Keep the `InstallSnapshot` ignore arm permanently.
  - `ready`: `std::mem::take` the three buffers into a `Ready`.
  - Add `pub mod core;` + `pub use core::{Config, RaftCore, Ready, ReadToken, Role};` to `lib.rs`. (Note: `core` shadows the std `core` crate name within this module path — that is fine since the crate uses `std`; do not add `#![no_std]`.)

- [ ] **Step 4: Run, verify pass.** `cargo test -p cairn-raft core::` → PASS. clippy + fmt clean.

- [ ] **Step 5: Commit.** `git commit -am "feat(raft): RaftCore skeleton — state, tick, ready() drain, pre-vote trigger"`

---

### Task 3: Election + pre-vote + vote granting

**Files:**
- Create: `crates/raft/src/core/election.rs`
- Modify: `crates/raft/src/core/mod.rs` (`mod election;`; route `RequestVote`/`RequestVoteResp` in `step`; call `start_prevote`/`become_candidate` as designed)

**Interfaces:**
- Produces (private methods on `RaftCore<S>`): `start_prevote(&mut self)`, `become_candidate(&mut self) -> Result<()>`, `become_leader(&mut self) -> Result<()>`, `become_follower(&mut self, term: Term, leader: Option<NodeId>) -> Result<()>`, `handle_request_vote(&mut self, from, RequestVoteReq) -> Result<()>`, `handle_vote_resp(&mut self, from, RequestVoteResp) -> Result<()>`.

**Design note — disambiguating pre-vote vs real-vote responses (the frozen `RequestVoteResp` has no flag):** A node is in exactly one of `PreCandidate`/`Candidate` at a time and only ever has one election of its own in flight. Route a `RequestVoteResp` by current role: `PreCandidate` → count pre-votes; `Candidate` → count real votes; any other role → ignore. Real candidacy **increments `current_term`**, so a straggling pre-vote response (older term) is discarded by the standard rule "ignore any response whose `term < current_term`". A same-term straggler at worst over-counts a pre-vote (⇒ at most a spurious real election, which still needs real votes to win) — never a safety violation. Dedup voters via the `votes: BTreeSet<NodeId>`.

**Design note — up-to-date check & persist ordering:** grant a (real) vote iff `(req.last_log_term, req.last_log_index) >= (self.last_term(), self.last_index())` lexicographically **and** (`voted_for` is `None` or already this candidate) for `req.term == current_term`. If `req.term > current_term`, first `become_follower(req.term, None)` (persist), then evaluate. **Persist `voted_for` via `save_hard_state` before emitting the granting `RequestVoteResp`.** Pre-vote requests (`pre_vote: true`) are answered **without** persisting and **without** mutating `voted_for` or `current_term`: grant iff the same up-to-date check passes and `req.term >= current_term` and (we have not heard from a leader within the election timeout — the pre-vote "leader stickiness" guard; for Plan C, granting on the up-to-date + term check alone is acceptable and simpler — implement that, note the stickiness refinement as deferred).

- [ ] **Step 1: Write failing tests** (in `core/election.rs` `#[cfg(test)]`). Use a helper that builds a follower with a seeded log.

```rust
// grants vote to an up-to-date candidate and persists voted_for
#[test]
fn grants_vote_to_up_to_date_candidate() {
    let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), MemStorage::default()).unwrap();
    c.step(1, Message::RequestVote(RequestVoteReq {
        term: 1, candidate_id: 1, last_log_index: 0, last_log_term: 0, pre_vote: false,
    })).unwrap();
    let r = c.ready();
    assert!(r.messages.iter().any(|(to, m)| *to == 1 && matches!(
        m, Message::RequestVoteResp(v) if v.vote_granted && v.term == 1)));
    assert_eq!(c.current_term(), 1);
    // persisted
    // (expose via a test accessor or re-open a clone of storage — see helper)
}

// rejects a candidate whose log is behind
#[test]
fn rejects_behind_candidate() {
    let mut s = MemStorage::default();
    s.append(&[LogEntry { term: 2, index: 1, command: vec![] }]).unwrap();
    let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), s).unwrap();
    c.step(1, Message::RequestVote(RequestVoteReq {
        term: 3, candidate_id: 1, last_log_index: 0, last_log_term: 0, pre_vote: false,
    })).unwrap();
    let r = c.ready();
    assert!(r.messages.iter().any(|(_, m)| matches!(
        m, Message::RequestVoteResp(v) if !v.vote_granted)));
}

// does not double-vote in the same term
#[test]
fn no_double_vote_same_term() {
    let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), MemStorage::default()).unwrap();
    let rv = |cid| Message::RequestVote(RequestVoteReq {
        term: 1, candidate_id: cid, last_log_index: 0, last_log_term: 0, pre_vote: false });
    c.step(1, rv(1)).unwrap();
    let _ = c.ready();
    c.step(3, rv(3)).unwrap();
    let r = c.ready();
    assert!(r.messages.iter().any(|(to, m)| *to == 3 && matches!(
        m, Message::RequestVoteResp(v) if !v.vote_granted)));
}

// pre-vote grant does not mutate term or voted_for
#[test]
fn prevote_does_not_persist() {
    let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), MemStorage::default()).unwrap();
    c.step(1, Message::RequestVote(RequestVoteReq {
        term: 1, candidate_id: 1, last_log_index: 0, last_log_term: 0, pre_vote: true,
    })).unwrap();
    assert_eq!(c.current_term(), 0);   // unchanged
    let r = c.ready();
    assert!(r.messages.iter().any(|(_, m)| matches!(
        m, Message::RequestVoteResp(v) if v.vote_granted)));
}

// wins election with a majority of real votes and appends a no-op
#[test]
fn wins_election_with_majority() {
    let mut c = RaftCore::new(cfg(1, &[1, 2, 3]), MemStorage::default()).unwrap();
    for _ in 0..40 { c.tick().unwrap(); }          // -> PreCandidate, pre-vote out
    let _ = c.ready();
    // grant pre-votes from 2 and 3
    c.step(2, Message::RequestVoteResp(RequestVoteResp { term: 1, vote_granted: true })).unwrap();
    assert_eq!(c.role(), Role::Candidate);         // pre-vote won -> real candidate (term 1)
    assert_eq!(c.current_term(), 1);
    let _ = c.ready();
    c.step(2, Message::RequestVoteResp(RequestVoteResp { term: 1, vote_granted: true })).unwrap();
    assert_eq!(c.role(), Role::Leader);
    // leader appends a no-op in its term
    assert!(c.commit_index() <= 1);
}
```

- [ ] **Step 2: Run, verify fail.** `cargo test -p cairn-raft core::election` → FAIL.

- [ ] **Step 3: Implement election.**
  - `start_prevote`: `role = PreCandidate`; `votes = {self}`; `reset_election_timer()`; broadcast `RequestVote { term: current_term+1, candidate_id: self, last_log_index, last_log_term, pre_vote: true }` to peers != self.
  - `handle_vote_resp` when `PreCandidate`: ignore if `!granted` or `resp.term != current_term` (pre-vote responses carry responder term == our current_term when they'd grant; treat `resp.term > current_term` as step-down via `become_follower`). Insert `from` into `votes`; if `votes.len()` reaches quorum → `become_candidate()`.
  - `become_candidate`: `current_term += 1`; `voted_for = Some(self)`; **persist** (`save_hard_state`); `role = Candidate`; `votes = {self}`; `reset_election_timer()`; broadcast real `RequestVote { term: current_term, .., pre_vote: false }`.
  - `handle_vote_resp` when `Candidate`: if `resp.term > current_term` → `become_follower(resp.term, None)`; ignore if `resp.term < current_term` or `!granted`; else insert into `votes`; quorum → `become_leader()`.
  - `become_leader`: `role = Leader`; `leader_id = Some(self)`; init `next_index[p] = last_index()+1`, `match_index[p] = 0` for peers != self; **append a no-op** `LogEntry { term: current_term, index: last_index()+1, command: vec![] }` via `storage.append` (persist), set own `match_index`/`next_index` accordingly; `broadcast_append()` (Task 4). Reset `readable_term = None` (a fresh leader can't serve reads until its no-op commits).
  - `become_follower(term, leader)`: if `term > current_term` set `current_term = term`, `voted_for = None`, **persist**; `role = Follower`; `leader_id = leader`; `reset_election_timer()`.
  - `handle_request_vote`: implement the up-to-date + persist-ordering design note. Emit `RequestVoteResp { term: current_term (post any step-up), vote_granted }`. For `pre_vote`, never persist/mutate; reply granted/again on the up-to-date check with `term: max(current_term, req.term)` semantics (reply `term = current_term`).
  - Route in `step`: `RequestVote(r) => handle_request_vote(from, r)`, `RequestVoteResp(r) => handle_vote_resp(from, r)`.
  - Add a `#[cfg(test)]` accessor `pub(crate) fn stored_hard_state(&self) -> HardState { self.storage.hard_state() }` to let tests assert persistence.

- [ ] **Step 4: Run, verify pass.** `cargo test -p cairn-raft core::election` → PASS. clippy + fmt clean.

- [ ] **Step 5: Commit.** `git commit -am "feat(raft): pre-vote, election, vote granting with persist ordering"`

---

### Task 4: Replication — AppendEntries send, receive, consistency check, back-up

**Files:**
- Create: `crates/raft/src/core/replication.rs`
- Modify: `crates/raft/src/core/mod.rs` (`mod replication;`; route `AppendEntries`/`AppendEntriesResp`; wire `broadcast_append()` into leader heartbeat tick + `become_leader`)

**Interfaces:**
- Produces (private on `RaftCore<S>`): `propose(&mut self, Vec<u8>) -> Result<Option<LogIndex>>` (public), `broadcast_append(&mut self) -> Result<()>`, `send_append_to(&mut self, peer: NodeId) -> Result<()>`, `handle_append_entries(&mut self, from, AppendEntriesReq) -> Result<()>`, `handle_append_resp(&mut self, from, AppendEntriesResp) -> Result<()>`.

**Design notes:**
- `send_append_to(peer)`: `let ni = next_index[peer]; let prev = ni - 1; let prev_term = term(prev) or snapshot.last_term for prev==snapshot.last_index or 0 for prev==0; entries = entries_from(ni); leader_commit = commit_index`. Emit `AppendEntries`.
- `handle_append_entries` (follower path): if `req.term < current_term` → reply `AppendEntriesResp { term: current_term, success: false, conflict_index: None }`. Else `become_follower(req.term, Some(req.leader_id))` semantics (adopt leader, reset election timer, step down if candidate/leader). Consistency check on `prev_log_index`/`prev_log_term`:
  - If `prev_log_index > last_index()` → reject with `conflict_index: Some(last_index()+1)`.
  - Else let `t = term(prev_log_index)` (treating index 0 as term 0, snapshot boundary via `snapshot.last_term`). If `t != prev_log_term` → reject with `conflict_index` = first index of the conflicting term (scan back to the start of that term; a correct simple choice is the index where the term differs; for Plan C, returning `Some(prev_log_index)` is acceptable but prefer the term-start for fewer round-trips — implement term-start).
  - On match: for each new entry, if an existing entry at that index has a different term, `truncate_suffix(entry.index)` then append the rest; skip entries already present with matching term. Persist via `storage.append`. Then set `commit_index = min(req.leader_commit, last_index())` if it advances, and buffer newly-committed entries to apply (Task 5's `advance_apply`). Reply `success: true, conflict_index: None, term: current_term`.
- `handle_append_resp` (leader path): if `resp.term > current_term` → `become_follower(resp.term, None)`. If not leader, ignore. On `success`: update `match_index[from]` and `next_index[from]` to reflect the entries just sent (track the "up-to index" — simplest: on success set `match_index[from] = prev_log_index + entries_len`; recompute from the request context by remembering the last sent `next_index` — to avoid stateful bookkeeping, compute `match_index[from] = max(match_index[from], sent_up_to)` where `sent_up_to` is captured by having `send_append_to` record `last_sent[peer]`). Then update `last_contact_tick[from] = tick_count` and call `maybe_advance_commit()` (Task 5). On failure: back up `next_index[from]` using `conflict_index` (set `next_index[from] = max(1, conflict_index)`), then `send_append_to(from)` again.
- **Heartbeats:** in `tick`, a `Leader` with `heartbeat_elapsed >= heartbeat_interval` resets the counter and calls `broadcast_append()` (sends AppendEntries to every peer; empty when caught up).

To track `sent_up_to` cleanly, add private `last_sent: BTreeMap<NodeId, LogIndex>` set inside `send_append_to` to `prev + entries.len()`.

- [ ] **Step 1: Write failing tests.** Cover: leader `propose` appends + emits AppendEntries; follower accepts matching AppendEntries and appends; follower rejects on prev-term mismatch with a `conflict_index`; follower truncates a conflicting suffix then appends; leader backs up `next_index` on rejection and retries; a stale-term AppendEntries is rejected. Example (truncation):

```rust
#[test]
fn follower_truncates_conflicting_suffix() {
    let mut s = MemStorage::default();
    s.append(&[LogEntry{term:1,index:1,command:vec![]},
               LogEntry{term:1,index:2,command:vec![9]}]).unwrap();
    let mut c = RaftCore::new(cfg(2, &[1,2,3]), s).unwrap();
    // leader at term 2 overwrites index 2 with a term-2 entry
    c.step(1, Message::AppendEntries(AppendEntriesReq{
        term:2, leader_id:1, prev_log_index:1, prev_log_term:1,
        entries: vec![LogEntry{term:2,index:2,command:vec![7]}], leader_commit:0,
    })).unwrap();
    let r = c.ready();
    assert!(r.messages.iter().any(|(_,m)| matches!(
        m, Message::AppendEntriesResp(a) if a.success)));
    // index 2 now has term 2 command [7]
}
```

- [ ] **Step 2: Run, verify fail.**
- [ ] **Step 3: Implement replication + heartbeat wiring per the design notes.**
- [ ] **Step 4: Run, verify pass.** clippy + fmt clean.
- [ ] **Step 5: Commit.** `git commit -am "feat(raft): log replication, consistency check, conflict back-up"`

---

### Task 5: Commit advancement + apply buffering

**Files:**
- Modify: `crates/raft/src/core/replication.rs` (add `maybe_advance_commit`, `advance_apply`) and `core/mod.rs` if a helper lands there.

**Interfaces:**
- Produces (private): `maybe_advance_commit(&mut self) -> Result<()>` (leader), `advance_apply(&mut self)` (all roles — moves `last_applied` up to `commit_index`, pushing entries into `apply_buf` and flipping `readable_term`).

**Design notes:**
- `maybe_advance_commit` (leader only): collect `match_index` for all nodes (self = `last_index()`), find the highest index `N > commit_index` such that a majority have `match_index >= N` **and** `term(N) == current_term` (the §5.4.2 current-term rule). Set `commit_index = N`; then `advance_apply()`.
- `advance_apply`: while `last_applied < commit_index`, `last_applied += 1`, fetch the entry (`storage` read), push a clone into `apply_buf`; if the entry's `term == current_term`, set `readable_term = Some(current_term)` (leader can now serve reads). Skip pushing entries at/below the snapshot boundary. Empty-command (no-op) entries are still pushed — the apply consumer ignores them.
- Followers call `advance_apply` after adopting `leader_commit` in Task 4.

- [ ] **Step 1: Write failing tests.** Cover:
  - leader advances commit only when a majority match AND the entry is in the current term (a prior-term entry replicated to a majority does **not** commit until a current-term entry above it does);
  - committed entries land in `Ready.apply` in index order, exactly once;
  - a follower applies up to `min(leader_commit, last_index)`.

```rust
#[test]
fn no_commit_of_prior_term_by_count_alone() {
    // 3 nodes; leader id=1 at term 2 has a term-1 entry replicated to a majority,
    // but must not mark it committed until a term-2 entry above it commits.
    // Assert commit_index stays 0 after acks for the term-1 entry, then advances
    // once the term-2 no-op is acked by a majority.
    // (Construct via propose + simulated AppendEntriesResp acks.)
}
```

- [ ] **Step 2: Run, verify fail.**
- [ ] **Step 3: Implement commit math + apply.**
- [ ] **Step 4: Run, verify pass.** clippy + fmt clean.
- [ ] **Step 5: Commit.** `git commit -am "feat(raft): commit-by-majority (current-term rule) + apply buffering"`

---

### Task 6: Read-index linearizable reads

**Files:**
- Create: `crates/raft/src/core/read_index.rs`
- Modify: `crates/raft/src/core/mod.rs` (`mod read_index;`; call `maybe_release_reads()` from `tick` and after commit/ack updates; make `read_index` public)

**Interfaces:**
- Produces: `pub fn read_index(&mut self, token: ReadToken)`, private `maybe_release_reads(&mut self)`.

**Design note — leadership confirmation without a tagged heartbeat (the frozen `AppendEntriesResp` has no round field):** track `last_contact_tick[peer]` = `tick_count` at the last successful `AppendEntriesResp` (current term) from `peer` (updated in Task 4). A read registered at `registered_tick = tick_count` is leadership-confirmed once a **quorum** of nodes have `last_contact_tick > registered_tick` (self counts, always current). This is safe: a quorum affirmed this leader's authority *strictly after* the read began, and election safety gives one leader per term, so no other leader served the read's term in between; a higher term would have stepped us down. Release the read (push `token` to `reads_buf`) once **all** hold: (a) `role == Leader`, (b) `readable_term == Some(current_term)` (a current-term entry has committed — closes the new-leader gap), (c) the quorum-contact condition above, and (d) `last_applied >= floor`. `floor` is `commit_index` captured at registration.

- `read_index(token)`: if `role != Leader`, drop the token (driver redirects to the leader; the core never falsely releases). Else push `PendingRead { token, floor: commit_index, registered_tick: tick_count }`; then `maybe_release_reads()`.
- `maybe_release_reads`: for each pending read satisfying (a)–(d), push its token to `reads_buf` and remove it. Call this from `tick`, after `handle_append_resp` updates contact, and after commit advances.

- [ ] **Step 1: Write failing tests.** Cover:
  - a read is **not** released before a quorum has contacted the leader after registration;
  - a read is **not** released before the leader has committed a current-term entry (new-leader gap);
  - a read is released once quorum-contact + apply catch-up + current-term-commit hold, echoing the same token;
  - a `read_index` on a follower is never released.

```rust
#[test]
fn read_waits_for_current_term_commit_and_quorum_contact() {
    // Build a leader (term 1) via the election helper; before its no-op commits,
    // register read_index(7); tick + provide acks; assert token 7 only appears in
    // Ready.reads after the no-op commits AND a quorum acked post-registration.
}
```

- [ ] **Step 2: Run, verify fail.**
- [ ] **Step 3: Implement read-index.**
- [ ] **Step 4: Run, verify pass.** clippy + fmt clean.
- [ ] **Step 5: Commit.** `git commit -am "feat(raft): read-index linearizable reads (quorum-contact confirmation)"`

---

### Task 7: Deterministic multi-node simulation + safety invariants

**Files:**
- Create: `crates/raft/tests/raft_sim.rs`

**Interfaces:**
- Consumes: `cairn_raft::{RaftCore, Config, MemStorage, Message, Ready, NodeId, LogEntry}`.
- Produces (test-local): a `Cluster` harness — owns `Vec<RaftCore<MemStorage>>`, a synchronous in-flight message queue `Vec<(NodeId /*from*/, NodeId /*to*/, Message)>`, a seeded `SplitMix64` for fault decisions, a per-node applied-log `Vec<Vec<LogEntry>>`, and controls: `tick_all()`, `deliver_one()`/`deliver_all()`, `drop_between(a,b)` (partition set), `crash(node)`/`restart(node)` (rebuild core from its `MemStorage`, which survives), `run(steps)`.

**Harness rules (determinism):** single task; iterate nodes and the message queue in index/FIFO order; all fault decisions come from the seeded rng; **no wall clock, no `HashMap`**. After each node step, drain `ready()`: enqueue `messages` (respecting active partitions), append `apply` to that node's applied-log, ignore `reads` (or record for a read-linearizability check — optional in Plan C).

**Safety invariants (assert after every scenario):**
1. **Election safety** — collect `(term, leader_id)` observed; no two distinct leaders in the same term. (Track each node's `(current_term, role==Leader)` across the run.)
2. **Log matching** — for any two nodes, if entries at index `i` share a term, all entries `<= i` are equal.
3. **State-machine safety** — no two nodes' applied-logs disagree at the same index.
4. **Leader completeness** — every entry present in a node's `commit_index`-prefix appears in the log of any node that later becomes leader.

- [ ] **Step 1: Write the harness + a first failing scenario** (`clean_election`): 3 nodes, no faults, run until a leader emerges; assert exactly one leader and that a proposed command replicates + applies on all nodes; assert invariants 1–4.
- [ ] **Step 2: Run, verify it fails or drives out bugs.** `cargo test -p cairn-raft --test raft_sim` → iterate until green.
- [ ] **Step 3: Add scenarios** (each its own `#[test]`, each asserting invariants 1–4):
  - `leader_crash_reelection` — elect, crash the leader, run, assert a new leader in a higher term and continued replication.
  - `partition_and_heal` — split into majority/minority, propose on the majority side, heal, assert the minority catches up and no divergence.
  - `dropped_appends_backup` — drop a follower's AppendEntries for a while, then allow; assert `conflict_index` back-up converges the follower.
  - `reordered_delivery` — deliver queued messages in a seeded permutation; assert safety holds.
  - `restart_persistence` — crash+restart a node mid-run; assert it resumes without losing committed entries.
- [ ] **Step 4: Run all scenarios green under a fixed seed.** clippy + fmt clean.
- [ ] **Step 5: Commit.** `git commit -am "test(raft): deterministic multi-node sim + safety invariants"`

---

### Task 8: Restart recovery unit test + whole-branch review

**Files:**
- Modify: `crates/raft/src/core/mod.rs` (add a focused restart unit test if not fully covered by the sim) and any fixes from review.

- [ ] **Step 1: Restart unit test.** Build a core, drive it to persist `current_term`/`voted_for` and some log via `MemStorage`; construct a *new* `RaftCore` over the same `MemStorage`; assert `current_term`, `voted_for` (via `stored_hard_state`), `last_index`/`last_term`, and that `commit_index`/`last_applied` reset to the snapshot boundary (volatile state is not persisted — correct per Raft). Assert no committed entry is lost.
- [ ] **Step 2: Run full workspace green.** `cargo test --workspace && cargo clippy --all-targets -- -D warnings && cargo fmt --check`.
- [ ] **Step 3: Whole-branch adversarial review (opus).** Dispatch a fresh reviewer over the entire `feat/raft-core` diff with the spec + this plan as the rubric, hunting cross-cutting consensus bugs (commit-safety off-by-one, vote-persistence ordering, read-index staleness, sim non-determinism, back-up convergence). Fix findings via the receiving-code-review loop.
- [ ] **Step 4: Update `docs/HANDOFF.md`** — mark Plan C ✅, note any tracked/deferred items (pre-vote leader-stickiness refinement; read-index lease optimization; InstallSnapshot handling → Plan D), and set Plan D as NEXT.
- [ ] **Step 5: Final commit + open PR.** `git commit -am "test(raft): restart recovery; docs: RaftCore handoff"` then open the PR for review + merge to `main`.

---

## Self-review (author checklist — completed)

- **Spec coverage:** trait/MemStorage (T1) ✓; core seam + tick/ready (T2) ✓; pre-vote/election/vote (T3) ✓; replication + consistency check + back-up (T4) ✓; commit-by-majority + current-term rule + apply (T5) ✓; read-index (T6) ✓; deterministic sim + 4 safety invariants (T7) ✓; restart recovery (T8) ✓. Out-of-scope items (snapshots, membership, disk adapter, TCP) explicitly deferred to Plan D/E. ✓
- **Placeholder scan:** no TBD/TODO; every task has concrete tests + implementation notes. Sim scenario bodies (T5/T6/T7) give exact construction recipes rather than full transcriptions — acceptable, they are test-authoring guidance for the implementer, not production code. ✓
- **Type consistency:** `RaftStorage`/`MemStorage` (T1) used unchanged in T2–T8; `Ready { messages, apply, reads }`, `ReadToken=u64`, `Role`, `Config` stable across tasks; `propose -> Result<Option<LogIndex>>` consistent; `last_sent`/`last_contact_tick`/`readable_term`/`pending_reads` introduced where first used and reused consistently. ✓
