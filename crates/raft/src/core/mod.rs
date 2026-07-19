use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::error::Result;
use crate::rpc::Message;
use crate::storage::RaftStorage;
use crate::types::{HardState, LogEntry, LogIndex, NodeId, SnapshotMeta, Term};

mod election;
mod read_index;
mod replication;
mod snapshot;

pub type ReadToken = u64;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

#[derive(Default, Debug)]
pub struct Ready {
    pub messages: Vec<(NodeId, Message)>,
    pub apply: Vec<LogEntry>,
    pub reads: Vec<ReadToken>,
    pub restore: Option<(SnapshotMeta, Vec<u8>)>,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    pub election_timeout: u64,
    pub heartbeat_interval: u64,
    pub seed: u64,
}

/// Hand-rolled deterministic PRNG. Not cryptographic — used only to jitter
/// election timeouts so peers don't all wake up on the same tick.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// election timeout in [t, 2t)
    fn election_timeout(&mut self, base: u64) -> u64 {
        base + self.next_u64() % base.max(1)
    }
}

/// A read barrier registered by `read_index`, waiting on the conditions
/// `maybe_release_reads` (core/read_index.rs) checks before it can be
/// released: current-term readability, post-registration quorum contact,
/// and apply catch-up past `floor`.
struct PendingRead {
    token: ReadToken,
    floor: LogIndex,
    /// Snapshot of `send_count` taken when this read registered — see
    /// `read_index`/`maybe_release_reads` (core/read_index.rs) for how a
    /// peer's `ack_count` exceeding its entry here proves that peer
    /// affirmed this leader's authority via a send made AFTER this read
    /// began, not merely a reply PROCESSED after it.
    barrier: BTreeMap<NodeId, u64>,
}

/// Pure, synchronous, I/O-free Raft consensus step function. All I/O
/// (storage, transport) is pushed to the caller via `Ready`; `RaftCore`
/// itself never blocks or performs side effects beyond mutating its own
/// state and buffering outbound messages/applies/read releases.
pub struct RaftCore<S: RaftStorage> {
    config: Config,
    storage: S,
    role: Role,
    leader_id: Option<NodeId>,
    commit_index: LogIndex,
    /// Tracks the apply-loop's progress against `commit_index`; read/written
    /// by `advance_apply` (core/replication.rs) and read by the read-index
    /// quorum-contact gate (core/read_index.rs).
    last_applied: LogIndex,
    /// Ticks elapsed since the last leader contact / election reset.
    elapsed: u64,
    /// Randomized target (in ticks) at which a follower/candidate starts a
    /// new election round.
    election_deadline: u64,
    heartbeat_elapsed: u64,
    votes: BTreeSet<NodeId>,
    next_index: BTreeMap<NodeId, LogIndex>,
    match_index: BTreeMap<NodeId, LogIndex>,
    /// Per-peer FIFO queue of "up-to" indices (`prev_log_index +
    /// entries.len()`) for AppendEntries sent but not yet acknowledged,
    /// pushed by `send_append_to`. Because `last_index` is monotonic
    /// non-decreasing, the up-to values pushed for a peer are monotonic
    /// non-decreasing too, so popping front-first on a success always
    /// yields the SMALLEST outstanding up-to — a safe lower bound on what
    /// the follower actually persisted for the request being acknowledged,
    /// even under arbitrary reordering/loss of responses. This is what
    /// lets `handle_append_resp` advance `match_index` without over-
    /// advancing it past two overlapping in-flight requests (see the
    /// Task-4 self-review / Task-4 fix-pass-1 report).
    inflight: BTreeMap<NodeId, VecDeque<LogIndex>>,
    /// Per-peer count of AppendEntries sent, incremented once per send by
    /// `send_append_to` (core/replication.rs). Monotonic for the lifetime
    /// of a leadership term (reset in `become_leader`). `read_index`
    /// snapshots this as a read's `PendingRead::barrier`.
    send_count: BTreeMap<NodeId, u64>,
    /// Per-peer count of same-term successful AppendEntriesResp replies
    /// processed, incremented once per ack by `handle_append_resp`
    /// (core/replication.rs). Read by read_index.rs's quorum-contact gate:
    /// `ack_count[P] > barrier[P]` proves, by pigeonhole, that P acked a
    /// send made after the read registered — see `maybe_release_reads` for
    /// the full argument.
    ack_count: BTreeMap<NodeId, u64>,
    /// Reads registered via `read_index` awaiting release; see
    /// core/read_index.rs.
    pending_reads: Vec<PendingRead>,
    /// Term whose entry has committed, enabling reads at or before it.
    readable_term: Option<Term>,
    rng: SplitMix64,
    outbox: Vec<(NodeId, Message)>,
    apply_buf: Vec<LogEntry>,
    reads_buf: Vec<ReadToken>,
    /// Set when a snapshot arrives that the caller must install into its
    /// state machine before further applies proceed; drained by `ready()`
    /// into `Ready.restore`. Nothing populates this yet — that lands in
    /// Task 3 (InstallSnapshot RPC handling).
    restore_buf: Option<(SnapshotMeta, Vec<u8>)>,
}

impl<S: RaftStorage> RaftCore<S> {
    pub fn new(config: Config, storage: S) -> Result<Self> {
        let snapshot = storage.snapshot_meta();
        let rng = SplitMix64(config.seed ^ config.id);
        let mut core = RaftCore {
            config,
            storage,
            role: Role::Follower,
            leader_id: None,
            commit_index: snapshot.last_index,
            last_applied: snapshot.last_index,
            elapsed: 0,
            election_deadline: 0,
            heartbeat_elapsed: 0,
            votes: BTreeSet::new(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            inflight: BTreeMap::new(),
            send_count: BTreeMap::new(),
            ack_count: BTreeMap::new(),
            pending_reads: Vec::new(),
            readable_term: None,
            rng,
            outbox: Vec::new(),
            apply_buf: Vec::new(),
            reads_buf: Vec::new(),
            restore_buf: None,
        };
        core.reset_election_timer();
        Ok(core)
    }

    pub fn tick(&mut self) -> Result<()> {
        match self.role {
            Role::Leader => {
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.config.heartbeat_interval {
                    self.heartbeat_elapsed = 0;
                    self.broadcast_append()?;
                }
            }
            Role::Follower | Role::PreCandidate | Role::Candidate => {
                self.elapsed += 1;
                if self.elapsed >= self.election_deadline {
                    self.start_prevote()?;
                }
            }
        }
        self.maybe_release_reads();
        Ok(())
    }

    pub fn ready(&mut self) -> Ready {
        Ready {
            messages: std::mem::take(&mut self.outbox),
            apply: std::mem::take(&mut self.apply_buf),
            reads: std::mem::take(&mut self.reads_buf),
            restore: std::mem::take(&mut self.restore_buf),
        }
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn current_term(&self) -> Term {
        self.storage.hard_state().current_term
    }

    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }

    /// Consumes this core and returns its storage, discarding all volatile
    /// state (role, leader_id, election/heartbeat timers, replication
    /// tracking, pending reads, ...). This is the supported way to model a
    /// process crash + restart: a real deployment loses everything except
    /// what its `RaftStorage` impl actually persisted, and restarting means
    /// constructing a fresh `RaftCore::new` over that same durable storage.
    /// Minimal by design — it exposes no more than a caller already has
    /// via `Config` + a persistent `S`, just lets it be recovered from a
    /// live core instead of held onto separately from construction time.
    pub fn into_storage(self) -> S {
        self.storage
    }

    /// Dispatch skeleton. `InstallSnapshot`/`InstallSnapshotResp` are
    /// permanently ignored here (snapshot install is out of RaftCore's
    /// scope).
    pub fn step(&mut self, from: NodeId, msg: Message) -> Result<()> {
        match msg {
            Message::InstallSnapshot(_) | Message::InstallSnapshotResp(_) => Ok(()),
            Message::RequestVote(req) => self.handle_request_vote(from, req),
            Message::RequestVoteResp(resp) => self.handle_vote_resp(from, resp),
            Message::AppendEntries(req) => self.handle_append_entries(from, req),
            Message::AppendEntriesResp(resp) => self.handle_append_resp(from, resp),
        }
    }

    fn reset_election_timer(&mut self) {
        self.elapsed = 0;
        self.election_deadline = self.rng.election_timeout(self.config.election_timeout);
    }

    #[cfg(test)]
    pub(crate) fn stored_hard_state(&self) -> HardState {
        self.storage.hard_state()
    }

    #[cfg(test)]
    pub(crate) fn match_index_of(&self, peer: NodeId) -> LogIndex {
        self.match_index.get(&peer).copied().unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn ack_count_of(&self, peer: NodeId) -> u64 {
        self.ack_count.get(&peer).copied().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemStorage;

    fn cfg(id: NodeId, peers: &[NodeId]) -> Config {
        Config {
            id,
            peers: peers.to_vec(),
            election_timeout: 10,
            heartbeat_interval: 3,
            seed: 42,
        }
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
        for _ in 0..40 {
            c.tick().unwrap();
        } // exceed any randomized deadline in [10,20)
          // Pre-vote must have begun: role is PreCandidate and RequestVote{pre_vote:true} was emitted.
        assert_eq!(c.role(), Role::PreCandidate);
        let r = c.ready();
        assert!(r.messages.iter().any(|(_, m)| matches!(
            m, Message::RequestVote(rv) if rv.pre_vote && rv.term == 1)));
    }

    // Restart recovery: `RaftCore::new` reloads persisted hard state (term,
    // vote) and the log from storage, but volatile state (commit_index,
    // last_applied, role, ...) is never persisted and must reset. This test
    // drives a real 3-node election + two proposals + majority acks to
    // build up non-trivial persisted AND volatile state in one incarnation,
    // reclaims the storage via `into_storage`, constructs a fresh core over
    // it (modeling a crash + restart), and checks both halves: persisted
    // state survives (including every committed entry), volatile state
    // resets to the snapshot boundary rather than carrying over.
    #[test]
    fn restart_recovers_persisted_state_and_loses_no_committed_entry() {
        use crate::rpc::{AppendEntriesResp, RequestVoteResp};

        let config = cfg(1, &[1, 2, 3]);
        let mut core = RaftCore::new(config.clone(), MemStorage::default()).unwrap();

        // Drive past the election timeout into pre-vote, then win a real
        // election with a single peer grant (self + peer 2 = quorum of 2/3).
        for _ in 0..40 {
            core.tick().unwrap();
        }
        assert_eq!(core.role(), Role::PreCandidate);
        let _ = core.ready();
        // Pre-vote grant: a fresh peer at term 0 echoes its own current_term
        // (0), not the candidate's prospective term.
        core.step(
            2,
            Message::RequestVoteResp(RequestVoteResp {
                term: 0,
                vote_granted: true,
                pre_vote: true,
            }),
        )
        .unwrap();
        assert_eq!(core.role(), Role::Candidate);
        let _ = core.ready();
        // Real vote grant: echoes the (now bumped) candidate's current_term.
        core.step(
            2,
            Message::RequestVoteResp(RequestVoteResp {
                term: 1,
                vote_granted: true,
                pre_vote: false,
            }),
        )
        .unwrap();
        assert_eq!(core.role(), Role::Leader);
        let _ = core.ready(); // drain the no-op broadcast from become_leader

        // Propose two more entries, so the log holds three entries (no-op,
        // "x", "y") in the leader's term.
        assert_eq!(core.propose(b"set x=1".to_vec()).unwrap(), Some(2));
        assert_eq!(core.propose(b"set y=2".to_vec()).unwrap(), Some(3));
        let _ = core.ready();

        // Peer 2 acks all three outstanding AppendEntries in FIFO order,
        // taking commit_index (and, via advance_apply, last_applied) to 3 —
        // self + peer 2 already form a quorum of 3.
        let term = core.current_term();
        let success = || {
            Message::AppendEntriesResp(AppendEntriesResp {
                term,
                success: true,
                conflict_index: None,
            })
        };
        core.step(2, success()).unwrap();
        core.step(2, success()).unwrap();
        core.step(2, success()).unwrap();

        // Confirm pre-crash state is genuinely non-trivial before we throw
        // this incarnation away.
        let pre_term = core.current_term();
        let pre_commit = core.commit_index();
        let pre_voted_for = core.stored_hard_state().voted_for;
        let pre_last_index = core.storage.last_index();
        let pre_last_term = core.storage.last_term();
        let pre_entries = core.storage.entries_from(1);
        assert_eq!(pre_term, 1);
        assert_eq!(pre_commit, 3);
        assert_eq!(pre_voted_for, Some(1));
        assert_eq!(pre_last_index, 3);
        assert_eq!(pre_last_term, 1);
        assert_eq!(pre_entries.len(), 3);
        let pre_ready = core.ready();
        assert_eq!(pre_ready.apply.len(), 3); // all three applied this incarnation

        // Crash + restart: reclaim the storage (the only thing a real
        // process would still have) and construct a brand new core over it
        // with the same config, exactly as a restarting node would.
        let storage = core.into_storage();
        let restarted = RaftCore::new(config, storage).unwrap();

        // Persisted state is recovered exactly.
        assert_eq!(restarted.current_term(), pre_term);
        assert_eq!(restarted.stored_hard_state().voted_for, pre_voted_for);
        assert_eq!(restarted.storage.last_index(), pre_last_index);
        assert_eq!(restarted.storage.last_term(), pre_last_term);

        // No committed entry is lost: the entire log (all of it committed
        // pre-crash) survives byte-for-byte.
        assert_eq!(restarted.storage.entries_from(1), pre_entries);

        // Volatile state resets to the snapshot boundary rather than
        // carrying over the pre-crash view. This is correct Raft, not a
        // bug: a restarted node has no reliable memory of what it had
        // confirmed committed via peer acks, so it must re-learn
        // commit_index from a leader's AppendEntries (leader_commit)
        // instead of trusting its own stale pre-crash value — assuming the
        // old commit_index without re-confirmation could let a node apply
        // an entry that a new leader's log has since diverged from. With
        // no snapshot, that boundary is 0.
        assert_eq!(restarted.role(), Role::Follower);
        assert_eq!(restarted.commit_index(), 0);
        assert_ne!(restarted.commit_index(), pre_commit);
        // `last_applied` is private but this test module nests inside
        // `core`, so it's directly reachable — same reset applies to it.
        assert_eq!(restarted.last_applied, 0);
    }

    #[test]
    fn install_snapshot_is_ignored_not_panicked() {
        let mut c = RaftCore::new(cfg(1, &[1, 2, 3]), MemStorage::default()).unwrap();
        let msg = Message::InstallSnapshot(crate::rpc::InstallSnapshotReq {
            term: 1,
            leader_id: 2,
            last_index: 0,
            last_term: 0,
            data: vec![],
        });
        assert!(c.step(2, msg).is_ok());
    }
}
