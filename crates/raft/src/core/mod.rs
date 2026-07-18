use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::error::Result;
use crate::rpc::Message;
use crate::storage::RaftStorage;
use crate::types::{HardState, LogEntry, LogIndex, NodeId, Term};

mod election;
mod read_index;
mod replication;

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
    registered_tick: u64,
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
    /// Wired up in Task 5 to track the apply-loop's progress against
    /// `commit_index`.
    #[allow(dead_code)]
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
    /// Wired up in Task 4/5 to track per-peer AppendEntries liveness.
    #[allow(dead_code)]
    last_contact_tick: BTreeMap<NodeId, u64>,
    /// Wired up in Task 4+ for AppendEntries/heartbeat pacing diagnostics.
    #[allow(dead_code)]
    tick_count: u64,
    /// Reads registered via `read_index` awaiting release; see
    /// core/read_index.rs.
    pending_reads: Vec<PendingRead>,
    /// Term whose entry has committed, enabling reads at or before it.
    readable_term: Option<Term>,
    rng: SplitMix64,
    outbox: Vec<(NodeId, Message)>,
    apply_buf: Vec<LogEntry>,
    reads_buf: Vec<ReadToken>,
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
            last_contact_tick: BTreeMap::new(),
            tick_count: 0,
            pending_reads: Vec::new(),
            readable_term: None,
            rng,
            outbox: Vec::new(),
            apply_buf: Vec::new(),
            reads_buf: Vec::new(),
        };
        core.reset_election_timer();
        Ok(core)
    }

    pub fn tick(&mut self) -> Result<()> {
        self.tick_count += 1;
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
