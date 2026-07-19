//! Deterministic multi-node simulation harness for `RaftCore`.
//!
//! `Cluster` drives several `RaftCore<MemStorage>` instances through a
//! single-threaded, logical-time event loop and proves the four Raft safety
//! invariants hold under fault injection (crash/restart, partition, dropped
//! appends, reordered delivery). Everything here is built strictly on
//! `cairn_raft`'s public API (plus one minimal `into_storage` accessor added
//! to `RaftCore` to make crash+restart expressible — see its doc comment in
//! `crates/raft/src/core/mod.rs`).
//!
//! Determinism: a single controlling loop iterates nodes by index order and
//! the message queue in FIFO (or seeded-permutation, under `reorder`) order.
//! The only source of randomness anywhere is `SplitMix64`, hand-rolled here
//! (the core's own copy is private) and seeded once per `Cluster`. No
//! `std::time`, no threads, no `HashMap`/`HashSet` — `BTreeMap`/`BTreeSet`/
//! `Vec` throughout, so iteration order can never leak nondeterminism.

use std::collections::{BTreeMap, BTreeSet};

use cairn_raft::{
    ConfChange, Config, LogEntry, LogIndex, MemStorage, Message, NodeId, RaftCore, Role,
    SnapshotMeta, Term,
};

/// Hand-rolled deterministic PRNG for the harness's own fault decisions
/// (currently: which in-flight message to deliver next, when `reorder` is
/// on). Not the same instance as `RaftCore`'s internal copy — that one is
/// private to the crate and each node's election-timeout jitter is already
/// derived from `Config::seed ^ id`, so this is a *second*, independent
/// seeded stream that only ever influences the harness's delivery order.
struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        SplitMix64(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform index in `[0, n)`. `n == 0` is the caller's responsibility to
    /// avoid (never called on an empty queue below).
    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

/// A snapshot of one node's applied log taken at a moment it was observed to
/// be `Role::Leader`, used by `assert_leader_completeness_pairwise`.
struct LeaderSnapshot {
    term: Term,
    leader: NodeId,
    applied: Vec<LogEntry>,
}

/// Deterministic multi-node harness. Owns every node plus the in-flight
/// message queue and drives them through a single controlling loop —
/// `RaftCore` itself does no I/O, so the harness is the entire "runtime."
struct Cluster {
    ids: Vec<NodeId>,
    index_of: BTreeMap<NodeId, usize>,
    configs: Vec<Config>,
    /// `None` means crashed (see `crashed_storage` for its retained state).
    nodes: Vec<Option<RaftCore<MemStorage>>>,
    crashed_storage: Vec<Option<MemStorage>>,
    /// FIFO under normal operation; `deliver_one` pulls from a
    /// seed-chosen position instead when `reorder` is on.
    inflight: Vec<(NodeId, NodeId, Message)>,
    /// Per-node applied log, indexed the same way as `ids`/`nodes`. This is
    /// the observable projection the safety invariants are checked against
    /// (see `assert_log_agreement`'s doc comment for why).
    applied: Vec<Vec<LogEntry>>,
    /// Two-sided network partition, if any: nodes are mutually reachable
    /// iff both are in the same side (or no partition is active).
    partitions: Option<(Vec<NodeId>, Vec<NodeId>)>,
    /// Targeted fault for `dropped_appends_backup`: AppendEntries addressed
    /// to one of these ids are dropped at send time.
    block_appends_to: BTreeSet<NodeId>,
    /// When true, `deliver_one` picks a random queued message (via `rng`)
    /// instead of the front, modeling reordered network delivery.
    reorder: bool,
    rng: SplitMix64,
    /// Every `(term, leader_id)` pair observed over the run — the raw
    /// material for `assert_election_safety`.
    leader_observations: Vec<(Term, NodeId)>,
    leader_snapshots: Vec<LeaderSnapshot>,
    /// Canonical committed history: an entry lands here the first time ANY
    /// node applies it (a node only ever applies committed entries, so
    /// "applied somewhere" is exactly "committed"). `record_committed`
    /// asserts a newly observed entry never conflicts with what's already
    /// recorded at that index — a second state-machine-safety check,
    /// independent of `assert_log_agreement`'s pairwise scan. This is the
    /// ground truth `assert_leader_completeness_containment` checks a
    /// settled leader against.
    committed_by_index: BTreeMap<LogIndex, LogEntry>,
    /// Count of `deliver_one` calls, while `reorder` is on, that picked a
    /// queue position other than the front from a queue with more than one
    /// candidate — i.e. a delivery provably NOT in FIFO order. Used by
    /// `reordered_delivery` to prove reordering actually happened rather
    /// than merely running with the flag set on a queue that never had more
    /// than one message in it.
    non_fifo_deliveries: usize,
    /// Timing/seed the cluster was constructed with, retained so
    /// `register_node` can bootstrap a mid-run joiner with the exact same
    /// `Config` shape every original node got (see `Cluster::new`'s comment
    /// on why every node shares one base seed).
    election_timeout: u64,
    heartbeat_interval: u64,
    seed: u64,
    /// The membership the harness currently expects to be live, mutated
    /// only by `add_voter`/`remove_voter` after a successful
    /// `propose_conf_change`. This is what the voter-aware invariant
    /// checks (`assert_leader_completeness_containment`,
    /// `assert_membership_converged`) hold nodes to — NOT `self.ids`, which
    /// only ever grows and never reflects a removal.
    expected_voters: BTreeSet<NodeId>,
    /// Every node id, in order, that has ever processed a `Ready.restore`
    /// (i.e. genuinely installed a leader's `InstallSnapshot`, not merely
    /// backfilled via ordinary `AppendEntries`). `snapshot_catch_up` and
    /// `kill_and_replace` assert against this directly to prove they
    /// actually exercised the InstallSnapshot path, not just that the node
    /// somehow ended up caught up.
    restore_events: Vec<NodeId>,
}

impl Cluster {
    fn new(ids: &[NodeId], election_timeout: u64, heartbeat_interval: u64, seed: u64) -> Self {
        let mut index_of = BTreeMap::new();
        let mut configs = Vec::new();
        let mut nodes = Vec::new();
        for (idx, &id) in ids.iter().enumerate() {
            index_of.insert(id, idx);
            let config = Config {
                id,
                peers: ids.to_vec(),
                election_timeout,
                heartbeat_interval,
                // Deliberately the SAME base seed for every node: RaftCore
                // XORs it with `id` internally (see Config's doc in
                // core/mod.rs), which already gives each node a distinct
                // deterministic election-timeout stream without the harness
                // having to vary anything per node itself.
                seed,
            };
            let core = RaftCore::new(config.clone(), MemStorage::default())
                .expect("RaftCore::new must succeed with fresh MemStorage");
            configs.push(config);
            nodes.push(Some(core));
        }
        let n = ids.len();
        Cluster {
            ids: ids.to_vec(),
            index_of,
            configs,
            nodes,
            crashed_storage: (0..n).map(|_| None).collect(),
            inflight: Vec::new(),
            applied: (0..n).map(|_| Vec::new()).collect(),
            partitions: None,
            block_appends_to: BTreeSet::new(),
            reorder: false,
            rng: SplitMix64::new(seed ^ 0xA5A5_A5A5_A5A5_A5A5),
            leader_observations: Vec::new(),
            leader_snapshots: Vec::new(),
            committed_by_index: BTreeMap::new(),
            non_fifo_deliveries: 0,
            election_timeout,
            heartbeat_interval,
            seed,
            expected_voters: ids.iter().copied().collect(),
            restore_events: Vec::new(),
        }
    }

    /// Registers a brand-new node mid-run (fresh `MemStorage`, empty log),
    /// extending every parallel vec + `index_of`. `peers` bootstraps the
    /// joiner's own `Config::peers` — only consulted by `recompute_voters`
    /// as the last-resort fallback before any config entry or snapshot
    /// exists on this node (see membership.rs's doc comment), so callers
    /// pass the TARGET membership (not the pre-join one) to keep the
    /// joiner's own quorum math sane from tick 1, before it ever hears from
    /// a leader.
    fn register_node(&mut self, id: NodeId, peers: Vec<NodeId>) {
        assert!(
            !self.index_of.contains_key(&id),
            "register_node({id}): a node with this id is already registered"
        );
        let config = Config {
            id,
            peers,
            election_timeout: self.election_timeout,
            heartbeat_interval: self.heartbeat_interval,
            seed: self.seed,
        };
        let core = RaftCore::new(config.clone(), MemStorage::default())
            .expect("RaftCore::new must succeed with fresh MemStorage");
        self.index_of.insert(id, self.ids.len());
        self.ids.push(id);
        self.configs.push(config);
        self.nodes.push(Some(core));
        self.crashed_storage.push(None);
        self.applied.push(Vec::new());
    }

    fn reachable(&self, a: NodeId, b: NodeId) -> bool {
        if a == b {
            return true;
        }
        match &self.partitions {
            None => true,
            Some((side_a, side_b)) => {
                (side_a.contains(&a) && side_a.contains(&b))
                    || (side_b.contains(&a) && side_b.contains(&b))
            }
        }
    }

    fn should_drop_on_send(&self, from: NodeId, to: NodeId, msg: &Message) -> bool {
        if !self.reachable(from, to) {
            return true;
        }
        if self.block_appends_to.contains(&to) && matches!(msg, Message::AppendEntries(_)) {
            return true;
        }
        false
    }

    /// Appends a just-applied entry to `applied[idx]`, tolerating the
    /// specific re-application pattern a crash+restart produces: on
    /// reconstruction `RaftCore::new` resets `commit_index`/`last_applied`
    /// to the storage's snapshot boundary (0 here — this harness never
    /// snapshots), so the next heartbeat's `advance_apply` walks forward
    /// from index 1 again and re-emits entries this node already applied
    /// before the crash. That's expected and safe (the durable log itself
    /// was never touched) as long as the re-applied content is
    /// byte-identical to what's already recorded — which this asserts.
    fn record_applied(&mut self, idx: usize, entry: LogEntry) {
        let expected = self.applied[idx].len() as LogIndex + 1;
        if entry.index < expected {
            let prior = self.applied[idx][(entry.index - 1) as usize].clone();
            assert_eq!(
                prior, entry,
                "node {} re-applied index {} with DIFFERENT content after restart \
                 — state machine safety violation",
                self.ids[idx], entry.index
            );
            return;
        }
        assert_eq!(
            entry.index, expected,
            "node {} applied out of order or with a gap: expected index {expected}, got {}",
            self.ids[idx], entry.index
        );
        self.record_committed(entry.clone());
        self.applied[idx].push(entry);
    }

    /// Records `entry` as committed the first time any node applies it. If
    /// this index was already recorded, asserts the content matches exactly
    /// — two different entries ever being committed at the same index would
    /// itself be a state-machine-safety violation, independent of which
    /// nodes observed which one.
    fn record_committed(&mut self, entry: LogEntry) {
        match self.committed_by_index.get(&entry.index) {
            Some(existing) => assert_eq!(
                existing, &entry,
                "COMMITTED HISTORY CONFLICT at index {}: previously observed {existing:?}, now \
                 {entry:?} — two different entries committed at the same index \
                 (state machine safety violation)",
                entry.index
            ),
            None => {
                self.committed_by_index.insert(entry.index, entry);
            }
        }
    }

    fn record_leader_if_leading(&mut self, idx: usize) {
        let node = self.nodes[idx]
            .as_ref()
            .expect("node must be alive to record");
        if node.role() != Role::Leader {
            return;
        }
        let id = self.ids[idx];
        let term = node.current_term();
        self.leader_observations.push((term, id));
        self.leader_snapshots.push(LeaderSnapshot {
            term,
            leader: id,
            applied: self.applied[idx].clone(),
        });
    }

    /// Drains `idx`'s `ready()`: installs a pending snapshot restore (if
    /// any), files applied entries, enqueues outbound messages (subject to
    /// partition/block-appends filtering at send time), and records a
    /// leader snapshot if `idx` is currently leading. Reads are
    /// intentionally ignored — read-linearizability is out of scope for
    /// Task 7's safety invariants (Plan C treats it as optional).
    ///
    /// `restore` is processed BEFORE `apply`, in that order, within this
    /// one drained batch — the explicit Plan E driver contract: a follower
    /// that just installed a snapshot must have its state machine caught up
    /// to the snapshot's `last_index` before any further `apply` entries
    /// (which start at `last_index + 1`) are filed, or `record_applied`'s
    /// contiguity invariant would see a gap.
    fn drain_ready(&mut self, idx: usize) {
        let self_id = self.ids[idx];
        let ready = self.nodes[idx]
            .as_mut()
            .expect("drain_ready called on a crashed node")
            .ready();
        if let Some((meta, data)) = ready.restore {
            self.apply_restore(idx, meta, data);
        }
        for entry in ready.apply {
            self.record_applied(idx, entry);
        }
        for (to, msg) in ready.messages {
            if self.should_drop_on_send(self_id, to, &msg) {
                continue;
            }
            self.inflight.push((self_id, to, msg));
        }
        let _ = ready.reads; // deliberately unobserved, see doc comment above
        self.record_leader_if_leading(idx);
    }

    /// Installs a follower's `Ready.restore` into the harness's tracked
    /// applied-log projection for node `idx`. The sim's "state machine" is
    /// the applied-log projection itself, so a snapshot's `data` blob is
    /// modeled as exactly `bincode::serialize(&Vec<LogEntry>)` of the
    /// leader's applied prefix at compaction time (see `compact_leader`) —
    /// `bincode` is deterministic and `LogEntry` already derives
    /// `Serialize`/`Deserialize`, so no hand-rolled encoding is needed.
    fn apply_restore(&mut self, idx: usize, meta: SnapshotMeta, data: Vec<u8>) {
        let snapshot: Vec<LogEntry> = bincode::deserialize(&data).unwrap_or_else(|e| {
            panic!(
                "node {}: Ready.restore's data did not decode as Vec<LogEntry>: {e}",
                self.ids[idx]
            )
        });
        assert_eq!(
            snapshot.len() as LogIndex,
            meta.last_index,
            "node {}: restored snapshot has {} entries but meta.last_index is {}",
            self.ids[idx],
            snapshot.len(),
            meta.last_index
        );
        for (k, entry) in snapshot.iter().enumerate() {
            assert_eq!(
                entry.index,
                k as LogIndex + 1,
                "node {}: restored snapshot is non-contiguous at position {k} (entry.index {})",
                self.ids[idx],
                entry.index
            );
        }
        // State-machine safety across the snapshot boundary: wherever this
        // node's own prior applied prefix overlaps the restored snapshot,
        // the content must agree exactly. The core only ever installs a
        // snapshot strictly ahead of the follower's commit_index, so in
        // practice the prior prefix is always shorter than or equal to the
        // snapshot — but check the overlap regardless rather than assume it.
        let prior = self.applied[idx].clone();
        for k in 0..snapshot.len().min(prior.len()) {
            assert_eq!(
                snapshot[k],
                prior[k],
                "node {}: restored snapshot CONTRADICTS its own already-applied entry at index \
                 {} — state machine safety violation across a snapshot boundary",
                self.ids[idx],
                k + 1
            );
        }
        // A restored snapshot must never contradict what was committed
        // elsewhere in the cluster either.
        for entry in &snapshot {
            self.record_committed(entry.clone());
        }
        self.applied[idx] = snapshot;
        self.restore_events.push(self.ids[idx]);
    }

    fn restored_nodes(&self) -> &[NodeId] {
        &self.restore_events
    }

    fn tick_all(&mut self) {
        for idx in 0..self.nodes.len() {
            if self.nodes[idx].is_none() {
                continue;
            }
            self.nodes[idx]
                .as_mut()
                .expect("checked Some above")
                .tick()
                .expect("tick must not error");
            self.drain_ready(idx);
        }
    }

    /// Pops and delivers one in-flight message: FIFO under normal
    /// operation, or a seed-chosen position when `reorder` is on. Returns
    /// `false` once the queue is empty.
    fn deliver_one(&mut self) -> bool {
        if self.inflight.is_empty() {
            return false;
        }
        let pick = if self.reorder {
            self.rng.below(self.inflight.len())
        } else {
            0
        };
        if self.reorder && pick != 0 && self.inflight.len() > 1 {
            self.non_fifo_deliveries += 1;
        }
        let (from, to, msg) = self.inflight.remove(pick);
        let Some(&idx) = self.index_of.get(&to) else {
            return true; // unknown recipient: nothing to do, message is lost
        };
        if self.nodes[idx].is_none() {
            return true; // crashed target: message is lost
        }
        self.nodes[idx]
            .as_mut()
            .expect("checked Some above")
            .step(from, msg)
            .expect("step must not error");
        self.drain_ready(idx);
        true
    }

    fn deliver_all(&mut self) {
        let mut iterations = 0usize;
        while self.deliver_one() {
            iterations += 1;
            assert!(
                iterations < 1_000_000,
                "deliver_all did not converge — possible unbounded message amplification"
            );
        }
    }

    /// One "round" = tick every live node once, then drain the network
    /// until quiescent. Repeated `rounds` times.
    fn run(&mut self, rounds: usize) {
        for _ in 0..rounds {
            self.tick_all();
            self.deliver_all();
        }
    }

    fn propose_on(&mut self, id: NodeId, cmd: Vec<u8>) -> Option<LogIndex> {
        let idx = self.index_of[&id];
        let result = self.nodes[idx]
            .as_mut()
            .expect("propose target must be alive")
            .propose(cmd)
            .expect("propose must not error");
        self.drain_ready(idx);
        result
    }

    fn current_leaders_among(&self, subset: &[NodeId]) -> Vec<NodeId> {
        subset
            .iter()
            .copied()
            .filter(|id| {
                let idx = self.index_of[id];
                self.nodes[idx]
                    .as_ref()
                    .is_some_and(|n| n.role() == Role::Leader)
            })
            .collect()
    }

    fn current_leaders(&self) -> Vec<NodeId> {
        let ids = self.ids.clone();
        self.current_leaders_among(&ids)
    }

    fn current_term_of(&self, id: NodeId) -> Term {
        let idx = self.index_of[&id];
        self.nodes[idx]
            .as_ref()
            .expect("node must be alive")
            .current_term()
    }

    fn applied_of(&self, id: NodeId) -> &[LogEntry] {
        &self.applied[self.index_of[&id]]
    }

    fn crash(&mut self, id: NodeId) {
        let idx = self.index_of[&id];
        let node = self.nodes[idx].take().expect("crash target must be alive");
        self.crashed_storage[idx] = Some(node.into_storage());
    }

    fn restart(&mut self, id: NodeId) {
        let idx = self.index_of[&id];
        let storage = self.crashed_storage[idx]
            .take()
            .expect("restart target must be crashed");
        let core = RaftCore::new(self.configs[idx].clone(), storage)
            .expect("restart must succeed over retained storage");
        self.nodes[idx] = Some(core);
    }

    fn partition(&mut self, side_a: &[NodeId], side_b: &[NodeId]) {
        self.partitions = Some((side_a.to_vec(), side_b.to_vec()));
    }

    fn heal(&mut self) {
        self.partitions = None;
    }

    fn block_appends(&mut self, id: NodeId) {
        self.block_appends_to.insert(id);
    }

    fn unblock_appends(&mut self, id: NodeId) {
        self.block_appends_to.remove(&id);
    }

    fn set_reorder(&mut self, on: bool) {
        self.reorder = on;
    }

    fn non_fifo_deliveries(&self) -> usize {
        self.non_fifo_deliveries
    }

    /// Snapshots `leader`'s applied prefix through `up_to`. Models the
    /// snapshot's opaque `data` blob as
    /// `bincode::serialize(&applied[..up_to])` (see `apply_restore`'s doc
    /// comment for why that's the right fixture-side model of "the state
    /// machine"), then calls `RaftCore::compact`, which enforces `up_to` is
    /// in `(current_snapshot_base, last_applied]` — a well-formed scenario
    /// must never violate that range, so a rejection here is an `.expect`,
    /// not a `Result` the caller has to handle.
    fn compact_leader(&mut self, leader: NodeId, up_to: LogIndex) {
        let idx = self.index_of[&leader];
        let applied_prefix = &self.applied[idx];
        assert!(
            applied_prefix.len() as LogIndex >= up_to,
            "compact_leader({leader}, {up_to}): leader has only applied {} entries",
            applied_prefix.len()
        );
        let data = bincode::serialize(&applied_prefix[..up_to as usize])
            .expect("bincode serialization of [LogEntry] must not fail");
        self.nodes[idx]
            .as_mut()
            .expect("compact_leader target must be alive")
            .compact(up_to, data)
            .expect("compact must succeed for a well-formed scenario — index out of range");
        self.drain_ready(idx);
    }

    /// Proposes `change` against whichever node currently claims leadership
    /// among `self.expected_voters`, retrying across a bounded round budget
    /// — mirroring `propose_on_current_leader` — whenever there's no leader
    /// yet, or the leader refuses (`Ok(None)`: e.g. a prior conf change is
    /// still in flight, or this is a same-membership no-op).
    fn propose_conf_change_on_current_leader(&mut self, change: ConfChange) {
        let expected: Vec<NodeId> = self.expected_voters.iter().copied().collect();
        for _ in 0..200 {
            if let Some(leader) = self.current_leaders_among(&expected).first().copied() {
                let idx = self.index_of[&leader];
                let result = self.nodes[idx]
                    .as_mut()
                    .expect("leader must be alive")
                    .propose_conf_change(change)
                    .expect("propose_conf_change must not error");
                self.drain_ready(idx);
                if result.is_some() {
                    return;
                }
            }
            self.run(1);
        }
        panic!("propose_conf_change({change:?}) never succeeded within budget");
    }

    /// Grows the live membership by one voter. Registers `id` as a fresh
    /// node FIRST (bootstrapped to the TARGET membership, i.e. the current
    /// `expected_voters` plus `id`, so the joiner's own quorum math is sane
    /// from tick 1 — see `register_node`), so the leader's subsequent
    /// AppendEntries/InstallSnapshot to it are deliverable at all
    /// (`deliver_one` silently drops messages to an unknown recipient).
    /// Then proposes `AddVoter(id)` on the current leader, and settles the
    /// cluster before returning — Task 6's "each committed + caught up
    /// before the next": the caller can immediately treat `id` as a fully
    /// caught-up member.
    fn add_voter(&mut self, id: NodeId) {
        let mut target = self.expected_voters.clone();
        target.insert(id);
        self.register_node(id, target.into_iter().collect());

        self.propose_conf_change_on_current_leader(ConfChange::AddVoter(id));
        self.expected_voters.insert(id);
        self.settle();
    }

    /// Shrinks the live membership by one voter: proposes `RemoveVoter(id)`
    /// on the current leader, then settles. `id` stays registered and kept
    /// running (a real deployment can't force a removed process to stop) —
    /// see `assert_leader_completeness_containment`'s doc comment for how a
    /// removed-but-live node is tolerated without being allowed to become
    /// the settled leader.
    fn remove_voter(&mut self, id: NodeId) {
        self.propose_conf_change_on_current_leader(ConfChange::RemoveVoter(id));
        self.expected_voters.remove(&id);
        self.settle();
    }

    /// Heals any active partition, then drives `tick_all` + `deliver_all`
    /// rounds until the cluster is quiescent: two consecutive full rounds in
    /// a row produce no growth in any node's applied log (and, trivially,
    /// leave no in-flight messages, since `deliver_all` always drains the
    /// queue to empty before a round is considered complete). Bounded at
    /// `MAX_ROUNDS` — failing to quiesce by then is itself a bug (a
    /// liveness failure, or a fault left active that prevents convergence)
    /// and panics rather than silently returning early, since every caller
    /// relies on "settled" meaning something.
    ///
    /// Leader completeness can't be checked the instant a new leader is
    /// elected — a freshly elected leader hasn't caught up to the prior
    /// committed history yet. `settle()` is what makes checking it
    /// meaningful: only once the cluster has stopped changing does "the
    /// leader's applied log" mean anything stable to compare against the
    /// canonical committed history.
    fn settle(&mut self) {
        const MAX_ROUNDS: usize = 200;
        const STABLE_ROUNDS_REQUIRED: usize = 2;

        self.heal();

        let mut stable_rounds = 0;
        for _ in 0..MAX_ROUNDS {
            let before: Vec<usize> = self.applied.iter().map(Vec::len).collect();
            self.tick_all();
            self.deliver_all();
            let after: Vec<usize> = self.applied.iter().map(Vec::len).collect();

            if after == before && self.inflight.is_empty() {
                stable_rounds += 1;
                if stable_rounds >= STABLE_ROUNDS_REQUIRED {
                    return;
                }
            } else {
                stable_rounds = 0;
            }
        }
        panic!(
            "cluster failed to settle within {MAX_ROUNDS} rounds — applied logs (or in-flight \
             messages) kept changing; the cluster never reached quiescence"
        );
    }

    // --- Safety invariants (checked after every scenario) ---

    fn assert_invariants(&self) {
        self.assert_election_safety();
        self.assert_log_agreement();
        self.assert_leader_completeness_pairwise();
    }

    /// Settles the cluster (see `settle`'s doc comment for why this has to
    /// happen first), then checks every invariant `assert_invariants` does
    /// PLUS the strengthened leader-completeness containment check, which is
    /// only meaningful once the cluster is quiescent and there's exactly one
    /// current leader to check containment against.
    fn assert_invariants_after_settle(&mut self) {
        self.settle();
        self.assert_invariants();
        self.assert_leader_completeness_containment();
        self.assert_membership_converged();
    }

    /// Invariant 1 — ELECTION SAFETY: at most one leader per term. Checked
    /// directly against every `(term, leader)` pair ever observed.
    fn assert_election_safety(&self) {
        let mut seen: BTreeMap<Term, NodeId> = BTreeMap::new();
        for &(term, id) in &self.leader_observations {
            match seen.get(&term) {
                Some(&existing) if existing != id => panic!(
                    "ELECTION SAFETY VIOLATED: term {term} has two distinct leaders: \
                     {existing} and {id}"
                ),
                _ => {
                    seen.insert(term, id);
                }
            }
        }
    }

    /// Invariants 2 (LOG MATCHING) and 3 (STATE-MACHINE SAFETY), checked
    /// together on the applied-log projection.
    ///
    /// `RaftCore`'s public API doesn't expose the raw persisted log (only
    /// `commit_index`/`current_term`/`role`/`leader_id` and whatever
    /// `ready().apply` drains), so this compares that projection instead of
    /// the full log. That's the documented limitation: an uncommitted,
    /// still-mutable suffix on a follower could differ transiently between
    /// nodes without violating anything, and this check can't see it either
    /// way — which is fine, because LOG MATCHING and STATE-MACHINE SAFETY
    /// are both properties of *committed* state.
    ///
    /// Within that projection, one full-`LogEntry`-equality scan covers
    /// both properties at once: `record_applied`'s contiguity assert
    /// guarantees `applied[i]` is exactly node `i`'s log at indices
    /// `1..=len` in order with no gaps or duplicates, so for any two nodes,
    /// equal entries at every overlapping index `k` simultaneously proves
    /// "same term at index k implies identical entries up to k" (log
    /// matching) and "no two nodes disagree on committed content at index
    /// k" (state machine safety).
    fn assert_log_agreement(&self) {
        for i in 0..self.applied.len() {
            for j in (i + 1)..self.applied.len() {
                let a = &self.applied[i];
                let b = &self.applied[j];
                for k in 0..a.len().min(b.len()) {
                    assert_eq!(
                        a[k],
                        b[k],
                        "LOG MATCHING / STATE MACHINE SAFETY VIOLATED between node {} \
                         and node {} at applied index {}",
                        self.ids[i],
                        self.ids[j],
                        k + 1
                    );
                }
            }
        }
    }

    /// Invariant 4 — LEADER COMPLETENESS, pairwise REWRITE check: every
    /// entry present in a leader's applied log at the moment it was observed
    /// leading must agree, at every overlapping index, with every other such
    /// snapshot — including ones taken for leaders that led in earlier OR
    /// later terms.
    ///
    /// This is necessary but NOT sufficient on its own: comparing only over
    /// `0..min(a.len, b.len)` means it catches a leader that REWRITES an
    /// entry at an index it still has, but stays silent if a LATER leader's
    /// applied log is simply SHORTER — i.e. missing a committed entry
    /// entirely, never reaching the index where the disagreement would show
    /// up. That's exactly a leader-completeness violation (a new leader that
    /// lost prior-committed state), and it's why this is paired with
    /// `assert_leader_completeness_containment`, which checks containment
    /// against the full canonical committed history instead of pairwise
    /// snapshot overlap.
    fn assert_leader_completeness_pairwise(&self) {
        for i in 0..self.leader_snapshots.len() {
            for j in (i + 1)..self.leader_snapshots.len() {
                let a = &self.leader_snapshots[i];
                let b = &self.leader_snapshots[j];
                for k in 0..a.applied.len().min(b.applied.len()) {
                    assert_eq!(
                        a.applied[k],
                        b.applied[k],
                        "LEADER COMPLETENESS VIOLATED: leader {} (term {}) and leader {} \
                         (term {}) disagree at applied index {}",
                        a.leader,
                        a.term,
                        b.leader,
                        b.term,
                        k + 1
                    );
                }
            }
        }
    }

    /// Invariant 4 — LEADER COMPLETENESS, the real CONTAINMENT check: after
    /// the cluster has `settle()`d to exactly one current leader AMONG
    /// `expected_voters`, that leader's applied log must CONTAIN every
    /// entry ever committed over the whole run (`committed_by_index`, built
    /// in `record_committed`), at the correct index, byte-identical. Unlike
    /// `assert_leader_completeness_pairwise`, this cannot be fooled by a
    /// leader whose applied log is simply too SHORT: every committed index
    /// is checked explicitly, so a missing index is a `None` from
    /// `leader_applied.get(pos)` rather than a loop bound that silently
    /// never reaches it.
    ///
    /// Checked over `expected_voters`, NOT `self.ids`: a node the harness
    /// has removed (`remove_voter`) stays registered and running — a real
    /// deployment can't force a removed process to stop — and Raft's
    /// single-server membership scheme has no mechanism to stop a removed
    /// node from campaigning (it can still win votes from stale peers on
    /// log up-to-dateness alone; see the Raft dissertation §4.2.2). Such a
    /// node is tolerated as briefly disruptive, but must never be counted
    /// as, or required to be, THE settled leader — so "exactly one leader"
    /// is required only among the membership the harness currently expects
    /// to be live.
    ///
    /// Requires exactly one current leader among `expected_voters` —
    /// meaningless (and asserted against) otherwise, since "the leader"
    /// wouldn't be well defined.
    fn assert_leader_completeness_containment(&self) {
        let expected: Vec<NodeId> = self.expected_voters.iter().copied().collect();
        let leaders = self.current_leaders_among(&expected);
        assert_eq!(
            leaders.len(),
            1,
            "assert_leader_completeness_containment requires the cluster to have settled to \
             exactly one current leader among expected_voters {expected:?}; found {}: \
             {leaders:?}",
            leaders.len()
        );
        let leader = leaders[0];
        if let Err(violation) = check_containment(&self.committed_by_index, self.applied_of(leader))
        {
            panic!("LEADER COMPLETENESS VIOLATED for leader {leader}: {violation}");
        }
    }

    /// Voter-aware companion invariant, checked after every scenario
    /// settles (see `assert_invariants_after_settle`): every node the
    /// harness currently expects to be a voter must have actually
    /// converged to that exact membership — `core.voters()` (the ground
    /// truth every quorum/peer-iteration decision inside `RaftCore` is made
    /// against) must equal `expected_voters`, not a stale bootstrap
    /// config or a mid-flight change. Pairwise applied-log agreement across
    /// ALL registered nodes (not just expected voters) is already covered
    /// by `assert_log_agreement`, so it isn't repeated here.
    fn assert_membership_converged(&self) {
        for &id in &self.expected_voters {
            let idx = self.index_of[&id];
            // A node the harness never restarted (e.g. `leader_crash_reelection`
            // crashes the leader and moves on without ever calling
            // `remove_voter`) has no core left to query and plays no further
            // part in convergence — skip it rather than treat "still
            // registered as an expected voter, but permanently crashed" as a
            // membership-convergence failure.
            let Some(node) = self.nodes[idx].as_ref() else {
                continue;
            };
            let live: BTreeSet<NodeId> = node.voters().into_iter().collect();
            assert_eq!(
                live, self.expected_voters,
                "node {id}'s live voter set has not converged to the expected membership \
                 {:?}, found {live:?}",
                self.expected_voters
            );
        }
    }
}

/// Pure containment check, factored out of `Cluster` so it can be unit
/// tested directly against synthetic and real fixtures (see
/// `containment_check_detects_a_lost_committed_entry` and
/// `containment_check_is_discriminating_on_real_scenario_data` below),
/// proving the containment invariant is actually discriminating and not
/// vacuous.
///
/// Does `leader_applied` contain every entry in `committed`, at the index
/// `entry.index` implies (1-based, so `applied[index - 1]`), byte-identical?
/// Returns `Err` describing the first violation found, `Ok(())` if none.
fn check_containment(
    committed: &BTreeMap<LogIndex, LogEntry>,
    leader_applied: &[LogEntry],
) -> Result<(), String> {
    for (&index, entry) in committed {
        let pos = (index - 1) as usize;
        match leader_applied.get(pos) {
            Some(actual) if actual == entry => {}
            Some(actual) => {
                return Err(format!(
                    "leader log MISMATCHES committed index {index}: expected {entry:?}, found \
                     {actual:?}"
                ));
            }
            None => {
                return Err(format!(
                    "leader log is MISSING committed index {index} ({entry:?}); leader's \
                     applied log has only {} entries",
                    leader_applied.len()
                ));
            }
        }
    }
    Ok(())
}

/// Runs rounds until exactly one node among `subset` is observed leading,
/// or panics after a generous budget. 500 rounds is far more than a 3-node
/// cluster with `election_timeout: 10` should ever need even through
/// repeated split-vote retries (each retry redraws a timeout in
/// `[10, 20)`), so hitting the budget indicates a genuine liveness bug
/// rather than an unlucky seed.
fn elect(cluster: &mut Cluster, subset: &[NodeId]) -> NodeId {
    for _ in 0..500 {
        cluster.run(1);
        let leaders = cluster.current_leaders_among(subset);
        if leaders.len() == 1 {
            return leaders[0];
        }
    }
    panic!("no stable leader emerged among {subset:?} within budget");
}

/// Proposes `cmd` against whichever node currently claims leadership,
/// retrying against a fresh leader if the first attempt's target has
/// stepped down by the time `propose_on` runs (relevant under `reorder`,
/// where heartbeat delivery can be delayed enough for a spurious election).
fn propose_on_current_leader(cluster: &mut Cluster, ids: &[NodeId], cmd: Vec<u8>) {
    for _ in 0..50 {
        if let Some(leader) = cluster.current_leaders_among(ids).first().copied() {
            if cluster.propose_on(leader, cmd.clone()).is_some() {
                return;
            }
        }
        cluster.run(1);
    }
    panic!("failed to propose {cmd:?} on any leader within budget");
}

fn non_empty_commands(entries: &[LogEntry]) -> Vec<Vec<u8>> {
    entries
        .iter()
        .filter(|e| !e.command.is_empty())
        .map(|e| e.command.clone())
        .collect()
}

const IDS: [NodeId; 3] = [1, 2, 3];

#[test]
fn clean_election_and_replication() {
    let mut cluster = Cluster::new(&IDS, 10, 3, 1);
    let leader = elect(&mut cluster, &IDS);

    for cmd in [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()] {
        propose_on_current_leader(&mut cluster, &IDS, cmd);
        cluster.run(20);
    }
    cluster.run(30);

    assert_eq!(
        cluster.current_leaders(),
        vec![leader],
        "exactly one leader must remain, and it must be the one originally elected"
    );

    let expected = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
    for &id in &IDS {
        assert_eq!(
            non_empty_commands(cluster.applied_of(id)),
            expected,
            "node {id} must apply all proposed commands, in order"
        );
    }

    cluster.assert_invariants_after_settle();
}

#[test]
fn leader_crash_reelection() {
    let mut cluster = Cluster::new(&IDS, 10, 3, 2);
    let leader1 = elect(&mut cluster, &IDS);
    let term1 = cluster.current_term_of(leader1);

    propose_on_current_leader(&mut cluster, &IDS, b"before-crash".to_vec());
    cluster.run(20);

    cluster.crash(leader1);

    let survivors: Vec<NodeId> = IDS.into_iter().filter(|&id| id != leader1).collect();
    let leader2 = elect(&mut cluster, &survivors);
    assert_ne!(
        leader2, leader1,
        "the crashed leader cannot be the new leader"
    );
    let term2 = cluster.current_term_of(leader2);
    assert!(
        term2 > term1,
        "reelection must occur in a strictly higher term (was {term1}, now {term2})"
    );

    propose_on_current_leader(&mut cluster, &survivors, b"after-crash".to_vec());
    cluster.run(30);

    for &id in &survivors {
        assert!(
            non_empty_commands(cluster.applied_of(id)).contains(&b"after-crash".to_vec()),
            "surviving node {id} must replicate the post-crash command"
        );
    }

    cluster.assert_invariants_after_settle();
}

#[test]
fn partition_and_heal() {
    let mut cluster = Cluster::new(&IDS, 10, 3, 3);
    elect(&mut cluster, &IDS);

    let majority = [1, 2];
    let minority = [3];
    cluster.partition(&majority, &minority);
    cluster.run(30);

    let majority_leader = elect(&mut cluster, &majority);
    propose_on_current_leader(&mut cluster, &majority, b"majority-write".to_vec());
    cluster.run(30);

    assert!(
        !non_empty_commands(cluster.applied_of(3)).contains(&b"majority-write".to_vec()),
        "minority node must not see the majority-only write while still partitioned"
    );
    assert!(
        non_empty_commands(cluster.applied_of(majority_leader))
            .contains(&b"majority-write".to_vec()),
        "majority side must have committed its own write"
    );

    cluster.heal();
    cluster.run(60);

    for &id in &IDS {
        assert!(
            non_empty_commands(cluster.applied_of(id)).contains(&b"majority-write".to_vec()),
            "node {id} must catch up on the majority write once healed"
        );
    }

    cluster.assert_invariants_after_settle();
}

#[test]
fn dropped_appends_backup() {
    let mut cluster = Cluster::new(&IDS, 10, 3, 4);
    let leader = elect(&mut cluster, &IDS);
    let target = IDS.into_iter().find(|&id| id != leader).unwrap();
    let other = IDS
        .into_iter()
        .find(|&id| id != leader && id != target)
        .unwrap();

    cluster.block_appends(target);

    for i in 0..5 {
        propose_on_current_leader(&mut cluster, &[leader], format!("cmd-{i}").into_bytes());
        cluster.run(10);
    }

    assert!(
        cluster.applied_of(target).len() < cluster.applied_of(other).len(),
        "the blocked follower must have fallen behind while its inbound appends were dropped"
    );

    cluster.unblock_appends(target);
    cluster.run(60);

    assert_eq!(
        cluster.applied_of(target).len(),
        cluster.applied_of(other).len(),
        "the blocked follower must converge via conflict back-up once appends are allowed again"
    );
    cluster.assert_invariants_after_settle();
}

#[test]
fn reordered_delivery() {
    let mut cluster = Cluster::new(&IDS, 10, 3, 5);
    cluster.set_reorder(true);

    elect(&mut cluster, &IDS);
    for cmd in [b"x".to_vec(), b"y".to_vec(), b"z".to_vec()] {
        propose_on_current_leader(&mut cluster, &IDS, cmd);
        cluster.run(20);
    }
    cluster.run(40);

    assert_eq!(
        cluster.current_leaders().len(),
        1,
        "a leader must still be identifiable despite reordered delivery"
    );
    for &id in &IDS {
        assert!(
            !cluster.applied_of(id).is_empty(),
            "node {id} must make progress despite reordered message delivery"
        );
    }
    assert!(
        cluster.non_fifo_deliveries() > 0,
        "reordered_delivery must actually exercise a non-FIFO delivery at least once — a \
         3-node cluster's leader fans AppendEntries out to 2 followers per round, so multiple \
         messages should genuinely be in flight at once; if this fails, the queue never had \
         more than one candidate and the scenario isn't testing what it claims to"
    );

    cluster.assert_invariants_after_settle();
}

#[test]
fn restart_persistence() {
    let mut cluster = Cluster::new(&IDS, 10, 3, 6);
    let leader = elect(&mut cluster, &IDS);
    let follower = IDS.into_iter().find(|&id| id != leader).unwrap();

    propose_on_current_leader(&mut cluster, &IDS, b"one".to_vec());
    cluster.run(20);
    propose_on_current_leader(&mut cluster, &IDS, b"two".to_vec());
    cluster.run(20);

    let before_crash = cluster.applied_of(follower).to_vec();
    assert!(
        !before_crash.is_empty(),
        "follower must have applied something before crash for this scenario to be meaningful"
    );

    cluster.crash(follower);
    cluster.restart(follower);

    // Fresh contact from the leader is what lets the restarted node's
    // commit_index/last_applied (reset to 0 by reconstruction) catch back
    // up — see `record_applied`'s doc comment on the resulting re-apply.
    cluster.run(40);

    let after_restart = cluster.applied_of(follower);
    assert!(
        after_restart.len() >= before_crash.len(),
        "restart must not lose any previously-applied entry"
    );
    assert_eq!(
        &after_restart[..before_crash.len()],
        &before_crash[..],
        "the restarted node's applied prefix must match its pre-crash history exactly"
    );

    propose_on_current_leader(&mut cluster, &IDS, b"three".to_vec());
    cluster.run(30);

    assert!(
        non_empty_commands(cluster.applied_of(follower)).contains(&b"three".to_vec()),
        "restarted node must continue replicating new commands"
    );

    cluster.assert_invariants_after_settle();
}

const IDS5: [NodeId; 5] = [1, 2, 3, 4, 5];

/// Log compaction + InstallSnapshot catch-up (Task 6): a follower falls far
/// enough behind, WHILE compaction happens on the majority, that its needed
/// entries are structurally gone from the leader's log by the time it
/// reconnects — the only possible path back to convergence is a genuine
/// `InstallSnapshot`, not ordinary `AppendEntries` conflict backup. Proven
/// directly (not just inferred from the outcome) via `restored_nodes()`.
#[test]
fn snapshot_catch_up() {
    let mut cluster = Cluster::new(&IDS, 10, 3, 7);
    elect(&mut cluster, &IDS);

    let majority = [1, 2];
    let minority = [3];
    cluster.partition(&majority, &minority);
    cluster.run(30);

    let majority_leader = elect(&mut cluster, &majority);
    for cmd in [
        b"s1".to_vec(),
        b"s2".to_vec(),
        b"s3".to_vec(),
        b"s4".to_vec(),
    ] {
        propose_on_current_leader(&mut cluster, &majority, cmd);
        cluster.run(20);
    }

    let leader_applied_len = cluster.applied_of(majority_leader).len() as LogIndex;
    assert!(
        leader_applied_len >= 4,
        "scenario setup needs a non-trivial committed prefix to compact away"
    );

    // Node 3's next_index has been frozen at wherever it was when the
    // partition began (no message ever reached it to advance it) — well
    // below the leader's current applied length. Compacting through the
    // ENTIRE applied prefix guarantees node 3's needed entries no longer
    // exist anywhere in the leader's log: only InstallSnapshot can recover it.
    cluster.compact_leader(majority_leader, leader_applied_len);

    assert!(
        !cluster.restored_nodes().contains(&3),
        "node 3 must not have restored anything yet — it's still partitioned away"
    );

    cluster.heal();
    cluster.run(60);

    assert!(
        cluster.restored_nodes().contains(&3),
        "node 3 must have genuinely installed a leader snapshot (Ready.restore) to converge — \
         its needed entries were structurally compacted away, so ordinary AppendEntries \
         conflict-backup alone could not have gotten it there"
    );
    assert_eq!(
        cluster.applied_of(3),
        cluster.applied_of(majority_leader),
        "node 3 must converge to exactly the majority leader's applied state after healing"
    );

    cluster.assert_invariants_after_settle();
}

/// Single-server membership growth (Task 6): 3 -> 4 -> 5 voters, one at a
/// time, each committed and caught up (via `add_voter`'s internal
/// `settle()`) before the next change is proposed. Proposes across each
/// transition to prove the cluster keeps making progress throughout, not
/// just before/after.
#[test]
fn grow_three_to_five() {
    let mut cluster = Cluster::new(&IDS, 10, 3, 8);
    elect(&mut cluster, &IDS);

    propose_on_current_leader(&mut cluster, &IDS, b"before-grow".to_vec());
    cluster.run(20);

    cluster.add_voter(4);
    let voters4: Vec<NodeId> = vec![1, 2, 3, 4];
    propose_on_current_leader(&mut cluster, &voters4, b"after-4".to_vec());
    cluster.run(20);

    cluster.add_voter(5);
    let voters5: Vec<NodeId> = vec![1, 2, 3, 4, 5];
    propose_on_current_leader(&mut cluster, &voters5, b"after-5".to_vec());
    cluster.run(30);

    for &id in &voters5 {
        let cmds = non_empty_commands(cluster.applied_of(id));
        assert!(
            cmds.contains(&b"before-grow".to_vec()),
            "node {id} must have the pre-growth command"
        );
        assert!(
            cmds.contains(&b"after-4".to_vec()),
            "node {id} must have the command proposed after growing to 4"
        );
        assert!(
            cmds.contains(&b"after-5".to_vec()),
            "node {id} must have the command proposed after growing to 5"
        );
    }

    // Election safety (at most one leader per term) is checked over the
    // WHOLE run's leader_observations by assert_invariants_after_settle
    // below; membership convergence to exactly {1,2,3,4,5} is checked by
    // assert_membership_converged, also below.
    cluster.assert_invariants_after_settle();
}

/// Single-server membership shrink (Task 6): 5 -> 4 -> 3 voters. The first
/// removal takes an ordinary follower; the second removal specifically
/// targets the CURRENT LEADER, which must force a clean re-election among
/// the remaining 3. The removed leader must not linger as A leader anywhere
/// (asserted cluster-wide below); `assert_leader_completeness_containment`
/// (via `assert_invariants_after_settle`) independently enforces that a
/// removed-but-live node is never THE settled leader — see its doc comment.
#[test]
fn shrink_five_to_three() {
    let mut cluster = Cluster::new(&IDS5, 10, 3, 9);
    elect(&mut cluster, &IDS5);

    propose_on_current_leader(&mut cluster, &IDS5, b"before-shrink".to_vec());
    cluster.run(20);

    let leader_before = cluster.current_leaders_among(&IDS5)[0];
    let first_removed = IDS5.into_iter().find(|&id| id != leader_before).unwrap();
    cluster.remove_voter(first_removed);

    let remaining4: Vec<NodeId> = IDS5.into_iter().filter(|&id| id != first_removed).collect();
    propose_on_current_leader(&mut cluster, &remaining4, b"after-first-remove".to_vec());
    cluster.run(20);

    let leader_to_remove = cluster.current_leaders_among(&remaining4)[0];
    cluster.remove_voter(leader_to_remove);

    let remaining3: Vec<NodeId> = remaining4
        .into_iter()
        .filter(|&id| id != leader_to_remove)
        .collect();
    let new_leader = elect(&mut cluster, &remaining3);
    // Discriminating check: exactly one leader CLUSTER-WIDE, and it is the
    // newly elected one — not the removed node lingering. `assert_ne!` on
    // `new_leader` alone would be vacuous (`elect` only ever returns an id
    // from `remaining3`, which excludes `leader_to_remove` by construction).
    // A removed-but-live node still winning a re-election (Raft dissertation
    // §4.2.2 disruption — neither RequestVote nor AppendEntries checks sender
    // membership yet) would surface here as a second leader or as the removed
    // id, failing this assert instead of slipping through.
    assert_eq!(
        cluster.current_leaders(),
        vec![new_leader],
        "removing the leader must force a clean re-election among the remaining 3: exactly one \
         leader cluster-wide, and it must be the newly elected node, not the removed one"
    );

    propose_on_current_leader(&mut cluster, &remaining3, b"after-leader-remove".to_vec());
    cluster.run(30);

    for &id in &remaining3 {
        let cmds = non_empty_commands(cluster.applied_of(id));
        assert!(cmds.contains(&b"before-shrink".to_vec()));
        assert!(cmds.contains(&b"after-first-remove".to_vec()));
        assert!(cmds.contains(&b"after-leader-remove".to_vec()));
    }

    cluster.assert_invariants_after_settle();
}

/// Kill-and-replace (Task 6): a follower crashes, the leader compacts its
/// applied prefix while the crashed node is down, the crashed node is
/// removed from the voter set, and a brand-new node joins in its place. The
/// replacement's log starts empty and everything it would need was just
/// compacted away, so it must catch up via a genuine InstallSnapshot — this
/// is checked directly via `restored_nodes()`, not merely inferred from the
/// outcome, exercising the same path `snapshot_catch_up` proves in
/// isolation, but composed with membership change.
#[test]
fn kill_and_replace() {
    let mut cluster = Cluster::new(&IDS, 10, 3, 10);
    let leader = elect(&mut cluster, &IDS);

    for cmd in [b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()] {
        propose_on_current_leader(&mut cluster, &IDS, cmd);
        cluster.run(20);
    }

    let victim = IDS.into_iter().find(|&id| id != leader).unwrap();
    cluster.crash(victim);

    let current_leader = cluster
        .current_leaders_among(&IDS)
        .first()
        .copied()
        .expect("a leader must still be up with only a non-leader crashed");
    let leader_applied_len = cluster.applied_of(current_leader).len() as LogIndex;
    cluster.compact_leader(current_leader, leader_applied_len);

    cluster.remove_voter(victim);

    let replacement: NodeId = 4;
    cluster.add_voter(replacement);

    assert!(
        cluster.restored_nodes().contains(&replacement),
        "the replacement node must have caught up via a genuine InstallSnapshot — everything it \
         needed was compacted away before it ever joined, so ordinary AppendEntries \
         conflict-backup alone could not have gotten it there"
    );

    let remaining: Vec<NodeId> = IDS
        .into_iter()
        .filter(|&id| id != victim)
        .chain(std::iter::once(replacement))
        .collect();
    propose_on_current_leader(&mut cluster, &remaining, b"after-replace".to_vec());
    cluster.run(30);

    for &id in &remaining {
        assert!(
            non_empty_commands(cluster.applied_of(id)).contains(&b"after-replace".to_vec()),
            "node {id} must replicate the post-replacement command"
        );
    }

    cluster.assert_invariants_after_settle();
}

#[test]
fn determinism() {
    fn run_scenario(seed: u64) -> (NodeId, Vec<Vec<LogEntry>>) {
        let mut cluster = Cluster::new(&IDS, 10, 3, seed);
        let leader = elect(&mut cluster, &IDS);
        for cmd in [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()] {
            propose_on_current_leader(&mut cluster, &IDS, cmd);
            cluster.run(20);
        }
        cluster.run(30);
        cluster.assert_invariants_after_settle();
        let histories = IDS
            .iter()
            .map(|&id| cluster.applied_of(id).to_vec())
            .collect();
        (leader, histories)
    }

    let run1 = run_scenario(42);
    let run2 = run_scenario(42);
    assert_eq!(
        run1, run2,
        "the same seed must produce an identical observable history \
         (same leader, same applied logs) on every run"
    );
}

/// Proves `check_containment` — the function behind
/// `assert_leader_completeness_containment` — is actually discriminating,
/// on a purely synthetic fixture: a canonical committed history of 3
/// entries, and a "leader" applied log that has index 1 but is MISSING
/// index 2 entirely (the log is simply too short to reach it). This is
/// exactly the bug the review flagged: the old pairwise check only compared
/// `0..min(a.len, b.len)`, so a later leader with a SHORTER applied log
/// that dropped a committed entry slipped through undetected. If this test
/// ever went green on `Ok(())`, the containment check would be vacuous.
#[test]
fn containment_check_detects_a_lost_committed_entry() {
    let mut committed = BTreeMap::new();
    committed.insert(1, LogEntry::normal(1, 1, b"a".to_vec()));
    committed.insert(2, LogEntry::normal(1, 2, b"b".to_vec()));
    committed.insert(3, LogEntry::normal(2, 3, b"c".to_vec()));

    // Leader's applied log has index 1 only — committed index 2 (and 3) are
    // simply beyond its length, never rewritten, just LOST.
    let leader_missing_committed_entries = vec![LogEntry::normal(1, 1, b"a".to_vec())];

    let result = check_containment(&committed, &leader_missing_committed_entries);
    assert!(
        result.is_err(),
        "containment check MUST detect a leader whose applied log is missing a committed entry, \
         got Ok(()) instead: {result:?}"
    );

    // A leader log that HAS every committed index, correctly, must pass.
    let leader_with_everything = vec![
        LogEntry::normal(1, 1, b"a".to_vec()),
        LogEntry::normal(1, 2, b"b".to_vec()),
        LogEntry::normal(2, 3, b"c".to_vec()),
    ];
    assert!(
        check_containment(&committed, &leader_with_everything).is_ok(),
        "containment check must NOT flag a leader log that actually contains everything \
         committed"
    );
}

/// The same proof, but against REAL data from a genuine `Cluster` run rather
/// than a synthetic fixture: runs a normal scenario to a settled single
/// leader, confirms the containment check currently passes, then removes
/// the leader's last applied entry from a COPY of its log and confirms the
/// same check now fails. This rules out the possibility that
/// `check_containment` only "works" against hand-built fixtures shaped
/// exactly to trip it.
#[test]
fn containment_check_is_discriminating_on_real_scenario_data() {
    let mut cluster = Cluster::new(&IDS, 10, 3, 100);
    elect(&mut cluster, &IDS);
    for cmd in [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()] {
        propose_on_current_leader(&mut cluster, &IDS, cmd);
        cluster.run(20);
    }
    cluster.settle();

    let leaders = cluster.current_leaders();
    assert_eq!(
        leaders.len(),
        1,
        "scenario setup must settle to exactly one leader"
    );
    let leader = leaders[0];

    check_containment(&cluster.committed_by_index, cluster.applied_of(leader))
        .expect("a healthy, settled cluster's leader must contain everything committed");

    let mut truncated = cluster.applied_of(leader).to_vec();
    truncated
        .pop()
        .expect("leader must have applied at least one entry for this scenario to be meaningful");
    let result = check_containment(&cluster.committed_by_index, &truncated);
    assert!(
        result.is_err(),
        "containment check must detect a leader log with a committed entry artificially \
         removed, got Ok(()) instead: {result:?}"
    );
}
