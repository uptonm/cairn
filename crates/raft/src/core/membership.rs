use super::*;
use crate::error::Error;
use crate::types::EntryType;

/// A single-server membership change: exactly one voter added or removed at
/// a time. This is Raft §6's simpler alternative to joint consensus — safe
/// because a config that differs from its predecessor by at most one member
/// always has an overlapping majority with it, so there's never a moment
/// where the old and new configurations could elect two independent
/// leaders.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ConfChange {
    AddVoter(NodeId),
    RemoveVoter(NodeId),
}

/// Encodes a voter set as a length-prefixed list of little-endian `u64`
/// node ids: a 4-byte count followed by `count` 8-byte ids, emitted in
/// `BTreeSet`'s ascending iteration order so the same set always encodes to
/// the same bytes.
pub(crate) fn encode_voters(voters: &BTreeSet<NodeId>) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + voters.len() * 8);
    buf.extend_from_slice(&(voters.len() as u32).to_le_bytes());
    for &id in voters {
        buf.extend_from_slice(&id.to_le_bytes());
    }
    buf
}

/// Inverse of `encode_voters`. Malformed bytes (too short for the length
/// prefix, or a length prefix that doesn't match the remaining byte count)
/// are reported as `Error::Corruption` rather than panicking — a corrupted
/// or truncated `ConfigChange` entry must never crash the process.
pub(crate) fn decode_voters(bytes: &[u8]) -> Result<BTreeSet<NodeId>> {
    if bytes.len() < 4 {
        return Err(Error::Corruption(
            "voters encoding shorter than the 4-byte length prefix".into(),
        ));
    }
    let mut len_bytes = [0u8; 4];
    len_bytes.copy_from_slice(&bytes[0..4]);
    let count = u32::from_le_bytes(len_bytes) as usize;
    let expected = 4 + count * 8;
    if bytes.len() != expected {
        return Err(Error::Corruption(format!(
            "voters encoding length mismatch: header claims {count} ids ({expected} bytes total), got {} bytes",
            bytes.len()
        )));
    }
    Ok(bytes[4..]
        .chunks_exact(8)
        .map(|chunk| {
            let mut id_bytes = [0u8; 8];
            id_bytes.copy_from_slice(chunk);
            NodeId::from_le_bytes(id_bytes)
        })
        .collect())
}

impl<S: RaftStorage> RaftCore<S> {
    /// The node's live voter set, ascending. This is the membership every
    /// quorum and peer-iteration decision is made against (see
    /// `recompute_voters`), so it's the ground truth for observing a
    /// single-server change taking effect — appended (effect-on-append),
    /// committed, reverted on truncation, or recovered from a snapshot.
    /// Exposed for the same reason as `into_storage`: the deterministic sim
    /// harness needs to assert membership converged without reaching into
    /// crate internals.
    pub fn voters(&self) -> Vec<NodeId> {
        self.voters.iter().copied().collect()
    }

    /// Recomputes the live voter set with this precedence:
    /// 1. the membership encoded by the highest-index `ConfigChange` entry
    ///    currently present in the live log tail (whether committed or not
    ///    — effect-on-append);
    /// 2. else the config persisted alongside the current snapshot, if one
    ///    exists (see `RaftStorage::save_snapshot` / `compact`) — this is
    ///    what lets a config that's been compacted out of the log (its
    ///    `ConfigChange` entry gone) still be recovered correctly, rather
    ///    than silently reverting to bootstrap. A present snapshot whose
    ///    recorded config is empty or undecodable is a corrupt store, not a
    ///    reason to adopt bootstrap peers — it's reported as `Corruption`;
    /// 3. else the bootstrap `config.peers`, for a node that has never seen
    ///    a config entry OR a snapshot at all.
    ///
    /// Called from `RaftCore::new` (so a config entry already durable
    /// before a restart is honored — from the log tail or, if that's been
    /// compacted away, from the snapshot — rather than silently reverted to
    /// the bootstrap peers) and after any log mutation that can add or
    /// remove the latest config entry: an append (`propose_conf_change`,
    /// and a follower adopting a leader's config entry via
    /// `handle_append_entries`) or a truncation (`handle_append_entries`'s
    /// conflict path, which can revert a config entry that used to be the
    /// latest — effect-on-append implies revert-on-truncation).
    pub(super) fn recompute_voters(&mut self) -> Result<()> {
        let snapshot_base = self.storage.snapshot_meta().last_index;
        let latest_config_change = self
            .storage
            .entries_from(snapshot_base + 1)
            .into_iter()
            .rfind(|e| e.entry_type == EntryType::ConfigChange);
        self.voters = match latest_config_change {
            Some(entry) => decode_voters(&entry.command)?,
            None => match self.storage.read_snapshot()? {
                Some((_, _, config)) => {
                    if config.is_empty() {
                        return Err(Error::Corruption(
                            "snapshot present but its recorded config is empty".into(),
                        ));
                    }
                    decode_voters(&config)?
                }
                None => self.config.peers.iter().copied().collect(),
            },
        };
        Ok(())
    }

    /// Proposes a single-server membership change. Leader-only (`Ok(None)`
    /// otherwise, mirroring `propose`). Refuses — `Ok(None)`, not an error,
    /// since the caller is expected to retry once the reason clears —
    /// rather than append when:
    /// - a `ConfigChange` is already in flight, i.e. present at an index
    ///   past `commit_index` (the one-in-flight rule: two overlapping
    ///   uncommitted membership changes could produce a moment where no
    ///   majority of the old config overlaps a majority of the new one);
    /// - the requested change is a no-op against the CURRENT live voter set
    ///   (adding an already-present voter, or removing a non-voter).
    ///
    /// On success: appends the new full voter set as a `ConfigChange`
    /// entry, makes it live immediately via `recompute_voters`
    /// (effect-on-append — this node's own future vote/quorum math uses the
    /// new config right away, even before it commits), seeds replication
    /// bookkeeping for a newly added node, and kicks off replication.
    pub fn propose_conf_change(&mut self, change: ConfChange) -> Result<Option<LogIndex>> {
        if self.role != Role::Leader {
            return Ok(None);
        }

        let has_in_flight_conf_change = self
            .storage
            .entries_from(self.commit_index + 1)
            .iter()
            .any(|e| e.entry_type == EntryType::ConfigChange);
        if has_in_flight_conf_change {
            return Ok(None);
        }

        let mut new_voters = self.voters.clone();
        let changes_membership = match change {
            ConfChange::AddVoter(id) => new_voters.insert(id),
            ConfChange::RemoveVoter(id) => new_voters.remove(&id),
        };
        if !changes_membership {
            return Ok(None);
        }

        let index = self.storage.last_index() + 1;
        let entry = LogEntry::config_change(self.current_term(), index, encode_voters(&new_voters));
        self.storage.append(std::slice::from_ref(&entry))?;
        // Mirrors `propose`: self already durably holds this entry the
        // instant it's appended, so its own match/next_index must reflect
        // that immediately — otherwise self's contribution to the (now
        // possibly larger) quorum this same entry needs would be
        // understated, which could stall its own commit.
        let self_id = self.config.id;
        self.match_index.insert(self_id, index);
        self.next_index.insert(self_id, index + 1);
        self.recompute_voters()?;

        if let ConfChange::AddVoter(id) = change {
            let next = self.storage.last_index() + 1;
            self.next_index.insert(id, next);
            self.match_index.insert(id, 0);
            self.send_count.entry(id).or_insert(0);
            self.ack_count.entry(id).or_insert(0);
        }

        self.broadcast_append()?;
        Ok(Some(index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::{AppendEntriesReq, AppendEntriesResp, RequestVoteResp};
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

    fn success_resp(term: Term) -> Message {
        Message::AppendEntriesResp(AppendEntriesResp {
            term,
            success: true,
            conflict_index: None,
        })
    }

    /// Drives a 3-node cluster (self id 1) all the way to Leader via the
    /// same pre-vote -> vote round trip exercised elsewhere in core/.
    fn elect_leader(id: NodeId, peers: &[NodeId]) -> RaftCore<MemStorage> {
        let mut c = RaftCore::new(cfg(id, peers), MemStorage::default()).unwrap();
        for _ in 0..40 {
            c.tick().unwrap();
        }
        let _ = c.ready();
        let others: Vec<NodeId> = peers.iter().copied().filter(|&p| p != id).collect();
        c.step(
            others[0],
            Message::RequestVoteResp(RequestVoteResp {
                term: 0,
                vote_granted: true,
                pre_vote: true,
            }),
        )
        .unwrap();
        let _ = c.ready();
        c.step(
            others[0],
            Message::RequestVoteResp(RequestVoteResp {
                term: 1,
                vote_granted: true,
                pre_vote: false,
            }),
        )
        .unwrap();
        assert_eq!(c.role(), Role::Leader);
        let _ = c.ready(); // drain the no-op broadcast from become_leader
        c
    }

    /// `elect_leader` plus a full ack round on the leader's own no-op
    /// broadcast: both peers acknowledge it, committing index 1 and —
    /// importantly for tests that go on to call `propose_conf_change` —
    /// draining each peer's `inflight` queue. Without this, a peer would
    /// still have that original heartbeat's extent queued ahead of
    /// whatever `propose_conf_change` sends next, and the FIFO `inflight`
    /// contract (see replication.rs) means a single subsequent ack would
    /// only pop THAT stale extent, not the one covering the new entry.
    fn elect_leader_with_committed_noop(id: NodeId, peers: &[NodeId]) -> RaftCore<MemStorage> {
        let mut c = elect_leader(id, peers);
        let term = c.current_term();
        let others: Vec<NodeId> = peers.iter().copied().filter(|&p| p != id).collect();
        for peer in others {
            c.step(peer, success_resp(term)).unwrap();
        }
        let _ = c.ready();
        assert_eq!(c.commit_index(), 1);
        c
    }

    // --- encode/decode round trip ---

    #[test]
    fn encode_decode_voters_round_trips() {
        let voters: BTreeSet<NodeId> = [1, 2, 3, 9999].into_iter().collect();
        let bytes = encode_voters(&voters);
        assert_eq!(decode_voters(&bytes).unwrap(), voters);
    }

    #[test]
    fn encode_decode_empty_voters_round_trips() {
        let voters: BTreeSet<NodeId> = BTreeSet::new();
        let bytes = encode_voters(&voters);
        assert_eq!(decode_voters(&bytes).unwrap(), voters);
    }

    #[test]
    fn decode_malformed_voters_is_corruption_not_panic() {
        assert!(decode_voters(&[]).is_err());
        assert!(decode_voters(&[1, 2, 3]).is_err()); // shorter than length prefix
                                                     // Header claims 2 ids (16 bytes) but only 8 bytes follow.
        let mut bad = (2u32).to_le_bytes().to_vec();
        bad.extend_from_slice(&1u64.to_le_bytes());
        assert!(decode_voters(&bad).is_err());
    }

    // --- propose_conf_change: effect-on-append + quorum + one-in-flight ---

    #[test]
    fn propose_conf_change_when_not_leader_returns_none() {
        let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), MemStorage::default()).unwrap();
        assert_eq!(
            c.propose_conf_change(ConfChange::AddVoter(4)).unwrap(),
            None
        );
    }

    #[test]
    fn add_voter_appends_config_change_and_voters_immediately_include_it() {
        let mut c = elect_leader(1, &[1, 2, 3]);
        let idx = c.propose_conf_change(ConfChange::AddVoter(4)).unwrap();
        assert_eq!(idx, Some(2)); // index 1 was the leader's no-op

        // Effect-on-append: voters() reflects the new node immediately,
        // before this entry has committed.
        assert_eq!(c.voters(), vec![1, 2, 3, 4]);

        let entries = c.storage.entries_from(2);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry_type, EntryType::ConfigChange);
        assert_eq!(
            decode_voters(&entries[0].command).unwrap(),
            c.voters().into_iter().collect()
        );
    }

    #[test]
    fn add_voter_grows_quorum_size() {
        let mut c = elect_leader(1, &[1, 2, 3]);
        assert_eq!(c.quorum(), 2); // 3 voters -> majority of 2

        c.propose_conf_change(ConfChange::AddVoter(4)).unwrap();
        assert_eq!(c.quorum(), 3); // 4 voters -> majority of 3
    }

    #[test]
    fn add_voter_seeds_replication_state_for_the_new_node() {
        let mut c = elect_leader(1, &[1, 2, 3]);
        let last_index_before = c.storage.last_index();
        c.propose_conf_change(ConfChange::AddVoter(4)).unwrap();

        assert_eq!(c.match_index_of(4), 0);
        // next_index seeded past the just-appended config entry itself;
        // any mismatch self-corrects via the standard conflict-backup path.
        assert_eq!(c.next_index.get(&4).copied(), Some(last_index_before + 2));
    }

    #[test]
    fn remove_voter_of_non_member_is_a_no_op_refusal() {
        let mut c = elect_leader(1, &[1, 2, 3]);
        assert_eq!(
            c.propose_conf_change(ConfChange::RemoveVoter(99)).unwrap(),
            None
        );
        assert_eq!(c.voters(), vec![1, 2, 3]);
    }

    #[test]
    fn add_voter_of_existing_member_is_a_no_op_refusal() {
        let mut c = elect_leader(1, &[1, 2, 3]);
        assert_eq!(
            c.propose_conf_change(ConfChange::AddVoter(2)).unwrap(),
            None
        );
    }

    #[test]
    fn second_propose_before_first_commits_is_refused() {
        let mut c = elect_leader(1, &[1, 2, 3]);
        assert!(c
            .propose_conf_change(ConfChange::AddVoter(4))
            .unwrap()
            .is_some());
        // The first change is still uncommitted (index > commit_index) —
        // a second change must be refused, not queued or applied.
        assert_eq!(
            c.propose_conf_change(ConfChange::AddVoter(5)).unwrap(),
            None
        );
        assert_eq!(c.voters(), vec![1, 2, 3, 4]);
    }

    // --- Critical: configuration must survive compaction + restart ---

    /// A `ConfigChange` that grows the voter set, once committed, must
    /// remain the live membership even after it's compacted away into a
    /// snapshot and the process restarts. Before the fix, `recompute_voters`
    /// only ever looked at the LIVE LOG for a `ConfigChange` entry, so once
    /// `compact()` swept the entry into a snapshot, a restart found no
    /// config entry in the (now-empty) post-snapshot log and silently fell
    /// back to the bootstrap `config.peers` — reverting membership to a
    /// stale, smaller set and corrupting quorum math (split-brain risk).
    #[test]
    fn config_survives_compaction_and_restart() {
        let mut c = elect_leader_with_committed_noop(1, &[1, 2, 3]);
        let term = c.current_term();

        assert!(c
            .propose_conf_change(ConfChange::AddVoter(4))
            .unwrap()
            .is_some());
        let _ = c.ready();
        c.step(2, success_resp(term)).unwrap();
        c.step(3, success_resp(term)).unwrap();
        assert_eq!(c.commit_index(), 2);
        assert_eq!(c.last_applied, 2);
        assert_eq!(c.voters(), vec![1, 2, 3, 4]);

        // Compact past the ConfigChange entry (index 2): the live log no
        // longer holds any ConfigChange entry after this.
        c.compact(2, b"state".to_vec()).unwrap();
        assert_eq!(c.storage.entries_from(1).len(), 0);

        // Reclaim storage (models a crash) and construct a fresh core over
        // it with the ORIGINAL bootstrap config (peers [1,2,3]) — if the fix
        // works, the restarted node's membership must come from the
        // snapshot, not this bootstrap list.
        let storage = c.into_storage();
        let restarted = RaftCore::new(cfg(1, &[1, 2, 3]), storage).unwrap();

        assert_eq!(restarted.voters(), vec![1, 2, 3, 4]);
        assert_eq!(restarted.quorum(), 3); // majority of 4, not of the bootstrap 3
    }

    /// Same regression as `config_survives_compaction_and_restart`, but the
    /// snapshot is taken with an EMPTY state-machine payload. A snapshot's
    /// config must survive even when its `data` is empty: before the fix,
    /// `MemStorage` used `data.is_empty()` as the has-snapshot predicate, so
    /// an empty-payload snapshot read back as `None`, the recorded config was
    /// discarded, and membership silently reverted to the bootstrap peers —
    /// the wrong (smaller) quorum, a split-brain risk.
    #[test]
    fn config_survives_compaction_with_empty_snapshot_payload() {
        let mut c = elect_leader_with_committed_noop(1, &[1, 2, 3]);
        let term = c.current_term();

        assert!(c
            .propose_conf_change(ConfChange::AddVoter(4))
            .unwrap()
            .is_some());
        let _ = c.ready();
        c.step(2, success_resp(term)).unwrap();
        c.step(3, success_resp(term)).unwrap();
        assert_eq!(c.commit_index(), 2);
        assert_eq!(c.last_applied, 2);
        assert_eq!(c.voters(), vec![1, 2, 3, 4]);

        // Compact past the ConfigChange entry with an EMPTY payload.
        c.compact(2, vec![]).unwrap();
        assert_eq!(c.storage.entries_from(1).len(), 0);

        let storage = c.into_storage();
        let restarted = RaftCore::new(cfg(1, &[1, 2, 3]), storage).unwrap();

        assert_eq!(restarted.voters(), vec![1, 2, 3, 4]);
        assert_eq!(restarted.quorum(), 3); // majority of 4, not of the bootstrap 3
    }

    #[test]
    fn propose_conf_change_allowed_again_once_prior_change_commits() {
        let mut c = elect_leader_with_committed_noop(1, &[1, 2, 3]);
        let term = c.current_term();
        c.propose_conf_change(ConfChange::AddVoter(4)).unwrap();
        let _ = c.ready();

        // Effect-on-append already raised the live quorum to 3-of-{1,2,3,4}
        // (node 4 hasn't replicated anything yet), so committing this very
        // entry needs both original peers' acks, not just one — self alone
        // plus a single peer is no longer enough once the config committed
        // itself grows the cluster.
        c.step(2, success_resp(term)).unwrap();
        c.step(3, success_resp(term)).unwrap();
        assert_eq!(c.commit_index(), 2);

        assert!(c
            .propose_conf_change(ConfChange::AddVoter(5))
            .unwrap()
            .is_some());
        assert_eq!(c.voters(), vec![1, 2, 3, 4, 5]);
    }

    // --- leader step-down on committing its own removal ---

    #[test]
    fn committing_remove_voter_self_steps_the_leader_down() {
        let mut c = elect_leader_with_committed_noop(1, &[1, 2, 3]);
        let term = c.current_term();

        c.propose_conf_change(ConfChange::RemoveVoter(1)).unwrap();
        let _ = c.ready();
        assert_eq!(c.role(), Role::Leader, "must not step down on append alone");
        // Effect-on-append: self is already excluded from the live set...
        assert_eq!(c.voters(), vec![2, 3]);

        // ...but the new quorum is now over {2, 3} (self excluded from its
        // own tally per "self counts iff self ∈ voters"), so committing it
        // needs both remaining voters' acks, not just one.
        c.step(2, success_resp(term)).unwrap();
        assert_eq!(
            c.role(),
            Role::Leader,
            "one ack is not yet a quorum of {{2,3}}"
        );
        c.step(3, success_resp(term)).unwrap();

        assert_eq!(c.commit_index(), 2); // the RemoveVoter entry itself
        assert_eq!(
            c.role(),
            Role::Follower,
            "must step down once its own removal commits"
        );
        assert_eq!(c.leader_id(), None);
    }

    #[test]
    fn committing_a_conf_change_that_keeps_self_does_not_step_down() {
        let mut c = elect_leader_with_committed_noop(1, &[1, 2, 3]);
        let term = c.current_term();
        c.propose_conf_change(ConfChange::AddVoter(4)).unwrap();
        let _ = c.ready();

        // Quorum of the new 4-voter config is 3; self + both original peers
        // reaches it (the newly added node 4 hasn't caught up yet).
        c.step(2, success_resp(term)).unwrap();
        c.step(3, success_resp(term)).unwrap();
        assert_eq!(c.commit_index(), 2);
        assert_eq!(c.role(), Role::Leader);
    }

    // --- truncation reverts a config entry ---

    #[test]
    fn truncation_that_removes_a_config_entry_reverts_voters() {
        let mut s = MemStorage::default();
        let voters: BTreeSet<NodeId> = [1, 2, 3, 4].into_iter().collect();
        s.append(&[
            LogEntry::normal(1, 1, vec![]),
            LogEntry::config_change(1, 2, encode_voters(&voters)),
        ])
        .unwrap();
        let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), s).unwrap();
        assert_eq!(c.voters(), vec![1, 2, 3, 4]);

        // A new leader at term 2 overwrites index 2 with a plain entry,
        // truncating away the ConfigChange that used to be the latest.
        c.step(
            1,
            Message::AppendEntries(AppendEntriesReq {
                term: 2,
                leader_id: 1,
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![LogEntry::normal(2, 2, vec![7])],
                leader_commit: 0,
            }),
        )
        .unwrap();
        let r = c.ready();
        assert!(r
            .messages
            .iter()
            .any(|(_, m)| matches!(m, Message::AppendEntriesResp(a) if a.success)));

        // The ConfigChange entry is gone; voters revert to the bootstrap
        // config.peers (no config entry left in the log).
        assert_eq!(c.voters(), vec![1, 2, 3]);
    }

    // --- follower adopts a leader's ConfigChange via AppendEntries ---

    #[test]
    fn follower_adopts_conf_change_via_append_entries() {
        let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), MemStorage::default()).unwrap();
        assert_eq!(c.voters(), vec![1, 2, 3]);

        let voters: BTreeSet<NodeId> = [1, 2, 3, 4].into_iter().collect();
        c.step(
            1,
            Message::AppendEntries(AppendEntriesReq {
                term: 1,
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![LogEntry::config_change(1, 1, encode_voters(&voters))],
                leader_commit: 0,
            }),
        )
        .unwrap();
        let r = c.ready();
        assert!(r
            .messages
            .iter()
            .any(|(_, m)| matches!(m, Message::AppendEntriesResp(a) if a.success)));

        assert_eq!(c.voters(), vec![1, 2, 3, 4]);
        assert_eq!(c.quorum(), 3);
    }
}
