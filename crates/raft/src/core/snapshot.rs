use super::membership::{decode_voters, encode_voters};
use super::*;
use crate::error::Error;
use crate::rpc::{InstallSnapshotReq, InstallSnapshotResp};

impl<S: RaftStorage> RaftCore<S> {
    /// Snapshots the state machine as of `index`, persisting `data` as the
    /// new snapshot and compacting the log prefix up to and including
    /// `index` (`RaftStorage::save_snapshot` does both atomically).
    ///
    /// `index` must be strictly beyond the current snapshot base and no
    /// further than `last_applied`: compacting an unapplied (or even
    /// uncommitted) entry would let a crash lose state no snapshot ever
    /// captured, since the snapshot is presented as a complete substitute
    /// for everything up to and including it. Moving the base backwards or
    /// leaving it in place is also rejected — `save_snapshot` itself only
    /// guards against strictly-backwards moves, so `compact` enforces the
    /// stricter "always advances" invariant this call site actually needs.
    pub fn compact(&mut self, index: LogIndex, data: Vec<u8>) -> Result<()> {
        let current_base = self.storage.snapshot_meta().last_index;
        if index <= current_base || index > self.last_applied {
            return Err(Error::Corruption(format!(
                "compact index {index} out of range: current snapshot base {current_base}, last_applied {}",
                self.last_applied
            )));
        }
        let last_term = self
            .storage
            .term(index)?
            .ok_or_else(|| Error::Corruption(format!("compact index {index} has no term")))?;
        let meta = SnapshotMeta {
            last_index: index,
            last_term,
        };
        // The configuration is part of the snapshot's state, not just the
        // state machine's: persisting the live voter set alongside `data`
        // is what lets a restart or a follower installing this snapshot
        // recover the correct membership even after the `ConfigChange`
        // entry that produced it is compacted out of the log.
        let config = encode_voters(&self.voters);
        self.storage.save_snapshot(meta, &data, &config)
    }

    /// Sends `peer` the whole current snapshot in one message (chunking
    /// deferred). Called from `send_append_to` (core/replication.rs) once it
    /// determines `peer`'s `next_index` has fallen at or below the snapshot
    /// base, meaning the entries it needs were already compacted away.
    ///
    /// Guards against sending when there's no real snapshot yet (base ==
    /// 0): this also sidesteps the zero-byte-payload/`read_snapshot ==
    /// None` ambiguity a caller could otherwise hit — `read_snapshot`
    /// returning `None` is only ever expected in that no-snapshot case, so
    /// this guard means the `ok_or_else` below firing is always a genuine
    /// storage inconsistency (metadata present, bytes missing), never the
    /// ordinary "nothing to send" case.
    ///
    /// Deliberately does NOT push onto `inflight`/`send_count`: those stay
    /// keyed on AppendEntries only — a peer being snapshotted is by
    /// definition not confirming reads, so it has nothing to contribute to
    /// that accounting.
    pub(super) fn send_install_snapshot(&mut self, peer: NodeId) -> Result<()> {
        if self.storage.snapshot_meta().last_index == 0 {
            return Ok(());
        }
        let (meta, data, config) = self.storage.read_snapshot()?.ok_or_else(|| {
            Error::Corruption("snapshot metadata present but bytes missing".into())
        })?;
        // Sends exactly the config the snapshot recorded (not the live
        // `self.voters`, which may have moved on since) — the follower must
        // adopt the config as-of-this-snapshot, matching the state-machine
        // bytes it's installing alongside it.
        let req = InstallSnapshotReq {
            term: self.current_term(),
            leader_id: self.config.id,
            last_index: meta.last_index,
            last_term: meta.last_term,
            data,
            config,
        };
        self.outbox.push((peer, Message::InstallSnapshot(req)));
        Ok(())
    }

    /// Follower-side InstallSnapshot handler. A stale sender (behind our
    /// term) is rejected outright. Otherwise the sender is adopted as
    /// leader (`become_follower` persists on term bump, steps down, resets
    /// the election timer, and sets `leader_id` unconditionally — so that
    /// happens even on a same-term contact from an already-known leader).
    ///
    /// A snapshot at or behind what we already hold (by snapshot base OR by
    /// `commit_index`) is stale/redundant: it must be acknowledged but never
    /// installed, since installing it would regress state this node has
    /// already moved past — a safety violation the instant that regressed
    /// `commit_index`/`last_applied` gets re-applied against a state
    /// machine that's already ahead of it.
    pub(super) fn handle_install_snapshot(
        &mut self,
        from: NodeId,
        req: InstallSnapshotReq,
    ) -> Result<()> {
        let current_term = self.current_term();
        if req.term < current_term {
            self.outbox.push((
                from,
                Message::InstallSnapshotResp(InstallSnapshotResp { term: current_term }),
            ));
            return Ok(());
        }

        self.become_follower(req.term, Some(req.leader_id))?;
        let current_term = self.current_term();

        if req.last_index <= self.storage.snapshot_meta().last_index
            || req.last_index <= self.commit_index
        {
            self.outbox.push((
                from,
                Message::InstallSnapshotResp(InstallSnapshotResp { term: current_term }),
            ));
            return Ok(());
        }

        let meta = SnapshotMeta {
            last_index: req.last_index,
            last_term: req.last_term,
        };
        self.storage.save_snapshot(meta, &req.data, &req.config)?;
        self.commit_index = req.last_index;
        self.last_applied = req.last_index;
        // Adopt the snapshot's recorded config immediately, then let
        // `recompute_voters` have the final say: any ConfigChange retained
        // in a live tail beyond this snapshot's base (index > last_index)
        // is more current than the snapshot itself and must win.
        self.voters = decode_voters(&req.config)?;
        self.recompute_voters()?;
        // The driver must reload its state machine from these bytes before
        // any further applies proceed — drained into Ready.restore.
        self.restore_buf = Some((meta, req.data));

        self.outbox.push((
            from,
            Message::InstallSnapshotResp(InstallSnapshotResp { term: current_term }),
        ));
        Ok(())
    }

    /// Leader-side InstallSnapshotResp handler. A higher-term reply means
    /// this leader is stale and must step down. Otherwise, on success, the
    /// follower's progress is set to the snapshot base we just sent it, and
    /// replication resumes from there with ordinary AppendEntries for
    /// whatever post-snapshot entries exist.
    pub(super) fn handle_install_snapshot_resp(
        &mut self,
        from: NodeId,
        resp: InstallSnapshotResp,
    ) -> Result<()> {
        if resp.term > self.current_term() {
            return self.become_follower(resp.term, None);
        }
        if self.role != Role::Leader {
            return Ok(());
        }
        let base = self.storage.snapshot_meta().last_index;
        let match_idx = self.match_index.get(&from).copied().unwrap_or(0).max(base);
        self.match_index.insert(from, match_idx);
        self.next_index.insert(from, base + 1);
        self.send_append_to(from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::{AppendEntriesResp, InstallSnapshotReq, RequestVoteResp};
    use crate::storage::MemStorage;
    use crate::types::{HardState, LogEntry};

    fn voters_bytes(ids: &[NodeId]) -> Vec<u8> {
        encode_voters(&ids.iter().copied().collect())
    }

    fn cfg(id: NodeId, peers: &[NodeId]) -> Config {
        Config {
            id,
            peers: peers.to_vec(),
            election_timeout: 10,
            heartbeat_interval: 3,
            seed: 42,
        }
    }

    /// Drives a 3-node cluster (self id 1) to Leader via the same
    /// pre-vote -> vote round trip exercised in election.rs/replication.rs,
    /// draining the outbox as we go so callers see only what happens next.
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

    /// Elects a leader (id 1, peers [1,2,3]) and drives it to a real,
    /// non-trivial committed+applied log: no-op@1, "set x=1"@2, "set
    /// y=2"@3, all term 1. Peer 2's acks alone form a quorum with self, so
    /// commit_index and last_applied both land on 3 — mirrors the pattern
    /// in `restart_recovers_persisted_state_and_loses_no_committed_entry`
    /// (core/mod.rs).
    fn leader_with_committed_entries() -> RaftCore<MemStorage> {
        let mut c = elect_leader(1, &[1, 2, 3]);
        assert_eq!(c.propose(b"set x=1".to_vec()).unwrap(), Some(2));
        assert_eq!(c.propose(b"set y=2".to_vec()).unwrap(), Some(3));
        let _ = c.ready();

        let term = c.current_term();
        let success = || {
            Message::AppendEntriesResp(AppendEntriesResp {
                term,
                success: true,
                conflict_index: None,
            })
        };
        c.step(2, success()).unwrap();
        c.step(2, success()).unwrap();
        c.step(2, success()).unwrap();
        let _ = c.ready();

        assert_eq!(c.commit_index(), 3);
        assert_eq!(c.last_applied, 3);
        c
    }

    #[test]
    fn compact_beyond_last_applied_is_rejected() {
        let mut c = leader_with_committed_entries();
        assert!(c.compact(4, b"snap".to_vec()).is_err());
        // Rejected attempt must not have mutated storage.
        assert_eq!(c.storage.snapshot_meta(), SnapshotMeta::default());
    }

    #[test]
    fn compact_at_or_below_current_snapshot_is_rejected() {
        let mut c = leader_with_committed_entries();
        // No snapshot yet: base is 0, so index 0 is "at" the current base.
        assert!(c.compact(0, b"snap".to_vec()).is_err());
    }

    #[test]
    fn second_compact_at_or_before_prior_snapshot_is_rejected() {
        let mut c = leader_with_committed_entries();
        c.compact(2, b"first".to_vec()).unwrap();
        // Same index again: not strictly beyond the new base.
        assert!(c.compact(2, b"second".to_vec()).is_err());
        // Earlier index: strictly behind the new base.
        assert!(c.compact(1, b"second".to_vec()).is_err());
        // Original snapshot must be undisturbed by the rejected calls.
        assert_eq!(
            c.storage.read_snapshot().unwrap(),
            Some((
                SnapshotMeta {
                    last_index: 2,
                    last_term: 1
                },
                b"first".to_vec(),
                voters_bytes(&[1, 2, 3])
            ))
        );
    }

    #[test]
    fn compact_at_exactly_last_applied_is_allowed() {
        // The right-edge boundary is `<=`, not `<`: compacting through the
        // state machine's exact watermark is the normal case, not an
        // excluded one.
        let mut c = leader_with_committed_entries();
        assert_eq!(c.last_applied, 3);
        c.compact(3, b"state".to_vec()).unwrap();
        assert_eq!(c.storage.snapshot_meta().last_index, 3);
    }

    // --- Task 3: InstallSnapshot send + receive + resp ---

    #[test]
    fn leader_sends_install_snapshot_to_peer_behind_snapshot_base() {
        let mut c = leader_with_committed_entries();
        // Peer 2 acked through index 3 (next_index[2] == 4); peer 3 never
        // acked anything (next_index[3] == 1, its become_leader default).
        c.compact(2, b"state".to_vec()).unwrap();

        c.broadcast_append().unwrap();
        let r = c.ready();

        // Peer 3's next_index (1) is at/below the new snapshot base (2): it
        // must get InstallSnapshot, not AppendEntries.
        let to_3 = r
            .messages
            .iter()
            .find(|(to, _)| *to == 3)
            .map(|(_, m)| m.clone())
            .expect("peer 3 must receive something");
        match to_3 {
            Message::InstallSnapshot(req) => {
                assert_eq!(req.last_index, 2);
                assert_eq!(req.last_term, 1);
                assert_eq!(req.data, b"state".to_vec());
            }
            other => panic!("expected InstallSnapshot to peer 3, got {other:?}"),
        }

        // Peer 2's next_index (4) is beyond the snapshot base: still a
        // normal AppendEntries.
        let to_2 = r
            .messages
            .iter()
            .find(|(to, _)| *to == 2)
            .map(|(_, m)| m.clone())
            .expect("peer 2 must receive something");
        assert!(matches!(to_2, Message::AppendEntries(_)));
    }

    #[test]
    fn follower_installs_fresh_snapshot_and_advances_state() {
        let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), MemStorage::default()).unwrap();
        c.step(
            1,
            Message::InstallSnapshot(InstallSnapshotReq {
                term: 1,
                leader_id: 1,
                last_index: 5,
                last_term: 1,
                data: b"state".to_vec(),
                config: voters_bytes(&[1, 2, 3]),
            }),
        )
        .unwrap();
        let r = c.ready();

        assert_eq!(
            c.storage.snapshot_meta(),
            SnapshotMeta {
                last_index: 5,
                last_term: 1
            }
        );
        assert_eq!(c.commit_index(), 5);
        assert_eq!(c.last_applied, 5);
        assert_eq!(
            r.restore,
            Some((
                SnapshotMeta {
                    last_index: 5,
                    last_term: 1
                },
                b"state".to_vec()
            ))
        );
        assert!(r
            .messages
            .iter()
            .any(|(_, m)| matches!(m, Message::InstallSnapshotResp(resp) if resp.term == 1)));
        assert_eq!(c.leader_id(), Some(1));
    }

    #[test]
    fn stale_install_snapshot_is_a_no_op_ack_and_does_not_regress_state() {
        let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), MemStorage::default()).unwrap();
        c.step(
            1,
            Message::InstallSnapshot(InstallSnapshotReq {
                term: 1,
                leader_id: 1,
                last_index: 5,
                last_term: 1,
                data: b"state".to_vec(),
                config: voters_bytes(&[1, 2, 3]),
            }),
        )
        .unwrap();
        let _ = c.ready();
        assert_eq!(c.commit_index(), 5);

        // A second install at or below what's already installed must not
        // regress commit_index/last_applied/snapshot, even with different
        // (bogus) bytes.
        c.step(
            1,
            Message::InstallSnapshot(InstallSnapshotReq {
                term: 1,
                leader_id: 1,
                last_index: 3,
                last_term: 1,
                data: b"stale-bogus".to_vec(),
                config: b"also-bogus".to_vec(),
            }),
        )
        .unwrap();
        let r2 = c.ready();

        assert_eq!(c.storage.snapshot_meta().last_index, 5);
        assert_eq!(c.commit_index(), 5);
        assert_eq!(c.last_applied, 5);
        assert!(r2.restore.is_none());
        assert!(r2
            .messages
            .iter()
            .any(|(_, m)| matches!(m, Message::InstallSnapshotResp(_))));
    }

    #[test]
    fn follower_rejects_install_snapshot_from_stale_term() {
        let mut s = MemStorage::default();
        s.save_hard_state(&HardState {
            current_term: 5,
            voted_for: None,
        })
        .unwrap();
        let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), s).unwrap();
        c.step(
            1,
            Message::InstallSnapshot(InstallSnapshotReq {
                term: 3,
                leader_id: 1,
                last_index: 9,
                last_term: 3,
                data: b"nope".to_vec(),
                config: b"nope-too".to_vec(),
            }),
        )
        .unwrap();
        let r = c.ready();
        assert!(r.messages.iter().any(|(_, m)| matches!(
            m, Message::InstallSnapshotResp(resp) if resp.term == 5
        )));
        assert_eq!(c.storage.snapshot_meta().last_index, 0);
        assert_eq!(c.commit_index(), 0);
        assert_eq!(c.leader_id(), None); // a stale sender must not be adopted
    }

    #[test]
    fn install_snapshot_resp_advances_progress_and_resumes_append_entries() {
        let mut c = leader_with_committed_entries();
        c.compact(2, b"state".to_vec()).unwrap();
        c.broadcast_append().unwrap(); // sends InstallSnapshot to peer 3
        let _ = c.ready();
        let term = c.current_term();

        c.step(
            3,
            Message::InstallSnapshotResp(crate::rpc::InstallSnapshotResp { term }),
        )
        .unwrap();

        // match_index[3] takes the snapshot base (2); next_index[3] == 3,
        // so the AppendEntries that follows carries prev_log_index == 2.
        assert_eq!(c.match_index_of(3), 2);
        let r = c.ready();
        assert!(r.messages.iter().any(|(to, m)| *to == 3
            && matches!(m, Message::AppendEntries(req) if req.prev_log_index == 2)));
    }

    #[test]
    fn install_snapshot_resp_with_higher_term_steps_leader_down() {
        let mut c = leader_with_committed_entries();
        let higher = c.current_term() + 5;

        c.step(
            3,
            Message::InstallSnapshotResp(crate::rpc::InstallSnapshotResp { term: higher }),
        )
        .unwrap();

        assert_eq!(c.role(), Role::Follower);
        assert_eq!(c.current_term(), higher);
    }

    // --- Critical: InstallSnapshot must convey the config, not just data ---

    /// A follower whose `next_index` has fallen behind a leader's compacted
    /// config-change gets InstallSnapshot instead of AppendEntries. After
    /// installing it, the follower's membership must match the leader's
    /// config as of the snapshot — not the follower's own bootstrap peers.
    /// Before the fix, `InstallSnapshotReq` carried no config at all, so a
    /// freshly-installed follower had no way to learn the membership change
    /// that got compacted away on the leader, and `recompute_voters` fell
    /// back to its own stale bootstrap list.
    #[test]
    fn install_snapshot_conveys_config() {
        use crate::core::membership::ConfChange;

        // Bootstrap 3-node cluster: elect_leader's single-vote-grant helper
        // assumes a quorum of 2, so membership grows via ConfChange rather
        // than starting the election itself at 4+ voters.
        let mut leader = elect_leader(1, &[1, 2, 3]);
        let term = leader.current_term();
        let ack = |t: Term| {
            Message::AppendEntriesResp(AppendEntriesResp {
                term: t,
                success: true,
                conflict_index: None,
            })
        };
        leader.step(2, ack(term)).unwrap();
        leader.step(3, ack(term)).unwrap();
        let _ = leader.ready();
        assert_eq!(leader.commit_index(), 1);

        // Grow membership {1,2,3} -> {1,2,3,4}; quorum becomes 3.
        assert!(leader
            .propose_conf_change(ConfChange::AddVoter(4))
            .unwrap()
            .is_some());
        let _ = leader.ready();
        leader.step(2, ack(term)).unwrap();
        leader.step(3, ack(term)).unwrap();
        let _ = leader.ready();
        assert_eq!(leader.commit_index(), 2);
        assert_eq!(leader.voters(), vec![1, 2, 3, 4]);

        // Grow again {1,2,3,4} -> {1,2,3,4,5}; quorum stays 3 (self + 2 + 3
        // — peer 4 never contributes an ack, so it never catches up).
        assert!(leader
            .propose_conf_change(ConfChange::AddVoter(5))
            .unwrap()
            .is_some());
        let _ = leader.ready();
        leader.step(2, ack(term)).unwrap();
        leader.step(3, ack(term)).unwrap();
        let _ = leader.ready();
        assert_eq!(leader.commit_index(), 3);
        assert_eq!(leader.voters(), vec![1, 2, 3, 4, 5]);

        leader.compact(3, b"state".to_vec()).unwrap();

        // Peer 4 never acked anything: next_index[4] (seeded to 3 when it
        // was added) is at/below the new snapshot base (3) -> it must
        // receive InstallSnapshot, not AppendEntries.
        leader.broadcast_append().unwrap();
        let r = leader.ready();
        let install = r
            .messages
            .iter()
            .find(|(to, _)| *to == 4)
            .map(|(_, m)| m.clone())
            .expect("peer 4 must receive something");
        let req = match install {
            Message::InstallSnapshot(req) => req,
            other => panic!("expected InstallSnapshot to peer 4, got {other:?}"),
        };

        // Peer 4 restarts fresh with its ORIGINAL bootstrap config (no 5) —
        // if the fix works, installing the snapshot must adopt the leader's
        // recorded config, not fall back to this bootstrap list.
        let mut follower = RaftCore::new(cfg(4, &[1, 2, 3, 4]), MemStorage::default()).unwrap();
        follower.step(1, Message::InstallSnapshot(req)).unwrap();
        let _ = follower.ready();

        assert_eq!(follower.voters(), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn valid_compact_drops_prefix_and_preserves_committed_state() {
        let mut c = leader_with_committed_entries();
        let pre_last_index = c.storage.last_index();
        let pre_last_term = c.storage.last_term();
        let pre_commit = c.commit_index();

        c.compact(2, b"state".to_vec()).unwrap();

        assert_eq!(
            c.storage.snapshot_meta(),
            SnapshotMeta {
                last_index: 2,
                last_term: 1
            }
        );
        assert_eq!(
            c.storage.read_snapshot().unwrap(),
            Some((
                SnapshotMeta {
                    last_index: 2,
                    last_term: 1
                },
                b"state".to_vec(),
                voters_bytes(&[1, 2, 3])
            ))
        );
        // Entries <= 2 are gone; entry 3 survives.
        assert_eq!(
            c.storage.entries_from(1),
            vec![LogEntry::normal(1, 3, b"set y=2".to_vec())]
        );
        // Compaction never touches these.
        assert_eq!(c.storage.last_index(), pre_last_index);
        assert_eq!(c.storage.last_term(), pre_last_term);
        assert_eq!(c.commit_index(), pre_commit);
        assert_eq!(c.last_applied, pre_commit);
        // The compacted boundary itself still resolves a term.
        assert_eq!(c.storage.term(2).unwrap(), Some(1));
    }
}
