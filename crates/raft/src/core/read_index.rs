use super::*;

impl<S: RaftStorage> RaftCore<S> {
    /// Registers a linearizable read barrier for `token`.
    ///
    /// Only a leader can ever satisfy the guarantee this provides. If this
    /// node isn't the leader right now, the caller (the external driver) is
    /// expected to redirect the read to whichever node it believes is
    /// leader, so the token is dropped here rather than queued — the core
    /// must never release a read it can't back up.
    pub fn read_index(&mut self, token: ReadToken) {
        if self.role != Role::Leader {
            return;
        }
        self.pending_reads.push(PendingRead {
            token,
            floor: self.commit_index,
            registered_tick: self.tick_count,
        });
        self.maybe_release_reads();
    }

    /// Releases every pending read whose linearizability is now confirmed,
    /// pushing its token to `reads_buf` for the caller to drain via
    /// `ready()`.
    ///
    /// A read is confirmed once all of:
    /// (a) this node is still the leader;
    /// (b) `readable_term == Some(current_term)` — a current-term entry has
    ///     applied, closing the new-leader gap so `commit_index` is
    ///     meaningful for this term;
    /// (c) a quorum of nodes affirmed contact with this leader strictly
    ///     AFTER the read registered (self always counts — it's always in
    ///     touch with itself). The frozen `AppendEntriesResp` carries no
    ///     round/read-id to tag a heartbeat with, so per-peer
    ///     `last_contact_tick` strictly newer than `registered_tick` stands
    ///     in for it: election safety guarantees one leader per term, so a
    ///     quorum affirming this leader's authority after the read began
    ///     means no other leader could have served this term in the
    ///     interval — a higher-term leader would have stepped this one down
    ///     instead;
    /// (d) `last_applied >= floor`, the commit_index captured when the read
    ///     registered.
    ///
    /// If this node is no longer leader when this runs, every pending read
    /// is dropped outright (cleared, not released) — a deposed leader has
    /// no business vouching for reads it can no longer guarantee.
    pub(super) fn maybe_release_reads(&mut self) {
        if self.role != Role::Leader {
            self.pending_reads.clear();
            return;
        }
        let current_term = self.current_term();
        if self.readable_term != Some(current_term) {
            return;
        }
        let quorum = self.quorum();
        let self_id = self.config.id;
        let last_applied = self.last_applied;
        let last_contact_tick = &self.last_contact_tick;
        let peers = &self.config.peers;

        let mut released = Vec::new();
        self.pending_reads.retain(|read| {
            let contacted = peers
                .iter()
                .filter(|&&peer| {
                    peer == self_id
                        || last_contact_tick
                            .get(&peer)
                            .is_some_and(|&t| t > read.registered_tick)
                })
                .count();
            let confirmed = contacted >= quorum && last_applied >= read.floor;
            if confirmed {
                released.push(read.token);
            }
            !confirmed
        });
        self.reads_buf.extend(released);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::{AppendEntriesReq, AppendEntriesResp, RequestVoteResp};
    use crate::storage::MemStorage;
    use crate::types::HardState;

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

    /// Drives a 3-node cluster (self id 1) all the way to Leader, draining
    /// the outbox as we go. Mirrors `replication::tests::elect_leader`
    /// (duplicated here rather than shared, matching this codebase's
    /// per-module test-helper convention).
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
                term: 1,
                vote_granted: true,
            }),
        )
        .unwrap();
        let _ = c.ready();
        c.step(
            others[0],
            Message::RequestVoteResp(RequestVoteResp {
                term: 1,
                vote_granted: true,
            }),
        )
        .unwrap();
        assert_eq!(c.role(), Role::Leader);
        let _ = c.ready(); // drain the no-op broadcast from become_leader
        c
    }

    #[test]
    fn read_on_follower_is_never_released() {
        let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), MemStorage::default()).unwrap();
        assert_eq!(c.role(), Role::Follower);
        c.read_index(7);
        assert!(c.pending_reads.is_empty(), "follower must not queue reads");
        for _ in 0..60 {
            c.tick().unwrap();
            let r = c.ready();
            assert!(!r.reads.contains(&7));
        }
    }

    /// Hand-built leader isolating the (b) readable_term gate from the (c)
    /// quorum-contact gate: quorum contact is pre-satisfied via directly
    /// set `last_contact_tick`, but `readable_term` is still `None` (a
    /// freshly elected leader whose no-op hasn't applied yet), so the read
    /// must stay pending until a current-term entry actually applies.
    #[test]
    fn read_waits_for_current_term_commit() {
        let mut c = RaftCore::new(cfg(1, &[1, 2, 3]), MemStorage::default()).unwrap();
        c.storage
            .save_hard_state(&HardState {
                current_term: 1,
                voted_for: Some(1),
            })
            .unwrap();
        c.role = Role::Leader;
        c.leader_id = Some(1);
        c.tick_count = 10;
        assert_eq!(c.readable_term, None);

        c.read_index(7);
        assert_eq!(c.pending_reads.len(), 1);

        // Quorum contact already satisfied strictly after registration...
        c.last_contact_tick.insert(2, 11);
        c.tick().unwrap();
        assert!(
            c.ready().reads.is_empty(),
            "must not release before a current-term entry has applied"
        );
        assert_eq!(c.pending_reads.len(), 1, "read must remain pending");

        // ...now the leader's current-term entry applies (closing the
        // new-leader gap) and the apply loop catches up to the read's
        // floor: the read must release.
        c.readable_term = Some(1);
        c.last_applied = 0; // floor was captured as commit_index == 0
        c.tick().unwrap();
        let r = c.ready();
        assert_eq!(r.reads, vec![7]);
    }

    /// Isolates the (c) quorum-contact-after-registration gate: the leader
    /// is already fully "readable" (current-term entry applied), but no
    /// peer has contacted it since the read registered. Contact is
    /// established via a real, delivered `AppendEntriesResp`.
    #[test]
    fn read_waits_for_quorum_contact_after_registration() {
        let mut c = elect_leader(1, &[1, 2, 3]);
        let term = c.current_term();

        // Commit + apply the no-op so readable_term is set, without
        // leaving any registered read pending yet.
        c.step(2, success_resp(term)).unwrap();
        let _ = c.ready();
        assert_eq!(c.readable_term, Some(term));
        assert_eq!(c.commit_index(), 1);

        // Register the read now, at the current tick. No peer has
        // contacted the leader *since* this registration yet (peer 2's
        // last contact was recorded at an earlier or equal tick).
        c.read_index(9);
        assert_eq!(c.pending_reads.len(), 1);
        let registered_tick = c.pending_reads[0].registered_tick;
        assert!(c.last_contact_tick.get(&2).copied().unwrap_or(0) <= registered_tick);

        c.tick().unwrap();
        assert!(
            c.ready().reads.is_empty(),
            "must not release without post-registration quorum contact"
        );

        // Advance the clock, then deliver a fresh ack: this pushes
        // last_contact_tick[2] strictly past registered_tick.
        c.tick().unwrap();
        c.step(2, success_resp(term)).unwrap();
        let r = c.ready();
        assert_eq!(r.reads, vec![9]);
    }

    #[test]
    fn read_dropped_on_step_down() {
        let mut c = elect_leader(1, &[1, 2, 3]);
        let term = c.current_term();

        // Get the leader fully readable and register a read.
        c.step(2, success_resp(term)).unwrap();
        let _ = c.ready();
        c.read_index(11);
        assert_eq!(c.pending_reads.len(), 1);

        // A higher-term AppendEntries from another leader deposes this
        // node.
        c.step(
            3,
            Message::AppendEntries(AppendEntriesReq {
                term: term + 1,
                leader_id: 3,
                prev_log_index: c.commit_index(),
                prev_log_term: term,
                entries: vec![],
                leader_commit: c.commit_index(),
            }),
        )
        .unwrap();
        assert_eq!(c.role(), Role::Follower);

        // `handle_append_entries` (the step-down path) doesn't itself call
        // `maybe_release_reads` — only `tick()` and `handle_append_resp`'s
        // success branch do — so the drop is only guaranteed visible after
        // the next tick.
        c.tick().unwrap();
        assert!(
            c.pending_reads.is_empty(),
            "stepped-down leader must drop pending reads"
        );

        for _ in 0..60 {
            c.tick().unwrap();
            let r = c.ready();
            assert!(!r.reads.contains(&11), "must never release a dropped read");
        }
    }

    #[test]
    fn released_token_echoes_input() {
        let mut c = elect_leader(1, &[1, 2, 3]);
        let term = c.current_term();

        c.step(2, success_resp(term)).unwrap();
        let _ = c.ready();

        c.read_index(42);
        c.tick().unwrap();
        c.step(2, success_resp(term)).unwrap();

        let r = c.ready();
        assert_eq!(r.reads, vec![42]);
    }
}
