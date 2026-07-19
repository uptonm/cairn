use super::*;

impl<S: RaftStorage> RaftCore<S> {
    /// Registers a linearizable read barrier for `token`.
    ///
    /// Only a leader can ever satisfy the guarantee this provides. If this
    /// node isn't the leader right now, the caller (the external driver) is
    /// expected to redirect the read to whichever node it believes is
    /// leader, so the token is dropped here rather than queued — the core
    /// must never release a read it can't back up.
    ///
    /// Snapshots `send_count` as this read's `barrier` BEFORE forcing a
    /// fresh broadcast, then forces that broadcast: a quiescent leader
    /// (nothing due to send yet) would otherwise never generate a
    /// post-barrier send for any peer to ack, and the read could never
    /// confirm. See `maybe_release_reads` for how the barrier is used.
    pub fn read_index(&mut self, token: ReadToken) -> Result<()> {
        if self.role != Role::Leader {
            return Ok(());
        }
        let barrier = self.send_count.clone();
        self.pending_reads.push(PendingRead {
            token,
            floor: self.commit_index,
            barrier,
        });
        self.broadcast_append()?;
        self.maybe_release_reads();
        Ok(())
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
    ///     touch with itself). Checked, per peer, as `ack_count[peer] >
    ///     barrier[peer]` (missing entries treated as 0), where `barrier`
    ///     is the `send_count` snapshot `read_index` took when this read
    ///     registered. Why this check is right (C2 fix): `ack_count[peer]`
    ///     only ever increments on a genuine same-term success
    ///     (`handle_append_resp`'s stale-term guard), and each such success
    ///     corresponds to a distinct send — messages aren't duplicated (TCP
    ///     and the sim both hold this invariant). If `ack_count[peer] >
    ///     barrier[peer]`, peer has acked strictly more sends than were
    ///     outstanding at registration time, so by pigeonhole at least one
    ///     of its acked sends has sequence number greater than the
    ///     snapshot — i.e. was SENT after this read registered, not merely
    ///     replied to after. That's what a tick-based "last processed
    ///     contact" comparison got wrong: a delayed reply to a
    ///     PRE-registration heartbeat, merely PROCESSED after
    ///     registration, could satisfy a tick check without attesting
    ///     anything about the peer post-registration. A peer that defects
    ///     to a higher term stops producing same-term successes entirely
    ///     (its higher-term reply steps this leader down instead), so its
    ///     `ack_count` can never cross a fresh barrier again. Election
    ///     safety (one leader per term) then means a quorum clearing this
    ///     bar rules out any other leader having served this term in the
    ///     interval.
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
        let ack_count = &self.ack_count;
        let peers = &self.config.peers;

        let mut released = Vec::new();
        self.pending_reads.retain(|read| {
            let contacted = peers
                .iter()
                .filter(|&&peer| {
                    peer == self_id
                        || ack_count.get(&peer).copied().unwrap_or(0)
                            > read.barrier.get(&peer).copied().unwrap_or(0)
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
        // Pre-vote grant: a fresh peer at term 0 echoes its own current_term
        // (0), not the candidate's prospective term.
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
        // Real vote grant: echoes the (now bumped) candidate's current_term.
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

    #[test]
    fn read_on_follower_is_never_released() {
        let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), MemStorage::default()).unwrap();
        assert_eq!(c.role(), Role::Follower);
        c.read_index(7).unwrap();
        assert!(c.pending_reads.is_empty(), "follower must not queue reads");
        for _ in 0..60 {
            c.tick().unwrap();
            let r = c.ready();
            assert!(!r.reads.contains(&7));
        }
    }

    /// Hand-built leader isolating the (b) readable_term gate from the (c)
    /// quorum-contact gate: quorum contact is pre-satisfied via directly
    /// set `ack_count`, but `readable_term` is still `None` (a freshly
    /// elected leader whose no-op hasn't applied yet), so the read must
    /// stay pending until a current-term entry actually applies.
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
        assert_eq!(c.readable_term, None);

        c.read_index(7).unwrap();
        assert_eq!(c.pending_reads.len(), 1);
        let _ = c.ready(); // drain the forced broadcast_append this triggers

        // Quorum contact already satisfied strictly after registration...
        c.ack_count.insert(2, 1);
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
    /// peer has confirmed the barrier since the read registered. Contact is
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

        // Register the read now. No peer has acked past this read's
        // barrier yet (peer 2's ack_count is at most what the barrier
        // snapshotted).
        c.read_index(9).unwrap();
        assert_eq!(c.pending_reads.len(), 1);
        let barrier_for_2 = c.pending_reads[0].barrier.get(&2).copied().unwrap_or(0);
        assert!(c.ack_count.get(&2).copied().unwrap_or(0) <= barrier_for_2);
        let _ = c.ready(); // drain the forced broadcast_append this triggers

        c.tick().unwrap();
        assert!(
            c.ready().reads.is_empty(),
            "must not release without post-registration quorum contact"
        );

        // Deliver a fresh ack: this pushes ack_count[2] strictly past the
        // read's barrier for peer 2.
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
        c.read_index(11).unwrap();
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

        c.read_index(42).unwrap();
        let _ = c.ready(); // drain the forced broadcast_append this triggers
        c.tick().unwrap();
        c.step(2, success_resp(term)).unwrap();

        let r = c.ready();
        assert_eq!(r.reads, vec![42]);
    }

    // --- Bug 1 (fix pass 1): stale-term AppendEntriesResp must not count
    // as post-registration contact ---
    //
    // Reviewer's exact scenario: `handle_append_resp` rejected only
    // `resp.term > current_term` (step down), so a delayed reply from an
    // OLDER term fell through to the success branch and stamped contact,
    // letting a stale reply satisfy read-index's quorum-contact gate for a
    // read a quorum never actually confirmed in the current term. This is a
    // stale read / linearizability violation.

    /// Hand-built leader at an arbitrary `term`, already fully "readable"
    /// (current-term entry applied, per (b)) and with its own
    /// self-contact/apply state satisfied, so the only thing a test needs
    /// to drive is the (c) quorum-contact gate. Bypasses the election
    /// round trip (which always lands on term 1) so the test can pick a
    /// `term` high enough that `term - 1` is a distinct, meaningful older
    /// term for a stale reply.
    fn leader_at_term(term: Term) -> RaftCore<MemStorage> {
        let mut s = MemStorage::default();
        s.save_hard_state(&HardState {
            current_term: term,
            voted_for: Some(1),
        })
        .unwrap();
        let mut c = RaftCore::new(cfg(1, &[1, 2, 3]), s).unwrap();
        c.role = Role::Leader;
        c.leader_id = Some(1);
        c.readable_term = Some(term);
        c
    }

    #[test]
    fn stale_term_append_resp_does_not_release_read() {
        let term: Term = 5;
        let mut c = leader_at_term(term);

        // Register the read (forces a broadcast, bumping send_count for
        // every peer past the barrier snapshot).
        c.read_index(99).unwrap();
        assert_eq!(c.pending_reads.len(), 1);
        let _ = c.ready();

        // Deliver a delayed AppendEntriesResp from an OLDER term. Under the
        // bug this counted as contact anyway (ack_count bumped regardless
        // of term), satisfying quorum (self + peer 2) even though peer 2
        // never affirmed this leader in the current term.
        c.tick().unwrap();
        c.step(
            2,
            Message::AppendEntriesResp(AppendEntriesResp {
                term: term - 1,
                success: true,
                conflict_index: None,
            }),
        )
        .unwrap();

        // Tick several more times: the read must never release, because
        // the only "contact" on record is a stale-term reply that must be
        // ignored.
        for _ in 0..10 {
            c.tick().unwrap();
            let r = c.ready();
            assert!(
                r.reads.is_empty(),
                "stale-term AppendEntriesResp must not release a pending read"
            );
        }
        assert_eq!(c.pending_reads.len(), 1, "read must remain pending");
    }

    /// Companion to the above: proves the fix doesn't over-reject. A
    /// CURRENT-term success reply still refreshes contact and still
    /// releases the read.
    #[test]
    fn current_term_append_resp_still_releases_read() {
        let term: Term = 5;
        let mut c = leader_at_term(term);

        c.read_index(100).unwrap();
        assert_eq!(c.pending_reads.len(), 1);
        let _ = c.ready(); // drain the forced broadcast_append this triggers

        c.tick().unwrap();
        c.step(2, success_resp(term)).unwrap();

        let r = c.ready();
        assert_eq!(r.reads, vec![100]);
    }

    // --- Bug 3: readable_term gate must be `== Some(current_term())`, not
    // `is_some()` ---

    #[test]
    fn read_not_released_when_readable_term_is_stale() {
        let term: Term = 5;
        let mut c = leader_at_term(term);
        // Simulate a stale leftover readable_term from a prior term rather
        // than the fresh-leader `None` case already covered by
        // `read_waits_for_current_term_commit`.
        c.readable_term = Some(term - 1);

        c.read_index(55).unwrap();
        assert_eq!(c.pending_reads.len(), 1);
        let _ = c.ready(); // drain the forced broadcast_append this triggers

        c.tick().unwrap();
        c.step(2, success_resp(term)).unwrap();

        let r = c.ready();
        assert!(
            r.reads.is_empty(),
            "a stale (non-current) readable_term must not satisfy the gate"
        );
        assert_eq!(c.pending_reads.len(), 1);
    }

    // --- C2 (whole-branch review): read-index leadership confirmation must
    // be a per-read send-count barrier, not a tick-based "last processed
    // contact" timestamp ---
    //
    // The bug: `last_contact_tick[peer]` was stamped when a same-term
    // success was PROCESSED, but that success only attests the peer
    // followed this leader when the peer SENT its reply — which can be
    // BEFORE the read registered, and before the peer defected to a higher
    // term. A delayed reply to a PRE-read heartbeat, processed after the
    // read registers, wrongly satisfied `tick > registered_tick` and
    // confirmed leadership even though it says nothing about the peer's
    // state after the read began. Serving the read at that point could miss
    // a write the peer helped a newer leader commit in the meantime — a
    // linearizability violation.
    //
    // The fix: `barrier[peer]` snapshots `send_count[peer]` at
    // registration. A peer only confirms once `ack_count[peer] >
    // barrier[peer]` — i.e. it has acked MORE sends than were outstanding
    // at registration time, which by pigeonhole means at least one acked
    // send was made (not just replied to) after the read began.
    //
    // This test drives the exact failure shape: a send to peer 2 is already
    // outstanding (unacked) BEFORE the read registers. The read's barrier
    // for peer 2 is 1 (the pre-registration send). The delayed ack for that
    // very send arrives after registration — under the old tick-based model
    // this alone would release the read (it's PROCESSED after
    // registration); under the barrier model it only brings ack_count up TO
    // the barrier, not past it, so the read must stay pending. Only a
    // second ack — for the read's own forced post-registration send —
    // pushes ack_count strictly past the barrier and releases it.
    #[test]
    fn read_requires_ack_to_a_post_registration_send() {
        let term: Term = 5;
        let mut c = leader_at_term(term);
        // last_applied (0) already meets floor (commit_index, also 0) and
        // readable_term is already current, so only the (c)
        // quorum-contact-after-registration gate is under test here.

        // A send to peer 2 is already outstanding BEFORE the read
        // registers (e.g. a heartbeat sent moments earlier that hasn't
        // been acked yet).
        c.broadcast_append().unwrap();
        let _ = c.ready();
        assert_eq!(c.send_count.get(&2).copied(), Some(1));
        assert_eq!(c.ack_count.get(&2).copied().unwrap_or(0), 0);

        c.read_index(77).unwrap();
        assert_eq!(c.pending_reads.len(), 1);
        let barrier_for_2 = c.pending_reads[0].barrier.get(&2).copied().unwrap_or(0);
        assert_eq!(
            barrier_for_2, 1,
            "barrier must snapshot the pre-registration send_count"
        );
        let _ = c.ready(); // drain the forced broadcast_append this triggers
        assert_eq!(
            c.send_count.get(&2).copied(),
            Some(2),
            "read_index must force a fresh post-registration send"
        );

        // Advance the clock so a tick-based model would treat any
        // subsequently-processed ack as "post-registration" contact.
        c.tick().unwrap();
        let _ = c.ready();

        // The delayed ack for the PRE-registration send arrives. It must
        // NOT release the read: it only brings ack_count[2] up to the
        // barrier (1), not past it.
        c.step(2, success_resp(term)).unwrap();
        assert_eq!(c.ack_count.get(&2).copied(), Some(1));
        assert!(
            c.ready().reads.is_empty(),
            "an ack for a pre-registration send must not release the read"
        );
        assert_eq!(c.pending_reads.len(), 1, "read must remain pending");

        // The ack for the post-registration (forced) send arrives too:
        // ack_count[2] now exceeds the barrier, proving peer 2 affirmed
        // this leader's authority via a send made after the read began.
        // Self + peer 2 form a quorum of 2/3.
        c.step(2, success_resp(term)).unwrap();
        assert_eq!(c.ack_count.get(&2).copied(), Some(2));
        let r = c.ready();
        assert_eq!(r.reads, vec![77]);
    }
}
