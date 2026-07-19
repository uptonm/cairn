use super::*;
use crate::rpc::{AppendEntriesReq, AppendEntriesResp};

impl<S: RaftStorage> RaftCore<S> {
    /// Appends `command` to the log at `current_term`/`last_index() + 1`
    /// and kicks off replication to every peer. Returns `Ok(None)` (no-op)
    /// when this node isn't the leader — the caller is expected to forward
    /// the write elsewhere rather than treat that as an error.
    pub fn propose(&mut self, command: Vec<u8>) -> Result<Option<LogIndex>> {
        if self.role != Role::Leader {
            return Ok(None);
        }
        let index = self.storage.last_index() + 1;
        let entry = LogEntry {
            term: self.current_term(),
            index,
            command,
        };
        self.storage.append(std::slice::from_ref(&entry))?;

        let self_id = self.config.id;
        self.match_index.insert(self_id, index);
        self.next_index.insert(self_id, index + 1);

        self.broadcast_append()?;
        // Mirrors become_leader's call: a leader whose own append alone
        // forms a majority (the solo-cluster case) gets no
        // AppendEntriesResp to trigger commit advancement from — without
        // this, a solo leader's proposals would never commit.
        self.maybe_advance_commit()?;
        Ok(Some(index))
    }

    /// Sends an AppendEntries (possibly empty, i.e. a heartbeat) to every
    /// peer other than self. Called both from `become_leader` and from the
    /// leader heartbeat branch of `tick`.
    pub(super) fn broadcast_append(&mut self) -> Result<()> {
        let self_id = self.config.id;
        let peers: Vec<NodeId> = self
            .config
            .peers
            .iter()
            .copied()
            .filter(|&p| p != self_id)
            .collect();
        for peer in peers {
            self.send_append_to(peer)?;
        }
        Ok(())
    }

    /// Sends `peer` everything from `next_index[peer]` onward, along with
    /// the immediately preceding (index, term) pair for the consistency
    /// check. Pushes this request's "up-to" extent onto `inflight[peer]`
    /// (FIFO) so a later success response can advance `match_index` by a
    /// safe lower bound without re-deriving the request's extent — see the
    /// `inflight` field doc for why FIFO pop-front is safe under
    /// overlapping in-flight requests.
    ///
    /// `pub(super)` because `handle_install_snapshot_resp`
    /// (core/snapshot.rs) calls this to resume ordinary replication once a
    /// follower has caught up via InstallSnapshot.
    pub(super) fn send_append_to(&mut self, peer: NodeId) -> Result<()> {
        let ni = self.next_index.get(&peer).copied().unwrap_or(1);
        // The entries peer needs (from next_index onward) were already
        // compacted away by a snapshot — there's nothing in the log to send
        // it anymore, so send the snapshot instead.
        if ni <= self.storage.snapshot_meta().last_index {
            return self.send_install_snapshot(peer);
        }
        let prev = ni.saturating_sub(1);
        let prev_term = self.term_at(prev)?;
        let entries = self.storage.entries_from(ni);
        let sent_up_to = prev + entries.len() as LogIndex;
        self.inflight.entry(peer).or_default().push_back(sent_up_to);
        *self.send_count.entry(peer).or_default() += 1;

        let req = AppendEntriesReq {
            term: self.current_term(),
            leader_id: self.config.id,
            prev_log_index: prev,
            prev_log_term: prev_term,
            entries,
            leader_commit: self.commit_index,
        };
        self.outbox.push((peer, Message::AppendEntries(req)));
        Ok(())
    }

    /// Term at `index`, treating index 0 (the log's virtual origin) as
    /// term 0. `RaftStorage::term` already resolves the snapshot-boundary
    /// case; any other `None` here would mean the caller asked about an
    /// index outside the log, which replication never does (callers only
    /// look up `prev_log_index <= last_index()` or entry indices drawn
    /// from the log itself).
    fn term_at(&self, index: LogIndex) -> Result<Term> {
        if index == 0 {
            return Ok(0);
        }
        Ok(self.storage.term(index)?.unwrap_or(0))
    }

    /// Walks backward from `from_index` while the term stays `term`,
    /// returning the first (lowest) index that still belongs to it. Used
    /// to reply with a conflict_index that lets the leader skip an entire
    /// mismatched term in one round trip instead of backing off one index
    /// at a time.
    fn first_index_of_term(&self, term: Term, from_index: LogIndex) -> Result<LogIndex> {
        let mut idx = from_index;
        while idx > 1 && self.term_at(idx - 1)? == term {
            idx -= 1;
        }
        Ok(idx)
    }

    /// Follower-side AppendEntries handler: the log-matching consistency
    /// check plus conflict back-up. Never implements the leader-side
    /// commit-index majority rule (Task 5) — it only clamps `commit_index`
    /// to what leader_commit and this follower's own log agree on.
    pub(super) fn handle_append_entries(
        &mut self,
        from: NodeId,
        req: AppendEntriesReq,
    ) -> Result<()> {
        let current_term = self.current_term();
        if req.term < current_term {
            self.outbox.push((
                from,
                Message::AppendEntriesResp(AppendEntriesResp {
                    term: current_term,
                    success: false,
                    conflict_index: None,
                }),
            ));
            return Ok(());
        }

        // req.term >= current_term: the sender is a legitimate leader for
        // at least our term. Adopt it, stepping down if we were a
        // candidate/leader, and reset the election timer regardless of
        // whether the term actually advanced (this is also how heartbeats
        // suppress our own election timeouts).
        self.become_follower(req.term, Some(req.leader_id))?;
        let current_term = self.current_term();

        let last_index = self.storage.last_index();
        if req.prev_log_index > last_index {
            self.outbox.push((
                from,
                Message::AppendEntriesResp(AppendEntriesResp {
                    term: current_term,
                    success: false,
                    conflict_index: Some(last_index + 1),
                }),
            ));
            return Ok(());
        }

        let prev_term = self.term_at(req.prev_log_index)?;
        if prev_term != req.prev_log_term {
            let conflict_index = self.first_index_of_term(prev_term, req.prev_log_index)?;
            self.outbox.push((
                from,
                Message::AppendEntriesResp(AppendEntriesResp {
                    term: current_term,
                    success: false,
                    conflict_index: Some(conflict_index),
                }),
            ));
            return Ok(());
        }

        // Consistency check passed. Find the first new entry that's either
        // missing or conflicts with what we have; entries before it are
        // already present with a matching term (skip them, so a replayed
        // AppendEntries is a no-op). A conflicting entry means everything
        // from that index on is wrong and must be dropped before the new
        // entries are appended.
        let mut append_from: Option<LogIndex> = None;
        for entry in &req.entries {
            match self.storage.term(entry.index)? {
                Some(t) if t == entry.term => continue,
                Some(_) => {
                    self.storage.truncate_suffix(entry.index)?;
                    append_from = Some(entry.index);
                    break;
                }
                None => {
                    append_from = Some(entry.index);
                    break;
                }
            }
        }
        if let Some(from_index) = append_from {
            let new_entries: Vec<LogEntry> = req
                .entries
                .iter()
                .filter(|e| e.index >= from_index)
                .cloned()
                .collect();
            self.storage.append(&new_entries)?;
        }

        let new_commit = req.leader_commit.min(self.storage.last_index());
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
        }
        self.advance_apply();

        self.outbox.push((
            from,
            Message::AppendEntriesResp(AppendEntriesResp {
                term: current_term,
                success: true,
                conflict_index: None,
            }),
        ));
        Ok(())
    }

    /// Leader-side AppendEntries response handler: replication progress
    /// bookkeeping and conflict back-up/retry. Does not implement commit
    /// advancement (Task 5's `maybe_advance_commit`).
    pub(super) fn handle_append_resp(
        &mut self,
        from: NodeId,
        resp: AppendEntriesResp,
    ) -> Result<()> {
        if resp.term > self.current_term() {
            return self.become_follower(resp.term, None);
        }
        if self.role != Role::Leader {
            return Ok(());
        }
        // A reply from an OLDER term is stale — it can't attest to
        // anything about this leader's current term, so it must not touch
        // ack_count/match_index/next_index/inflight, nor trigger commit
        // advancement or a read release. Only a same-term reply proceeds
        // past this point (standard Raft rule: ignore replies whose term
        // doesn't match the term the request was sent in).
        if resp.term < self.current_term() {
            return Ok(());
        }

        if resp.success {
            // Pop the oldest outstanding request's extent, not the newest:
            // that's the smallest (safe) lower bound on what this peer has
            // actually persisted, whichever in-flight request this
            // response corresponds to. An empty queue means a duplicate or
            // otherwise stale success with nothing left to attribute it
            // to — don't advance match_index off it, and (see below) don't
            // count it toward ack_count either.
            if let Some(up_to) = self.inflight.get_mut(&from).and_then(VecDeque::pop_front) {
                let match_idx = self.match_index.get(&from).copied().unwrap_or(0).max(up_to);
                self.match_index.insert(from, match_idx);
                self.next_index.insert(from, match_idx + 1);
                // Gated on a genuine pop (not incremented unconditionally):
                // this is what lets read_index.rs prove — by pigeonhole —
                // that a peer acked a send made after a given read
                // registered, WITHOUT assuming the transport never
                // redelivers a success. A duplicate/redelivered success
                // finds an empty queue (no pop, no increment), so
                // ack_count[P] can never exceed the number of distinct
                // sends actually popped for P, which is <= send_count[P]
                // by construction (see PendingRead::barrier and
                // maybe_release_reads).
                *self.ack_count.entry(from).or_default() += 1;
            }
            self.maybe_advance_commit()?;
            // Fresh contact and/or a commit advance may have just satisfied
            // a pending read's release conditions (core/read_index.rs) —
            // release promptly rather than waiting for the next tick.
            self.maybe_release_reads();
        } else {
            // The peer's log actually conflicts with what we assumed, so
            // every extent currently in flight to it is void — there's no
            // request id to single one out, so discard them all rather
            // than let a later success get attributed to the wrong one.
            if let Some(queue) = self.inflight.get_mut(&from) {
                queue.clear();
            }
            let next = resp.conflict_index.unwrap_or(1).max(1);
            self.next_index.insert(from, next);
            self.send_append_to(from)?;
        }
        Ok(())
    }

    /// Leader-only majority-match commit advancement, per Raft §5.4.2: an
    /// index `N` becomes committed only once a majority of `match_index`
    /// values reach it *and* the entry actually stored at `N` was written
    /// in the leader's current term. Counting replicas alone is not
    /// sufficient for a prior-term entry — it can still be overwritten by a
    /// future leader that never learns it was "majority-replicated" here,
    /// so committing it now (and letting a state machine observe it) would
    /// be a safety violation the moment that overwrite happens. Once a
    /// majority reaches a *current-term* `N`, though, the Log Matching
    /// property guarantees every entry at or below `N` on those replicas is
    /// identical to the leader's, so committing `N` implicitly commits
    /// everything below it too — no separate check needed for those.
    ///
    /// `commit_index` only ever moves forward: we scan candidate indices
    /// from `last_index()` down to `commit_index + 1` and take the first
    /// (i.e. highest) one that qualifies, which is always `>= commit_index`
    /// by construction of the scan range.
    pub(super) fn maybe_advance_commit(&mut self) -> Result<()> {
        if self.role != Role::Leader {
            return Ok(());
        }
        let current_term = self.current_term();
        let quorum = self.quorum();
        let last_index = self.storage.last_index();

        let mut n = last_index;
        while n > self.commit_index {
            let acked = self
                .config
                .peers
                .iter()
                .filter(|&&peer| self.match_index.get(&peer).copied().unwrap_or(0) >= n)
                .count();
            if acked >= quorum && self.storage.term(n)? == Some(current_term) {
                self.commit_index = n;
                break;
            }
            n -= 1;
        }

        self.advance_apply();
        Ok(())
    }

    /// Moves `last_applied` up to `commit_index` (all roles — leader after
    /// `maybe_advance_commit`, follower after adopting `leader_commit`),
    /// pushing a clone of each newly-applied entry into `apply_buf` for the
    /// caller to drain via `ready()`. Entries at or below the snapshot
    /// boundary are skipped (their effects are already captured by the
    /// snapshot, so `entries_from` won't even return them) but
    /// `last_applied` still advances past them. An entry written in the
    /// current term flips `readable_term`, which is what lets a leader
    /// start serving linearizable reads (Task 6) — it now knows it has
    /// applied at least one entry from its own term, per §8.
    fn advance_apply(&mut self) {
        if self.last_applied >= self.commit_index {
            return;
        }
        let snapshot_last = self.storage.snapshot_meta().last_index;
        let current_term = self.current_term();
        let target = self.commit_index;
        let mut pending = self.storage.entries_from(self.last_applied + 1).into_iter();

        while self.last_applied < target {
            self.last_applied += 1;
            if self.last_applied <= snapshot_last {
                continue;
            }
            if let Some(entry) = pending.next() {
                if entry.term == current_term {
                    self.readable_term = Some(current_term);
                }
                self.apply_buf.push(entry);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::RequestVoteResp;
    use crate::storage::MemStorage;
    use crate::types::{HardState, LogEntry};

    fn cfg(id: NodeId, peers: &[NodeId]) -> Config {
        Config {
            id,
            peers: peers.to_vec(),
            election_timeout: 10,
            heartbeat_interval: 3,
            seed: 42,
        }
    }

    /// Drives a 3-node cluster (self id 1) all the way to Leader via the
    /// same pre-vote -> vote round trip exercised in election.rs, draining
    /// the outbox as we go so callers see only what happens next.
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
    fn leader_propose_appends_and_emits_append_entries() {
        let mut c = elect_leader(1, &[1, 2, 3]);
        let idx = c.propose(b"set x".to_vec()).unwrap();
        assert_eq!(idx, Some(2)); // index 1 was the leader's no-op

        let r = c.ready();
        assert!(r.messages.iter().any(|(_, m)| matches!(
            m, Message::AppendEntries(req) if req.entries.iter().any(|e| e.command == b"set x")
        )));
    }

    #[test]
    fn propose_when_not_leader_returns_none() {
        let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), MemStorage::default()).unwrap();
        assert_eq!(c.propose(b"x".to_vec()).unwrap(), None);
    }

    #[test]
    fn follower_accepts_matching_append_entries_and_appends() {
        let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), MemStorage::default()).unwrap();
        c.step(
            1,
            Message::AppendEntries(AppendEntriesReq {
                term: 1,
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![LogEntry {
                    term: 1,
                    index: 1,
                    command: b"a".to_vec(),
                }],
                leader_commit: 0,
            }),
        )
        .unwrap();
        let r = c.ready();
        assert!(r.messages.iter().any(|(_, m)| matches!(
            m, Message::AppendEntriesResp(a) if a.success && a.term == 1 && a.conflict_index.is_none()
        )));
        assert_eq!(c.current_term(), 1);
        assert_eq!(c.leader_id(), Some(1));

        // A follow-up heartbeat referencing index 1/term 1 as prev must
        // also succeed, proving the entry was actually persisted.
        c.step(
            1,
            Message::AppendEntries(AppendEntriesReq {
                term: 1,
                leader_id: 1,
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![],
                leader_commit: 0,
            }),
        )
        .unwrap();
        let r2 = c.ready();
        assert!(r2
            .messages
            .iter()
            .any(|(_, m)| matches!(m, Message::AppendEntriesResp(a) if a.success)));
    }

    #[test]
    fn follower_rejects_on_prev_term_mismatch_with_conflict_index() {
        let mut s = MemStorage::default();
        s.append(&[
            LogEntry {
                term: 1,
                index: 1,
                command: vec![],
            },
            LogEntry {
                term: 1,
                index: 2,
                command: vec![],
            },
            LogEntry {
                term: 2,
                index: 3,
                command: vec![],
            },
        ])
        .unwrap();
        let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), s).unwrap();
        c.step(
            1,
            Message::AppendEntries(AppendEntriesReq {
                term: 3,
                leader_id: 1,
                prev_log_index: 3,
                prev_log_term: 3, // follower actually has term 2 at index 3
                entries: vec![],
                leader_commit: 0,
            }),
        )
        .unwrap();
        let r = c.ready();
        let resp = r
            .messages
            .iter()
            .find_map(|(_, m)| match m {
                Message::AppendEntriesResp(a) => Some(a.clone()),
                _ => None,
            })
            .expect("follower must reply");
        assert!(!resp.success);
        // term 2 only occupies index 3 -> conflict_index is the start of
        // that term.
        assert_eq!(resp.conflict_index, Some(3));
    }

    // Verbatim from the task-4 brief.
    #[test]
    fn follower_truncates_conflicting_suffix() {
        let mut s = MemStorage::default();
        s.append(&[
            LogEntry {
                term: 1,
                index: 1,
                command: vec![],
            },
            LogEntry {
                term: 1,
                index: 2,
                command: vec![9],
            },
        ])
        .unwrap();
        let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), s).unwrap();
        // leader at term 2 overwrites index 2 with a term-2 entry
        c.step(
            1,
            Message::AppendEntries(AppendEntriesReq {
                term: 2,
                leader_id: 1,
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![LogEntry {
                    term: 2,
                    index: 2,
                    command: vec![7],
                }],
                leader_commit: 0,
            }),
        )
        .unwrap();
        let r = c.ready();
        assert!(r.messages.iter().any(|(_, m)| matches!(
            m, Message::AppendEntriesResp(a) if a.success)));
        // index 2 now has term 2 command [7]
        assert_eq!(c.stored_hard_state().current_term, 2);
    }

    #[test]
    fn leader_backs_up_next_index_on_rejection_and_retries() {
        let mut c = elect_leader(1, &[1, 2, 3]);

        // Peer 2 rejects, reporting it has no entries at all.
        c.step(
            2,
            Message::AppendEntriesResp(AppendEntriesResp {
                term: 1,
                success: false,
                conflict_index: Some(1),
            }),
        )
        .unwrap();
        let r = c.ready();
        assert!(r.messages.iter().any(|(to, m)| *to == 2
            && matches!(m, Message::AppendEntries(req) if req.prev_log_index == 0)));
    }

    // --- match_index safety: FIFO in-flight queue (fix pass 1) ---
    //
    // Task 4's self-review flagged that the single scalar `last_sent[peer]`
    // is overwritten by every send, so two sends to the same peer before a
    // response arrives collapse to whatever the LAST send recorded — even
    // if the response actually belongs to an EARLIER, smaller-extent
    // request. That lets `match_index` jump ahead of what the follower
    // durably persisted, which is a state-machine-safety violation the
    // moment Task 5 computes `commit_index` from a majority of
    // `match_index`. These tests build the exact interleaving by hand
    // (bypassing the election protocol, which is irrelevant to send/response
    // bookkeeping) and pin the safe behavior.

    /// A hand-built leader (no election round-trip needed here) with a
    /// 10-entry log and peer 2 pinned to `next_index = 5`, `match_index = 0`
    /// — i.e. believed to be far behind. Two overlapping AppendEntries are
    /// sent before any response is processed: the first covers 5..=10
    /// (up_to 10), then the log grows to 11 and a second send — still from
    /// `next_index = 5`, since that only advances on success — covers
    /// 5..=11 (up_to 11).
    fn leader_with_two_overlapping_sends() -> RaftCore<MemStorage> {
        let mut s = MemStorage::default();
        s.append(
            &(1..=10)
                .map(|i| LogEntry {
                    term: 1,
                    index: i,
                    command: vec![],
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let mut c = RaftCore::new(cfg(1, &[1, 2, 3]), s).unwrap();
        c.role = Role::Leader;
        c.leader_id = Some(1);
        c.next_index.insert(2, 5);
        c.match_index.insert(2, 0);

        c.send_append_to(2).unwrap(); // up_to = 10
        let _ = c.ready();

        c.storage
            .append(&[LogEntry {
                term: 1,
                index: 11,
                command: vec![],
            }])
            .unwrap();
        c.send_append_to(2).unwrap(); // up_to = 11, next_index[2] still 5
        let _ = c.ready();
        c
    }

    fn success_resp(term: Term) -> Message {
        Message::AppendEntriesResp(AppendEntriesResp {
            term,
            success: true,
            conflict_index: None,
        })
    }

    #[test]
    fn interleaved_sends_do_not_over_advance_match_index() {
        let mut c = leader_with_two_overlapping_sends();
        let term = c.current_term();

        // The FIRST request's success arrives. The follower only durably
        // persisted through 10 (what that request covered) — match_index
        // must reflect that, not the second (still-unacknowledged)
        // request's extent of 11. Against the old scalar `last_sent` this
        // reads back whatever the LAST send recorded (11) regardless of
        // which response this is — an over-advancement.
        c.step(2, success_resp(term)).unwrap();
        assert_eq!(c.match_index_of(2), 10);
    }

    #[test]
    fn second_success_converges_match_index_to_the_larger_extent() {
        let mut c = leader_with_two_overlapping_sends();
        let term = c.current_term();

        c.step(2, success_resp(term)).unwrap();
        c.step(2, success_resp(term)).unwrap();
        assert_eq!(c.match_index_of(2), 11);
    }

    #[test]
    fn failure_clears_inflight_so_only_the_resend_can_advance_match_index() {
        let mut c = leader_with_two_overlapping_sends();
        let term = c.current_term();

        // Peer rejects: its log actually conflicts, so BOTH previously-sent
        // extents (10 and 11) are void, not just whichever request this
        // response happens to correspond to — there's no request id to
        // match against, so a conservative implementation must discard all
        // of them.
        c.step(
            2,
            Message::AppendEntriesResp(AppendEntriesResp {
                term,
                success: false,
                conflict_index: Some(1),
            }),
        )
        .unwrap();
        let r = c.ready();
        assert!(r.messages.iter().any(|(to, m)| *to == 2
            && matches!(m, Message::AppendEntries(req) if req.prev_log_index == 0)));

        // A single success now must be attributed to the fresh resend (up
        // to 11, the current log tip), not to the stale pre-failure "10"
        // that a non-clearing queue would still have at its front.
        c.step(2, success_resp(term)).unwrap();
        assert_eq!(c.match_index_of(2), 11);
    }

    // --- Task 5: commit-index advancement + apply buffering ---

    #[test]
    fn commit_advances_on_majority_current_term() {
        let mut c = elect_leader(1, &[1, 2, 3]);
        let term = c.current_term();
        assert_eq!(c.commit_index(), 0);

        // Peer 2 acks the leader's term-1 no-op (index 1). Self already
        // counts (match_index[1] == 1), so this single ack reaches quorum
        // (2 of 3) and the entry is from the current term.
        c.step(2, success_resp(term)).unwrap();

        assert_eq!(c.commit_index(), 1);
        let r = c.ready();
        assert_eq!(r.apply.len(), 1);
        assert_eq!(r.apply[0].index, 1);
        assert_eq!(r.apply[0].term, term);
    }

    /// Hand-built leader at term 2 whose log already holds a term-1 entry
    /// (index 1) followed by the term-2 leader no-op (index 2). Peers start
    /// believed to have nothing, so the test controls exactly how far each
    /// peer's acks advance `match_index` via the `inflight` FIFO, the same
    /// technique `leader_with_two_overlapping_sends` above uses.
    fn leader_term2_prior_and_current_entries() -> RaftCore<MemStorage> {
        let mut s = MemStorage::default();
        s.append(&[
            LogEntry {
                term: 1,
                index: 1,
                command: vec![],
            },
            LogEntry {
                term: 2,
                index: 2,
                command: vec![],
            },
        ])
        .unwrap();
        s.save_hard_state(&HardState {
            current_term: 2,
            voted_for: Some(1),
        })
        .unwrap();
        let mut c = RaftCore::new(cfg(1, &[1, 2, 3]), s).unwrap();
        c.role = Role::Leader;
        c.leader_id = Some(1);
        c.match_index.insert(1, 2); // self already has both entries
        c.next_index.insert(1, 3);
        for peer in [2, 3] {
            c.next_index.insert(peer, 1);
            c.match_index.insert(peer, 0);
        }
        c
    }

    #[test]
    fn no_commit_of_prior_term_by_count_alone() {
        let mut c = leader_term2_prior_and_current_entries();
        let term = c.current_term();
        assert_eq!(term, 2);

        // Peer 2 acks up through index 1 only (the term-1 entry).
        c.inflight.entry(2).or_default().push_back(1);
        c.step(2, success_resp(term)).unwrap();
        assert_eq!(c.match_index_of(2), 1);

        // A majority (self + peer 2) now has the term-1 entry, but §5.4.2
        // forbids committing it by replica count alone: index 1 is not a
        // current-term entry.
        assert_eq!(c.commit_index(), 0);
        assert!(c.ready().apply.is_empty());

        // Peer 2 now also acks the term-2 entry (index 2): a majority
        // reaches index 2, which IS current-term, so both 1 and 2 commit
        // together in one jump.
        c.inflight.entry(2).or_default().push_back(2);
        c.step(2, success_resp(term)).unwrap();
        assert_eq!(c.commit_index(), 2);
        let r = c.ready();
        let indices: Vec<_> = r.apply.iter().map(|e| e.index).collect();
        assert_eq!(indices, vec![1, 2]);
    }

    #[test]
    fn apply_is_in_order_and_once() {
        let mut c = elect_leader(1, &[1, 2, 3]);
        let term = c.current_term();
        c.propose(b"a".to_vec()).unwrap(); // index 2
        c.propose(b"b".to_vec()).unwrap(); // index 3
        let _ = c.ready(); // drain AppendEntries messages

        // Peer 2 acks each outstanding request in FIFO order (no-op,
        // then "a", then "b"), advancing commit_index one step at a time.
        c.step(2, success_resp(term)).unwrap();
        c.step(2, success_resp(term)).unwrap();
        c.step(2, success_resp(term)).unwrap();

        assert_eq!(c.commit_index(), 3);
        let r = c.ready();
        let indices: Vec<_> = r.apply.iter().map(|e| e.index).collect();
        assert_eq!(indices, vec![1, 2, 3]);

        // Nothing new committed since the last ready() -> empty apply.
        let r2 = c.ready();
        assert!(r2.apply.is_empty());
    }

    #[test]
    fn follower_applies_up_to_min_leader_commit_last_index() {
        let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), MemStorage::default()).unwrap();
        c.step(
            1,
            Message::AppendEntries(AppendEntriesReq {
                term: 1,
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![
                    LogEntry {
                        term: 1,
                        index: 1,
                        command: b"a".to_vec(),
                    },
                    LogEntry {
                        term: 1,
                        index: 2,
                        command: b"b".to_vec(),
                    },
                ],
                leader_commit: 5, // beyond this follower's last_index (2)
            }),
        )
        .unwrap();

        // Clamped to last_index, not the (out-of-range) leader_commit.
        assert_eq!(c.commit_index(), 2);
        let r = c.ready();
        let indices: Vec<_> = r.apply.iter().map(|e| e.index).collect();
        assert_eq!(indices, vec![1, 2]);
    }

    #[test]
    fn stale_term_append_entries_rejected() {
        let mut s = MemStorage::default();
        s.save_hard_state(&HardState {
            current_term: 5,
            voted_for: None,
        })
        .unwrap();
        let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), s).unwrap();
        c.step(
            1,
            Message::AppendEntries(AppendEntriesReq {
                term: 3,
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }),
        )
        .unwrap();
        let r = c.ready();
        assert!(r.messages.iter().any(|(_, m)| matches!(
            m, Message::AppendEntriesResp(a) if !a.success && a.term == 5 && a.conflict_index.is_none()
        )));
        assert_eq!(c.leader_id(), None); // stale AE must not adopt the sender as leader
    }

    // --- read-index ack_count must gate on a genuine inflight pop
    // (duplicate-ack safety) ---
    //
    // `maybe_release_reads` (core/read_index.rs) proves a read safe via
    // pigeonhole: `ack_count[P] > barrier[P]` implies P acked a send made
    // after the read registered, PROVIDED every same-term success bumping
    // `ack_count` corresponds to a DISTINCT send. `match_index` already
    // gets this for free — it only advances when `inflight.pop_front()`
    // yields `Some` (an empty queue means a duplicate/stale success with
    // nothing left to attribute it to). `ack_count` must use the same
    // gate, or a duplicated/redelivered success can inflate it past a
    // read's barrier with no corresponding post-registration send.
    #[test]
    fn duplicate_success_does_not_inflate_ack_count_or_release_read() {
        let mut c = elect_leader(1, &[1, 2, 3]);
        let term = c.current_term();

        // Get the leader fully readable: peer 2 acks the no-op (index 1),
        // reaching quorum (self + peer 2) and setting readable_term.
        c.step(2, success_resp(term)).unwrap();
        let _ = c.ready();
        assert_eq!(c.readable_term, Some(term));
        assert_eq!(c.commit_index(), 1);
        assert_eq!(c.ack_count_of(2), 1);

        // Arrange one outstanding in-flight send to peer 2, distinct from
        // what's already been acked, so send_count[2] > ack_count[2] —
        // exactly the state a real send-in-progress leaves behind.
        c.inflight.entry(2).or_default().push_back(2);
        *c.send_count.entry(2).or_default() += 1;
        let send_before = c.send_count.get(&2).copied().unwrap_or(0);
        assert!(
            send_before > c.ack_count_of(2),
            "must have an unacked outstanding send"
        );

        // Register a read by hand: barrier[2] snapshots send_count[2] as
        // it stands right now (including the outstanding send above),
        // mirroring what `read_index` does before it forces a broadcast.
        c.pending_reads.push(PendingRead {
            token: 7,
            floor: c.commit_index,
            barrier: c.send_count.clone(),
        });
        let barrier_for_2 = send_before;

        // Deliver that outstanding send's success: it pops the one
        // in-flight entry, so ack_count[2] legitimately advances — but
        // only up TO the barrier (this send was already outstanding, and
        // so already reflected in send_count, at registration time), not
        // past it.
        c.step(2, success_resp(term)).unwrap();
        assert_eq!(c.ack_count_of(2), barrier_for_2);
        let r = c.ready();
        assert!(
            r.reads.is_empty(),
            "ack_count == barrier must not release (needs strictly >)"
        );

        // Deliver a DUPLICATE of that same success (a redelivered/retried
        // network message). inflight[2] is now empty — there is no
        // distinct send left to attribute this to, so it must be a no-op
        // for ack_count, exactly like the existing match_index guard.
        let ack_after_first = c.ack_count_of(2);
        c.step(2, success_resp(term)).unwrap();
        assert_eq!(
            c.ack_count_of(2),
            ack_after_first,
            "a duplicate success on an empty inflight queue must not increment ack_count"
        );
        let r2 = c.ready();
        assert!(
            r2.reads.is_empty(),
            "a duplicate ack must never release a read past its barrier"
        );

        // Now a genuine post-registration send/ack: pushes a fresh
        // in-flight entry, and its success legitimately pops it, pushing
        // ack_count strictly past the barrier for a real quorum (self +
        // peer 2) — proving the fix didn't over-restrict.
        c.inflight.entry(2).or_default().push_back(3);
        *c.send_count.entry(2).or_default() += 1;
        c.step(2, success_resp(term)).unwrap();
        assert!(c.ack_count_of(2) > barrier_for_2);
        let r3 = c.ready();
        assert_eq!(r3.reads, vec![7]);
    }
}
