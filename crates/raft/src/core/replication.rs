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
    /// check. Records `last_sent[peer]` so a later success response can
    /// advance `match_index` without re-deriving the request's extent.
    fn send_append_to(&mut self, peer: NodeId) -> Result<()> {
        let ni = self.next_index.get(&peer).copied().unwrap_or(1);
        let prev = ni.saturating_sub(1);
        let prev_term = self.term_at(prev)?;
        let entries = self.storage.entries_from(ni);
        let sent_up_to = prev + entries.len() as LogIndex;
        self.last_sent.insert(peer, sent_up_to);

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

        if resp.success {
            let sent_up_to = self.last_sent.get(&from).copied().unwrap_or(0);
            let match_idx = self
                .match_index
                .get(&from)
                .copied()
                .unwrap_or(0)
                .max(sent_up_to);
            self.match_index.insert(from, match_idx);
            self.next_index.insert(from, match_idx + 1);
            self.last_contact_tick.insert(from, self.tick_count);
            self.maybe_advance_commit()?;
        } else {
            let next = resp.conflict_index.unwrap_or(1).max(1);
            self.next_index.insert(from, next);
            self.send_append_to(from)?;
        }
        Ok(())
    }

    /// No-op stub. Task 5 fills this in with the majority-match commit
    /// advancement rule (only committing entries replicated to a majority
    /// AND originally written in the leader's current term, per Raft
    /// §5.4.2).
    fn maybe_advance_commit(&mut self) -> Result<()> {
        Ok(())
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
}
