use super::*;
use crate::rpc::{RequestVoteReq, RequestVoteResp};

impl<S: RaftStorage> RaftCore<S> {
    /// Number of votes (including our own) needed to win an election over
    /// `config.peers` (which includes self).
    fn quorum(&self) -> usize {
        self.config.peers.len() / 2 + 1
    }

    /// Becomes a pre-candidate and broadcasts a pre-vote `RequestVote` to
    /// every peer. Pre-vote never persists or mutates `current_term`/
    /// `voted_for` — it only asks "would you vote for me if I called a real
    /// election at term `current_term + 1`?" so a partitioned node can't
    /// bump its term (and disrupt the cluster) unless it could actually win.
    pub(super) fn start_prevote(&mut self) {
        self.role = Role::PreCandidate;
        self.votes = BTreeSet::new();
        self.votes.insert(self.config.id);
        let req = RequestVoteReq {
            term: self.current_term() + 1,
            candidate_id: self.config.id,
            last_log_index: self.storage.last_index(),
            last_log_term: self.storage.last_term(),
            pre_vote: true,
        };
        let self_id = self.config.id;
        for &peer in &self.config.peers {
            if peer != self_id {
                self.outbox.push((peer, Message::RequestVote(req.clone())));
            }
        }
        self.reset_election_timer();
    }

    /// Promotes from pre-candidate (having won a pre-vote quorum) to a real
    /// candidate: bumps `current_term`, votes for self, persists *before*
    /// broadcasting the real `RequestVote`.
    fn become_candidate(&mut self) -> Result<()> {
        let new_term = self.current_term() + 1;
        let hs = HardState {
            current_term: new_term,
            voted_for: Some(self.config.id),
        };
        self.storage.save_hard_state(&hs)?;

        self.role = Role::Candidate;
        self.votes = BTreeSet::new();
        self.votes.insert(self.config.id);
        self.reset_election_timer();

        let req = RequestVoteReq {
            term: new_term,
            candidate_id: self.config.id,
            last_log_index: self.storage.last_index(),
            last_log_term: self.storage.last_term(),
            pre_vote: false,
        };
        let self_id = self.config.id;
        for &peer in &self.config.peers {
            if peer != self_id {
                self.outbox.push((peer, Message::RequestVote(req.clone())));
            }
        }
        Ok(())
    }

    /// Wins the election: becomes leader, initializes per-peer replication
    /// state, and appends a no-op entry in the new term (the standard Raft
    /// technique for committing entries from prior terms indirectly).
    ///
    /// `broadcast_append()` — the AppendEntries heartbeat/replication
    /// broadcast — is a stub for this task; Task 4 replaces it with the
    /// real implementation.
    fn become_leader(&mut self) -> Result<()> {
        self.role = Role::Leader;
        self.leader_id = Some(self.config.id);
        self.heartbeat_elapsed = 0;

        let self_id = self.config.id;
        let next = self.storage.last_index() + 1;
        for &peer in &self.config.peers {
            if peer != self_id {
                self.next_index.insert(peer, next);
                self.match_index.insert(peer, 0);
            }
        }

        let noop = LogEntry {
            term: self.current_term(),
            index: next,
            command: Vec::new(),
        };
        self.storage.append(std::slice::from_ref(&noop))?;
        self.next_index.insert(self_id, next + 1);
        self.match_index.insert(self_id, next);

        // A fresh leader can't serve linearizable reads until its own
        // no-op has committed, confirming it holds all previously
        // committed entries.
        self.readable_term = None;

        self.broadcast_append()
    }

    /// Steps down to follower. Persists the new term (and clears
    /// `voted_for`) *before* any message referencing the new term is
    /// emitted, but only when `term` actually advances `current_term` — a
    /// same-term step (e.g. discovering the current leader) must not touch
    /// storage.
    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) -> Result<()> {
        if term > self.current_term() {
            let hs = HardState {
                current_term: term,
                voted_for: None,
            };
            self.storage.save_hard_state(&hs)?;
        }
        self.role = Role::Follower;
        self.leader_id = leader;
        self.reset_election_timer();
        Ok(())
    }

    /// Temporary Task 3 stub for the AppendEntries broadcast; Task 4 gives
    /// this a real body (heartbeat/replication to every peer).
    fn broadcast_append(&mut self) -> Result<()> {
        Ok(())
    }

    pub(super) fn handle_request_vote(&mut self, from: NodeId, req: RequestVoteReq) -> Result<()> {
        let up_to_date = (req.last_log_term, req.last_log_index)
            >= (self.storage.last_term(), self.storage.last_index());

        if req.pre_vote {
            // Pre-vote never persists or mutates current_term/voted_for.
            // Deferred refinement: also require "no leader contact within
            // the election timeout" (leader-stickiness guard) — Plan C
            // accepts the simpler up-to-date + term check for now.
            let granted = req.term >= self.current_term() && up_to_date;
            self.outbox.push((
                from,
                Message::RequestVoteResp(RequestVoteResp {
                    term: self.current_term(),
                    vote_granted: granted,
                }),
            ));
            return Ok(());
        }

        if req.term > self.current_term() {
            self.become_follower(req.term, None)?;
        }
        let current_term = self.current_term();

        let granted = if req.term < current_term {
            false
        } else {
            let voted_for = self.storage.hard_state().voted_for;
            let can_vote = voted_for.is_none() || voted_for == Some(req.candidate_id);
            let granted = up_to_date && can_vote;
            if granted {
                // Persist voted_for BEFORE the granting response is queued.
                let hs = HardState {
                    current_term,
                    voted_for: Some(req.candidate_id),
                };
                self.storage.save_hard_state(&hs)?;
            }
            granted
        };

        self.outbox.push((
            from,
            Message::RequestVoteResp(RequestVoteResp {
                term: current_term,
                vote_granted: granted,
            }),
        ));
        Ok(())
    }

    pub(super) fn handle_vote_resp(&mut self, from: NodeId, resp: RequestVoteResp) -> Result<()> {
        match self.role {
            Role::PreCandidate => {
                // While pre-candidate, current_term is unchanged; the
                // pre-vote round we're running is for current_term + 1.
                let prospective_term = self.current_term() + 1;
                if resp.term > prospective_term {
                    return self.become_follower(resp.term, None);
                }
                if resp.term < prospective_term || !resp.vote_granted {
                    return Ok(());
                }
                self.votes.insert(from);
                if self.votes.len() >= self.quorum() {
                    self.become_candidate()?;
                }
                Ok(())
            }
            Role::Candidate => {
                let current_term = self.current_term();
                if resp.term > current_term {
                    return self.become_follower(resp.term, None);
                }
                if resp.term < current_term || !resp.vote_granted {
                    return Ok(());
                }
                self.votes.insert(from);
                if self.votes.len() >= self.quorum() {
                    self.become_leader()?;
                }
                Ok(())
            }
            Role::Follower | Role::Leader => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemStorage;
    use crate::types::LogEntry;

    fn cfg(id: NodeId, peers: &[NodeId]) -> Config {
        Config {
            id,
            peers: peers.to_vec(),
            election_timeout: 10,
            heartbeat_interval: 3,
            seed: 42,
        }
    }

    // grants vote to an up-to-date candidate and persists voted_for
    #[test]
    fn grants_vote_to_up_to_date_candidate() {
        let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), MemStorage::default()).unwrap();
        c.step(
            1,
            Message::RequestVote(RequestVoteReq {
                term: 1,
                candidate_id: 1,
                last_log_index: 0,
                last_log_term: 0,
                pre_vote: false,
            }),
        )
        .unwrap();
        let r = c.ready();
        assert!(r.messages.iter().any(|(to, m)| *to == 1
            && matches!(
                m, Message::RequestVoteResp(v) if v.vote_granted && v.term == 1)));
        assert_eq!(c.current_term(), 1);
        // persisted
        let hs = c.stored_hard_state();
        assert_eq!(hs.current_term, 1);
        assert_eq!(hs.voted_for, Some(1));
    }

    // rejects a candidate whose log is behind
    #[test]
    fn rejects_behind_candidate() {
        let mut s = MemStorage::default();
        s.append(&[LogEntry {
            term: 2,
            index: 1,
            command: vec![],
        }])
        .unwrap();
        let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), s).unwrap();
        c.step(
            1,
            Message::RequestVote(RequestVoteReq {
                term: 3,
                candidate_id: 1,
                last_log_index: 0,
                last_log_term: 0,
                pre_vote: false,
            }),
        )
        .unwrap();
        let r = c.ready();
        assert!(r.messages.iter().any(|(_, m)| matches!(
            m, Message::RequestVoteResp(v) if !v.vote_granted)));
    }

    // does not double-vote in the same term
    #[test]
    fn no_double_vote_same_term() {
        let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), MemStorage::default()).unwrap();
        let rv = |cid| {
            Message::RequestVote(RequestVoteReq {
                term: 1,
                candidate_id: cid,
                last_log_index: 0,
                last_log_term: 0,
                pre_vote: false,
            })
        };
        c.step(1, rv(1)).unwrap();
        let _ = c.ready();
        c.step(3, rv(3)).unwrap();
        let r = c.ready();
        assert!(r.messages.iter().any(|(to, m)| *to == 3
            && matches!(
                m, Message::RequestVoteResp(v) if !v.vote_granted)));
    }

    // pre-vote grant does not mutate term or voted_for
    #[test]
    fn prevote_does_not_persist() {
        let mut c = RaftCore::new(cfg(2, &[1, 2, 3]), MemStorage::default()).unwrap();
        c.step(
            1,
            Message::RequestVote(RequestVoteReq {
                term: 1,
                candidate_id: 1,
                last_log_index: 0,
                last_log_term: 0,
                pre_vote: true,
            }),
        )
        .unwrap();
        assert_eq!(c.current_term(), 0); // unchanged
        let r = c.ready();
        assert!(r
            .messages
            .iter()
            .any(|(_, m)| matches!(m, Message::RequestVoteResp(v) if v.vote_granted)));
    }

    // wins election with a majority of real votes and appends a no-op
    #[test]
    fn wins_election_with_majority() {
        let mut c = RaftCore::new(cfg(1, &[1, 2, 3]), MemStorage::default()).unwrap();
        for _ in 0..40 {
            c.tick().unwrap();
        } // -> PreCandidate, pre-vote out
        let _ = c.ready();
        // grant pre-votes from 2 and 3
        c.step(
            2,
            Message::RequestVoteResp(RequestVoteResp {
                term: 1,
                vote_granted: true,
            }),
        )
        .unwrap();
        assert_eq!(c.role(), Role::Candidate); // pre-vote won -> real candidate (term 1)
        assert_eq!(c.current_term(), 1);
        let _ = c.ready();
        c.step(
            2,
            Message::RequestVoteResp(RequestVoteResp {
                term: 1,
                vote_granted: true,
            }),
        )
        .unwrap();
        assert_eq!(c.role(), Role::Leader);
        // leader appends a no-op in its term
        assert!(c.commit_index() <= 1);
    }
}
