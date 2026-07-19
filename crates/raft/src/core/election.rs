use super::*;
use crate::rpc::{RequestVoteReq, RequestVoteResp};

impl<S: RaftStorage> RaftCore<S> {
    /// Number of votes (including our own, if we're currently a voter)
    /// needed to win an election over the live `voters` set. Also the
    /// majority threshold `maybe_advance_commit` (replication.rs) uses for
    /// `match_index` counting — same cluster, same majority definition.
    pub(super) fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    /// Becomes a pre-candidate and broadcasts a pre-vote `RequestVote` to
    /// every peer. Pre-vote never persists or mutates `current_term`/
    /// `voted_for` — it only asks "would you vote for me if I called a real
    /// election at term `current_term + 1`?" so a partitioned node can't
    /// bump its term (and disrupt the cluster) unless it could actually win.
    pub(super) fn start_prevote(&mut self) -> Result<()> {
        self.role = Role::PreCandidate;
        // A pre-candidate has no current leader (it's guessing at whether
        // it could win, not adopting anyone else's authority) — a stale
        // leader_id left over from before would misdirect a driver's client
        // redirects.
        self.leader_id = None;
        self.votes = BTreeSet::new();
        if self.voters.contains(&self.config.id) {
            self.votes.insert(self.config.id);
        }
        let req = RequestVoteReq {
            term: self.current_term() + 1,
            candidate_id: self.config.id,
            last_log_index: self.storage.last_index(),
            last_log_term: self.storage.last_term(),
            pre_vote: true,
        };
        let self_id = self.config.id;
        for &peer in &self.voters {
            if peer != self_id {
                self.outbox.push((peer, Message::RequestVote(req.clone())));
            }
        }
        self.reset_election_timer();
        // Solo cluster (peers == [self]): the self-vote seeded above
        // already meets quorum, but quorum is otherwise only re-checked
        // when a vote response arrives -- which a lone node never gets.
        // Promote immediately instead of hanging in PreCandidate forever.
        if self.votes.len() >= self.quorum() {
            self.become_candidate()?;
        }
        Ok(())
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
        if self.voters.contains(&self.config.id) {
            self.votes.insert(self.config.id);
        }
        self.reset_election_timer();

        let req = RequestVoteReq {
            term: new_term,
            candidate_id: self.config.id,
            last_log_index: self.storage.last_index(),
            last_log_term: self.storage.last_term(),
            pre_vote: false,
        };
        let self_id = self.config.id;
        for &peer in &self.voters {
            if peer != self_id {
                self.outbox.push((peer, Message::RequestVote(req.clone())));
            }
        }
        // Solo cluster: the self-vote seeded above already meets quorum;
        // there's no peer left to send a real-vote response, so promote
        // straight to leader instead of hanging in Candidate forever.
        if self.votes.len() >= self.quorum() {
            self.become_leader()?;
        }
        Ok(())
    }

    /// Wins the election: becomes leader, initializes per-peer replication
    /// state, and appends a no-op entry in the new term (the standard Raft
    /// technique for committing entries from prior terms indirectly).
    ///
    /// `broadcast_append()` (the AppendEntries heartbeat/replication
    /// broadcast) lives in `replication.rs`.
    fn become_leader(&mut self) -> Result<()> {
        self.role = Role::Leader;
        self.leader_id = Some(self.config.id);
        self.heartbeat_elapsed = 0;

        // A fresh term means every previously in-flight AppendEntries is
        // for a leadership/log-extent that may no longer be valid; discard
        // it rather than risk a stale success being popped against this
        // term's (re-initialized) next_index/match_index.
        self.inflight.clear();
        // A fresh leader must not inherit a prior incarnation's queued
        // reads — those were registered against a floor/quorum-contact
        // state that no longer means anything under this leadership.
        self.pending_reads.clear();
        // Same reasoning for the read-index send/ack barrier counters: a
        // stale send_count/ack_count relationship from a prior term (or
        // this node's prior leadership incarnation) says nothing about
        // contact under this term, and reusing it could let a leftover
        // ack_count already exceed a fresh read's barrier without this
        // term ever having sent anything.
        self.send_count.clear();
        self.ack_count.clear();

        let self_id = self.config.id;
        let next = self.storage.last_index() + 1;
        for &peer in &self.voters {
            if peer != self_id {
                self.next_index.insert(peer, next);
                self.match_index.insert(peer, 0);
            }
        }

        let noop = LogEntry::normal(self.current_term(), next, Vec::new());
        self.storage.append(std::slice::from_ref(&noop))?;
        self.next_index.insert(self_id, next + 1);
        self.match_index.insert(self_id, next);

        // A fresh leader can't serve linearizable reads until its own
        // no-op has committed, confirming it holds all previously
        // committed entries.
        self.readable_term = None;

        self.broadcast_append()?;
        // A solo leader (or, in general, a leader whose own append alone
        // forms a majority) gets no AppendEntriesResp to trigger commit
        // advancement from — handle_append_resp is the only other caller of
        // maybe_advance_commit, and a lone node never receives a reply.
        // Without this, its own no-op (and every write it accepts) would
        // never commit, readable_term would never get set, and reads would
        // never release. This is a no-op for a multi-node leader, whose
        // self-append alone can't reach quorum.
        self.maybe_advance_commit()
    }

    /// Steps down to follower. Persists the new term (and clears
    /// `voted_for`) *before* any message referencing the new term is
    /// emitted, but only when `term` actually advances `current_term` — a
    /// same-term step (e.g. discovering the current leader) must not touch
    /// storage.
    pub(super) fn become_follower(&mut self, term: Term, leader: Option<NodeId>) -> Result<()> {
        if term > self.current_term() {
            let hs = HardState {
                current_term: term,
                voted_for: None,
            };
            self.storage.save_hard_state(&hs)?;
        }
        self.role = Role::Follower;
        self.leader_id = leader;
        // Stepping down means this node is no longer tracking replication
        // progress for anyone; any in-flight AppendEntries bookkeeping is
        // now meaningless (and would be wrong to resurrect if this node
        // becomes leader again in a later term).
        self.inflight.clear();
        self.reset_election_timer();
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
                    // Echo OUR OWN (unbumped) current_term, not the
                    // candidate's prospective term (see the doc comment on
                    // `RequestVoteResp::pre_vote` for why term alone still
                    // can't be trusted as the discriminator: a peer already
                    // at T+1 could echo a pre-vote grant carrying term T+1,
                    // wire-identical in term to a real vote). `pre_vote: true`
                    // below is what actually lets a Candidate's tally in
                    // `handle_vote_resp` refuse to count this as a real vote,
                    // no matter what term it carries.
                    term: self.current_term(),
                    vote_granted: granted,
                    pre_vote: true,
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
                pre_vote: false,
            }),
        ));
        Ok(())
    }

    pub(super) fn handle_vote_resp(&mut self, from: NodeId, resp: RequestVoteResp) -> Result<()> {
        match self.role {
            Role::PreCandidate => {
                // A PreCandidate never sent a real RequestVote, so a
                // response with pre_vote == false is stale/foreign (e.g.
                // left over from a prior real candidacy in an earlier
                // term) and must not be tallied here.
                if !resp.pre_vote {
                    return Ok(());
                }
                // A pre-vote responder echoes its OWN current_term (see
                // handle_request_vote), not our prospective_term, so the
                // comparison here is against our own current_term too — a
                // peer at or behind our term can still legitimately grant a
                // pre-vote; only a peer ahead of us means we're stale.
                let current_term = self.current_term();
                if resp.term > current_term {
                    return self.become_follower(resp.term, None);
                }
                if !resp.vote_granted {
                    return Ok(());
                }
                self.votes.insert(from);
                if self.votes.len() >= self.quorum() {
                    self.become_candidate()?;
                }
                Ok(())
            }
            Role::Candidate => {
                // THE FIX (closes residual C1): a delayed pre-vote grant
                // must never count toward a real election's tally, even if
                // its term happens to equal our current_term (which it can
                // — a peer already at T+1 echoes a pre-vote grant carrying
                // term T+1). Routing by the explicit flag instead of by
                // term makes that coincidence harmless.
                if resp.pre_vote {
                    return Ok(());
                }
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
        s.append(&[LogEntry::normal(2, 1, vec![])]).unwrap();
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
        // Grant a pre-vote from peer 2. Under the fixed convention a
        // pre-vote responder echoes its OWN current_term, not the
        // candidate's prospective term — a fresh peer at term 0 grants with
        // term 0.
        c.step(
            2,
            Message::RequestVoteResp(RequestVoteResp {
                term: 0,
                vote_granted: true,
                pre_vote: true,
            }),
        )
        .unwrap();
        assert_eq!(c.role(), Role::Candidate); // pre-vote won -> real candidate (term 1)
        assert_eq!(c.current_term(), 1);
        let _ = c.ready();
        // Grant the REAL vote from peer 2: a real vote echoes the
        // candidate's (now bumped) current_term, so this carries term 1.
        c.step(
            2,
            Message::RequestVoteResp(RequestVoteResp {
                term: 1,
                vote_granted: true,
                pre_vote: false,
            }),
        )
        .unwrap();
        assert_eq!(c.role(), Role::Leader);
        // leader appends a no-op in its term
        assert!(c.commit_index() <= 1);
    }

    // C1 convention (whole-branch review): a pre-vote responder must echo
    // ITS OWN (unbumped) current_term, not the candidate's prospective
    // term — that's what lets a Candidate's tally (handle_vote_resp)
    // distinguish a genuine, timely pre-vote grant from one delivered late
    // after real candidacy has begun (see
    // prevote_straggler_is_not_counted_as_real_vote). This is the
    // happy-path companion: a real pre-vote round-trip through two nodes
    // (not a hand-built resp) that legitimately wins promotion.
    #[test]
    fn prevote_reply_echoes_responders_own_term_and_wins_real_round_trip() {
        // Node A: ticked past its election timeout -> PreCandidate at
        // prospective term 1 (current_term 0 + 1).
        let mut a = RaftCore::new(cfg(1, &[1, 2, 3]), MemStorage::default()).unwrap();
        for _ in 0..40 {
            a.tick().unwrap();
        }
        assert_eq!(a.role(), Role::PreCandidate);
        let a_ready = a.ready();
        let (to, req) = a_ready
            .messages
            .iter()
            .find_map(|(to, m)| match m {
                Message::RequestVote(rv) if rv.pre_vote => Some((*to, rv.clone())),
                _ => None,
            })
            .expect("A must emit a pre-vote RequestVote");
        assert_eq!(req.term, 1); // prospective term, current_term(A) is still 0

        // Node B: a fresh follower at term 0 answers A's real pre-vote req.
        let mut b = RaftCore::new(cfg(to, &[1, 2, 3]), MemStorage::default()).unwrap();
        b.step(1, Message::RequestVote(req)).unwrap();
        let b_ready = b.ready();
        let resp = b_ready
            .messages
            .iter()
            .find_map(|(_, m)| match m {
                Message::RequestVoteResp(r) => Some(r.clone()),
                _ => None,
            })
            .expect("B must emit a RequestVoteResp");
        assert!(resp.vote_granted);
        // The core assertion: B echoes ITS OWN unbumped current_term (0),
        // not A's prospective term (1).
        assert_eq!(resp.term, 0);
        assert_eq!(b.current_term(), 0); // pre-vote must not mutate B's term

        // Feed B's real response back into A.
        a.step(to, Message::RequestVoteResp(resp)).unwrap();
        assert_eq!(a.role(), Role::Candidate); // pre-vote quorum reached -> real candidate
    }

    // C1 (Critical, whole-branch review): a pre-vote grant that is
    // delivered LATE -- after the recipient has already promoted itself to
    // a real Candidate -- must never be misread as a real-vote grant for
    // the ongoing (real) election. If a pre-vote responder echoes the
    // CANDIDATE's prospective term (the pre-fix behavior) rather than its
    // own current term, a straggler carries the same term number the
    // Candidate is now running at and slips past the `resp.term ==
    // current_term` check in the Candidate branch of `handle_vote_resp`,
    // letting a node reach leadership on stragglers alone -- with peers
    // that never actually granted it a real vote. That's a second leader in
    // the same term waiting to happen.
    #[test]
    fn prevote_straggler_is_not_counted_as_real_vote() {
        // A: 3-node cluster, ticks past its election timeout into
        // PreCandidate (prospective term 1, current_term still 0).
        let mut a = RaftCore::new(cfg(1, &[1, 2, 3]), MemStorage::default()).unwrap();
        for _ in 0..40 {
            a.tick().unwrap();
        }
        assert_eq!(a.role(), Role::PreCandidate);
        let a_ready = a.ready();
        // A may have re-armed and re-sent pre-vote requests across several
        // election-timeout rounds within these 40 ticks (each round resets
        // its own deadline); only the latest request to each peer matters,
        // so keep the last one seen per peer rather than every round's.
        let mut pre_vote_reqs: BTreeMap<NodeId, RequestVoteReq> = BTreeMap::new();
        for (to, m) in &a_ready.messages {
            if let Message::RequestVote(rv) = m {
                if rv.pre_vote {
                    pre_vote_reqs.insert(*to, rv.clone());
                }
            }
        }
        assert_eq!(pre_vote_reqs.len(), 2, "A must pre-vote both peers");

        // B (id 2) and C (id 3): fresh followers at term 0, each answer A's
        // real pre-vote request through the real handler (not hand-built
        // responses), so this test reflects whatever convention the code
        // actually implements.
        let mut b = RaftCore::new(cfg(2, &[1, 2, 3]), MemStorage::default()).unwrap();
        let mut c = RaftCore::new(cfg(3, &[1, 2, 3]), MemStorage::default()).unwrap();
        let req_to_b = pre_vote_reqs[&2].clone();
        let req_to_c = pre_vote_reqs[&3].clone();
        b.step(1, Message::RequestVote(req_to_b)).unwrap();
        c.step(1, Message::RequestVote(req_to_c)).unwrap();
        let b_resp = b
            .ready()
            .messages
            .iter()
            .find_map(|(_, m)| match m {
                Message::RequestVoteResp(r) => Some(r.clone()),
                _ => None,
            })
            .expect("B must reply");
        let c_resp = c
            .ready()
            .messages
            .iter()
            .find_map(|(_, m)| match m {
                Message::RequestVoteResp(r) => Some(r.clone()),
                _ => None,
            })
            .expect("C must reply");
        assert!(b_resp.vote_granted && c_resp.vote_granted);

        // A processes B's grant first: reaches pre-vote quorum (self + B)
        // and promotes straight to a REAL Candidate at term 1, broadcasting
        // a real (non-pre-vote) RequestVote.
        a.step(2, Message::RequestVoteResp(b_resp)).unwrap();
        assert_eq!(a.role(), Role::Candidate);
        assert_eq!(a.current_term(), 1);
        let _ = a.ready(); // drain the real RequestVote broadcast

        // C's pre-vote grant was computed BEFORE A ever promoted, but is
        // only delivered now -- after A is already a real Candidate. It
        // must not be misinterpreted as a grant for the real election: A
        // must NOT reach leadership off a self-vote plus this straggler
        // alone.
        a.step(3, Message::RequestVoteResp(c_resp)).unwrap();
        assert_eq!(
            a.role(),
            Role::Candidate,
            "a delayed pre-vote grant must not, by itself, promote a Candidate to Leader"
        );
    }

    // Residual C1 (whole-branch review): the `prevote_straggler_is_not_counted_as_real_vote`
    // fix above relied on TERM to separate a straggling pre-vote grant from
    // a real vote -- but term alone breaks down the moment the straggler's
    // granter was itself already at the candidate's prospective term. A
    // peer at current_term == 1 that grants A's pre-vote echoes ITS OWN
    // current_term (1), which is wire-identical to a real vote cast for A's
    // term-1 election. Without routing strictly by the `pre_vote` flag, this
    // grant slips past the Candidate branch's `resp.term == current_term`
    // check and gets tallied as a real vote A never actually earned -- a
    // second leader in the same term. This test hand-builds that exact
    // interleaving and must fail if the Candidate branch doesn't filter
    // `resp.pre_vote == true`.
    #[test]
    fn prevote_grant_from_peer_at_prospective_term_is_not_a_real_vote() {
        let mut a = RaftCore::new(cfg(1, &[1, 2, 3]), MemStorage::default()).unwrap();
        for _ in 0..40 {
            a.tick().unwrap();
        }
        assert_eq!(a.role(), Role::PreCandidate);
        let _ = a.ready(); // drain the pre-vote RequestVote broadcast

        // Reach pre-vote quorum (self + peer 2) with an ordinary pre-vote
        // grant from a fresh peer at term 0 -> A promotes to real Candidate
        // at term 1.
        a.step(
            2,
            Message::RequestVoteResp(RequestVoteResp {
                term: 0,
                vote_granted: true,
                pre_vote: true,
            }),
        )
        .unwrap();
        assert_eq!(a.role(), Role::Candidate);
        assert_eq!(a.current_term(), 1);
        let _ = a.ready(); // drain the real RequestVote broadcast

        // A concurrent candidate elsewhere raced to current_term == 1 too,
        // and peer 3 granted THAT candidate's pre-vote, echoing peer 3's own
        // current_term (1) -- a pre-vote grant that happens to carry the
        // same term A is now running at. Fed to A, it must be ignored: A
        // must NOT become Leader off a self-vote plus this pre-vote grant
        // alone, because peer 3 never cast a real vote for A's election.
        a.step(
            3,
            Message::RequestVoteResp(RequestVoteResp {
                term: 1,
                vote_granted: true,
                pre_vote: true,
            }),
        )
        .unwrap();
        assert_eq!(
            a.role(),
            Role::Candidate,
            "a pre-vote grant carrying the candidate's own term must not be tallied as a real vote"
        );

        // A genuine real vote (pre_vote: false) at the same term must still
        // win, proving the fix filters by flag rather than breaking real
        // elections.
        a.step(
            3,
            Message::RequestVoteResp(RequestVoteResp {
                term: 1,
                vote_granted: true,
                pre_vote: false,
            }),
        )
        .unwrap();
        assert_eq!(a.role(), Role::Leader);
    }

    // Finding 2 (liveness gap): a single-node cluster has self already at
    // quorum the instant it seeds votes in start_prevote/become_candidate,
    // but quorum was previously only re-checked when a vote RESPONSE
    // arrived -- which a solo node never gets. It must self-promote all
    // the way to Leader in one tick sequence.
    //
    // I1: promotion alone isn't enough -- `maybe_advance_commit` was only
    // ever called from `handle_append_resp`, which a solo leader (no peers
    // to respond) never receives. Without a call from `become_leader`
    // itself, the solo leader's own no-op never commits, `readable_term`
    // never gets set, and reads can never release. Assert both halves:
    // promotion to Leader AND that its self-majority commits the no-op.
    #[test]
    fn single_node_cluster_self_promotes_to_leader() {
        let mut c = RaftCore::new(cfg(1, &[1]), MemStorage::default()).unwrap();
        for _ in 0..40 {
            c.tick().unwrap();
        }
        assert_eq!(c.role(), Role::Leader);
        assert_eq!(c.current_term(), 1);
        // The no-op appended in become_leader (index 1, term 1) must have
        // committed off self alone -- a solo node already IS a majority.
        assert_eq!(c.commit_index(), 1);
        let r = c.ready();
        assert_eq!(r.apply.len(), 1);
        assert_eq!(r.apply[0].index, 1);
        assert_eq!(r.apply[0].term, 1);
        // Readability follows: a fresh leader can't serve linearizable
        // reads until its own no-op has committed and applied.
        c.read_index(1).unwrap();
        let r2 = c.ready();
        assert_eq!(
            r2.reads,
            vec![1],
            "solo leader must be able to release a read once its no-op has committed"
        );
    }
}
