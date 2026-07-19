use super::*;
use crate::error::Error;

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
        self.storage.save_snapshot(meta, &data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::{AppendEntriesResp, RequestVoteResp};
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
                b"first".to_vec()
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
                b"state".to_vec()
            ))
        );
        // Entries <= 2 are gone; entry 3 survives.
        assert_eq!(
            c.storage.entries_from(1),
            vec![LogEntry {
                term: 1,
                index: 3,
                command: b"set y=2".to_vec(),
            }]
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
