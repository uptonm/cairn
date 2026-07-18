use crate::error::{Error, Result};
use crate::types::{HardState, LogEntry, LogIndex, SnapshotMeta, Term};

pub trait RaftStorage {
    fn hard_state(&self) -> HardState;
    fn save_hard_state(&mut self, hs: &HardState) -> Result<()>;
    fn last_index(&self) -> LogIndex;
    fn last_term(&self) -> Term;
    fn term(&self, index: LogIndex) -> Result<Option<Term>>;
    fn entries_from(&self, index: LogIndex) -> Vec<LogEntry>;
    fn snapshot_meta(&self) -> SnapshotMeta;
    fn append(&mut self, entries: &[LogEntry]) -> Result<()>;
    fn truncate_suffix(&mut self, index: LogIndex) -> Result<()>;
}

#[derive(Default)]
pub struct MemStorage {
    hs: HardState,
    entries: Vec<LogEntry>,
    snapshot: SnapshotMeta,
}

impl RaftStorage for MemStorage {
    fn hard_state(&self) -> HardState {
        self.hs.clone()
    }

    fn save_hard_state(&mut self, hs: &HardState) -> Result<()> {
        self.hs = hs.clone();
        Ok(())
    }

    fn last_index(&self) -> LogIndex {
        self.entries
            .last()
            .map_or(self.snapshot.last_index, |e| e.index)
    }

    fn last_term(&self) -> Term {
        self.entries
            .last()
            .map_or(self.snapshot.last_term, |e| e.term)
    }

    fn term(&self, index: LogIndex) -> Result<Option<Term>> {
        if index == 0 {
            return Ok(None);
        }
        if index < self.snapshot.last_index {
            return Ok(None);
        }
        if index == self.snapshot.last_index {
            return Ok(Some(self.snapshot.last_term));
        }
        // index > snapshot.last_index
        let pos = (index - self.snapshot.last_index - 1) as usize;
        Ok(self.entries.get(pos).map(|e| e.term))
    }

    fn entries_from(&self, index: LogIndex) -> Vec<LogEntry> {
        let start = index.max(self.snapshot.last_index + 1);
        (start..=self.last_index())
            .filter_map(|i| {
                let pos = (i - self.snapshot.last_index - 1) as usize;
                self.entries.get(pos).cloned()
            })
            .collect()
    }

    fn snapshot_meta(&self) -> SnapshotMeta {
        self.snapshot
    }

    fn append(&mut self, entries: &[LogEntry]) -> Result<()> {
        for (expected, entry) in (self.last_index() + 1..).zip(entries.iter()) {
            if entry.index != expected {
                return Err(Error::Corruption(format!(
                    "log append must be contiguous: expected index {expected}, got {}",
                    entry.index
                )));
            }
        }
        for entry in entries {
            self.entries.push(entry.clone());
        }
        Ok(())
    }

    fn truncate_suffix(&mut self, index: LogIndex) -> Result<()> {
        if index > self.last_index() {
            return Ok(());
        }
        self.entries.retain(|e| e.index < index);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LogEntry;

    fn e(term: Term, index: LogIndex) -> LogEntry {
        LogEntry {
            term,
            index,
            command: vec![],
        }
    }

    #[test]
    fn empty_storage_is_index0_term0() {
        let s = MemStorage::default();
        assert_eq!(s.last_index(), 0);
        assert_eq!(s.last_term(), 0);
        assert_eq!(s.term(0).unwrap(), None);
        assert_eq!(s.hard_state(), HardState::default());
    }

    #[test]
    fn append_then_read_back() {
        let mut s = MemStorage::default();
        s.append(&[e(1, 1), e(1, 2), e(2, 3)]).unwrap();
        assert_eq!(s.last_index(), 3);
        assert_eq!(s.last_term(), 2);
        assert_eq!(s.term(2).unwrap(), Some(1));
        assert_eq!(s.term(3).unwrap(), Some(2));
        assert_eq!(s.term(4).unwrap(), None);
        assert_eq!(s.entries_from(2), vec![e(1, 2), e(2, 3)]);
    }

    #[test]
    fn noncontiguous_append_is_corruption() {
        let mut s = MemStorage::default();
        assert!(s.append(&[e(1, 2)]).is_err());
    }

    #[test]
    fn truncate_suffix_drops_from_index() {
        let mut s = MemStorage::default();
        s.append(&[e(1, 1), e(1, 2), e(1, 3)]).unwrap();
        s.truncate_suffix(2).unwrap();
        assert_eq!(s.last_index(), 1);
        assert_eq!(s.term(2).unwrap(), None);
        s.truncate_suffix(9).unwrap(); // no-op past end
        assert_eq!(s.last_index(), 1);
    }

    #[test]
    fn save_and_load_hard_state() {
        let mut s = MemStorage::default();
        let hs = HardState {
            current_term: 4,
            voted_for: Some(2),
        };
        s.save_hard_state(&hs).unwrap();
        assert_eq!(s.hard_state(), hs);
    }
}
