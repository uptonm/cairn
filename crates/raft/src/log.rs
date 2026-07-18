use crate::error::Result;
use crate::oplog::{read_all, Op, OpWriter};
use crate::types::{LogEntry, LogIndex, SnapshotMeta, Term};
use std::path::Path;

pub struct RaftLog {
    writer: OpWriter,
    entries: Vec<LogEntry>,
    snapshot: SnapshotMeta,
}

impl RaftLog {
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("log.ops");
        let mut entries: Vec<LogEntry> = Vec::new();
        let mut snapshot = SnapshotMeta::default();
        for op in read_all(&path)? {
            apply_op(&mut entries, &mut snapshot, op);
        }
        let writer = OpWriter::create(&path)?;
        Ok(RaftLog {
            writer,
            entries,
            snapshot,
        })
    }

    pub fn append(&mut self, new_entries: &[LogEntry]) -> Result<()> {
        for entry in new_entries {
            debug_assert_eq!(
                entry.index,
                self.last_index() + 1,
                "log append must be contiguous"
            );
            self.writer.append(&Op::Append(entry.clone()))?;
            self.entries.push(entry.clone());
        }
        Ok(())
    }

    pub fn entry(&self, index: LogIndex) -> Option<&LogEntry> {
        if index <= self.snapshot.last_index {
            return None;
        }
        let pos = (index - self.snapshot.last_index - 1) as usize;
        self.entries.get(pos)
    }

    pub fn entries_from(&self, index: LogIndex) -> Vec<LogEntry> {
        let start = index.max(self.snapshot.last_index + 1);
        (start..=self.last_index())
            .filter_map(|i| self.entry(i).cloned())
            .collect()
    }

    pub fn last_index(&self) -> LogIndex {
        self.entries
            .last()
            .map_or(self.snapshot.last_index, |e| e.index)
    }

    pub fn last_term(&self) -> Term {
        self.entries
            .last()
            .map_or(self.snapshot.last_term, |e| e.term)
    }

    pub fn snapshot_meta(&self) -> SnapshotMeta {
        self.snapshot
    }

    pub fn truncate_suffix(&mut self, from_index: LogIndex) -> Result<()> {
        debug_assert!(
            from_index > self.snapshot.last_index,
            "cannot truncate into snapshot"
        );
        if from_index > self.last_index() {
            return Ok(());
        }
        self.writer.append(&Op::TruncateSuffix(from_index))?;
        self.entries.retain(|e| e.index < from_index);
        Ok(())
    }
}

fn apply_op(entries: &mut Vec<LogEntry>, snapshot: &mut SnapshotMeta, op: Op) {
    match op {
        Op::Append(e) => entries.push(e),
        Op::TruncateSuffix(from) => entries.retain(|e| e.index < from),
        Op::Compact { up_to, meta } => {
            entries.retain(|e| e.index > up_to);
            *snapshot = meta;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn e(term: u64, index: u64) -> LogEntry {
        LogEntry {
            term,
            index,
            command: vec![index as u8],
        }
    }

    #[test]
    fn append_and_read_back() {
        let dir = tempdir().unwrap();
        let mut log = RaftLog::open(dir.path()).unwrap();
        log.append(&[e(1, 1), e(1, 2), e(2, 3)]).unwrap();
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
        assert_eq!(log.entry(2), Some(&e(1, 2)));
        assert_eq!(log.entries_from(2), vec![e(1, 2), e(2, 3)]);
    }

    #[test]
    fn empty_log_reports_zero() {
        let dir = tempdir().unwrap();
        let log = RaftLog::open(dir.path()).unwrap();
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
        assert_eq!(log.entry(1), None);
    }

    #[test]
    fn truncate_suffix_removes_conflicting_tail() {
        let dir = tempdir().unwrap();
        let mut log = RaftLog::open(dir.path()).unwrap();
        log.append(&[e(1, 1), e(1, 2), e(1, 3), e(1, 4)]).unwrap();
        log.truncate_suffix(3).unwrap();
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.entry(3), None);
        // can append fresh entries at the truncated position
        log.append(&[e(5, 3)]).unwrap();
        assert_eq!(log.entry(3), Some(&e(5, 3)));
    }

    #[test]
    fn truncate_suffix_survives_reopen() {
        let dir = tempdir().unwrap();
        {
            let mut log = RaftLog::open(dir.path()).unwrap();
            log.append(&[e(1, 1), e(1, 2), e(1, 3)]).unwrap();
            log.truncate_suffix(2).unwrap();
        }
        let log = RaftLog::open(dir.path()).unwrap();
        assert_eq!(log.last_index(), 1);
        assert_eq!(log.entry(2), None);
    }
}
