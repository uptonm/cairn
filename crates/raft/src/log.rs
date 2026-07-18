use crate::error::{Error, Result};
use crate::oplog::{read_all_with_len, Op, OpWriter};
use crate::types::{LogEntry, LogIndex, SnapshotMeta, Term};
use std::fs::OpenOptions;
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
        let (ops, valid_len) = read_all_with_len(&path)?;
        for op in ops {
            apply_op(&mut entries, &mut snapshot, op);
        }
        // A crash can leave a torn/corrupt record past the valid prefix.
        // Truncate it away before opening the writer in append mode, or a
        // later append would land after the garbage and be lost again the
        // next time the file is replayed.
        if let Ok(meta) = std::fs::metadata(&path) {
            if meta.len() > valid_len {
                OpenOptions::new()
                    .write(true)
                    .open(&path)?
                    .set_len(valid_len)?;
            }
        }
        let writer = OpWriter::create(&path)?;
        Ok(RaftLog {
            writer,
            entries,
            snapshot,
        })
    }

    pub fn append(&mut self, new_entries: &[LogEntry]) -> Result<()> {
        for (expected, entry) in (self.last_index() + 1..).zip(new_entries.iter()) {
            if entry.index != expected {
                return Err(Error::Corruption(format!(
                    "log append must be contiguous: expected index {expected}, got {}",
                    entry.index
                )));
            }
        }
        for entry in new_entries {
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

    pub fn compact_prefix(&mut self, up_to: LogIndex, meta: SnapshotMeta) -> Result<()> {
        if up_to > self.last_index() {
            return Err(Error::Corruption(format!(
                "cannot compact past the log end: up_to {up_to} > last_index {}",
                self.last_index()
            )));
        }
        if meta.last_index != up_to {
            return Err(Error::Corruption(format!(
                "compact snapshot meta.last_index {} must equal up_to {up_to}",
                meta.last_index
            )));
        }
        self.writer.append(&Op::Compact { up_to, meta })?;
        self.entries.retain(|e| e.index > up_to);
        self.snapshot = meta;
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

    #[test]
    fn compact_prefix_drops_covered_entries() {
        let dir = tempdir().unwrap();
        let mut log = RaftLog::open(dir.path()).unwrap();
        log.append(&[e(1, 1), e(1, 2), e(2, 3), e(2, 4)]).unwrap();
        log.compact_prefix(
            2,
            SnapshotMeta {
                last_index: 2,
                last_term: 1,
            },
        )
        .unwrap();
        assert_eq!(log.entry(1), None);
        assert_eq!(log.entry(2), None);
        assert_eq!(log.entry(3), Some(&e(2, 3)));
        assert_eq!(log.last_index(), 4);
        assert_eq!(
            log.snapshot_meta(),
            SnapshotMeta {
                last_index: 2,
                last_term: 1
            }
        );
    }

    #[test]
    fn compact_to_empty_reports_snapshot_as_last() {
        let dir = tempdir().unwrap();
        let mut log = RaftLog::open(dir.path()).unwrap();
        log.append(&[e(1, 1), e(3, 2)]).unwrap();
        log.compact_prefix(
            2,
            SnapshotMeta {
                last_index: 2,
                last_term: 3,
            },
        )
        .unwrap();
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.last_term(), 3);
    }

    #[test]
    fn compaction_survives_reopen() {
        let dir = tempdir().unwrap();
        {
            let mut log = RaftLog::open(dir.path()).unwrap();
            log.append(&[e(1, 1), e(1, 2), e(2, 3)]).unwrap();
            log.compact_prefix(
                1,
                SnapshotMeta {
                    last_index: 1,
                    last_term: 1,
                },
            )
            .unwrap();
        }
        let log = RaftLog::open(dir.path()).unwrap();
        assert_eq!(log.entry(1), None);
        assert_eq!(log.entry(2), Some(&e(1, 2)));
        assert_eq!(
            log.snapshot_meta(),
            SnapshotMeta {
                last_index: 1,
                last_term: 1
            }
        );
    }

    #[test]
    fn torn_tail_is_truncated_so_later_appends_survive() {
        use std::io::Write;

        let dir = tempdir().unwrap();
        {
            let mut log = RaftLog::open(dir.path()).unwrap();
            log.append(&[e(1, 1), e(1, 2)]).unwrap();
        }

        // Simulate a crash mid-write: a valid record has begun (or partially
        // written) but never completed, leaving a torn tail on disk.
        let ops_path = dir.path().join("log.ops");
        let mut f = OpenOptions::new().append(true).open(&ops_path).unwrap();
        f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03])
            .unwrap();
        drop(f);

        {
            // Reopening must truncate the garbage tail away.
            let mut log = RaftLog::open(dir.path()).unwrap();
            assert_eq!(log.last_index(), 2);
            log.append(&[e(1, 3)]).unwrap();
        }

        let log = RaftLog::open(dir.path()).unwrap();
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.entry(3), Some(&e(1, 3)));
    }

    #[test]
    fn append_rejects_noncontiguous_index() {
        let dir = tempdir().unwrap();
        let mut log = RaftLog::open(dir.path()).unwrap();
        log.append(&[e(1, 1), e(1, 2)]).unwrap();

        let result = log.append(&[e(1, 4)]);
        assert!(matches!(result, Err(Error::Corruption(_))));

        // No partial mutation from the rejected append.
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.entry(4), None);
    }

    #[test]
    fn compact_rejects_up_to_past_end() {
        let dir = tempdir().unwrap();
        let mut log = RaftLog::open(dir.path()).unwrap();
        log.append(&[e(1, 1), e(1, 2)]).unwrap();

        let result = log.compact_prefix(
            5,
            SnapshotMeta {
                last_index: 5,
                last_term: 1,
            },
        );
        assert!(matches!(result, Err(Error::Corruption(_))));
        assert_eq!(log.last_index(), 2);
        assert_eq!(
            log.snapshot_meta(),
            SnapshotMeta {
                last_index: 0,
                last_term: 0
            }
        );
    }

    #[test]
    fn compact_rejects_meta_mismatch() {
        let dir = tempdir().unwrap();
        let mut log = RaftLog::open(dir.path()).unwrap();
        log.append(&[e(1, 1), e(1, 2), e(1, 3)]).unwrap();

        let result = log.compact_prefix(
            2,
            SnapshotMeta {
                last_index: 3,
                last_term: 1,
            },
        );
        assert!(matches!(result, Err(Error::Corruption(_))));
        assert_eq!(log.last_index(), 3);
        assert_eq!(
            log.snapshot_meta(),
            SnapshotMeta {
                last_index: 0,
                last_term: 0
            }
        );
    }
}
