use crate::error::Result;
use crate::memtable::Memtable;
use crate::sstable::{SsTableReader, SsTableWriter};
use crate::types::{InternalKey, Seqno};
use crate::wal::WalWriter;
use std::path::{Path, PathBuf};

const MEMTABLE_FLUSH_BYTES: usize = 4 * 1024 * 1024;
const MAX_SSTABLES_BEFORE_COMPACT: usize = 4;

pub struct Engine {
    dir: PathBuf,
    memtable: Memtable,
    wal: WalWriter,
    next_seqno: Seqno,
    // newest first
    sstables: Vec<(u64, SsTableReader)>,
    next_sst_id: u64,
}

impl Engine {
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let wal_path = dir.join("wal.log");
        let mut memtable = Memtable::new();
        let mut next_seqno: Seqno = 0;
        for (seqno, key, value) in WalWriter::read_all(&wal_path)? {
            memtable.put(key, value, seqno);
            next_seqno = next_seqno.max(seqno + 1);
        }
        let mut sst_ids: Vec<u64> = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(stem) = name.strip_suffix(".sst") {
                if let Ok(id) = stem.parse::<u64>() {
                    sst_ids.push(id);
                }
            }
        }
        sst_ids.sort_unstable();
        let mut sstables = Vec::new();
        for id in sst_ids.iter().rev() {
            let path = dir.join(format!("{id:06}.sst"));
            sstables.push((*id, SsTableReader::open(&path)?));
        }
        let next_sst_id = sst_ids.last().map_or(0, |m| m + 1);
        // `flush()` empties the WAL, so on reopen the WAL alone under-reports
        // next_seqno once anything has been flushed: the seqnos of flushed
        // entries only survive on disk inside the SSTables themselves. Scan
        // every loaded SSTable's entries too, so a reopen never reissues a
        // seqno that a compaction could later treat as "older" than a stale
        // pre-reopen value with a higher seqno.
        for (_, sst) in &sstables {
            for (ik, _) in sst.iter()? {
                next_seqno = next_seqno.max(ik.seqno + 1);
            }
        }
        let wal = WalWriter::create(&wal_path)?;
        Ok(Engine {
            dir: dir.to_path_buf(),
            memtable,
            wal,
            next_seqno,
            sstables,
            next_sst_id,
        })
    }

    fn write(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        let seqno = self.next_seqno;
        self.next_seqno += 1;
        self.wal.append(seqno, key, value)?;
        self.memtable
            .put(key.to_vec(), value.map(|v| v.to_vec()), seqno);
        if self.memtable.approx_size_bytes() >= MEMTABLE_FLUSH_BYTES {
            self.flush()?;
        }
        Ok(())
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write(key, Some(value))
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write(key, None)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(v) = self.memtable.get(key) {
            return Ok(v);
        }
        for (_, sst) in &self.sstables {
            if let Some((_, value)) = sst.get(key)? {
                return Ok(value);
            }
        }
        Ok(None)
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.memtable.approx_size_bytes() == 0 {
            return Ok(());
        }
        let id = self.next_sst_id;
        self.next_sst_id += 1;
        let final_path = self.dir.join(format!("{id:06}.sst"));
        let tmp_path = self.dir.join(format!("{id:06}.sst.tmp"));
        let mut w = SsTableWriter::create(&tmp_path)?;
        for (ik, value) in self.memtable.iter() {
            w.add(ik, value)?;
        }
        w.finish()?;
        // Atomically publish: a discoverable `.sst` is only ever a complete
        // file, since rename is atomic on POSIX. A crash before this point
        // leaves only an orphaned `.sst.tmp`, which discovery ignores.
        std::fs::rename(&tmp_path, &final_path)?;
        self.sstables
            .insert(0, (id, SsTableReader::open(&final_path)?));

        // Rotate the WAL before clearing the memtable: if WAL rotation
        // fails, the memtable must still hold the data (it's also safely
        // in the just-published SSTable, so this is redundant, not lost).
        let wal_path = self.dir.join("wal.log");
        std::fs::remove_file(&wal_path)?;
        self.wal = WalWriter::create(&wal_path)?;
        self.memtable = Memtable::new();

        if self.sstables.len() > MAX_SSTABLES_BEFORE_COMPACT {
            self.compact()?;
        }
        Ok(())
    }

    pub fn compact(&mut self) -> Result<()> {
        if self.sstables.len() < 2 {
            return Ok(());
        }
        // Merge every table's entries; entries per table are already sorted
        // newest-first within a key. Collect, sort by InternalKey, then keep
        // the first (newest) version seen per user_key and drop tombstones.
        let mut all: Vec<(InternalKey, Option<Vec<u8>>)> = Vec::new();
        for (_, sst) in &self.sstables {
            all.extend(sst.iter()?);
        }
        all.sort_by(|a, b| a.0.cmp(&b.0));

        let id = self.next_sst_id;
        self.next_sst_id += 1;
        let final_path = self.dir.join(format!("{id:06}.sst"));
        let tmp_path = self.dir.join(format!("{id:06}.sst.tmp"));
        let mut w = SsTableWriter::create(&tmp_path)?;
        let mut last_key: Option<Vec<u8>> = None;
        for (ik, value) in all {
            if last_key.as_deref() == Some(ik.user_key.as_slice()) {
                continue; // older version of a key we already wrote
            }
            last_key = Some(ik.user_key.clone());
            if value.is_none() {
                continue; // drop tombstone in a full merge
            }
            w.add(&ik, &value)?;
        }
        w.finish()?;
        // Atomically publish, same as flush(): a discoverable `.sst` is only
        // ever a complete file, since rename is atomic on POSIX. A crash
        // before this point leaves only an orphaned `.sst.tmp`, which
        // discovery ignores, so the store is never left with a truncated
        // compacted table.
        std::fs::rename(&tmp_path, &final_path)?;

        let old_ids: Vec<u64> = self.sstables.iter().map(|(id, _)| *id).collect();
        self.sstables = vec![(id, SsTableReader::open(&final_path)?)];
        for old in old_ids {
            // Propagate failures instead of swallowing them: a surviving old
            // table has a lower id, so it sorts after the new compacted
            // table, and a key whose newest version was a tombstone (and so
            // is absent from the compacted table) would fall through to the
            // stale value in that orphan and appear to un-delete. Fully
            // crash-atomic multi-file replacement (i.e. surviving a crash
            // between the rename above and these deletes) needs an on-disk
            // manifest of live SSTables; that's out of scope here and is
            // tracked for a later hardening/chaos-testing task.
            std::fs::remove_file(self.dir.join(format!("{old:06}.sst")))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn put_get_delete() {
        let dir = tempdir().unwrap();
        let mut e = Engine::open(dir.path()).unwrap();
        e.put(b"k", b"v").unwrap();
        assert_eq!(e.get(b"k").unwrap(), Some(b"v".to_vec()));
        e.delete(b"k").unwrap();
        assert_eq!(e.get(b"k").unwrap(), None);
    }

    #[test]
    fn reopen_recovers_from_wal() {
        let dir = tempdir().unwrap();
        {
            let mut e = Engine::open(dir.path()).unwrap();
            e.put(b"a", b"1").unwrap();
            e.put(b"b", b"2").unwrap();
        }
        let e = Engine::open(dir.path()).unwrap();
        assert_eq!(e.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(e.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn value_survives_flush() {
        let dir = tempdir().unwrap();
        let mut e = Engine::open(dir.path()).unwrap();
        e.put(b"k", b"v1").unwrap();
        e.flush().unwrap();
        assert_eq!(e.get(b"k").unwrap(), Some(b"v1".to_vec()));
    }

    #[test]
    fn newer_memtable_shadows_flushed_sstable() {
        let dir = tempdir().unwrap();
        let mut e = Engine::open(dir.path()).unwrap();
        e.put(b"k", b"old").unwrap();
        e.flush().unwrap();
        e.put(b"k", b"new").unwrap();
        assert_eq!(e.get(b"k").unwrap(), Some(b"new".to_vec()));
    }

    #[test]
    fn delete_after_flush_is_honored() {
        let dir = tempdir().unwrap();
        let mut e = Engine::open(dir.path()).unwrap();
        e.put(b"k", b"v").unwrap();
        e.flush().unwrap();
        e.delete(b"k").unwrap();
        e.flush().unwrap();
        assert_eq!(e.get(b"k").unwrap(), None);
    }

    #[test]
    fn unflushed_delete_shadows_flushed_value() {
        let dir = tempdir().unwrap();
        let mut e = Engine::open(dir.path()).unwrap();
        e.put(b"k", b"v").unwrap();
        e.flush().unwrap();
        e.delete(b"k").unwrap();
        assert_eq!(e.get(b"k").unwrap(), None);
    }

    #[test]
    fn sstables_reload_on_reopen() {
        let dir = tempdir().unwrap();
        {
            let mut e = Engine::open(dir.path()).unwrap();
            e.put(b"k", b"v").unwrap();
            e.flush().unwrap();
        }
        let e = Engine::open(dir.path()).unwrap();
        assert_eq!(e.get(b"k").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn compaction_keeps_newest_and_drops_tombstones() {
        let dir = tempdir().unwrap();
        let mut e = Engine::open(dir.path()).unwrap();
        e.put(b"a", b"old").unwrap();
        e.flush().unwrap();
        e.put(b"a", b"new").unwrap();
        e.put(b"b", b"1").unwrap();
        e.flush().unwrap();
        e.delete(b"b").unwrap();
        e.flush().unwrap();

        e.compact().unwrap();

        assert_eq!(e.get(b"a").unwrap(), Some(b"new".to_vec()));
        assert_eq!(e.get(b"b").unwrap(), None);
        // After full compaction only one SSTable remains.
        let ssts = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".sst"))
            .count();
        assert_eq!(ssts, 1);
    }

    #[test]
    fn data_survives_compaction_and_reopen() {
        let dir = tempdir().unwrap();
        {
            let mut e = Engine::open(dir.path()).unwrap();
            for i in 0..10u8 {
                e.put(&[i], &[i, i]).unwrap();
                e.flush().unwrap();
            }
            e.compact().unwrap();
        }
        let e = Engine::open(dir.path()).unwrap();
        for i in 0..10u8 {
            assert_eq!(e.get(&[i]).unwrap(), Some(vec![i, i]));
        }
    }

    #[test]
    fn seqno_recovered_across_reopen_prevents_stale_resurrection() {
        let dir = tempdir().unwrap();
        {
            // s0, s1; flush publishes k@1->C and empties the WAL.
            let mut e = Engine::open(dir.path()).unwrap();
            e.put(b"a", b"x").unwrap();
            e.put(b"k", b"C").unwrap();
            e.flush().unwrap();
        }
        {
            // Without SSTable-derived recovery, next_seqno would wrongly
            // reset to 0 here (the WAL is empty), so this put would reuse
            // seqno 0 and be outranked by the already-flushed k@1->C.
            let mut e = Engine::open(dir.path()).unwrap();
            e.put(b"k", b"Z").unwrap();
            e.flush().unwrap();
            e.compact().unwrap();

            assert_eq!(e.get(b"k").unwrap(), Some(b"Z".to_vec()));
            assert_eq!(e.get(b"a").unwrap(), Some(b"x".to_vec()));
        }
    }

    #[test]
    fn deleted_key_stays_absent_after_compaction_and_reopen() {
        let dir = tempdir().unwrap();
        {
            let mut e = Engine::open(dir.path()).unwrap();
            e.put(b"a", b"1").unwrap();
            e.flush().unwrap();
            e.put(b"b", b"keep").unwrap();
            e.flush().unwrap();
            e.delete(b"a").unwrap();
            e.flush().unwrap();

            e.compact().unwrap();
        }
        let e = Engine::open(dir.path()).unwrap();
        assert_eq!(e.get(b"a").unwrap(), None);
        assert_eq!(e.get(b"b").unwrap(), Some(b"keep".to_vec()));
    }
}
