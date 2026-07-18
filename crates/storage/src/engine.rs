use crate::error::Result;
use crate::memtable::Memtable;
use crate::sstable::{SsTableReader, SsTableWriter};
use crate::types::Seqno;
use crate::wal::WalWriter;
use std::path::{Path, PathBuf};

const MEMTABLE_FLUSH_BYTES: usize = 4 * 1024 * 1024;

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
        let path = self.dir.join(format!("{id:06}.sst"));
        let mut w = SsTableWriter::create(&path)?;
        for (ik, value) in self.memtable.iter() {
            w.add(ik, value)?;
        }
        w.finish()?;
        self.sstables.insert(0, (id, SsTableReader::open(&path)?));

        // Reset memtable and WAL: durability now lives in the SSTable.
        self.memtable = Memtable::new();
        let wal_path = self.dir.join("wal.log");
        std::fs::remove_file(&wal_path)?;
        self.wal = WalWriter::create(&wal_path)?;
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
}
