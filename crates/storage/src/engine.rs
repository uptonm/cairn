use crate::error::Result;
use crate::memtable::Memtable;
use crate::types::Seqno;
use crate::wal::WalWriter;
use std::path::{Path, PathBuf};

pub struct Engine {
    #[allow(dead_code)]
    dir: PathBuf,
    memtable: Memtable,
    wal: WalWriter,
    next_seqno: Seqno,
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
        let wal = WalWriter::create(&wal_path)?;
        Ok(Engine {
            dir: dir.to_path_buf(),
            memtable,
            wal,
            next_seqno,
        })
    }

    fn write(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        let seqno = self.next_seqno;
        self.next_seqno += 1;
        self.wal.append(seqno, key, value)?;
        self.memtable
            .put(key.to_vec(), value.map(|v| v.to_vec()), seqno);
        Ok(())
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write(key, Some(value))
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write(key, None)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.memtable.get(key).flatten())
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
}
