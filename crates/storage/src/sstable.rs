use crate::error::{Error, Result};
use crate::types::{InternalKey, Seqno};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

const MAGIC: &[u8; 8] = b"CAIRNSST";

pub struct SsTableWriter {
    file: BufWriter<File>,
    index: Vec<(Vec<u8>, Seqno, u64)>,
    offset: u64,
}

pub struct SsTableReader {
    entries: Vec<(InternalKey, Option<Vec<u8>>)>,
}

impl SsTableWriter {
    pub fn create(path: &Path) -> Result<Self> {
        Ok(SsTableWriter {
            file: BufWriter::new(File::create(path)?),
            index: Vec::new(),
            offset: 0,
        })
    }

    pub fn add(&mut self, ik: &InternalKey, value: &Option<Vec<u8>>) -> Result<()> {
        let start = self.offset;
        let mut buf = Vec::new();
        buf.extend_from_slice(&(ik.user_key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&ik.user_key);
        buf.extend_from_slice(&ik.seqno.to_le_bytes());
        match value {
            Some(v) => {
                buf.push(1u8);
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                buf.extend_from_slice(v);
            }
            None => buf.push(0u8),
        }
        self.file.write_all(&buf)?;
        self.offset += buf.len() as u64;
        self.index.push((ik.user_key.clone(), ik.seqno, start));
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        let index_offset = self.offset;
        let num = self.index.len() as u64;
        let mut idx = Vec::new();
        for (key, seqno, off) in &self.index {
            idx.extend_from_slice(&(key.len() as u32).to_le_bytes());
            idx.extend_from_slice(key);
            idx.extend_from_slice(&seqno.to_le_bytes());
            idx.extend_from_slice(&off.to_le_bytes());
        }
        self.file.write_all(&idx)?;
        self.file.write_all(&index_offset.to_le_bytes())?;
        self.file.write_all(&num.to_le_bytes())?;
        self.file.write_all(MAGIC)?;
        self.file.flush()?;
        self.file.get_ref().sync_all()?;
        Ok(())
    }
}

impl SsTableReader {
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;
        let len = file.metadata()?.len();
        if len < 24 {
            return Err(Error::Corruption("sstable too short".into()));
        }
        file.seek(SeekFrom::End(-8))?;
        let mut magic = [0u8; 8];
        file.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(Error::Corruption("bad sstable magic".into()));
        }
        file.seek(SeekFrom::End(-24))?;
        let mut foot = [0u8; 16];
        file.read_exact(&mut foot)?;
        let index_offset = u64::from_le_bytes(foot[0..8].try_into().unwrap());

        let mut r = BufReader::new(file);
        r.seek(SeekFrom::Start(0))?;
        let mut entries = Vec::new();
        let mut pos = 0u64;
        while pos < index_offset {
            let mut klen_b = [0u8; 4];
            r.read_exact(&mut klen_b)?;
            let klen = u32::from_le_bytes(klen_b) as usize;
            let mut key = vec![0u8; klen];
            r.read_exact(&mut key)?;
            let mut seqno_b = [0u8; 8];
            r.read_exact(&mut seqno_b)?;
            let seqno = u64::from_le_bytes(seqno_b);
            let mut hv = [0u8; 1];
            r.read_exact(&mut hv)?;
            let (value, consumed_val) = if hv[0] == 1 {
                let mut vlen_b = [0u8; 4];
                r.read_exact(&mut vlen_b)?;
                let vlen = u32::from_le_bytes(vlen_b) as usize;
                let mut v = vec![0u8; vlen];
                r.read_exact(&mut v)?;
                (Some(v), 4 + vlen)
            } else {
                (None, 0)
            };
            pos += (4 + klen + 8 + 1 + consumed_val) as u64;
            entries.push((
                InternalKey {
                    user_key: key,
                    seqno,
                },
                value,
            ));
        }
        Ok(SsTableReader { entries })
    }

    pub fn get(&self, user_key: &[u8]) -> Result<Option<(Seqno, Option<Vec<u8>>)>> {
        // entries are sorted; newest version of a key sorts first.
        Ok(self
            .entries
            .iter()
            .find(|(ik, _)| ik.user_key == user_key)
            .map(|(ik, v)| (ik.seqno, v.clone())))
    }

    pub fn iter(&self) -> Result<Vec<(InternalKey, Option<Vec<u8>>)>> {
        Ok(self.entries.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn ik(k: &[u8], s: Seqno) -> InternalKey {
        InternalKey {
            user_key: k.to_vec(),
            seqno: s,
        }
    }

    #[test]
    fn write_then_read_point_lookup() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("1.sst");
        let mut w = SsTableWriter::create(&path).unwrap();
        // sorted: a@2, a@1, b@3
        w.add(&ik(b"a", 2), &Some(b"a2".to_vec())).unwrap();
        w.add(&ik(b"a", 1), &Some(b"a1".to_vec())).unwrap();
        w.add(&ik(b"b", 3), &None).unwrap();
        w.finish().unwrap();

        let r = SsTableReader::open(&path).unwrap();
        assert_eq!(r.get(b"a").unwrap(), Some((2, Some(b"a2".to_vec()))));
        assert_eq!(r.get(b"b").unwrap(), Some((3, None)));
        assert_eq!(r.get(b"missing").unwrap(), None);
    }

    #[test]
    fn iter_yields_sorted_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("2.sst");
        let mut w = SsTableWriter::create(&path).unwrap();
        w.add(&ik(b"a", 1), &Some(b"x".to_vec())).unwrap();
        w.add(&ik(b"b", 1), &Some(b"y".to_vec())).unwrap();
        w.finish().unwrap();
        let r = SsTableReader::open(&path).unwrap();
        let got: Vec<_> = r
            .iter()
            .unwrap()
            .into_iter()
            .map(|(k, _)| (k.user_key, k.seqno))
            .collect();
        assert_eq!(got, vec![(b"a".to_vec(), 1), (b"b".to_vec(), 1)]);
    }
}
