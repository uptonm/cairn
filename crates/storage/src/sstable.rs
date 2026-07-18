use crate::bloom::Bloom;
use crate::error::{Error, Result};
use crate::types::{InternalKey, Seqno};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

const MAGIC: &[u8; 8] = b"CAIRNSST";
const BLOOM_BITS_PER_KEY: usize = 10;

pub struct SsTableWriter {
    file: BufWriter<File>,
    index: Vec<(Vec<u8>, Seqno, u64)>,
    offset: u64,
}

pub struct SsTableReader {
    entries: Vec<(InternalKey, Option<Vec<u8>>)>,
    bloom: Bloom,
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
        let bloom_offset = index_offset + idx.len() as u64;

        let keys: Vec<&[u8]> = self.index.iter().map(|(k, _, _)| k.as_slice()).collect();
        let bloom = Bloom::build(&keys, BLOOM_BITS_PER_KEY);
        let bloom_bytes = bloom.to_bytes();
        self.file.write_all(&bloom_bytes)?;

        self.file.write_all(&index_offset.to_le_bytes())?;
        self.file.write_all(&bloom_offset.to_le_bytes())?;
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
        if len < 32 {
            return Err(Error::Corruption("sstable too short".into()));
        }
        file.seek(SeekFrom::End(-8))?;
        let mut magic = [0u8; 8];
        file.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(Error::Corruption("bad sstable magic".into()));
        }
        file.seek(SeekFrom::End(-32))?;
        let mut foot = [0u8; 24];
        file.read_exact(&mut foot)?;
        let index_offset = u64::from_le_bytes(foot[0..8].try_into().unwrap());
        let bloom_offset = u64::from_le_bytes(foot[8..16].try_into().unwrap());

        let footer_start = len - 32;
        if index_offset > bloom_offset || bloom_offset > footer_start {
            return Err(Error::Corruption("invalid sstable offsets".into()));
        }
        let bloom_len = usize::try_from(footer_start - bloom_offset)
            .map_err(|_| Error::Corruption("sstable bloom is too large".into()))?;
        file.seek(SeekFrom::Start(bloom_offset))?;
        let mut bloom_bytes = vec![0u8; bloom_len];
        file.read_exact(&mut bloom_bytes)?;
        let bloom = Bloom::from_bytes(&bloom_bytes)?;

        let mut r = BufReader::new(file);
        r.seek(SeekFrom::Start(0))?;
        let mut entries = Vec::new();
        let mut pos = 0u64;
        while pos < index_offset {
            if index_offset - pos < 4 {
                return Err(Error::Corruption("truncated sstable key length".into()));
            }
            let mut klen_b = [0u8; 4];
            r.read_exact(&mut klen_b)?;
            pos += 4;
            let klen = u64::from(u32::from_le_bytes(klen_b));
            if klen.saturating_add(9) > index_offset - pos {
                return Err(Error::Corruption(
                    "sstable key length exceeds data section".into(),
                ));
            }
            let klen = klen as usize;
            let mut key = vec![0u8; klen];
            r.read_exact(&mut key)?;
            pos += klen as u64;
            let mut seqno_b = [0u8; 8];
            r.read_exact(&mut seqno_b)?;
            let seqno = u64::from_le_bytes(seqno_b);
            let mut hv = [0u8; 1];
            r.read_exact(&mut hv)?;
            pos += 9;
            let value = if hv[0] == 1 {
                if index_offset - pos < 4 {
                    return Err(Error::Corruption("truncated sstable value length".into()));
                }
                let mut vlen_b = [0u8; 4];
                r.read_exact(&mut vlen_b)?;
                pos += 4;
                let vlen = u64::from(u32::from_le_bytes(vlen_b));
                if vlen > index_offset - pos {
                    return Err(Error::Corruption(
                        "sstable value length exceeds data section".into(),
                    ));
                }
                let vlen = vlen as usize;
                let mut v = vec![0u8; vlen];
                r.read_exact(&mut v)?;
                pos += vlen as u64;
                Some(v)
            } else {
                None
            };
            entries.push((
                InternalKey {
                    user_key: key,
                    seqno,
                },
                value,
            ));
        }
        Ok(SsTableReader { entries, bloom })
    }

    pub fn get(&self, user_key: &[u8]) -> Result<Option<(Seqno, Option<Vec<u8>>)>> {
        if !self.bloom.contains(user_key) {
            return Ok(None);
        }
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
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom};
    use tempfile::tempdir;

    fn ik(k: &[u8], s: Seqno) -> InternalKey {
        InternalKey {
            user_key: k.to_vec(),
            seqno: s,
        }
    }

    fn overwrite_u32(path: &Path, offset: u64, value: u32) {
        let mut file = OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&value.to_le_bytes()).unwrap();
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
    fn bloom_short_circuits_absent_keys() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("3.sst");
        let mut w = SsTableWriter::create(&path).unwrap();
        w.add(&ik(b"apple", 1), &Some(b"a".to_vec())).unwrap();
        w.add(&ik(b"banana", 1), &Some(b"b".to_vec())).unwrap();
        w.add(&ik(b"cherry", 1), &Some(b"c".to_vec())).unwrap();
        w.finish().unwrap();

        let r = SsTableReader::open(&path).unwrap();
        // present keys still resolve correctly
        assert_eq!(r.get(b"apple").unwrap(), Some((1, Some(b"a".to_vec()))));
        assert_eq!(r.get(b"banana").unwrap(), Some((1, Some(b"b".to_vec()))));
        assert_eq!(r.get(b"cherry").unwrap(), Some((1, Some(b"c".to_vec()))));
        // absent key: the bloom filter must reject it before the linear scan,
        // and either way get() must return Ok(None).
        assert!(!r.bloom.contains(b"durian"));
        assert_eq!(r.get(b"durian").unwrap(), None);
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

    #[test]
    fn oversized_key_length_returns_corruption() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("oversized-key.sst");
        let mut w = SsTableWriter::create(&path).unwrap();
        w.add(&ik(b"k", 1), &Some(b"v".to_vec())).unwrap();
        w.finish().unwrap();

        overwrite_u32(&path, 0, u32::MAX);

        assert!(matches!(
            SsTableReader::open(&path),
            Err(Error::Corruption(_))
        ));
    }

    #[test]
    fn oversized_value_length_returns_corruption() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("oversized-value.sst");
        let mut w = SsTableWriter::create(&path).unwrap();
        w.add(&ik(b"k", 1), &Some(b"v".to_vec())).unwrap();
        w.finish().unwrap();

        overwrite_u32(&path, 4 + 1 + 8 + 1, u32::MAX);

        assert!(matches!(
            SsTableReader::open(&path),
            Err(Error::Corruption(_))
        ));
    }
}
