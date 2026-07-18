use crate::error::{Error, Result};
use crate::types::Seqno;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::Path;

pub struct WalWriter {
    file: File,
}

pub type WalRecord = (Seqno, Vec<u8>, Option<Vec<u8>>);

impl WalWriter {
    pub fn create(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(WalWriter { file })
    }

    pub fn append(&mut self, seqno: Seqno, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        let mut body = Vec::new();
        body.extend_from_slice(&seqno.to_le_bytes());
        body.extend_from_slice(&(key.len() as u32).to_le_bytes());
        body.extend_from_slice(key);
        match value {
            Some(v) => {
                body.push(1u8);
                body.extend_from_slice(&(v.len() as u32).to_le_bytes());
                body.extend_from_slice(v);
            }
            None => body.push(0u8),
        }
        let crc = crc32fast::hash(&body);
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&body)?;
        self.file.sync_all()?;
        Ok(())
    }

    pub fn read_all(path: &Path) -> Result<Vec<WalRecord>> {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let file_len = file.metadata()?.len();
        let mut r = BufReader::new(file);
        let mut out = Vec::new();
        let mut valid_len = 0u64;
        loop {
            let mut crc_buf = [0u8; 4];
            match r.read_exact(&mut crc_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let expected_crc = u32::from_le_bytes(crc_buf);
            let remaining = file_len.saturating_sub(valid_len.saturating_add(4));
            match Self::read_body(&mut r, remaining)? {
                Some(body) if crc32fast::hash(&body) == expected_crc => {
                    valid_len += 4 + body.len() as u64;
                    out.push(Self::decode_body(&body)?);
                }
                _ => break, // torn or corrupt tail: stop, keep prefix
            }
        }
        Ok(out)
    }

    fn read_body<R: Read>(r: &mut R, remaining: u64) -> Result<Option<Vec<u8>>> {
        let mut remaining = remaining;
        let mut header = [0u8; 12]; // seqno(8) + klen(4)
        if remaining < header.len() as u64 {
            return Ok(None);
        }
        if read_full_or_eof(r, &mut header)?.is_none() {
            return Ok(None);
        }
        remaining -= header.len() as u64;
        let klen = u64::from(u32::from_le_bytes(header[8..12].try_into().unwrap()));
        if klen.saturating_add(1) > remaining {
            return Ok(None);
        }
        let klen = klen as usize;
        let mut key = vec![0u8; klen];
        if read_full_or_eof(r, &mut key)?.is_none() {
            return Ok(None);
        }
        remaining -= klen as u64;
        let mut has_value = [0u8; 1];
        if read_full_or_eof(r, &mut has_value)?.is_none() {
            return Ok(None);
        }
        remaining -= 1;
        let mut body = Vec::new();
        body.extend_from_slice(&header);
        body.extend_from_slice(&key);
        body.extend_from_slice(&has_value);
        if has_value[0] == 1 {
            let mut vlen_buf = [0u8; 4];
            if remaining < vlen_buf.len() as u64 {
                return Ok(None);
            }
            if read_full_or_eof(r, &mut vlen_buf)?.is_none() {
                return Ok(None);
            }
            remaining -= vlen_buf.len() as u64;
            let vlen = u64::from(u32::from_le_bytes(vlen_buf));
            if vlen > remaining {
                return Ok(None);
            }
            let vlen = vlen as usize;
            let mut value = vec![0u8; vlen];
            if read_full_or_eof(r, &mut value)?.is_none() {
                return Ok(None);
            }
            body.extend_from_slice(&vlen_buf);
            body.extend_from_slice(&value);
        }
        Ok(Some(body))
    }

    fn decode_body(body: &[u8]) -> Result<WalRecord> {
        let seqno = u64::from_le_bytes(body[0..8].try_into().unwrap());
        let klen = u32::from_le_bytes(body[8..12].try_into().unwrap()) as usize;
        let key = body[12..12 + klen].to_vec();
        let has_value = body[12 + klen];
        let value = if has_value == 1 {
            let vstart = 12 + klen + 1 + 4;
            Some(body[vstart..].to_vec())
        } else {
            None
        };
        Ok((seqno, key, value))
    }
}

fn read_full_or_eof<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<Option<()>> {
    match r.read_exact(buf) {
        Ok(()) => Ok(Some(())),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(Error::Io(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom};
    use tempfile::tempdir;

    #[test]
    fn append_then_replay_roundtrips() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let mut w = WalWriter::create(&path).unwrap();
        w.append(1, b"a", Some(b"1")).unwrap();
        w.append(2, b"b", None).unwrap();
        drop(w);

        let records = WalWriter::read_all(&path).unwrap();
        assert_eq!(
            records,
            vec![
                (1, b"a".to_vec(), Some(b"1".to_vec())),
                (2, b"b".to_vec(), None),
            ]
        );
    }

    #[test]
    fn replay_stops_at_torn_tail_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let mut w = WalWriter::create(&path).unwrap();
        w.append(1, b"a", Some(b"1")).unwrap();
        drop(w);
        // Simulate a crash mid-write: append 3 garbage bytes.
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[0xAB, 0xCD, 0xEF]).unwrap();
        drop(f);

        let records = WalWriter::read_all(&path).unwrap();
        assert_eq!(records, vec![(1, b"a".to_vec(), Some(b"1".to_vec()))]);
    }

    #[test]
    fn crc_mismatch_on_complete_body_drops_record_and_keeps_prefix() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let mut w = WalWriter::create(&path).unwrap();
        w.append(1, b"a", Some(b"1")).unwrap();
        let len_after_first = std::fs::metadata(&path).unwrap().len();
        w.append(100, b"second-key", Some(b"second-value")).unwrap();
        let len_after_second = std::fs::metadata(&path).unwrap().len();
        drop(w);

        // Flip a byte well inside the second record's body (past its 4-byte
        // CRC prefix), simulating bit-flip corruption of a fully-written
        // record rather than a truncated one.
        let second_record_len = len_after_second - len_after_first;
        assert!(
            second_record_len > 10,
            "test record too short to corrupt safely"
        );
        let flip_offset = len_after_first + 4 + second_record_len / 2;

        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        f.seek(SeekFrom::Start(flip_offset)).unwrap();
        let mut byte = [0u8; 1];
        f.read_exact(&mut byte).unwrap();
        f.seek(SeekFrom::Start(flip_offset)).unwrap();
        f.write_all(&[byte[0] ^ 0xFF]).unwrap();
        drop(f);

        let records = WalWriter::read_all(&path).unwrap();
        assert_eq!(records, vec![(1, b"a".to_vec(), Some(b"1".to_vec()))]);
    }

    #[test]
    fn truncation_mid_record_keeps_prior_records() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let mut w = WalWriter::create(&path).unwrap();
        w.append(1, b"a", Some(b"1")).unwrap();
        let len_after_first = std::fs::metadata(&path).unwrap().len();
        w.append(2, b"second-key", Some(b"second-value")).unwrap();
        drop(w);

        // Truncate partway into the second record: past its 4-byte CRC
        // prefix but before its seqno/key/value body is fully written,
        // simulating a crash mid-write of the second record.
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(len_after_first + 6).unwrap();
        drop(f);

        let records = WalWriter::read_all(&path).unwrap();
        assert_eq!(records, vec![(1, b"a".to_vec(), Some(b"1".to_vec()))]);
    }

    #[test]
    fn oversized_key_length_stops_at_valid_prefix() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let mut w = WalWriter::create(&path).unwrap();
        w.append(1, b"a", Some(b"1")).unwrap();
        drop(w);

        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&0u32.to_le_bytes()).unwrap();
        f.write_all(&2u64.to_le_bytes()).unwrap();
        f.write_all(&u32::MAX.to_le_bytes()).unwrap();
        drop(f);

        assert_eq!(
            WalWriter::read_all(&path).unwrap(),
            vec![(1, b"a".to_vec(), Some(b"1".to_vec()))]
        );
    }

    #[test]
    fn oversized_value_length_stops_at_valid_prefix() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let mut w = WalWriter::create(&path).unwrap();
        w.append(1, b"a", Some(b"1")).unwrap();
        drop(w);

        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&0u32.to_le_bytes()).unwrap();
        f.write_all(&2u64.to_le_bytes()).unwrap();
        f.write_all(&1u32.to_le_bytes()).unwrap();
        f.write_all(b"k").unwrap();
        f.write_all(&[1]).unwrap();
        f.write_all(&u32::MAX.to_le_bytes()).unwrap();
        drop(f);

        assert_eq!(
            WalWriter::read_all(&path).unwrap(),
            vec![(1, b"a".to_vec(), Some(b"1".to_vec()))]
        );
    }
}
