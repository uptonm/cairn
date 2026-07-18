use crate::error::{Error, Result};
use crate::types::{LogEntry, LogIndex, SnapshotMeta};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::Path;

#[derive(Clone, Debug, PartialEq)]
pub enum Op {
    Append(LogEntry),
    TruncateSuffix(LogIndex),
    Compact { up_to: LogIndex, meta: SnapshotMeta },
}

pub struct OpWriter {
    file: File,
}

impl OpWriter {
    pub fn create(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(OpWriter { file })
    }

    pub fn append(&mut self, op: &Op) -> Result<()> {
        let body = encode(op);
        let crc = crc32fast::hash(&body);
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&body)?;
        self.file.sync_all()?;
        Ok(())
    }
}

fn encode(op: &Op) -> Vec<u8> {
    let mut b = Vec::new();
    match op {
        Op::Append(e) => {
            b.push(0u8);
            b.extend_from_slice(&e.term.to_le_bytes());
            b.extend_from_slice(&e.index.to_le_bytes());
            b.extend_from_slice(&(e.command.len() as u32).to_le_bytes());
            b.extend_from_slice(&e.command);
        }
        Op::TruncateSuffix(from) => {
            b.push(1u8);
            b.extend_from_slice(&from.to_le_bytes());
        }
        Op::Compact { up_to, meta } => {
            b.push(2u8);
            b.extend_from_slice(&up_to.to_le_bytes());
            b.extend_from_slice(&meta.last_index.to_le_bytes());
            b.extend_from_slice(&meta.last_term.to_le_bytes());
        }
    }
    b
}

pub fn read_all(path: &Path) -> Result<Vec<Op>> {
    let (ops, _len) = read_all_with_len(path)?;
    Ok(ops)
}

/// Replays every valid record in the op-log at `path`, returning the ops
/// together with the byte offset just past the last valid record. Any bytes
/// beyond that offset are a torn or corrupt tail and were not included.
/// Returns `(Vec::new(), 0)` if the file does not exist.
pub fn read_all_with_len(path: &Path) -> Result<(Vec<Op>, u64)> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), 0)),
        Err(e) => return Err(Error::Io(e)),
    };
    let file_len = file.metadata()?.len();
    let mut r = BufReader::new(file);
    let mut out = Vec::new();
    let mut valid_len: u64 = 0;
    loop {
        let mut crc_buf = [0u8; 4];
        match r.read_exact(&mut crc_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(Error::Io(e)),
        }
        let expected_crc = u32::from_le_bytes(crc_buf);
        let remaining = file_len.saturating_sub(valid_len.saturating_add(4));
        match read_body(&mut r, remaining)? {
            Some(body) if crc32fast::hash(&body) == expected_crc => match decode(&body) {
                Some(op) => {
                    valid_len += 4 + body.len() as u64;
                    out.push(op);
                }
                None => break,
            },
            _ => break,
        }
    }
    Ok((out, valid_len))
}

fn read_exact_or_eof<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<bool> {
    match r.read_exact(buf) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(Error::Io(e)),
    }
}

fn read_body<R: Read>(r: &mut R, remaining: u64) -> Result<Option<Vec<u8>>> {
    let mut remaining = remaining;
    let mut tag = [0u8; 1];
    if remaining < tag.len() as u64 {
        return Ok(None);
    }
    if !read_exact_or_eof(r, &mut tag)? {
        return Ok(None);
    }
    remaining -= tag.len() as u64;
    let mut body = vec![tag[0]];
    match tag[0] {
        0 => {
            let mut fixed = [0u8; 20]; // term(8)+index(8)+clen(4)
            if remaining < fixed.len() as u64 {
                return Ok(None);
            }
            if !read_exact_or_eof(r, &mut fixed)? {
                return Ok(None);
            }
            remaining -= fixed.len() as u64;
            let clen = u64::from(u32::from_le_bytes(fixed[16..20].try_into().unwrap()));
            if clen > remaining {
                return Ok(None);
            }
            let clen = clen as usize;
            let mut cmd = vec![0u8; clen];
            if !read_exact_or_eof(r, &mut cmd)? {
                return Ok(None);
            }
            body.extend_from_slice(&fixed);
            body.extend_from_slice(&cmd);
        }
        1 => {
            let mut fixed = [0u8; 8];
            if remaining < fixed.len() as u64 {
                return Ok(None);
            }
            if !read_exact_or_eof(r, &mut fixed)? {
                return Ok(None);
            }
            body.extend_from_slice(&fixed);
        }
        2 => {
            let mut fixed = [0u8; 24];
            if remaining < fixed.len() as u64 {
                return Ok(None);
            }
            if !read_exact_or_eof(r, &mut fixed)? {
                return Ok(None);
            }
            body.extend_from_slice(&fixed);
        }
        _ => return Ok(None),
    }
    Ok(Some(body))
}

fn decode(body: &[u8]) -> Option<Op> {
    match body.first()? {
        0 => {
            let term = u64::from_le_bytes(body[1..9].try_into().ok()?);
            let index = u64::from_le_bytes(body[9..17].try_into().ok()?);
            let clen = u32::from_le_bytes(body[17..21].try_into().ok()?) as usize;
            let command = body.get(21..21 + clen)?.to_vec();
            Some(Op::Append(LogEntry {
                term,
                index,
                command,
            }))
        }
        1 => {
            let from = u64::from_le_bytes(body[1..9].try_into().ok()?);
            Some(Op::TruncateSuffix(from))
        }
        2 => {
            let up_to = u64::from_le_bytes(body[1..9].try_into().ok()?);
            let last_index = u64::from_le_bytes(body[9..17].try_into().ok()?);
            let last_term = u64::from_le_bytes(body[17..25].try_into().ok()?);
            Some(Op::Compact {
                up_to,
                meta: SnapshotMeta {
                    last_index,
                    last_term,
                },
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn entry(term: u64, index: u64, cmd: &[u8]) -> LogEntry {
        LogEntry {
            term,
            index,
            command: cmd.to_vec(),
        }
    }

    #[test]
    fn append_then_replay_roundtrips_all_op_kinds() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ops");
        let mut w = OpWriter::create(&path).unwrap();
        let ops = vec![
            Op::Append(entry(1, 1, b"a")),
            Op::Append(entry(1, 2, b"b")),
            Op::TruncateSuffix(2),
            Op::Compact {
                up_to: 1,
                meta: SnapshotMeta {
                    last_index: 1,
                    last_term: 1,
                },
            },
        ];
        for op in &ops {
            w.append(op).unwrap();
        }
        drop(w);
        assert_eq!(read_all(&path).unwrap(), ops);
    }

    #[test]
    fn replay_stops_at_torn_tail() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ops");
        let mut w = OpWriter::create(&path).unwrap();
        w.append(&Op::Append(entry(1, 1, b"a"))).unwrap();
        drop(w);
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[0xDE, 0xAD, 0xBE]).unwrap();
        drop(f);
        assert_eq!(
            read_all(&path).unwrap(),
            vec![Op::Append(entry(1, 1, b"a"))]
        );
    }

    #[test]
    fn oversized_command_length_stops_at_valid_prefix() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ops");
        let mut w = OpWriter::create(&path).unwrap();
        w.append(&Op::Append(entry(1, 1, b"a"))).unwrap();
        drop(w);

        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&0u32.to_le_bytes()).unwrap();
        f.write_all(&[0]).unwrap();
        f.write_all(&1u64.to_le_bytes()).unwrap();
        f.write_all(&2u64.to_le_bytes()).unwrap();
        f.write_all(&u32::MAX.to_le_bytes()).unwrap();
        drop(f);

        assert_eq!(
            read_all(&path).unwrap(),
            vec![Op::Append(entry(1, 1, b"a"))]
        );
    }
}
