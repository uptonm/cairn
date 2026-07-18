use crate::types::HardState;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

fn fsync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

pub fn save_hard_state(path: &Path, hs: &HardState) -> crate::Result<()> {
    let mut body = Vec::with_capacity(17);
    body.extend_from_slice(&hs.current_term.to_le_bytes());
    match hs.voted_for {
        Some(id) => {
            body.push(1u8);
            body.extend_from_slice(&id.to_le_bytes());
        }
        None => {
            body.push(0u8);
            body.extend_from_slice(&0u64.to_le_bytes());
        }
    }
    let crc = crc32fast::hash(&body);
    let tmp = path.with_extension("tmp");
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(&crc.to_le_bytes())?;
        f.write_all(&body)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    // Losing a persisted vote after restart can permit a double-vote, so
    // making this directory entry durable is election-safety-critical.
    let dir = path
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let _ = fsync_dir(dir);
    Ok(())
}

pub fn load_hard_state(path: &Path) -> crate::Result<HardState> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HardState::default()),
        Err(e) => return Err(crate::Error::Io(e)),
    };
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    if buf.len() != 4 + 17 {
        return Ok(HardState::default());
    }
    let expected_crc = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let body = &buf[4..];
    if crc32fast::hash(body) != expected_crc {
        return Ok(HardState::default());
    }
    let current_term = u64::from_le_bytes(body[0..8].try_into().unwrap());
    let has_vote = body[8];
    let voted_for = if has_vote == 1 {
        Some(u64::from_le_bytes(body[9..17].try_into().unwrap()))
    } else {
        None
    };
    Ok(HardState {
        current_term,
        voted_for,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[cfg(target_os = "linux")]
    #[test]
    fn fsync_dir_accepts_directory() {
        let dir = tempdir().unwrap();
        fsync_dir(dir.path()).unwrap();
    }

    #[test]
    fn save_then_load_roundtrips() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("hardstate");
        let hs = HardState {
            current_term: 7,
            voted_for: Some(3),
        };
        save_hard_state(&path, &hs).unwrap();
        assert_eq!(load_hard_state(&path).unwrap(), hs);
    }

    #[test]
    fn missing_file_loads_default() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nope");
        assert_eq!(load_hard_state(&path).unwrap(), HardState::default());
    }

    #[test]
    fn no_vote_roundtrips() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("hardstate");
        let hs = HardState {
            current_term: 2,
            voted_for: None,
        };
        save_hard_state(&path, &hs).unwrap();
        assert_eq!(load_hard_state(&path).unwrap(), hs);
    }
}
