# LSM Storage Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a durable, ordered, crash-recoverable log-structured merge-tree (LSM) key-value storage engine in Rust — the foundational local store that Cairn's Raft state machine will later write into.

**Architecture:** Writes go to an append-only WAL (fsync'd) and an in-memory ordered memtable. When the memtable fills, it flushes to an immutable on-disk SSTable and the WAL rotates. Reads check the memtable, then SSTables newest-first, resolved by sequence number so the newest version of a key wins. Background leveled compaction merges SSTables and drops obsolete versions. This is the bottom layer of the Cairn spec (`docs/superpowers/specs/2026-07-18-cairn-distributed-kv-design.md`).

**Tech Stack:** Rust (stable), `cargo` workspace, `crc32fast` for WAL record checksums, `tempfile` (dev) for test isolation. No async here — the engine is synchronous; tokio enters at the Raft/transport layer in a later plan.

## Global Constraints

- Rust edition 2021, resolver 2. Workspace at repo root; this crate is `crates/storage`, package name `cairn-storage`.
- `cargo clippy --all-targets -- -D warnings` must pass at every commit.
- `cargo fmt` clean at every commit.
- No `unsafe`. No `.unwrap()` / `.expect()` in library code paths that handle I/O or untrusted on-disk bytes — return `Result` with the crate error type. `unwrap` is allowed in tests.
- Keys and values are `Vec<u8>` (arbitrary bytes), not `String`.
- Every SSTable and WAL record is checksummed; a checksum mismatch on read is a recoverable error, never a panic.
- Sequence numbers (`Seqno`) are `u64`, monotonically increasing per engine, never reused.

---

### Task 1: Workspace + crate scaffold and core types

**Files:**
- Create: `Cargo.toml` (workspace root)
- Create: `crates/storage/Cargo.toml`
- Create: `crates/storage/src/lib.rs`
- Create: `crates/storage/src/types.rs`
- Create: `crates/storage/src/error.rs`

**Interfaces:**
- Produces: `type Seqno = u64`; `enum Error` (crate error) with `Io(std::io::Error)`, `Corruption(String)`; `type Result<T> = std::result::Result<T, Error>`; `struct InternalKey { user_key: Vec<u8>, seqno: Seqno }` with ordering: ascending by `user_key`, then **descending** by `seqno` (newest version sorts first within a key).

- [ ] **Step 1: Scaffold the workspace and crate**

```bash
cd ~/Projects/cairn
cat > Cargo.toml <<'EOF'
[workspace]
resolver = "2"
members = ["crates/storage"]
EOF
mkdir -p crates/storage/src
cat > crates/storage/Cargo.toml <<'EOF'
[package]
name = "cairn-storage"
version = "0.1.0"
edition = "2021"

[dependencies]
crc32fast = "1"

[dev-dependencies]
tempfile = "3"
EOF
```

- [ ] **Step 2: Write the failing test for `InternalKey` ordering**

Create `crates/storage/src/types.rs`:

```rust
pub type Seqno = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InternalKey {
    pub user_key: Vec<u8>,
    pub seqno: Seqno,
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.user_key
            .cmp(&other.user_key)
            .then(other.seqno.cmp(&self.seqno)) // newer seqno sorts first
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newer_version_of_same_key_sorts_first() {
        let older = InternalKey { user_key: b"a".to_vec(), seqno: 1 };
        let newer = InternalKey { user_key: b"a".to_vec(), seqno: 2 };
        assert!(newer < older);
    }

    #[test]
    fn different_keys_sort_by_user_key() {
        let a = InternalKey { user_key: b"a".to_vec(), seqno: 9 };
        let b = InternalKey { user_key: b"b".to_vec(), seqno: 1 };
        assert!(a < b);
    }
}
```

- [ ] **Step 3: Add the error type and wire up `lib.rs`**

Create `crates/storage/src/error.rs`:

```rust
#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Corruption(String),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io: {e}"),
            Error::Corruption(m) => write!(f, "corruption: {m}"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;
```

Create `crates/storage/src/lib.rs`:

```rust
pub mod error;
pub mod types;

pub use error::{Error, Result};
pub use types::{InternalKey, Seqno};
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p cairn-storage`
Expected: PASS (2 tests in `types`).

- [ ] **Step 5: Lint, format, commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add -A
git commit -m "feat(storage): scaffold engine crate with InternalKey ordering"
```

---

### Task 2: Memtable

**Files:**
- Create: `crates/storage/src/memtable.rs`
- Modify: `crates/storage/src/lib.rs` (add `pub mod memtable;`)

**Interfaces:**
- Consumes: `InternalKey`, `Seqno` from Task 1.
- Produces: `struct Memtable`; `Memtable::new()`; `put(&mut self, key: Vec<u8>, value: Option<Vec<u8>>, seqno: Seqno)` where `value: None` is a tombstone (delete); `get(&self, key: &[u8]) -> Option<Option<Vec<u8>>>` returning `None` if the key is absent, `Some(None)` if the newest version is a tombstone, `Some(Some(v))` if present; `iter(&self) -> impl Iterator<Item = (&InternalKey, &Option<Vec<u8>>)>` in sorted order; `approx_size_bytes(&self) -> usize`.

- [ ] **Step 1: Write failing tests**

Create `crates/storage/src/memtable.rs`:

```rust
use crate::types::{InternalKey, Seqno};
use std::collections::BTreeMap;

pub struct Memtable {
    map: BTreeMap<InternalKey, Option<Vec<u8>>>,
    size: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_returns_newest_version() {
        let mut m = Memtable::new();
        m.put(b"k".to_vec(), Some(b"v1".to_vec()), 1);
        m.put(b"k".to_vec(), Some(b"v2".to_vec()), 2);
        assert_eq!(m.get(b"k"), Some(Some(b"v2".to_vec())));
    }

    #[test]
    fn tombstone_shadows_older_value() {
        let mut m = Memtable::new();
        m.put(b"k".to_vec(), Some(b"v".to_vec()), 1);
        m.put(b"k".to_vec(), None, 2);
        assert_eq!(m.get(b"k"), Some(None));
    }

    #[test]
    fn absent_key_returns_none() {
        let m = Memtable::new();
        assert_eq!(m.get(b"missing"), None);
    }

    #[test]
    fn iter_is_sorted_newest_first_within_key() {
        let mut m = Memtable::new();
        m.put(b"b".to_vec(), Some(b"x".to_vec()), 1);
        m.put(b"a".to_vec(), Some(b"y".to_vec()), 2);
        m.put(b"a".to_vec(), Some(b"z".to_vec()), 5);
        let keys: Vec<_> = m.iter().map(|(k, _)| (k.user_key.clone(), k.seqno)).collect();
        assert_eq!(keys, vec![
            (b"a".to_vec(), 5),
            (b"a".to_vec(), 2),
            (b"b".to_vec(), 1),
        ]);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p cairn-storage memtable`
Expected: FAIL — `Memtable::new` and methods not found.

- [ ] **Step 3: Implement `Memtable`**

Add above the `#[cfg(test)]` block in `memtable.rs`:

```rust
impl Memtable {
    pub fn new() -> Self {
        Memtable { map: BTreeMap::new(), size: 0 }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Option<Vec<u8>>, seqno: Seqno) {
        self.size += key.len() + value.as_ref().map_or(0, |v| v.len()) + 16;
        self.map.insert(InternalKey { user_key: key, seqno }, value);
    }

    pub fn get(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        // Newest version sorts first for a given user_key.
        self.map
            .range(InternalKey { user_key: key.to_vec(), seqno: Seqno::MAX }..)
            .next()
            .filter(|(ik, _)| ik.user_key == key)
            .map(|(_, v)| v.clone())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&InternalKey, &Option<Vec<u8>>)> {
        self.map.iter()
    }

    pub fn approx_size_bytes(&self) -> usize {
        self.size
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}
```

Add `pub mod memtable;` to `lib.rs`.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p cairn-storage memtable`
Expected: PASS (4 tests).

- [ ] **Step 5: Lint, format, commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add -A
git commit -m "feat(storage): memtable with seqno-versioned get and sorted iter"
```

---

### Task 3: Write-ahead log (WAL) with checksummed records and replay

**Files:**
- Create: `crates/storage/src/wal.rs`
- Modify: `crates/storage/src/lib.rs` (add `pub mod wal;`)

**Interfaces:**
- Consumes: `Seqno`, `Result`, `Error` from Task 1.
- Produces: `struct WalWriter` with `create(path)`, `append(&mut self, seqno: Seqno, key: &[u8], value: Option<&[u8]>) -> Result<()>` (fsyncs), and `fn read_all(path) -> Result<Vec<(Seqno, Vec<u8>, Option<Vec<u8>>)>>` that replays records and stops cleanly at the first torn/short tail record (crash-truncation tolerance).
- Record framing: `[u32 crc][u64 seqno][u32 klen][key][u8 has_value][u32 vlen][value]`, little-endian. `has_value == 0` means tombstone (vlen absent).

- [ ] **Step 1: Write failing tests**

Create `crates/storage/src/wal.rs`:

```rust
use crate::error::{Error, Result};
use crate::types::Seqno;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::Path;

pub struct WalWriter {
    file: File,
}

pub type WalRecord = (Seqno, Vec<u8>, Option<Vec<u8>>);

#[cfg(test)]
mod tests {
    use super::*;
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
        assert_eq!(records, vec![
            (1, b"a".to_vec(), Some(b"1".to_vec())),
            (2, b"b".to_vec(), None),
        ]);
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
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p cairn-storage wal`
Expected: FAIL — `WalWriter` methods not found.

- [ ] **Step 3: Implement the WAL**

Add above the test module in `wal.rs`:

```rust
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
        let mut r = BufReader::new(file);
        let mut out = Vec::new();
        loop {
            let mut crc_buf = [0u8; 4];
            match r.read_exact(&mut crc_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let expected_crc = u32::from_le_bytes(crc_buf);
            match Self::read_body(&mut r)? {
                Some(body) if crc32fast::hash(&body) == expected_crc => {
                    out.push(Self::decode_body(&body)?);
                }
                _ => break, // torn or corrupt tail: stop, keep prefix
            }
        }
        Ok(out)
    }

    fn read_body<R: Read>(r: &mut R) -> Result<Option<Vec<u8>>> {
        let mut header = [0u8; 12]; // seqno(8) + klen(4)
        if read_full_or_eof(r, &mut header)?.is_none() {
            return Ok(None);
        }
        let klen = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
        let mut key = vec![0u8; klen];
        if read_full_or_eof(r, &mut key)?.is_none() {
            return Ok(None);
        }
        let mut has_value = [0u8; 1];
        if read_full_or_eof(r, &mut has_value)?.is_none() {
            return Ok(None);
        }
        let mut body = Vec::new();
        body.extend_from_slice(&header);
        body.extend_from_slice(&key);
        body.extend_from_slice(&has_value);
        if has_value[0] == 1 {
            let mut vlen_buf = [0u8; 4];
            if read_full_or_eof(r, &mut vlen_buf)?.is_none() {
                return Ok(None);
            }
            let vlen = u32::from_le_bytes(vlen_buf) as usize;
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
```

Add `pub mod wal;` to `lib.rs`.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p cairn-storage wal`
Expected: PASS (2 tests).

- [ ] **Step 5: Lint, format, commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add -A
git commit -m "feat(storage): checksummed WAL with crash-tolerant replay"
```

---

### Task 4: Engine skeleton — WAL-backed memtable with recovery

**Files:**
- Create: `crates/storage/src/engine.rs`
- Modify: `crates/storage/src/lib.rs` (add `pub mod engine;` and re-export `Engine`)

**Interfaces:**
- Consumes: `Memtable` (Task 2), `WalWriter` (Task 3), `Result`.
- Produces: `struct Engine`; `Engine::open(dir: &Path) -> Result<Engine>` (replays `dir/wal.log` into a memtable, restores `next_seqno`); `put(&mut self, key: &[u8], value: &[u8]) -> Result<()>`; `delete(&mut self, key: &[u8]) -> Result<()>`; `get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>` (returns `None` for absent or tombstoned). Durability: `put`/`delete` write WAL (fsync) before updating the memtable.

- [ ] **Step 1: Write failing tests**

Create `crates/storage/src/engine.rs`:

```rust
use crate::error::Result;
use crate::memtable::Memtable;
use crate::types::Seqno;
use crate::wal::WalWriter;
use std::path::{Path, PathBuf};

pub struct Engine {
    dir: PathBuf,
    memtable: Memtable,
    wal: WalWriter,
    next_seqno: Seqno,
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
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p cairn-storage engine`
Expected: FAIL — `Engine::open` not found.

- [ ] **Step 3: Implement the engine skeleton**

Add above the test module in `engine.rs`:

```rust
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
        Ok(Engine { dir: dir.to_path_buf(), memtable, wal, next_seqno })
    }

    fn write(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        let seqno = self.next_seqno;
        self.next_seqno += 1;
        self.wal.append(seqno, key, value)?;
        self.memtable.put(key.to_vec(), value.map(|v| v.to_vec()), seqno);
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
```

Add to `lib.rs`:

```rust
pub mod engine;
pub use engine::Engine;
```

Note: the `dir` field is unused until Task 6 (SSTable flush). Prefix-allow it if clippy complains:
add `#[allow(dead_code)]` on the `dir` field for now; Task 6 removes the allow.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p cairn-storage engine`
Expected: PASS (2 tests).

- [ ] **Step 5: Lint, format, commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add -A
git commit -m "feat(storage): WAL-backed engine with put/get/delete and recovery"
```

---

### Task 5: SSTable writer and reader

**Files:**
- Create: `crates/storage/src/sstable.rs`
- Modify: `crates/storage/src/lib.rs` (add `pub mod sstable;`)

**Interfaces:**
- Consumes: `InternalKey`, `Seqno`, `Result`, `Error`.
- Produces: `SsTableWriter::create(path) -> Result<Self>`, `add(&mut self, ik: &InternalKey, value: &Option<Vec<u8>>) -> Result<()>` (entries MUST be added in `InternalKey` sorted order), `finish(self) -> Result<()>` (writes a trailing index + magic footer). `SsTableReader::open(path) -> Result<Self>`, `get(&self, user_key: &[u8]) -> Result<Option<(Seqno, Option<Vec<u8>>)>>` returning the newest version in this file for `user_key`, and `iter(&self) -> Result<Vec<(InternalKey, Option<Vec<u8>>)>>` in sorted order.
- On-disk layout: a sequence of length-prefixed `[u32 klen][key][u64 seqno][u8 has_value][u32 vlen?][value?]` entries, then a footer `[u64 index_offset][u64 num_entries][8-byte magic "CAIRNSST"]`. For this task the "index" is a full copy of `(user_key, seqno, file_offset)` for every entry (block index comes in Task 7's bloom task if needed; keep it simple and correct first).

- [ ] **Step 1: Write failing tests**

Create `crates/storage/src/sstable.rs`:

```rust
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn ik(k: &[u8], s: Seqno) -> InternalKey {
        InternalKey { user_key: k.to_vec(), seqno: s }
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
        let got: Vec<_> = r.iter().unwrap().into_iter()
            .map(|(k, _)| (k.user_key, k.seqno)).collect();
        assert_eq!(got, vec![(b"a".to_vec(), 1), (b"b".to_vec(), 1)]);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p cairn-storage sstable`
Expected: FAIL — writer/reader not implemented.

- [ ] **Step 3: Implement writer and reader**

Add above the test module in `sstable.rs`:

```rust
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
            entries.push((InternalKey { user_key: key, seqno }, value));
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
```

Add `pub mod sstable;` to `lib.rs`.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p cairn-storage sstable`
Expected: PASS (2 tests).

- [ ] **Step 5: Lint, format, commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add -A
git commit -m "feat(storage): SSTable writer/reader with footer index"
```

---

### Task 6: Memtable flush + multi-SSTable read path

**Files:**
- Modify: `crates/storage/src/engine.rs`

**Interfaces:**
- Consumes: `SsTableWriter`, `SsTableReader` (Task 5), `Memtable` (Task 2).
- Produces: adds to `Engine`: `flush(&mut self) -> Result<()>` (writes the memtable to a new numbered SSTable `NNNNNN.sst`, clears the memtable, truncates+rotates the WAL, records the SSTable in newest-first order); `get` now checks memtable first, then SSTables newest→oldest, returning the first version found (respecting tombstones). Auto-flush when `memtable.approx_size_bytes()` exceeds a threshold (`const MEMTABLE_FLUSH_BYTES: usize = 4 * 1024 * 1024`).

- [ ] **Step 1: Write failing tests**

Add to the `tests` module in `engine.rs`:

```rust
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
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p cairn-storage engine`
Expected: FAIL — `flush` not found; reopen test fails to see SSTables.

- [ ] **Step 3: Implement flush, SSTable tracking, and layered reads**

Replace the `Engine` struct and impl in `engine.rs` with:

```rust
use crate::sstable::{SsTableReader, SsTableWriter};

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
        self.memtable.put(key.to_vec(), value.map(|v| v.to_vec()), seqno);
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
```

Remove the earlier `Engine` struct/impl and the `#[allow(dead_code)]` from Task 4 — `dir` is now used.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p cairn-storage`
Expected: PASS (all engine tests, including the four new ones).

- [ ] **Step 5: Lint, format, commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add -A
git commit -m "feat(storage): memtable flush to SSTable and layered read path"
```

---

### Task 7: Bloom filters on SSTables

**Files:**
- Create: `crates/storage/src/bloom.rs`
- Modify: `crates/storage/src/sstable.rs` (write a bloom into the footer; check it in `get`)
- Modify: `crates/storage/src/lib.rs` (add `pub mod bloom;`)

**Interfaces:**
- Produces: `struct Bloom { bits: Vec<u8>, k: u32 }`; `Bloom::build(keys: &[&[u8]], bits_per_key: usize) -> Bloom`; `contains(&self, key: &[u8]) -> bool` (no false negatives); `to_bytes(&self) -> Vec<u8>` / `from_bytes(&[u8]) -> Result<Bloom>`. `SsTableReader::get` returns `Ok(None)` fast when the bloom says the key is absent.

- [ ] **Step 1: Write failing tests**

Create `crates/storage/src/bloom.rs`:

```rust
use crate::error::{Error, Result};

pub struct Bloom {
    bits: Vec<u8>,
    k: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let keys: Vec<&[u8]> = vec![b"apple", b"banana", b"cherry"];
        let b = Bloom::build(&keys, 10);
        for k in &keys {
            assert!(b.contains(k), "must contain inserted key");
        }
    }

    #[test]
    fn absent_key_usually_rejected() {
        let keys: Vec<&[u8]> = vec![b"apple"];
        let b = Bloom::build(&keys, 10);
        assert!(!b.contains(b"zzzzzzzz-not-present"));
    }

    #[test]
    fn roundtrips_through_bytes() {
        let keys: Vec<&[u8]> = vec![b"x", b"y"];
        let b = Bloom::build(&keys, 10);
        let restored = Bloom::from_bytes(&b.to_bytes()).unwrap();
        assert!(restored.contains(b"x"));
        assert!(restored.contains(b"y"));
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p cairn-storage bloom`
Expected: FAIL — `Bloom` not implemented.

- [ ] **Step 3: Implement the bloom filter**

Add above the test module in `bloom.rs` (double-hashing with FNV-1a seeds):

```rust
impl Bloom {
    pub fn build(keys: &[&[u8]], bits_per_key: usize) -> Bloom {
        let k = ((bits_per_key as f64) * 0.69).round().clamp(1.0, 30.0) as u32;
        let nbits = (keys.len() * bits_per_key).max(64);
        let nbytes = nbits.div_ceil(8);
        let mut bits = vec![0u8; nbytes];
        for key in keys {
            let (h1, h2) = double_hash(key);
            for i in 0..k {
                let bit = (h1.wrapping_add(h2.wrapping_mul(i as u64)) as usize) % (nbytes * 8);
                bits[bit / 8] |= 1 << (bit % 8);
            }
        }
        Bloom { bits, k }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        let nbytes = self.bits.len();
        let (h1, h2) = double_hash(key);
        for i in 0..self.k {
            let bit = (h1.wrapping_add(h2.wrapping_mul(i as u64)) as usize) % (nbytes * 8);
            if self.bits[bit / 8] & (1 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.bits.len());
        out.extend_from_slice(&self.k.to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Bloom> {
        if bytes.len() < 4 {
            return Err(Error::Corruption("bloom too short".into()));
        }
        let k = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        Ok(Bloom { bits: bytes[4..].to_vec(), k })
    }
}

fn double_hash(key: &[u8]) -> (u64, u64) {
    let h1 = fnv1a(key, 0xcbf29ce484222325);
    let h2 = fnv1a(key, 0x100000001b3).max(1);
    (h1, h2)
}

fn fnv1a(key: &[u8], seed: u64) -> u64 {
    let mut h = seed;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}
```

Add `pub mod bloom;` to `lib.rs`.

- [ ] **Step 4: Wire the bloom into the SSTable footer**

In `sstable.rs`, change `finish` to build a bloom over all user keys and append it before the footer, and change `open` to read + store it, and `get` to short-circuit. Update the footer to `[u64 index_offset][u64 bloom_offset][u64 num_entries][MAGIC]` (footer is now 32 bytes; adjust the `-8`/`-32` seeks and the `len < 32` guard). Add a `bloom: Bloom` field to `SsTableReader` and at the top of `get`:

```rust
        if !self.bloom.contains(user_key) {
            return Ok(None);
        }
```

Add a regression test in `sstable.rs` tests confirming a flushed table still returns correct results for present and absent keys (the existing two tests already cover this once the bloom is wired; add one explicit absent-key case).

- [ ] **Step 5: Run, lint, commit**

Run: `cargo test -p cairn-storage`
Expected: PASS (all tests).

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add -A
git commit -m "feat(storage): bloom filters short-circuit absent SSTable lookups"
```

---

### Task 8: Leveled compaction

**Files:**
- Modify: `crates/storage/src/engine.rs`

**Interfaces:**
- Produces: `Engine::compact(&mut self) -> Result<()>` — merges all current SSTables into one new SSTable, keeping only the newest version per user key and **dropping tombstones** (safe because this compaction merges every table, so no older shadowed version can exist elsewhere), deletes the merged input files, and rebuilds the `sstables` list. Auto-trigger compaction when SSTable count exceeds `const MAX_SSTABLES_BEFORE_COMPACT: usize = 4`.

- [ ] **Step 1: Write failing tests**

Add to the `tests` module in `engine.rs`:

```rust
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
        let ssts = std::fs::read_dir(dir.path()).unwrap()
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
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p cairn-storage engine`
Expected: FAIL — `compact` not found.

- [ ] **Step 3: Implement compaction**

Add to the `Engine` impl in `engine.rs`:

```rust
const MAX_SSTABLES_BEFORE_COMPACT: usize = 4;

impl Engine {
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
        let path = self.dir.join(format!("{id:06}.sst"));
        let mut w = SsTableWriter::create(&path)?;
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

        let old_ids: Vec<u64> = self.sstables.iter().map(|(id, _)| *id).collect();
        self.sstables = vec![(id, SsTableReader::open(&path)?)];
        for old in old_ids {
            let _ = std::fs::remove_file(self.dir.join(format!("{old:06}.sst")));
        }
        Ok(())
    }
}
```

In `flush`, after inserting the new SSTable, add:

```rust
        if self.sstables.len() > MAX_SSTABLES_BEFORE_COMPACT {
            self.compact()?;
        }
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p cairn-storage`
Expected: PASS (all tests).

- [ ] **Step 5: Lint, format, commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add -A
git commit -m "feat(storage): full leveled compaction dropping shadowed versions"
```

---

### Task 9: Property test against a reference model

**Files:**
- Create: `crates/storage/tests/model.rs`
- Modify: `crates/storage/Cargo.toml` (add `proptest` dev-dependency)

**Interfaces:**
- Consumes: the public `Engine` API.
- Produces: a proptest that applies a random sequence of `Put/Delete/Get/Flush/Compact` operations to both `Engine` and a reference `BTreeMap<Vec<u8>, Vec<u8>>`, asserting every `Get` agrees.

- [ ] **Step 1: Add the dev-dependency**

```bash
cd ~/Projects/cairn
cat >> crates/storage/Cargo.toml <<'EOF'
proptest = "1"
EOF
```

- [ ] **Step 2: Write the property test (this is the test; run it to see it drive out any bug)**

Create `crates/storage/tests/model.rs`:

```rust
use cairn_storage::Engine;
use proptest::prelude::*;
use std::collections::BTreeMap;
use tempfile::tempdir;

#[derive(Debug, Clone)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Get(Vec<u8>),
    Flush,
    Compact,
}

fn small_key() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(0u8..4, 1..3)
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (small_key(), proptest::collection::vec(any::<u8>(), 0..4))
            .prop_map(|(k, v)| Op::Put(k, v)),
        small_key().prop_map(Op::Delete),
        small_key().prop_map(Op::Get),
        Just(Op::Flush),
        Just(Op::Compact),
    ]
}

proptest! {
    #[test]
    fn engine_matches_btreemap(ops in proptest::collection::vec(op_strategy(), 1..200)) {
        let dir = tempdir().unwrap();
        let mut engine = Engine::open(dir.path()).unwrap();
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        for op in ops {
            match op {
                Op::Put(k, v) => { engine.put(&k, &v).unwrap(); model.insert(k, v); }
                Op::Delete(k) => { engine.delete(&k).unwrap(); model.remove(&k); }
                Op::Get(k) => {
                    prop_assert_eq!(engine.get(&k).unwrap(), model.get(&k).cloned());
                }
                Op::Flush => engine.flush().unwrap(),
                Op::Compact => engine.compact().unwrap(),
            }
        }
    }
}
```

- [ ] **Step 3: Run the property test**

Run: `cargo test -p cairn-storage --test model`
Expected: PASS. If it fails, proptest prints a minimal shrunk `ops` sequence — fix the engine bug it exposes, then re-run.

- [ ] **Step 4: Lint, format, commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add -A
git commit -m "test(storage): proptest engine against BTreeMap reference model"
```

---

### Task 10: Crash-recovery test and reopen-through-flush durability

**Files:**
- Create: `crates/storage/tests/recovery.rs`

**Interfaces:**
- Consumes: the public `Engine` API.
- Produces: tests that (a) acknowledged puts survive a drop+reopen without flush (WAL replay), and (b) a value written, flushed, then a further un-flushed put, survives reopen with the flushed value present and the WAL-only value present.

- [ ] **Step 1: Write the tests**

Create `crates/storage/tests/recovery.rs`:

```rust
use cairn_storage::Engine;
use tempfile::tempdir;

#[test]
fn unflushed_writes_survive_reopen() {
    let dir = tempdir().unwrap();
    {
        let mut e = Engine::open(dir.path()).unwrap();
        e.put(b"durable", b"yes").unwrap();
        // no flush: durability must come from the WAL alone
    }
    let e = Engine::open(dir.path()).unwrap();
    assert_eq!(e.get(b"durable").unwrap(), Some(b"yes".to_vec()));
}

#[test]
fn mixed_flushed_and_wal_writes_survive_reopen() {
    let dir = tempdir().unwrap();
    {
        let mut e = Engine::open(dir.path()).unwrap();
        e.put(b"a", b"1").unwrap();
        e.flush().unwrap();
        e.put(b"b", b"2").unwrap(); // WAL only
    }
    let e = Engine::open(dir.path()).unwrap();
    assert_eq!(e.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(e.get(b"b").unwrap(), Some(b"2".to_vec()));
}
```

- [ ] **Step 2: Run to verify pass**

Run: `cargo test -p cairn-storage --test recovery`
Expected: PASS.

- [ ] **Step 3: Lint, format, commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add -A
git commit -m "test(storage): crash-recovery durability for WAL and post-flush state"
```

---

### Task 11: Benchmark harness

**Files:**
- Create: `crates/storage/benches/engine_bench.rs`
- Modify: `crates/storage/Cargo.toml` (add `criterion` dev-dependency and `[[bench]]`)

**Interfaces:**
- Produces: criterion benchmarks for sequential-write throughput and point-read latency (cold: after flush + compaction). Output numbers feed the architecture writeup's before/after.

- [ ] **Step 1: Configure the bench target**

```bash
cd ~/Projects/cairn
cat >> crates/storage/Cargo.toml <<'EOF'
criterion = "0.5"

[[bench]]
name = "engine_bench"
harness = false
EOF
```

- [ ] **Step 2: Write the benchmark**

Create `crates/storage/benches/engine_bench.rs`:

```rust
use cairn_storage::Engine;
use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::tempdir;

fn bench_writes(c: &mut Criterion) {
    c.bench_function("put_1k_seq", |b| {
        b.iter_batched(
            || tempdir().unwrap(),
            |dir| {
                let mut e = Engine::open(dir.path()).unwrap();
                for i in 0..1000u32 {
                    e.put(&i.to_be_bytes(), b"value-payload").unwrap();
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_reads(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let mut e = Engine::open(dir.path()).unwrap();
    for i in 0..10_000u32 {
        e.put(&i.to_be_bytes(), b"value-payload").unwrap();
    }
    e.flush().unwrap();
    e.compact().unwrap();
    c.bench_function("get_hit_cold", |b| {
        let mut i = 0u32;
        b.iter(|| {
            let _ = e.get(&(i % 10_000).to_be_bytes()).unwrap();
            i = i.wrapping_add(1);
        });
    });
}

criterion_group!(benches, bench_writes, bench_reads);
criterion_main!(benches);
```

- [ ] **Step 3: Run the benchmark**

Run: `cargo bench -p cairn-storage`
Expected: criterion prints throughput/latency estimates for both benchmarks (no assertion; this establishes the baseline).

- [ ] **Step 4: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add -A
git commit -m "bench(storage): criterion baselines for write throughput and read latency"
```

---

## Self-Review

**Spec coverage** (against the LSM engine's role in `2026-07-18-cairn-distributed-kv-design.md` §"LSM storage engine"): WAL + fsync → Task 3; memtable → Task 2; SSTables → Task 5; leveled compaction → Task 8; bloom filters → Task 7; `put/get/scan/flush/snapshot` interface → `put/get/delete/flush` in Tasks 4/6 (range `scan` and `snapshot/restore` are deliberately deferred to the Raft-integration plan, where the state machine needs them — noted here so it is not lost). Seqno versioning threaded for the future MVCC layer → `InternalKey`/`Seqno` in Tasks 1–2. Every write checksummed → WAL crc (Task 3), SSTable footer magic (Task 5); a full per-block SSTable checksum is a follow-up noted for the compaction-hardening pass.

**Deferred-but-tracked** (belong to later Phase-1 plans, not this one): range `scan` iterator, `snapshot()/restore()` for Raft log compaction, and block-level SSTable checksums. Listed so the Raft-integration plan picks them up.

**Placeholder scan:** no TBD/TODO; every code step shows complete code; commands have expected outcomes.

**Type consistency:** `InternalKey { user_key, seqno }`, `Seqno = u64`, `Engine::{open,put,delete,get,flush,compact}`, `Memtable::{new,put,get,iter,approx_size_bytes}`, `SsTableWriter::{create,add,finish}`, `SsTableReader::{open,get,iter}`, `Bloom::{build,contains,to_bytes,from_bytes}`, `WalWriter::{create,append,read_all}` — names used consistently across tasks. `get` on `Memtable` returns `Option<Option<Vec<u8>>>`; `get` on `Engine` flattens to `Option<Vec<u8>>` — intentional and used consistently.
