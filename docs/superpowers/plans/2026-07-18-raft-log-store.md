# Raft Log Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the dedicated, durable, crash-recoverable Raft log store — index-addressed log entries with suffix truncation and prefix (snapshot) compaction, plus persisted hard state (`current_term`, `voted_for`) — the storage foundation the Raft core will sit on.

**Architecture:** An in-memory model of the log (entries from `snapshot_index+1` onward) backed by an append-only, CRC-checksummed *operation* file. Each durable op is `Append(entry)`, `TruncateSuffix(from_index)`, or `Compact(up_to_index, snapshot_meta)`; replaying the op file on open reconstructs the exact in-memory state. Hard state lives in a separate small CRC'd file rewritten+fsync'd on change (it mutates independently of the log). This is the first plan (A) of the Raft cycle — see `docs/superpowers/specs/2026-07-18-cairn-raft-design.md`.

**Tech Stack:** Rust (stable), new workspace crate `crates/raft` (package `cairn-raft`), `crc32fast` for checksums, `tempfile` (dev). No async and no Raft logic here — pure storage. The op-log/CRC/replay pattern mirrors the Phase-1 WAL (`crates/storage/src/wal.rs`).

## Global Constraints

- Rust edition 2021, resolver 2. New crate `crates/raft`, package name `cairn-raft`; add it to the workspace `members`.
- `cargo clippy --all-targets -- -D warnings` and `cargo fmt` clean at every commit.
- No `unsafe`. No `.unwrap()`/`.expect()` in library I/O paths EXCEPT `.try_into().unwrap()` on already-length-checked fixed-size slice→array conversions (unwrap allowed in tests).
- A CRC mismatch or torn/short tail record on read must stop cleanly and keep the valid prefix — never panic. Corrupt fixed-format state → recoverable error, never panic.
- Types: `NodeId = u64`, `Term = u64`, `LogIndex = u64`. Log indices are 1-based (index 0 means "empty/before first entry"), matching Raft convention. Commands are `Vec<u8>`.
- Every append fsyncs before returning. Hard-state save fsyncs before returning.

---

### Task 1: Crate scaffold + core types

**Files:**
- Modify: `Cargo.toml` (workspace root — add `crates/raft` to members)
- Create: `crates/raft/Cargo.toml`
- Create: `crates/raft/src/lib.rs`
- Create: `crates/raft/src/types.rs`
- Create: `crates/raft/src/error.rs`

**Interfaces:**
- Produces: `type NodeId = u64`, `type Term = u64`, `type LogIndex = u64`; `struct LogEntry { term: Term, index: LogIndex, command: Vec<u8> }` (Clone, PartialEq, Debug); `struct HardState { current_term: Term, voted_for: Option<NodeId> }` (Clone, PartialEq, Debug, Default); `struct SnapshotMeta { last_index: LogIndex, last_term: Term }` (Clone, Copy, PartialEq, Debug, Default); `enum Error { Io(std::io::Error), Corruption(String) }` with `From<io::Error>`, `Display`, `std::error::Error`; `type Result<T>`.

- [ ] **Step 1: Scaffold the crate and register it in the workspace**

```bash
cd ~/Projects/cairn
mkdir -p crates/raft/src
cat > crates/raft/Cargo.toml <<'EOF'
[package]
name = "cairn-raft"
version = "0.1.0"
edition = "2021"

[dependencies]
crc32fast = "1"

[dev-dependencies]
tempfile = "3"
EOF
```

Edit the workspace root `Cargo.toml` so `members` is `["crates/storage", "crates/raft"]`.

- [ ] **Step 2: Write the failing test for types**

Create `crates/raft/src/types.rs`:

```rust
pub type NodeId = u64;
pub type Term = u64;
pub type LogIndex = u64;

#[derive(Clone, Debug, PartialEq)]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub command: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct HardState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct SnapshotMeta {
    pub last_index: LogIndex,
    pub last_term: Term,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hard_state_default_is_term0_no_vote() {
        let hs = HardState::default();
        assert_eq!(hs.current_term, 0);
        assert_eq!(hs.voted_for, None);
    }

    #[test]
    fn log_entry_holds_command_bytes() {
        let e = LogEntry { term: 2, index: 5, command: b"set x".to_vec() };
        assert_eq!(e.term, 2);
        assert_eq!(e.index, 5);
        assert_eq!(e.command, b"set x");
    }
}
```

- [ ] **Step 3: Add the error type and wire up `lib.rs`**

Create `crates/raft/src/error.rs`:

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

Create `crates/raft/src/lib.rs`:

```rust
pub mod error;
pub mod types;

pub use error::{Error, Result};
pub use types::{HardState, LogEntry, LogIndex, NodeId, SnapshotMeta, Term};
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p cairn-raft`
Expected: PASS (2 tests).

- [ ] **Step 5: Lint, format, commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add -A
git commit -m "feat(raft): scaffold cairn-raft crate with core types"
```

---

### Task 2: Hard-state persistence

**Files:**
- Create: `crates/raft/src/hardstate.rs`
- Modify: `crates/raft/src/lib.rs` (add `pub mod hardstate;`)

**Interfaces:**
- Consumes: `HardState`, `NodeId`, `Term`, `Result`, `Error`.
- Produces: `fn save_hard_state(path: &Path, hs: &HardState) -> Result<()>` (writes `[u32 crc][u64 current_term][u8 has_vote][u64 voted_for]` LE, fsync) and `fn load_hard_state(path: &Path) -> Result<HardState>` (returns `HardState::default()` if the file is missing or the record is torn/corrupt — a fresh node has no persisted state).

- [ ] **Step 1: Write failing tests**

Create `crates/raft/src/hardstate.rs`:

```rust
use crate::error::{Error, Result};
use crate::types::HardState;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn save_then_load_roundtrips() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("hardstate");
        let hs = HardState { current_term: 7, voted_for: Some(3) };
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
        let hs = HardState { current_term: 2, voted_for: None };
        save_hard_state(&path, &hs).unwrap();
        assert_eq!(load_hard_state(&path).unwrap(), hs);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p cairn-raft hardstate`
Expected: FAIL — functions not found.

- [ ] **Step 3: Implement save/load**

Add above the test module in `hardstate.rs`:

```rust
pub fn save_hard_state(path: &Path, hs: &HardState) -> Result<()> {
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
        let mut f = OpenOptions::new().create(true).write(true).truncate(true).open(&tmp)?;
        f.write_all(&crc.to_le_bytes())?;
        f.write_all(&body)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

pub fn load_hard_state(path: &Path) -> Result<HardState> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HardState::default()),
        Err(e) => return Err(Error::Io(e)),
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
    Ok(HardState { current_term, voted_for })
}
```

Add `pub mod hardstate;` to `lib.rs`.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p cairn-raft hardstate`
Expected: PASS (3 tests).

- [ ] **Step 5: Lint, format, commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add -A
git commit -m "feat(raft): atomic CRC'd hard-state persistence with default-on-corrupt"
```

---

### Task 3: Durable op-record framing (append / truncate / compact)

**Files:**
- Create: `crates/raft/src/oplog.rs`
- Modify: `crates/raft/src/lib.rs` (add `pub mod oplog;`)

**Interfaces:**
- Consumes: `LogEntry`, `LogIndex`, `Term`, `SnapshotMeta`, `Result`, `Error`.
- Produces: `enum Op { Append(LogEntry), TruncateSuffix(LogIndex), Compact { up_to: LogIndex, meta: SnapshotMeta } }`; `struct OpWriter` with `create(path)` and `append(&mut self, op: &Op) -> Result<()>` (CRC'd record, fsync); `fn read_all(path) -> Result<Vec<Op>>` that replays and stops cleanly at the first torn/CRC-bad tail record.
- Record framing: `[u32 crc][u8 tag][payload]` LE; tag 0 = Append (`[u64 term][u64 index][u32 clen][command]`), tag 1 = TruncateSuffix (`[u64 from_index]`), tag 2 = Compact (`[u64 up_to][u64 last_index][u64 last_term]`). CRC covers `tag+payload`.

- [ ] **Step 1: Write failing tests**

Create `crates/raft/src/oplog.rs`:

```rust
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn entry(term: u64, index: u64, cmd: &[u8]) -> LogEntry {
        LogEntry { term, index, command: cmd.to_vec() }
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
            Op::Compact { up_to: 1, meta: SnapshotMeta { last_index: 1, last_term: 1 } },
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
        assert_eq!(read_all(&path).unwrap(), vec![Op::Append(entry(1, 1, b"a"))]);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p cairn-raft oplog`
Expected: FAIL — `OpWriter`/`read_all` not found.

- [ ] **Step 3: Implement the op writer and replay**

Add above the test module in `oplog.rs`:

```rust
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
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(Error::Io(e)),
    };
    let mut r = BufReader::new(file);
    let mut out = Vec::new();
    loop {
        let mut crc_buf = [0u8; 4];
        match r.read_exact(&mut crc_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(Error::Io(e)),
        }
        let expected_crc = u32::from_le_bytes(crc_buf);
        match read_body(&mut r)? {
            Some(body) if crc32fast::hash(&body) == expected_crc => match decode(&body) {
                Some(op) => out.push(op),
                None => break,
            },
            _ => break,
        }
    }
    Ok(out)
}

fn read_exact_or_eof<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<bool> {
    match r.read_exact(buf) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(Error::Io(e)),
    }
}

fn read_body<R: Read>(r: &mut R) -> Result<Option<Vec<u8>>> {
    let mut tag = [0u8; 1];
    if !read_exact_or_eof(r, &mut tag)? {
        return Ok(None);
    }
    let mut body = vec![tag[0]];
    match tag[0] {
        0 => {
            let mut fixed = [0u8; 20]; // term(8)+index(8)+clen(4)
            if !read_exact_or_eof(r, &mut fixed)? {
                return Ok(None);
            }
            let clen = u32::from_le_bytes(fixed[16..20].try_into().unwrap()) as usize;
            let mut cmd = vec![0u8; clen];
            if !read_exact_or_eof(r, &mut cmd)? {
                return Ok(None);
            }
            body.extend_from_slice(&fixed);
            body.extend_from_slice(&cmd);
        }
        1 => {
            let mut fixed = [0u8; 8];
            if !read_exact_or_eof(r, &mut fixed)? {
                return Ok(None);
            }
            body.extend_from_slice(&fixed);
        }
        2 => {
            let mut fixed = [0u8; 24];
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
            Some(Op::Append(LogEntry { term, index, command }))
        }
        1 => {
            let from = u64::from_le_bytes(body[1..9].try_into().ok()?);
            Some(Op::TruncateSuffix(from))
        }
        2 => {
            let up_to = u64::from_le_bytes(body[1..9].try_into().ok()?);
            let last_index = u64::from_le_bytes(body[9..17].try_into().ok()?);
            let last_term = u64::from_le_bytes(body[17..25].try_into().ok()?);
            Some(Op::Compact { up_to, meta: SnapshotMeta { last_index, last_term } })
        }
        _ => None,
    }
}
```

Add `pub mod oplog;` to `lib.rs`.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p cairn-raft oplog`
Expected: PASS (2 tests).

- [ ] **Step 5: Lint, format, commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add -A
git commit -m "feat(raft): CRC'd op-log framing for append/truncate/compact with torn-tail replay"
```

---

### Task 4: RaftLog — in-memory model backed by the op-log (append + reads)

**Files:**
- Create: `crates/raft/src/log.rs`
- Modify: `crates/raft/src/lib.rs` (add `pub mod log;` and re-export `RaftLog`)

**Interfaces:**
- Consumes: `Op`/`OpWriter`/`read_all` (Task 3), `LogEntry`, `LogIndex`, `Term`, `SnapshotMeta`.
- Produces: `struct RaftLog`; `RaftLog::open(dir: &Path) -> Result<RaftLog>` (replays `dir/log.ops`); `append(&mut self, entries: &[LogEntry]) -> Result<()>` (each entry's `index` must equal `last_index()+1`, contiguous; persists each as an `Op::Append`); `entry(&self, index) -> Option<&LogEntry>`; `entries_from(&self, index) -> Vec<LogEntry>`; `last_index(&self) -> LogIndex` (snapshot's last_index if the in-memory log is empty, else the last entry's index); `last_term(&self) -> Term`; `snapshot_meta(&self) -> SnapshotMeta`. In-memory entries are held in a `Vec<LogEntry>` covering `(snapshot.last_index, last_index]`.

- [ ] **Step 1: Write failing tests**

Create `crates/raft/src/log.rs`:

```rust
use crate::error::Result;
use crate::oplog::{read_all, Op, OpWriter};
use crate::types::{LogEntry, LogIndex, SnapshotMeta, Term};
use std::path::Path;

pub struct RaftLog {
    writer: OpWriter,
    entries: Vec<LogEntry>,
    snapshot: SnapshotMeta,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn e(term: u64, index: u64) -> LogEntry {
        LogEntry { term, index, command: vec![index as u8] }
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
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p cairn-raft log`
Expected: FAIL — `RaftLog` methods not found.

- [ ] **Step 3: Implement RaftLog (append + reads + replay wiring)**

Add above the test module in `log.rs`. Note `apply_op` is used by `open` to rebuild state and is written now so Tasks 5–6 extend it:

```rust
impl RaftLog {
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("log.ops");
        let mut entries: Vec<LogEntry> = Vec::new();
        let mut snapshot = SnapshotMeta::default();
        for op in read_all(&path)? {
            apply_op(&mut entries, &mut snapshot, op);
        }
        let writer = OpWriter::create(&path)?;
        Ok(RaftLog { writer, entries, snapshot })
    }

    pub fn append(&mut self, new_entries: &[LogEntry]) -> Result<()> {
        for entry in new_entries {
            debug_assert_eq!(entry.index, self.last_index() + 1, "log append must be contiguous");
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
        (start..=self.last_index()).filter_map(|i| self.entry(i).cloned()).collect()
    }

    pub fn last_index(&self) -> LogIndex {
        self.entries.last().map_or(self.snapshot.last_index, |e| e.index)
    }

    pub fn last_term(&self) -> Term {
        self.entries.last().map_or(self.snapshot.last_term, |e| e.term)
    }

    pub fn snapshot_meta(&self) -> SnapshotMeta {
        self.snapshot
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
```

Add to `lib.rs`:

```rust
pub mod log;
pub use log::RaftLog;
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p cairn-raft log`
Expected: PASS (2 tests).

- [ ] **Step 5: Lint, format, commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add -A
git commit -m "feat(raft): RaftLog in-memory model over the op-log (append + reads + replay)"
```

---

### Task 5: Suffix truncation (Raft conflict resolution)

**Files:**
- Modify: `crates/raft/src/log.rs`

**Interfaces:**
- Produces: `RaftLog::truncate_suffix(&mut self, from_index: LogIndex) -> Result<()>` — durably removes all entries with `index >= from_index` (persists `Op::TruncateSuffix(from_index)` then updates the in-memory `entries`). No-op if `from_index > last_index()`. Must not truncate into the snapshot prefix (`from_index > snapshot.last_index`); a caller violating that is a programming error — `debug_assert`.

- [ ] **Step 1: Write failing tests**

Add to the `tests` module in `log.rs`:

```rust
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
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p cairn-raft log`
Expected: FAIL — `truncate_suffix` not found.

- [ ] **Step 3: Implement truncate_suffix**

Add to the `impl RaftLog` block in `log.rs`:

```rust
    pub fn truncate_suffix(&mut self, from_index: LogIndex) -> Result<()> {
        debug_assert!(from_index > self.snapshot.last_index, "cannot truncate into snapshot");
        if from_index > self.last_index() {
            return Ok(());
        }
        self.writer.append(&Op::TruncateSuffix(from_index))?;
        self.entries.retain(|e| e.index < from_index);
        Ok(())
    }
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p cairn-raft log`
Expected: PASS (4 tests).

- [ ] **Step 5: Lint, format, commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add -A
git commit -m "feat(raft): durable suffix truncation for log-conflict resolution"
```

---

### Task 6: Prefix compaction + snapshot metadata

**Files:**
- Modify: `crates/raft/src/log.rs`

**Interfaces:**
- Produces: `RaftLog::compact_prefix(&mut self, up_to: LogIndex, meta: SnapshotMeta) -> Result<()>` — durably drops all entries with `index <= up_to` and records `meta` as the new snapshot (persists `Op::Compact { up_to, meta }` then updates in-memory state). After compaction, `last_index()`/`last_term()` fall back to `meta` when the in-memory log is empty, and `entry(i)` for `i <= up_to` returns `None`. `up_to` must be `<= last_index()`; `meta.last_index` should equal `up_to` (the snapshot covers through `up_to`).

- [ ] **Step 1: Write failing tests**

Add to the `tests` module in `log.rs`:

```rust
    #[test]
    fn compact_prefix_drops_covered_entries() {
        let dir = tempdir().unwrap();
        let mut log = RaftLog::open(dir.path()).unwrap();
        log.append(&[e(1, 1), e(1, 2), e(2, 3), e(2, 4)]).unwrap();
        log.compact_prefix(2, SnapshotMeta { last_index: 2, last_term: 1 }).unwrap();
        assert_eq!(log.entry(1), None);
        assert_eq!(log.entry(2), None);
        assert_eq!(log.entry(3), Some(&e(2, 3)));
        assert_eq!(log.last_index(), 4);
        assert_eq!(log.snapshot_meta(), SnapshotMeta { last_index: 2, last_term: 1 });
    }

    #[test]
    fn compact_to_empty_reports_snapshot_as_last() {
        let dir = tempdir().unwrap();
        let mut log = RaftLog::open(dir.path()).unwrap();
        log.append(&[e(1, 1), e(3, 2)]).unwrap();
        log.compact_prefix(2, SnapshotMeta { last_index: 2, last_term: 3 }).unwrap();
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.last_term(), 3);
    }

    #[test]
    fn compaction_survives_reopen() {
        let dir = tempdir().unwrap();
        {
            let mut log = RaftLog::open(dir.path()).unwrap();
            log.append(&[e(1, 1), e(1, 2), e(2, 3)]).unwrap();
            log.compact_prefix(1, SnapshotMeta { last_index: 1, last_term: 1 }).unwrap();
        }
        let log = RaftLog::open(dir.path()).unwrap();
        assert_eq!(log.entry(1), None);
        assert_eq!(log.entry(2), Some(&e(1, 2)));
        assert_eq!(log.snapshot_meta(), SnapshotMeta { last_index: 1, last_term: 1 });
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p cairn-raft log`
Expected: FAIL — `compact_prefix` not found.

- [ ] **Step 3: Implement compact_prefix**

Add to the `impl RaftLog` block in `log.rs`:

```rust
    pub fn compact_prefix(&mut self, up_to: LogIndex, meta: SnapshotMeta) -> Result<()> {
        debug_assert!(up_to <= self.last_index(), "cannot compact past the log end");
        self.writer.append(&Op::Compact { up_to, meta })?;
        self.entries.retain(|e| e.index > up_to);
        self.snapshot = meta;
        Ok(())
    }
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p cairn-raft log`
Expected: PASS (7 tests).

- [ ] **Step 5: Lint, format, commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add -A
git commit -m "feat(raft): prefix compaction with snapshot metadata"
```

---

### Task 7: Property test — RaftLog vs a reference model

**Files:**
- Create: `crates/raft/tests/log_model.rs`
- Modify: `crates/raft/Cargo.toml` (add `proptest` dev-dependency)

**Interfaces:**
- Consumes: the public `RaftLog` API + `HardState` persistence.
- Produces: a proptest applying random `Append/TruncateSuffix/CompactPrefix/Reopen` ops to `RaftLog` and to a reference `Vec<LogEntry>` model (with a tracked snapshot index), asserting `entry(i)`, `last_index()`, `last_term()`, and `snapshot_meta()` agree after each op — including across reopen.

- [ ] **Step 1: Add the dev-dependency**

```bash
cd ~/Projects/cairn
printf 'proptest = "1"\n' >> crates/raft/Cargo.toml
```

(Ensure the line lands under the existing `[dev-dependencies]` table in `crates/raft/Cargo.toml`; move it there if `cargo` complains.)

- [ ] **Step 2: Write the property test**

Create `crates/raft/tests/log_model.rs`:

```rust
use cairn_raft::{LogEntry, RaftLog, SnapshotMeta};
use proptest::prelude::*;
use tempfile::tempdir;

#[derive(Debug, Clone)]
enum Op {
    Append,
    Truncate(u64),
    Compact(u64),
    Reopen,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        Just(Op::Append),
        (0u64..8).prop_map(Op::Truncate),
        (0u64..8).prop_map(Op::Compact),
        Just(Op::Reopen),
    ]
}

proptest! {
    #[test]
    fn raftlog_matches_reference(ops in proptest::collection::vec(op_strategy(), 1..120)) {
        let dir = tempdir().unwrap();
        let mut log = RaftLog::open(dir.path()).unwrap();
        let mut model: Vec<LogEntry> = Vec::new();
        let mut snap = SnapshotMeta::default();
        let mut next_term = 1u64;

        for op in ops {
            match op {
                Op::Append => {
                    let index = model.last().map_or(snap.last_index, |e| e.index) + 1;
                    let entry = LogEntry { term: next_term, index, command: vec![index as u8] };
                    log.append(std::slice::from_ref(&entry)).unwrap();
                    model.push(entry);
                    next_term += 1;
                }
                Op::Truncate(rel) => {
                    let from = snap.last_index + 1 + rel;
                    let last = model.last().map_or(snap.last_index, |e| e.index);
                    if from > snap.last_index && from <= last {
                        log.truncate_suffix(from).unwrap();
                        model.retain(|e| e.index < from);
                    }
                }
                Op::Compact(rel) => {
                    let up_to = snap.last_index + rel;
                    let last = model.last().map_or(snap.last_index, |e| e.index);
                    if up_to > snap.last_index && up_to <= last {
                        let term = model.iter().find(|e| e.index == up_to).map(|e| e.term).unwrap_or(snap.last_term);
                        let meta = SnapshotMeta { last_index: up_to, last_term: term };
                        log.compact_prefix(up_to, meta).unwrap();
                        model.retain(|e| e.index > up_to);
                        snap = meta;
                    }
                }
                Op::Reopen => {
                    drop(log);
                    log = RaftLog::open(dir.path()).unwrap();
                }
            }

            let model_last = model.last().map_or(snap.last_index, |e| e.index);
            let model_last_term = model.last().map_or(snap.last_term, |e| e.term);
            prop_assert_eq!(log.last_index(), model_last);
            prop_assert_eq!(log.last_term(), model_last_term);
            prop_assert_eq!(log.snapshot_meta(), snap);
            for e in &model {
                prop_assert_eq!(log.entry(e.index), Some(e));
            }
        }
    }
}
```

- [ ] **Step 3: Run the property test (twice — randomized)**

Run: `cargo test -p cairn-raft --test log_model` then run it once more.
Expected: PASS both times. If it fails, proptest prints a shrunk `ops` sequence — fix the `RaftLog` bug it exposes (do not weaken the test).

- [ ] **Step 4: Lint, format, commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add -A
git commit -m "test(raft): proptest RaftLog against a reference model incl. reopen"
```

---

## Self-Review

**Spec coverage** (against `2026-07-18-cairn-raft-design.md` §"Raft log store"): `append` → Task 4; `entries_from`/`entry`/`last_index`/`last_term` → Task 4; `truncate_suffix` → Task 5; `compact_prefix` + snapshot meta → Task 6; `save/load_hard_state` → Task 2; CRC + crash-tolerant replay → Task 3 (op-log) + Task 2 (hard state); reopen recovery → Task 4 `open` (+ Tasks 5/6 durable ops) and pinned by the Task 7 `Reopen` proptest op. The `save_hard_state`/`load_hard_state` free functions are the store this cycle needs; the Raft core (next plan) will call them.

**Deferred-but-tracked** (belong to later Raft plans, not this one): the `Transport` trait + TCP/in-memory transports (Plan B), the `RaftCore` step function (Plan C), snapshots/membership at the algorithm level (Plan D), the node driver + apply callback (Plan E). Listed so they are not lost.

**Placeholder scan:** no TBD/TODO; every code step shows complete code; commands have expected outcomes.

**Type consistency:** `NodeId=u64`, `Term=u64`, `LogIndex=u64`; `LogEntry { term, index, command }`; `HardState { current_term, voted_for }`; `SnapshotMeta { last_index, last_term }`; `Op::{Append, TruncateSuffix, Compact { up_to, meta }}`; `OpWriter::{create, append}` + `read_all`; `RaftLog::{open, append, entry, entries_from, last_index, last_term, snapshot_meta, truncate_suffix, compact_prefix}`; `save_hard_state`/`load_hard_state`. Names are consistent across tasks; `apply_op` (Task 4) already handles the `TruncateSuffix`/`Compact` ops that Tasks 5–6 emit, so replay works as those tasks land.
