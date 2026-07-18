# Raft Transport Notes

## Final Architecture

The transport layer exposes the frozen `Transport` trait over two implementations:

- `InMemoryTransport` is one endpoint in a deterministic, in-process network.
  `InMemoryTransport::cluster` creates every endpoint together and returns a separate,
  cloneable `InMemoryNetwork` fault-control handle.
- `TcpTransport` binds one Tokio TCP listener per node. Outbound connections are
  opened lazily, cached, and replaced after a failed write.

The in-memory control plane is separate from endpoint ownership so a test harness can
move every endpoint into a node task while retaining one handle for fault injection.
Messages admitted to the scheduler receive sequence numbers starting at the supplied
`seed`; equal-deadline deliveries are ordered by that sequence number (FIFO). The
constructor must run with an active Tokio runtime because it spawns the scheduler; it
returns an I/O error otherwise.

TCP connections start with an eight-byte little-endian sender node ID. Every subsequent
frame on that connection is exactly a four-byte little-endian payload length followed
by one bincode-serialized `Message`. The receiver combines the handshake identity with
each decoded message to produce `(from_node, message)`.

## Final Public API

```rust
pub fn InMemoryTransport::cluster(
    node_ids: impl IntoIterator<Item = NodeId>,
    seed: u64,
) -> crate::Result<(InMemoryNetwork, HashMap<NodeId, InMemoryTransport>)>;

pub fn InMemoryNetwork::partition(&self, a: NodeId, b: NodeId) -> crate::Result<()>;
pub fn InMemoryNetwork::heal(&self) -> crate::Result<()>;
pub fn InMemoryNetwork::drop_next(&self, from: NodeId, to: NodeId) -> crate::Result<()>;
pub fn InMemoryNetwork::set_delay(
    &self,
    from: NodeId,
    to: NodeId,
    delay: Duration,
) -> crate::Result<()>;
```

All in-memory fault hooks validate their node IDs. `partition(a, b)` uses an
order-independent key, so it blocks both directions (including `a == b`).
`drop_next(from, to)` and `set_delay(from, to, delay)` are directional.
Each `drop_next` increments a counter, so repeated calls stack and consume one matching
send each. `set_delay(from, to, Duration::ZERO)` removes that directional delay.
`heal()` clears all partitions only; it does not clear delays or pending drops.

Fault rules are evaluated while `send` starts, before the delivery enters the scheduler.
Changing a rule does not retroactively alter an admitted delivery. Partitioned and
dropped sends return `Ok(())`; an admitted send completes after its destination channel
accepts the message. Unknown, closed, or unavailable destinations return an I/O error.

## TCP Wire Protocol and Lifecycle

The exact TCP constructors and peer-update APIs are:

```rust
pub async fn TcpTransport::bind(
    node_id: NodeId,
    bind_addr: SocketAddr,
    peers: HashMap<NodeId, SocketAddr>,
) -> crate::Result<TcpTransport>;

pub fn TcpTransport::local_addr(&self) -> SocketAddr;

pub async fn TcpTransport::set_peer(&self, node_id: NodeId, addr: SocketAddr);
```

`bind` binds the listener and `local_addr` returns its resolved address. The first write
on every outbound connection is an eight-byte little-endian sender `NodeId` handshake.
Each following message is a four-byte little-endian `u32` payload length followed by the
bincode-serialized `Message`. The inbound reader pairs the connection's handshake ID
with every decoded frame for `recv`.

Outbound writers are opened on first send and cached per peer address. A failed write
evicts the writer and the same `send` makes one fresh connection/write attempt; a failed
second attempt is returned as an I/O error. Updating a peer address makes the cached
writer ineligible for reuse.

Inbound work has at most 32 reader tasks and a 256-message channel. Frames at or below
16 MiB reserve their encoded payload size from a shared 16 MiB byte budget, held until
`recv` removes the message. A frame larger than 16 MiB reserves the entire budget and
is therefore admitted exclusively rather than rejected; one valid oversized frame can
consume more than 16 MiB of memory. Frames are read in at most 64 KiB chunks. Every
handshake, length, and payload read must make progress within one second; timeout, EOF,
malformed bincode, trailing bytes, allocation failure, or channel closure closes only
that reader. Listener accept failures retry with exponential backoff (10 ms through
1 s) at most eight times, then terminate the listener and abort active readers. Dropping
`TcpTransport` signals listener shutdown and aborts readers.

## Errors and Contract Deviations

Listener bind failures are returned by `bind`; unknown peers and outbound connect or
frame-write failures are returned by `send`, all as the crate's existing I/O error.
Serialization and malformed-frame failures use the existing corruption error. Inbound
connection read failures close only that reader. Accept-retry exhaustion terminates the
listener, aborts its readers, and closes inbound delivery; it is not returned as an I/O
error.

The sender-ID handshake is protocol metadata required to implement
`Transport::recv(&mut self) -> Option<(NodeId, Message)>`, because `Message` has no
sender field. It is therefore an intentional wire-level addition beyond the frozen
message values. The trait has no error return from `recv`, so accept-retry exhaustion
and the resulting inbound-channel closure surface as `recv() == None`, rather than a
listener error. There are no other deviations from the frozen user contract.

# Historical Raft Transport Implementation Plan

> Completed implementation record. The checklist items below are historical.

**Goal:** Add serializable Raft RPCs and deterministic in-memory and reconnecting TCP
implementations of the frozen transport trait.

**Architecture:** RPC values are plain serde data types. The in-memory implementation
shares a fault-controlled scheduler between endpoint receivers. The TCP implementation
uses a listener task, per-connection reader tasks, and lazily cached outbound writers.

**Tech stack:** Rust 2021, Tokio, async-trait, serde/serde_derive, and bincode.

## Global Constraints

- Modify only new files under `crates/raft/src/`, `crates/raft/src/lib.rs`,
  `crates/raft/src/types.rs`, `crates/raft/Cargo.toml`, and this required note.
- Do not modify `crates/storage/`, repository docs, `log.rs`, `oplog.rs`,
  `hardstate.rs`, or `error.rs`.
- Use no unsafe code and no `unwrap` or `expect` in library paths.
- Every behavior change follows a witnessed failing test, minimal implementation,
  passing focused test, and logical commit.
- Final gates are crate tests, workspace build, formatting, and all-target clippy with
  warnings denied.

---

### Task 1: Serializable RPC contract

**Files:**

- Modify: `crates/raft/Cargo.toml`
- Modify: `crates/raft/src/types.rs`
- Create: `crates/raft/src/rpc.rs`
- Modify: `crates/raft/src/lib.rs`

**Interfaces:**

- Produce the six frozen request/response structs and six-variant `Message` enum.
- Add serde traits to `LogEntry`; type aliases already serialize as `u64`.
- Export `rpc`, `Message`, and every RPC struct from the crate root.

- [x] Add an `rpc::tests::all_message_variants_roundtrip_with_bincode` test that builds
  one value of every enum variant, serializes each with `bincode::serialize`,
  deserializes with `bincode::deserialize::<Message>`, and asserts equality.
- [x] Run `cargo test -p cairn-raft rpc::tests` and confirm the missing RPC types or
  serde implementations make it fail.
- [x] Add the requested dependencies, exact frozen derives/fields, `LogEntry` serde
  derives, module declarations, and root re-exports.
- [x] Re-run `cargo test -p cairn-raft rpc::tests` and the full crate test suite.
- [x] Commit as `feat(raft): add serializable RPC contract`.

### Task 2: Transport trait and deterministic in-memory network

**Files:**

- Create: `crates/raft/src/transport.rs`
- Create: `crates/raft/src/transport/in_memory.rs`
- Modify: `crates/raft/src/lib.rs`

**Interfaces:**

```rust
#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, to: NodeId, msg: Message) -> crate::Result<()>;
    async fn recv(&mut self) -> Option<(NodeId, Message)>;
}

pub fn InMemoryTransport::cluster(
    node_ids: impl IntoIterator<Item = NodeId>,
    seed: u64,
) -> crate::Result<(InMemoryNetwork, HashMap<NodeId, InMemoryTransport>)>;

pub fn InMemoryNetwork::partition(&self, a: NodeId, b: NodeId) -> crate::Result<()>;
pub fn InMemoryNetwork::heal(&self) -> crate::Result<()>;
pub fn InMemoryNetwork::drop_next(&self, from: NodeId, to: NodeId) -> crate::Result<()>;
pub fn InMemoryNetwork::set_delay(
    &self,
    from: NodeId,
    to: NodeId,
    delay: Duration,
) -> crate::Result<()>;
```

- [x] Add Tokio tests for three-node delivery, partition then heal, and exactly one
  directional dropped message. Use timeouts only to prove non-delivery; compare
  received `(NodeId, Message)` values directly.
- [x] Run `cargo test -p cairn-raft transport::in_memory::tests` and confirm it fails
  because the transport module and types do not exist.
- [x] Implement a shared scheduler with validated node IDs, symmetric partition keys,
  directional drop counters and delays, stable seeded sequence numbers, and one
  receiver per endpoint. Closed destination receivers return an I/O error.
- [x] Re-run focused and full crate tests.
- [x] Commit as `feat(raft): add deterministic in-memory transport`.

### Task 3: Framed reconnecting TCP transport

**Files:**

- Create: `crates/raft/src/transport/tcp.rs`
- Modify: `crates/raft/src/transport.rs`
- Modify: `crates/raft/src/lib.rs`

**Interfaces:**

```rust
pub async fn TcpTransport::bind(
    node_id: NodeId,
    bind_addr: SocketAddr,
    peers: HashMap<NodeId, SocketAddr>,
) -> crate::Result<TcpTransport>;

pub fn TcpTransport::local_addr(&self) -> SocketAddr;

pub async fn TcpTransport::set_peer(&self, node_id: NodeId, addr: SocketAddr);
```

- [x] Add a two-node Tokio test that binds both nodes to `127.0.0.1:0`, exchanges every
  `Message` variant in both directions, and asserts sender IDs and payload equality.
- [x] Run `cargo test -p cairn-raft transport::tcp::tests` and confirm it fails because
  `TcpTransport` is absent.
- [x] Implement the sender handshake, LE length framing, bincode encoding/decoding,
  listener and reader tasks, lazy writer cache, failed-writer eviction, one reconnect
  attempt per send, and inbound channel.
- [x] Re-run focused and full crate tests.
- [x] Commit as `feat(raft): add framed TCP transport`.

### Task 4: Notes, quality gates, and review

**Files:**

- Modify: `crates/raft/TRANSPORT_NOTES.md`

- [x] Replace planned wording with final exact signatures and record any actual
  contract deviations.
- [x] Run `cargo fmt --all -- --check`.
- [x] Run `cargo test -p cairn-raft`.
- [x] Run `cargo build`.
- [x] Run `cargo clippy --all-targets -- -D warnings`.
- [x] Review the complete branch diff against the frozen contract and permitted files.
- [x] Commit final note corrections as `docs(raft): finalize transport usage notes`.
