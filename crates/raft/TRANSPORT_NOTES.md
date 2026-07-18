# Raft Transport Notes

## Architecture

The transport layer exposes the frozen `Transport` trait over two implementations:

- `InMemoryTransport` is one endpoint in a deterministic, in-process network.
  `InMemoryTransport::cluster` creates every endpoint together and returns a separate
  cloneable `InMemoryNetwork` fault-control handle.
- `TcpTransport` binds one Tokio TCP listener per node. Outbound connections are
  opened lazily, cached, and replaced after a failed write.

The in-memory control plane is separate from endpoint ownership so a test harness can
move every endpoint into a node task while retaining one handle for fault injection.
Messages are assigned stable sequence numbers, and equal-deadline deliveries use those
numbers for deterministic FIFO tie-breaking. The constructor seed initializes this
scheduling state and is retained as the extension point for later seeded chaos policies.

TCP connections start with an eight-byte little-endian sender node ID. Every subsequent
frame on that connection is exactly a four-byte little-endian payload length followed
by one bincode-serialized `Message`. The receiver combines the handshake identity with
each decoded message to produce `(from_node, message)`.

## Planned Public API

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

`partition(a, b)` blocks both directions. `drop_next(from, to)` is directional,
consumes exactly one matching send, and repeated calls stack. `set_delay(from, to,
Duration::ZERO)` clears the directional delay. `heal()` clears every partition but
does not clear delays or pending one-shot drops.

Fault rules are evaluated when `send` begins. Changing a rule does not retroactively
alter a send already admitted by the scheduler.

## Error Handling

Unknown peers, listener and connection failures, and frame writes return the crate's
existing I/O error. Serialization and malformed-frame failures use the existing
corruption error. A malformed inbound TCP connection is closed without stopping the
node listener.

## Test Plan

1. Round-trip every `Message` variant through bincode.
2. Deliver messages among three in-memory endpoints.
3. Verify bidirectional partition/heal behavior and one-shot directional drops.
4. Round-trip every `Message` variant between two OS-assigned loopback sockets.
5. Run crate tests, workspace build, formatting, and all-target clippy with warnings
   denied.

## Deviations

No contract deviations are planned. The sender-ID handshake precedes framed messages
because `Message` intentionally contains no source node ID while `Transport::recv`
must return one.

# Raft Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development or superpowers:executing-plans to implement
> this plan task-by-task. Steps use checkbox syntax for tracking.

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

- [ ] Add an `rpc::tests::all_message_variants_roundtrip_with_bincode` test that builds
  one value of every enum variant, serializes each with `bincode::serialize`,
  deserializes with `bincode::deserialize::<Message>`, and asserts equality.
- [ ] Run `cargo test -p cairn-raft rpc::tests` and confirm the missing RPC types or
  serde implementations make it fail.
- [ ] Add the requested dependencies, exact frozen derives/fields, `LogEntry` serde
  derives, module declarations, and root re-exports.
- [ ] Re-run `cargo test -p cairn-raft rpc::tests` and the full crate test suite.
- [ ] Commit as `feat(raft): add serializable RPC contract`.

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

- [ ] Add Tokio tests for three-node delivery, partition then heal, and exactly one
  directional dropped message. Use timeouts only to prove non-delivery; compare
  received `(NodeId, Message)` values directly.
- [ ] Run `cargo test -p cairn-raft transport::in_memory::tests` and confirm it fails
  because the transport module and types do not exist.
- [ ] Implement a shared scheduler with validated node IDs, symmetric partition keys,
  directional drop counters and delays, stable seeded sequence numbers, and one
  receiver per endpoint. Closed destination receivers return an I/O error.
- [ ] Re-run focused and full crate tests.
- [ ] Commit as `feat(raft): add deterministic in-memory transport`.

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

- [ ] Add a two-node Tokio test that binds both nodes to `127.0.0.1:0`, exchanges every
  `Message` variant in both directions, and asserts sender IDs and payload equality.
- [ ] Run `cargo test -p cairn-raft transport::tcp::tests` and confirm it fails because
  `TcpTransport` is absent.
- [ ] Implement the sender handshake, LE length framing, bincode encoding/decoding,
  listener and reader tasks, lazy writer cache, failed-writer eviction, one reconnect
  attempt per send, and inbound channel.
- [ ] Re-run focused and full crate tests.
- [ ] Commit as `feat(raft): add framed TCP transport`.

### Task 4: Notes, quality gates, and review

**Files:**

- Modify: `crates/raft/TRANSPORT_NOTES.md`

- [ ] Replace planned wording with final exact signatures and record any actual
  contract deviations.
- [ ] Run `cargo fmt --all -- --check`.
- [ ] Run `cargo test -p cairn-raft`.
- [ ] Run `cargo build`.
- [ ] Run `cargo clippy -p cairn-raft --all-targets -- -D warnings`.
- [ ] Review the complete branch diff against the frozen contract and permitted files.
- [ ] Commit final note corrections as `docs(raft): finalize transport usage notes`.
