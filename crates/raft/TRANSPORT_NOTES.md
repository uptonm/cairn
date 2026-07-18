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
