<p align="center">
  <img src="./apps/site/public/icon-192.png" width="96" alt="cairn logo">
</p>

<h1 align="center"><code>cairn</code></h1>

<p align="center"><strong>Build a distributed database from first principles—and test its guarantees under failure.</strong></p>

<p align="center">
  A Rust key-value system with a custom LSM engine and tested single-group Raft core.<br>
  MVCC, multi-Raft, shard routing, and a chaos harness follow in deliberate layers.
</p>

<p align="center">
  <a href="https://cairn.uptonm.dev">Website</a> ·
  <a href="https://cairn.uptonm.dev/docs">Documentation</a> ·
  <a href="https://cairn.uptonm.dev/docs/architecture">Architecture</a> ·
  <a href="#status">Current status</a>
</p>

## Status

cairn is a systems portfolio project under active development, not a
production database. The repository currently contains libraries and test
harnesses; there is no runnable database server or cluster binary yet.

| Layer | State | Scope |
| --- | --- | --- |
| LSM storage engine | Implemented | Checksummed WAL, ordered memtable, indexed SSTables, bloom filters, atomic flush, compaction, recovery tests, and model-based property tests; remaining crash-atomicity work is tracked in the handoff |
| Single-group Raft | Implemented | On-disk log library, RPC types, pre-vote/election, replication, majority commit, read-index, deterministic simulations, and two transports; the core currently uses in-memory storage |
| Snapshots and membership | Next | Snapshot installation, log-compaction integration, and joint-consensus configuration changes |
| Node runtime | Planned | Durable storage adapter, async driver, and real-TCP integration |
| Chaos verification | Planned | Integrate the existing standalone linearizability checker with recorded fault histories |
| Transactions and sharding | Planned | MVCC, multi-Raft, shard router, placement control plane, and cluster dashboard |

The Raft simulation checks election safety, log matching, state-machine safety,
and leader completeness across partitions, drops, reordering, and
crash/restart scenarios. The storage engine is independently exercised against
a `BTreeMap` reference model and explicit recovery cases.

## Workspace

| Workspace | Purpose |
| --- | --- |
| [`crates/storage`](./crates/storage) | Standalone LSM-backed local key-value engine and Criterion benchmarks |
| [`crates/raft`](./crates/raft) | Raft log, storage contract, core state machine, RPC, transports, checker, and simulation tests |
| [`apps/site`](./apps/site) | Next.js marketing and Fumadocs documentation site for [cairn.uptonm.dev](https://cairn.uptonm.dev) |
| [`docs`](./docs) | Design specifications, implementation plans, and the current engineering handoff |

## Test the systems code

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

Run the storage-engine benchmarks separately:

```bash
cargo bench -p cairn-storage
```

## Run the documentation site

```bash
cd apps/site
bun install
bun run dev
```

Other site commands:

```bash
bun run build
bun run lint
bun run format
```

Local and preview site environments stay public. Production requires
`NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY`, `CLERK_SECRET_KEY`, `GATES_ORG_ID`, and
`GATES_APP_ID=cairn` for the fleet gate configuration.

Start with the [architecture guide](https://cairn.uptonm.dev/docs/architecture)
for the dependency order, the
[benchmarks](https://cairn.uptonm.dev/docs/benchmarks) for reproducible storage
numbers, and [`docs/HANDOFF.md`](./docs/HANDOFF.md) for the most current
implementation status and known limitations.
