export const siteConfig = {
  name: "cairn",
  title: "cairn — Distributed KV Store in Rust",
  description:
    "A distributed key-value store built from scratch in Rust: a custom LSM storage engine, Raft consensus with linearizable reads, MVCC transactions, and a sharded control plane.",
  shortDescription:
    "A from-scratch, sharded, Raft-replicated, LSM-backed distributed key-value store.",
  url: "https://cairn.uptonm.dev",
  githubUrl: "https://github.com/uptonm/cairn",
  author: {
    name: "Mike Upton",
    url: "https://uptonm.dev",
  },
  keywords: [
    "distributed systems",
    "distributed key-value store",
    "database",
    "Rust",
    "Raft consensus",
    "linearizable reads",
    "linearizability",
    "LSM tree",
    "log-structured merge-tree",
    "LSM storage engine",
    "MVCC",
    "sharding",
    "systems programming",
    "portfolio project",
  ],
} as const;

export type SiteConfig = typeof siteConfig;
