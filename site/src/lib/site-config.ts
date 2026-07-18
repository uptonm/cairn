export const siteConfig = {
  name: "cairn",
  title:
    "cairn — a from-scratch, Raft-replicated, LSM-backed distributed KV store",
  description:
    "cairn is a distributed key-value store built from scratch in Rust: a custom log-structured merge-tree storage engine, Raft consensus, MVCC transactions, and a sharded control plane. Built to demonstrate hard-systems architecture, not to ship a product.",
  shortDescription:
    "A from-scratch, sharded, Raft-replicated, LSM-backed distributed key-value store.",
  url: "https://cairn.uptonm.dev",
  githubUrl: "https://github.com/uptonm/cairn",
  author: {
    name: "Mike Upton",
    url: "https://github.com/uptonm",
  },
  keywords: [
    "distributed systems",
    "key-value store",
    "database",
    "Rust",
    "Raft consensus",
    "LSM tree",
    "log-structured merge-tree",
    "MVCC",
    "sharding",
    "systems programming",
    "portfolio project",
  ],
} as const;

export type SiteConfig = typeof siteConfig;
