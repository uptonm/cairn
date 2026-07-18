import { ArrowRight } from "lucide-react";
import { SectionHeading } from "./section-heading";
import { type PhaseStatus, StatusBadge } from "./status-badge";

const phases: {
  n: number;
  title: string;
  status: PhaseStatus;
  summary: string;
  demo: string;
}[] = [
  {
    n: 1,
    title: "Custom LSM storage engine",
    status: "shipped",
    summary:
      "WAL, memtable, SSTables, bloom filters, full compaction. 32 tests, including property-based verification against a BTreeMap reference model.",
    demo: "Durable, crash-recoverable local KV store",
  },
  {
    n: 2,
    title: "Raft consensus (single group)",
    status: "in-progress",
    summary:
      "Design spec resolved: real TCP transport behind a pluggable trait, a dedicated Raft log store as the first buildable unit.",
    demo: "Chaos-tested replicated linearizable KV",
  },
  {
    n: 3,
    title: "MVCC transactions",
    status: "planned",
    summary:
      "Multi-key transactions at snapshot isolation, layered on Raft's commit ordering for a version source.",
    demo: "Snapshot-isolation transactions, checker-proven",
  },
  {
    n: 4,
    title: "Multi-Raft",
    status: "planned",
    summary:
      "Many independent Raft groups on one node set, each owning a contiguous key range — sequenced after 1–3 are chaos-tested.",
    demo: "Many key ranges, per-group consensus",
  },
  {
    n: 5,
    title: "Shard router + control plane",
    status: "planned",
    summary:
      "The one language boundary: a TypeScript/Bun control plane for placement, routing, and a live cluster dashboard.",
    demo: "Live sharded cluster, visualized, fault-tested",
  },
];

export function Roadmap() {
  return (
    <section
      id="roadmap"
      className="scroll-mt-14 border-b border-border/60 bg-muted/20 py-20 sm:py-28"
    >
      <div className="mx-auto max-w-6xl px-4 sm:px-6">
        <div className="flex flex-wrap items-end justify-between gap-4">
          <SectionHeading
            eyebrow="Roadmap"
            title="Every phase ends at a finished system"
            description="If work stops after any phase, what's already built stands alone — never a broken half of a larger system. Status is honest: one phase shipped, one in design, three planned."
          />
          <a
            href="/docs/roadmap"
            className="inline-flex shrink-0 items-center gap-1.5 font-mono text-sm text-primary hover:underline"
          >
            Full roadmap <ArrowRight className="size-3.5" aria-hidden />
          </a>
        </div>

        <ol className="mt-12 grid gap-4 lg:grid-cols-5">
          {phases.map((phase) => (
            <li
              key={phase.n}
              className="flex flex-col rounded-xl border border-border/60 bg-card/60 p-5"
            >
              <div className="flex items-center justify-between">
                <span className="font-mono text-xs text-muted-foreground">
                  Phase {phase.n}
                </span>
                <StatusBadge status={phase.status} />
              </div>
              <h3 className="mt-3 text-sm font-semibold leading-snug">
                {phase.title}
              </h3>
              <p className="mt-2 flex-1 text-xs leading-relaxed text-muted-foreground">
                {phase.summary}
              </p>
              <p className="mt-4 border-t border-border/60 pt-3 font-mono text-[11px] text-muted-foreground">
                {phase.demo}
              </p>
            </li>
          ))}
        </ol>
      </div>
    </section>
  );
}
