import { ArrowUpRight } from "lucide-react";
import { SectionHeading } from "./section-heading";

const decisions = [
  {
    href: "/docs/decisions/raft-over-paxos",
    title: "Raft over Paxos",
    summary:
      "Raft's decomposition into election, replication, and safety gives a chaos suite crisp invariants to check a captured history against — a legibility Paxos variants don't specify for free.",
  },
  {
    href: "/docs/decisions/lsm-over-btree",
    title: "LSM over B-tree",
    summary:
      "Sequential-append writes now, background merge cost later — and why the Raft log store stays a separate component instead of another LSM consumer.",
  },
  {
    href: "/docs/decisions/atomic-flush",
    title: "Atomic flush via temp+rename",
    summary:
      "A flush or compaction is never discoverable until it's fully written — write to .sst.tmp, rename into place. Closes a real restart-bricking bug.",
  },
  {
    href: "/docs/decisions/seqno-recovery",
    title: "Seqno recovery across restarts",
    summary:
      "A property test found a real bug: reopening after a flush under-reported next_seqno and silently resurrected stale data. Here's the repro and the fix.",
  },
];

export function DesignDecisions() {
  return (
    <section id="decisions" className="scroll-mt-14 bg-muted/20 py-20 sm:py-28">
      <div className="mx-auto max-w-6xl px-4 sm:px-6">
        <SectionHeading
          eyebrow="Design decisions"
          title="Written as ADRs, not marketing copy"
          description="The choice made, the alternative seriously considered, and why the tradeoff went the way it did — including a bug a property test actually caught."
        />

        <div className="mt-12 grid gap-4 sm:grid-cols-2">
          {decisions.map((d) => (
            <a
              key={d.href}
              href={d.href}
              className="group flex flex-col rounded-xl border border-border/60 bg-card/60 p-6 transition-colors hover:border-primary/40 hover:bg-card"
            >
              <div className="flex items-center justify-between gap-2">
                <h3 className="font-semibold tracking-tight">{d.title}</h3>
                <ArrowUpRight
                  className="size-4 shrink-0 text-muted-foreground transition-transform group-hover:translate-x-0.5 group-hover:-translate-y-0.5 group-hover:text-primary"
                  aria-hidden
                />
              </div>
              <p className="mt-3 text-sm leading-relaxed text-muted-foreground">
                {d.summary}
              </p>
            </a>
          ))}
        </div>
      </div>
    </section>
  );
}
