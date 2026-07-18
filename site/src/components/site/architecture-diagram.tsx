import { ArrowDown } from "lucide-react";
import { cn } from "@/lib/utils";
import { SectionHeading } from "./section-heading";
import { type PhaseStatus, StatusBadge } from "./status-badge";

const layers: {
  title: string;
  subtitle: string;
  detail: string;
  status: PhaseStatus;
  lang: "Rust" | "TS/Bun";
}[] = [
  {
    title: "Control plane / shard router",
    subtitle: "placement · split/rebalance · cluster dashboard",
    detail:
      "Routes client keys to Raft groups, decides shard placement, and renders a live view of cluster and leadership state.",
    status: "planned",
    lang: "TS/Bun",
  },
  {
    title: "Multi-Raft",
    subtitle: "many Raft groups, one node set",
    detail:
      "One Raft instance per key range, sharing a transport, so the cluster can host many independently-replicated shards.",
    status: "planned",
    lang: "Rust",
  },
  {
    title: "MVCC transaction layer",
    subtitle: "snapshot isolation, multi-key",
    detail:
      "Versioned keys ordered through the Raft log, with snapshot-isolation conflict checks at commit and GC folded into compaction.",
    status: "planned",
    lang: "Rust",
  },
  {
    title: "Raft consensus",
    subtitle: "election · replication · read-index · snapshot · membership",
    detail:
      "Full single-group Raft: pre-vote election, log replication, linearizable read-index reads, and joint-consensus membership changes.",
    status: "in-progress",
    lang: "Rust",
  },
  {
    title: "Custom LSM storage engine",
    subtitle: "WAL · memtable · SSTables · leveled compaction · bloom",
    detail:
      "Durable, crash-recoverable local key-value storage. Shipped and tested — 32 tests passing, including property-based verification.",
    status: "shipped",
    lang: "Rust",
  },
];

export function ArchitectureDiagram() {
  return (
    <section
      id="architecture"
      className="scroll-mt-14 border-b border-border/60 py-20 sm:py-28"
    >
      <div className="mx-auto max-w-6xl px-4 sm:px-6">
        <SectionHeading
          eyebrow="Architecture"
          title="Built strictly bottom-up"
          description="Every layer has one job and a narrow interface to the layer above it, so it can be built and proven correct in isolation. If work stopped after any layer, what exists below it is still a complete, working system."
        />

        <div className="mt-14 flex flex-col items-stretch gap-0">
          {layers.map((layer, i) => (
            <div key={layer.title} className="flex flex-col items-center">
              <div
                className={cn(
                  "group relative w-full max-w-3xl rounded-xl border p-5 transition-colors sm:p-6",
                  layer.status === "shipped"
                    ? "border-primary/40 bg-primary/[0.06]"
                    : "border-border/60 bg-card/60",
                )}
              >
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div className="flex items-center gap-3">
                    <span className="font-mono text-xs text-muted-foreground">
                      {String(layers.length - i).padStart(2, "0")}
                    </span>
                    <h3 className="font-semibold tracking-tight sm:text-lg">
                      {layer.title}
                    </h3>
                    <span className="hidden rounded-full border border-border/60 px-2 py-0.5 font-mono text-[11px] text-muted-foreground sm:inline-block">
                      {layer.lang}
                    </span>
                  </div>
                  <StatusBadge status={layer.status} />
                </div>
                <p className="mt-2 font-mono text-xs text-muted-foreground">
                  {layer.subtitle}
                </p>
                <p className="mt-3 text-sm leading-relaxed text-muted-foreground">
                  {layer.detail}
                </p>
              </div>

              {i < layers.length - 1 ? (
                <ArrowDown
                  className="my-2 size-4 shrink-0 text-muted-foreground/50"
                  aria-hidden
                />
              ) : null}
            </div>
          ))}
        </div>

        <p className="mx-auto mt-8 max-w-3xl text-center text-xs text-muted-foreground">
          Layers are ordered by dependency (bottom = foundation). The source
          spec groups the LSM engine and Raft consensus as one delivery cycle —
          see the{" "}
          <a
            href="/docs/roadmap"
            className="underline underline-offset-4 hover:text-foreground"
          >
            roadmap
          </a>{" "}
          for phase-by-phase status.
        </p>
      </div>
    </section>
  );
}
