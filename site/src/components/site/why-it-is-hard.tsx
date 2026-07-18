import { Database, GitBranch, Layers, ShieldCheck } from "lucide-react";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { SectionHeading } from "./section-heading";

const pillars = [
  {
    icon: Database,
    title: "A storage engine, not a wrapper",
    description:
      'No embedded database underneath. The WAL, memtable, SSTables, bloom filters, and compaction are all hand-built in Rust — the part most "distributed KV stores" skip by reaching for RocksDB.',
  },
  {
    icon: GitBranch,
    title: "Real consensus, not a leader flag",
    description:
      "Raft with pre-vote, log replication, read-index linearizable reads, snapshotting, and joint-consensus membership changes — driven through a deterministic simulator so consensus bugs are reproducible, not folklore.",
  },
  {
    icon: Layers,
    title: "Layered by dependency, not by guesswork",
    description:
      "Storage → consensus → transactions → sharding → routing. Each layer has one job and a narrow interface, so the system is a complete, working thing at the end of every phase — never a broken half of a bigger one.",
  },
  {
    icon: ShieldCheck,
    title: "Guarantees you can check, not just claim",
    description:
      "Linearizability and snapshot isolation are properties a chaos/Jepsen-style suite has to prove against a captured history — partitions, drops, crashes, and clock skew included.",
  },
];

export function WhyItIsHard() {
  return (
    <section className="border-b border-border/60 bg-muted/20 py-20 sm:py-28">
      <div className="mx-auto max-w-6xl px-4 sm:px-6">
        <SectionHeading
          eyebrow="Why it's hard"
          title="The complexity is the point"
          description="cairn isn't hard because of scale — it's hard because consensus, storage-engine internals, transaction isolation, and shard placement are each genuinely difficult problems on their own, and the design has to be right at every seam between them."
        />

        <div className="mt-12 grid gap-5 sm:grid-cols-2">
          {pillars.map((pillar) => (
            <Card key={pillar.title} className="border-border/60 bg-card/60">
              <CardHeader>
                <div className="mb-1 flex size-9 items-center justify-center rounded-lg border border-primary/30 bg-primary/10">
                  <pillar.icon className="size-4.5 text-primary" aria-hidden />
                </div>
                <CardTitle className="text-lg">{pillar.title}</CardTitle>
              </CardHeader>
              <CardContent>
                <CardDescription className="text-sm leading-relaxed">
                  {pillar.description}
                </CardDescription>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    </section>
  );
}
