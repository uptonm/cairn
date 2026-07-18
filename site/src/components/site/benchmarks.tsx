import { SectionHeading } from "./section-heading";

const moduleTests: { module: string; count: number }[] = [
  { module: "engine", count: 11 },
  { module: "bloom", count: 5 },
  { module: "wal", count: 4 },
  { module: "memtable", count: 4 },
  { module: "sstable", count: 3 },
  { module: "types", count: 2 },
  { module: "recovery", count: 2 },
];

const maxCount = Math.max(...moduleTests.map((m) => m.count));

export function Benchmarks() {
  return (
    <section
      id="benchmarks"
      className="scroll-mt-14 border-b border-border/60 py-20 sm:py-28"
    >
      <div className="mx-auto max-w-6xl px-4 sm:px-6">
        <SectionHeading
          eyebrow="Benchmarks"
          title="Real numbers from the shipped engine"
          description="Single-node, single-thread criterion microbenchmarks on the storage engine — not a production SLA, and not a distributed-system number yet. Full methodology in the docs."
        />

        <div className="mt-12 grid gap-5 sm:grid-cols-3">
          <StatTile
            label="Sequential write throughput"
            value="~1.8M"
            unit="puts/sec"
            footnote="552µs per 1,000 sequential puts"
          />
          <StatTile
            label="Cold point-read latency"
            value="6.1"
            unit="µs"
            footnote="after flush + compaction, bloom-filtered lookup"
          />
          <StatTile
            label="Crate safety constraints"
            value="Zero"
            unit="unsafe"
            footnote="no .unwrap()/.expect() in I/O paths either"
          />
        </div>

        <div className="mt-8 rounded-xl border border-border/60 bg-card/60 p-6">
          <div className="flex flex-wrap items-baseline justify-between gap-2">
            <h3 className="text-sm font-semibold">
              Test coverage by module —{" "}
              <span className="font-mono font-normal text-muted-foreground">
                crates/storage
              </span>
            </h3>
            <p className="font-mono text-xs text-muted-foreground">
              32 tests total
            </p>
          </div>

          <ul
            className="mt-6 space-y-3"
            aria-label="Test count per storage engine module"
          >
            {moduleTests.map((m) => (
              <li key={m.module} className="flex items-center gap-3">
                <span className="w-20 shrink-0 font-mono text-xs text-muted-foreground">
                  {m.module}
                </span>
                <div className="relative h-6 flex-1 rounded-full bg-[var(--viz-grid)]/40">
                  <div
                    className="h-6 rounded-full bg-[var(--viz-series-1)] transition-[width]"
                    style={{ width: `${(m.count / maxCount) * 100}%` }}
                    title={`${m.module}: ${m.count} tests`}
                  />
                </div>
                <span className="w-6 shrink-0 text-right font-mono text-xs tabular-nums text-foreground">
                  {m.count}
                </span>
              </li>
            ))}
          </ul>

          <p className="mt-6 border-t border-border/60 pt-4 text-xs leading-relaxed text-muted-foreground">
            Plus one property-based test (
            <code className="font-mono">engine_matches_btreemap</code>) that
            runs ~200 randomized <code className="font-mono">Put</code>/
            <code className="font-mono">Delete</code>/
            <code className="font-mono">Flush</code>/
            <code className="font-mono">Compact</code>/
            <code className="font-mono">Reopen</code> operations per execution
            against a <code className="font-mono">BTreeMap</code> reference
            model — not reflected in the counts above since it isn&rsquo;t a
            fixed number of assertions. It caught a real bug: see{" "}
            <a
              href="/docs/decisions/seqno-recovery"
              className="underline underline-offset-4 hover:text-foreground"
            >
              seqno recovery
            </a>
            .
          </p>
        </div>
      </div>
    </section>
  );
}

function StatTile({
  label,
  value,
  unit,
  footnote,
}: {
  label: string;
  value: string;
  unit: string;
  footnote: string;
}) {
  return (
    <div className="rounded-xl border border-border/60 bg-card/60 p-6">
      <p className="text-sm text-muted-foreground">{label}</p>
      <p className="mt-2 flex items-baseline gap-1.5">
        <span className="font-mono text-4xl font-semibold tracking-tight text-foreground">
          {value}
        </span>
        <span className="font-mono text-sm text-primary">{unit}</span>
      </p>
      <p className="mt-2 font-mono text-xs text-muted-foreground">{footnote}</p>
    </div>
  );
}
