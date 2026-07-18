import { ArrowRight } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { siteConfig } from "@/lib/site-config";
import { GithubIcon } from "./github-icon";

export function Hero() {
  return (
    <section className="relative overflow-hidden border-b border-border/60">
      <div
        aria-hidden
        className="bg-grid bg-radial-fade pointer-events-none absolute inset-0"
      />
      <div
        aria-hidden
        className="pointer-events-none absolute -top-32 left-1/2 h-96 w-[48rem] -translate-x-1/2 rounded-full bg-primary/20 blur-[120px]"
      />

      <div className="relative mx-auto max-w-6xl px-4 pb-20 pt-20 sm:px-6 sm:pb-28 sm:pt-28">
        <div className="flex flex-col items-center text-center">
          <Badge
            variant="outline"
            className="rounded-full border-primary/30 bg-primary/5 px-3 py-1 font-mono text-xs text-primary"
          >
            Phase 1 shipped — custom LSM engine, 32 tests passing
          </Badge>

          <h1 className="mt-6 text-balance font-mono text-5xl font-semibold tracking-tighter sm:text-6xl md:text-7xl">
            cairn
          </h1>

          <p className="mt-5 max-w-2xl text-balance text-lg font-medium text-foreground/90 sm:text-xl">
            A from-scratch, sharded, Raft-replicated, LSM-backed distributed
            key-value store.
          </p>

          <p className="mt-4 max-w-xl text-balance text-sm leading-relaxed text-muted-foreground sm:text-base">
            Built in Rust to demonstrate hard-systems architecture — a custom
            storage engine, real consensus, multi-key transactions, and a
            sharded cluster — not to ship a product. Every guarantee is proven
            with tests, not asserted in a README.
          </p>

          <div className="mt-8 flex flex-col gap-3 sm:flex-row">
            <Button
              size="lg"
              nativeButton={false}
              render={
                // biome-ignore lint/a11y/useAnchorContent: Button's "View on GitHub" children are merged into this anchor at runtime via base-ui's `render` prop.
                <a
                  href={siteConfig.githubUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                />
              }
            >
              <GithubIcon className="size-4" aria-hidden />
              View on GitHub
            </Button>
            <Button
              size="lg"
              variant="outline"
              nativeButton={false}
              render={
                // biome-ignore lint/a11y/useAnchorContent: Button's "Read the docs" children are merged into this anchor at runtime via base-ui's `render` prop.
                <a href="/docs" />
              }
            >
              Read the docs
              <ArrowRight className="size-4" aria-hidden />
            </Button>
          </div>

          <div className="mt-14 w-full max-w-2xl overflow-hidden rounded-xl border border-border/60 bg-card text-left shadow-2xl shadow-black/10">
            <div className="flex items-center gap-1.5 border-b border-border/60 bg-muted/40 px-4 py-2.5">
              <span className="size-2.5 rounded-full bg-[#e34948]" />
              <span className="size-2.5 rounded-full bg-[#eda100]" />
              <span className="size-2.5 rounded-full bg-[#1baf7a]" />
              <span className="ml-2 font-mono text-xs text-muted-foreground">
                cargo bench -p cairn-storage
              </span>
            </div>
            <pre className="overflow-x-auto p-4 font-mono text-[13px] leading-relaxed">
              <code>
                <span className="text-muted-foreground">
                  {"// sequential writes, 1,000 puts\n"}
                </span>
                put_1k_seq{"              "}
                <span className="text-primary">time:</span> [548.11µs 552.30µs
                556.90µs]
                {"\n\n"}
                <span className="text-muted-foreground">
                  {"// cold point-read, post flush + compaction\n"}
                </span>
                get_hit_cold{"            "}
                <span className="text-primary">time:</span> [6.02µs 6.10µs
                6.19µs]
              </code>
            </pre>
          </div>
        </div>
      </div>
    </section>
  );
}
