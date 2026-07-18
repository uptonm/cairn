import { ArchitectureDiagram } from "@/components/site/architecture-diagram";
import { Benchmarks } from "@/components/site/benchmarks";
import { DesignDecisions } from "@/components/site/design-decisions";
import { Hero } from "@/components/site/hero";
import { Roadmap } from "@/components/site/roadmap";
import { SiteFooter } from "@/components/site/site-footer";
import { SiteHeader } from "@/components/site/site-header";
import { WhyItIsHard } from "@/components/site/why-it-is-hard";

export default function Home() {
  return (
    <>
      <SiteHeader />
      <main className="flex-1">
        <Hero />
        <WhyItIsHard />
        <ArchitectureDiagram />
        <Roadmap />
        <Benchmarks />
        <DesignDecisions />
      </main>
      <SiteFooter />
    </>
  );
}
