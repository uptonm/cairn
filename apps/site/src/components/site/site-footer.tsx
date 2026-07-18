import { Landmark } from "lucide-react";
import Link from "next/link";
import { siteConfig } from "@/lib/site-config";
import { GithubIcon } from "./github-icon";

const columns = [
  {
    title: "Project",
    links: [
      { href: "/#architecture", label: "Architecture" },
      { href: "/#roadmap", label: "Roadmap" },
      { href: "/#benchmarks", label: "Benchmarks" },
      { href: "/#decisions", label: "Design decisions" },
    ],
  },
  {
    title: "Docs",
    links: [
      { href: "/docs", label: "Introduction" },
      { href: "/docs/architecture", label: "Architecture" },
      { href: "/docs/lsm-engine", label: "LSM engine" },
      { href: "/docs/decisions", label: "ADRs" },
    ],
  },
  {
    title: "Source",
    links: [
      {
        href: siteConfig.githubUrl,
        label: "GitHub repository",
        external: true,
      },
      {
        href: `${siteConfig.githubUrl}/tree/main/crates/storage`,
        label: "crates/storage",
        external: true,
      },
      {
        href: `${siteConfig.githubUrl}/tree/main/docs`,
        label: "Design specs",
        external: true,
      },
    ],
  },
];

export function SiteFooter() {
  return (
    <footer className="border-t border-border/60">
      <div className="mx-auto max-w-6xl px-4 py-14 sm:px-6">
        <div className="grid gap-10 md:grid-cols-[1.4fr_1fr_1fr_1fr]">
          <div>
            <div className="flex items-center gap-2 font-mono text-sm font-semibold tracking-tight">
              <Landmark className="size-4 text-primary" aria-hidden />
              cairn
            </div>
            <p className="mt-3 max-w-xs text-sm text-muted-foreground">
              {siteConfig.shortDescription} Built to demonstrate hard-systems
              architecture — not a product.
            </p>
            <a
              href={siteConfig.githubUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="mt-4 inline-flex items-center gap-2 text-sm font-medium text-foreground hover:text-primary"
            >
              <GithubIcon className="size-4" aria-hidden />
              github.com/uptonm/cairn
            </a>
          </div>

          {columns.map((col) => (
            <div key={col.title}>
              <h3 className="font-mono text-xs font-medium uppercase tracking-wider text-muted-foreground">
                {col.title}
              </h3>
              <ul className="mt-3 space-y-2">
                {col.links.map((link) => (
                  <li key={link.href}>
                    {"external" in link && link.external ? (
                      <a
                        href={link.href}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-sm text-muted-foreground transition-colors hover:text-foreground"
                      >
                        {link.label}
                      </a>
                    ) : (
                      <Link
                        href={link.href}
                        className="text-sm text-muted-foreground transition-colors hover:text-foreground"
                      >
                        {link.label}
                      </Link>
                    )}
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>

        <div className="mt-12 flex flex-col gap-2 border-t border-border/60 pt-6 text-xs text-muted-foreground sm:flex-row sm:items-center sm:justify-between">
          <p>
            &copy; {new Date().getFullYear()} {siteConfig.author.name}. Source
            available under the repository&rsquo;s license.
          </p>
          <p className="font-mono">
            cairn is a portfolio project — not production software.
          </p>
        </div>
      </div>
    </footer>
  );
}
