"use client";

import { Landmark, Menu } from "lucide-react";
import Link from "next/link";
import { useState } from "react";
import { Button } from "@/components/ui/button";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet";
import { siteConfig } from "@/lib/site-config";
import { GithubIcon } from "./github-icon";
import { ThemeToggle } from "./theme-toggle";

const navLinks = [
  { href: "/#architecture", label: "Architecture" },
  { href: "/#roadmap", label: "Roadmap" },
  { href: "/#benchmarks", label: "Benchmarks" },
  { href: "/#decisions", label: "Design decisions" },
  { href: "/docs", label: "Docs" },
];

export function SiteHeader() {
  const [open, setOpen] = useState(false);

  return (
    <header className="sticky top-0 z-50 border-b border-border/60 bg-background/80 backdrop-blur-md">
      <div className="mx-auto flex h-14 max-w-6xl items-center justify-between px-4 sm:px-6">
        <Link
          href="/"
          className="flex items-center gap-2 font-mono text-sm font-semibold tracking-tight"
        >
          <Landmark className="size-4 text-primary" aria-hidden />
          cairn
        </Link>

        <nav
          aria-label="Primary"
          className="hidden items-center gap-6 text-sm text-muted-foreground md:flex"
        >
          {navLinks.map((link) => (
            <Link
              key={link.href}
              href={link.href}
              className="transition-colors hover:text-foreground"
            >
              {link.label}
            </Link>
          ))}
        </nav>

        <div className="flex items-center gap-1">
          <ThemeToggle />
          <Button
            variant="ghost"
            size="icon"
            className="hidden sm:inline-flex"
            nativeButton={false}
            render={
              // biome-ignore lint/a11y/useAnchorContent: Button's children are merged into this anchor at runtime via base-ui's `render` prop; aria-label covers the icon-only accessible name.
              <a
                href={siteConfig.githubUrl}
                target="_blank"
                rel="noopener noreferrer"
                aria-label="cairn on GitHub"
              />
            }
          >
            <GithubIcon className="size-4" aria-hidden />
          </Button>
          <Button
            variant="default"
            size="sm"
            className="hidden sm:inline-flex"
            nativeButton={false}
            render={
              // biome-ignore lint/a11y/useAnchorContent: Button's "Star on GitHub" children are merged into this anchor at runtime via base-ui's `render` prop.
              <a
                href={siteConfig.githubUrl}
                target="_blank"
                rel="noopener noreferrer"
              />
            }
          >
            Star on GitHub
          </Button>

          <Sheet open={open} onOpenChange={setOpen}>
            <SheetTrigger
              render={
                <Button
                  variant="ghost"
                  size="icon"
                  className="md:hidden"
                  aria-label="Open menu"
                >
                  <Menu className="size-5" aria-hidden />
                </Button>
              }
            />
            <SheetContent side="right" className="w-72">
              <SheetHeader>
                <SheetTitle className="font-mono">cairn</SheetTitle>
              </SheetHeader>
              <nav aria-label="Mobile" className="flex flex-col gap-1 px-4">
                {navLinks.map((link) => (
                  <Link
                    key={link.href}
                    href={link.href}
                    onClick={() => setOpen(false)}
                    className="rounded-md px-2 py-2.5 text-sm text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
                  >
                    {link.label}
                  </Link>
                ))}
                <a
                  href={siteConfig.githubUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="mt-2 flex items-center gap-2 rounded-md px-2 py-2.5 text-sm font-medium text-foreground hover:bg-muted"
                >
                  <GithubIcon className="size-4" aria-hidden />
                  GitHub
                </a>
              </nav>
            </SheetContent>
          </Sheet>
        </div>
      </div>
    </header>
  );
}
