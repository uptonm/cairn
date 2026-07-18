# cairn marketing/docs site — build notes

## Stack

- **Bun** — package manager and script runner.
- **TypeScript** throughout, no `.js`/`.jsx`.
- **Next.js 16 (App Router, Turbopack)** — `next build`/`next dev` are
  Turbopack by default in this version.
- **Tailwind CSS v4** — CSS-first config (`@theme`, `@import "tailwindcss"`),
  no `tailwind.config.js`.
- **shadcn/ui** on the `base-nova` style/`@base-ui/react` primitives (the
  current `shadcn` CLI default) — `Button`, `Badge`, `Card`, `Tabs`,
  `Separator`, `Accordion`, `Tooltip`, `Sheet`.
- **Fumadocs** (`fumadocs-ui` + `fumadocs-core` + `fumadocs-mdx`) for the
  `/docs` section — MDX content, generated sidebar/TOC, breadcrumbs,
  prev/next nav, and local Orama search, wired into the same Next app as the
  marketing site (not a separate deployment).
- **Biome** for lint + format (`bun run lint` / `bun run format`).

## Running it

```bash
cd apps/site
bun install
bun run dev     # http://localhost:3000 (or next free port)
bun run build   # production build — must succeed before merging
```

`predev`/`prebuild` run `fumadocs-mdx` to regenerate the `.source/`
collection from `content/docs/**/*.mdx` — it's gitignored and rebuilt every
run, same as `.next/`.

Verified locally: `bun run build` completes with `Compiled successfully`,
TypeScript passes, and all 23 routes prerender (see route table below).
`bun run lint` passes (4 warnings, 0 errors — see Known gaps).

## Routes / sections built

**Marketing (`/`)** — single page, section-anchored (`#architecture`,
`#roadmap`, `#benchmarks`, `#decisions`):

- `Hero` — name, tagline, hook, primary GitHub CTA, secondary docs CTA, a
  terminal-styled card showing real `criterion` output.
- `WhyItIsHard` — four pillars (storage engine, consensus, layering,
  provable guarantees).
- `ArchitectureDiagram` — the five-layer stack rendered as styled cards +
  connectors (no image asset), each tagged with language and status.
- `Roadmap` — five-phase status grid (shipped / in-progress / planned),
  links to the full `/docs/roadmap`.
- `Benchmarks` — three stat tiles (write throughput, read latency, safety
  constraints) plus a hand-rolled single-series bar chart of test counts
  per storage-engine module (see Design decisions below for why it's not
  Recharts).
- `DesignDecisions` — four ADR cards linking into `/docs/decisions/*`.
- `SiteHeader` (sticky, blurred, mobile `Sheet` menu) and `SiteFooter`.

**Docs (`/docs/**`)** — Fumadocs `DocsLayout`, content in
`content/docs/**/*.mdx`:

- `/docs` — introduction (what cairn is, guarantees, non-goals).
- `/docs/architecture` — the layered design + data flow + testing strategy.
- `/docs/lsm-engine` (+ `wal`, `memtable`, `sstables-and-bloom`,
  `compaction`) — internals of the shipped Phase 1 engine.
- `/docs/roadmap` — phase-by-phase status, expanded from the homepage grid.
- `/docs/benchmarks` — the same numbers as the homepage, plus methodology
  and a reproduction command.
- `/docs/decisions` (+ `raft-over-paxos`, `lsm-over-btree`, `atomic-flush`,
  `seqno-recovery`) — ADR-style writeups, two of which (`atomic-flush`,
  `seqno-recovery`) are drawn directly from real commits in `crates/storage`
  history, including a bug a property test actually caught.

## Content sourcing — no fabrication

All copy is pulled from the repo's own source of truth:

- `docs/superpowers/specs/2026-07-18-cairn-distributed-kv-design.md` —
  architecture, guarantees, non-goals, phase summary.
- `docs/superpowers/plans/2026-07-18-lsm-storage-engine.md` — LSM engine
  internals (WAL framing, memtable ordering, SSTable layout, bloom filter,
  compaction).
- The Raft design spec (`docs/superpowers/specs/2026-07-18-cairn-raft-design.md`),
  read from the `feat/raft-log-store` branch since it isn't merged to `main`
  in this worktree yet — used for the roadmap's honest "in design" status
  and the Raft-over-Paxos ADR.
- **Real numbers, not estimates**: `cargo test -p cairn-storage` was run in
  this worktree — 32 tests pass (29 unit + 1 property + 2 integration),
  matching what's published on the benchmarks page. Test-per-module counts
  on the homepage bar chart come from `grep -c '#\[test\]'` against the
  actual `crates/storage/src/*.rs` files, not the plan doc's estimates.
- The two "design decisions" about atomic flush and seqno recovery are
  written from `git show` on the actual fix commits
  (`97b3085`, `22601da`, `c9ddd49`) — the seqno-recovery ADR in particular
  documents a real bug the property test caught, with the actual repro and
  fix.
- Benchmark numbers (552µs/1,000 puts, 6.1µs cold read) were provided in the
  task brief as the real `cargo bench` output and are presented with an
  explicit "single-node, single-thread, not a production SLA" caveat rather
  than extrapolated into distributed-system claims.

## SEO measures

- Per-route `<title>`/`<meta description>` — a title template in the root
  layout (`%s — cairn`) plus `generateMetadata` on every docs page from
  MDX frontmatter.
- Open Graph + Twitter card tags (`summary_large_image`) on every route via
  the root layout's `openGraph`/`twitter` metadata, referencing a single
  generated OG image.
- **Generated OG image** at `/opengraph-image` (`next/og` `ImageResponse`,
  1200×630) — brand mark, name, tagline, and phase-status chips rendered as
  an image at request/build time, not a static asset.
- `sitemap.xml` (`src/app/sitemap.ts`) — enumerates `/` plus every page
  `fumadocs`'s `source.getPages()` returns, so it can't drift from the docs
  tree.
- `robots.txt` (`src/app/robots.ts`) — allow-all, points at the sitemap.
  Not blocked for indexing since this is a public flagship demo (the "do
  not deploy" instruction covers *this task*, not a permanent
  no-index policy).
- Canonical URLs via `alternates.canonical` on the root layout and on every
  docs page (`generateMetadata`).
- Semantic headings throughout (MDX `h1`–`h3`, one `h1` per docs page from
  `DocsTitle`); marketing sections use `h2`/`h3` under a single hero `h1`.
- `JSON-LD` `SoftwareApplication` schema in the root layout (name,
  description, repo URL, author, language list).
- Static generation: every route in the table below is `○` (static) or `●`
  (SSG via `generateStaticParams`) except `/api/search`, which is
  necessarily dynamic.

```
Route (app)
┌ ○ /
├ ○ /_not-found
├ ƒ /api/search
├ ● /docs/[[...slug]]        (14 pages, generateStaticParams)
├ ○ /icon.svg
├ ○ /opengraph-image
├ ○ /robots.txt
└ ○ /sitemap.xml
```

## Design decisions (site build itself)

- **Theme**: a warm "stone + amber" palette (`src/app/globals.css`) instead
  of shadcn's default neutral gray — dark-mode-first (`defaultTheme="dark"`
  via `fumadocs-ui`'s `RootProvider`, which wraps `next-themes`), with an
  independently-tuned light mode, not an inverted dark palette. Chosen to
  fit "cairn" (stacked waymarker stones) and to read as a database/terminal
  product rather than a generic shadcn starter.
- **`fumadocs-ui/css/shadcn.css`** maps Fumadocs' `--color-fd-*` tokens onto
  our own shadcn CSS variables, so the docs shell (sidebar, TOC, search
  dialog) inherits the site's palette instead of shipping a second one.
- **Charts follow the `dataviz` skill's palette verbatim** — the categorical
  blue (`#2a78d6`/`#3987e5`) and status-good green
  (`#0ca30c`) hex values in `globals.css`'s `--viz-*` tokens are the skill's
  validated reference palette, applied to the exact reference surfaces
  (`#fcfcfb` light / `#1a1a19` dark), so the existing validator result
  applies unchanged.
- **Benchmarks chart is hand-rolled HTML/CSS, not Recharts**: with only two
  headline numbers and one 7-category single-series comparison, a
  client-side charting library was unnecessary weight (`recharts` was
  installed, then removed — YAGNI). The bar chart follows the skill's mark
  spec by hand (rounded caps, direct value labels, single-series → no
  legend box needed) and is a server component, avoiding a hydration
  boundary for something this simple.
- **`GithubIcon` is a hand-written SVG**, not `lucide-react`'s `Github`
  export — the installed `lucide-react@1.25.0` (a recent major version) has
  dropped brand/logo glyphs entirely; every other icon on the site is a
  real `lucide-react` import.
- **`base-ui` `render` prop, not `asChild`**: the shadcn `base-nova` style
  used here is built on `@base-ui/react`, which uses a `render={<a />}` prop
  for polymorphism instead of Radix's `asChild` + `Slot` pattern. Every
  link-styled-as-button on the site uses this; anchors that get their
  visible content merged in at runtime (rather than written literally
  inside the `render` JSX) carry a `biome-ignore` for
  `lint/a11y/useAnchorContent`, since Biome's static check can't see the
  runtime merge.

## Known gaps / TODOs

- **Domain**: metadata (`siteConfig.url`), sitemap, and OG tags all point at
  `https://cairn.uptonm.dev`, matching the homelab's `*.uptonm.io` Caddy
  convention — not yet provisioned or deployed. Confirm before going live.
- **No dynamic per-doc-page OG images** — every route shares the one root
  `/opengraph-image`. A per-page variant (title overlay) would be a nice
  follow-up, not required for launch.
- **No dark/light screenshot regression testing** — verified manually via
  Playwright screenshots in both modes at desktop and mobile widths during
  this build; no automated visual test exists yet.
- **Search is local Orama** (`fumadocs-core/search/server`'s
  `createFromSource`), not a hosted/cloud index — fine at this content
  size, would need revisiting if the docs tree grows substantially.
- **Biome reports 4 warnings** (`lint/complexity/noImportantStyles`) on the
  `prefers-reduced-motion` block in `globals.css` — `!important` is
  intentional there (the standard pattern for a global motion override) and
  left as-is rather than suppressed or removed.
- **The Raft design spec lives on the unmerged `feat/raft-log-store`
  branch**, not `main`, in this repo. The roadmap and ADRs describe it as
  "in design," which is accurate to that branch's actual content (a design
  spec and a log-store implementation *plan*, no `crates/raft` code yet) —
  worth re-checking this page once that branch merges.
