# cairn — marketing site + docs

The public site for [cairn](https://github.com/uptonm/cairn), a from-scratch
distributed key-value store. Next.js (App Router) + Tailwind + shadcn/ui for
the marketing pages, [Fumadocs](https://fumadocs.dev) for `/docs`.

See [`BUILD_NOTES.md`](./BUILD_NOTES.md) for the stack, routes, SEO measures,
and design decisions in full.

## Getting started

```bash
bun install
bun run dev     # http://localhost:3000 (or next free port)
```

## Scripts

- `bun run dev` — local dev server (Turbopack).
- `bun run build` — production build; must succeed before merging.
- `bun run lint` / `bun run format` — Biome.

Content for `/docs` lives in `content/docs/**/*.mdx`. The `.source/`
directory it compiles to is generated (`predev`/`prebuild` regenerate it)
and gitignored.
