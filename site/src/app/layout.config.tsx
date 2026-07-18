import type { BaseLayoutProps } from "fumadocs-ui/layouts/shared";
import { Landmark } from "lucide-react";

/**
 * Shared layout configuration for both the marketing shell (via the site
 * header) and the fumadocs docs layout, so branding stays in one place.
 */
export const baseOptions: BaseLayoutProps = {
  nav: {
    title: (
      <span className="flex items-center gap-2 font-mono font-semibold tracking-tight">
        <Landmark className="size-4 text-primary" aria-hidden />
        cairn
      </span>
    ),
  },
  links: [
    { text: "Architecture", url: "/#architecture" },
    { text: "Roadmap", url: "/#roadmap" },
    { text: "Benchmarks", url: "/#benchmarks" },
    { text: "Docs", url: "/docs" },
    {
      text: "GitHub",
      url: "https://github.com/uptonm/cairn",
      external: true,
    },
  ],
};
