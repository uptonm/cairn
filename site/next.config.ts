import { createMDX } from "fumadocs-mdx/next";
import type { NextConfig } from "next";

const withMDX = createMDX();

const nextConfig: NextConfig = {
  reactStrictMode: true,
  images: {
    // Static export-friendly; no remote images are used on the site.
    unoptimized: true,
  },
};

export default withMDX(nextConfig);
