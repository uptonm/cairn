import type { MetadataRoute } from "next";
import { siteConfig } from "@/lib/site-config";

export default function manifest(): MetadataRoute.Manifest {
  return {
    name: "cairn — Distributed KV Store in Rust",
    short_name: "cairn",
    description: siteConfig.description,
    start_url: "/",
    display: "standalone",
    background_color: "#141110",
    theme_color: "#141110",
    icons: [
      {
        src: "/icon-192.png",
        sizes: "192x192",
        type: "image/png",
      },
      {
        src: "/icon-512.png",
        sizes: "512x512",
        type: "image/png",
      },
    ],
  };
}
