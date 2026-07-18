import type { Metadata } from "next";
import { siteConfig } from "@/lib/site-config";

export const socialImage = {
  url: "/og.png",
  width: 1200,
  height: 630,
  alt: "cairn — distributed systems, built from scratch",
};

export function createPageMetadata({
  title,
  description,
  path,
}: {
  title: string;
  description?: string;
  path: string;
}): Metadata {
  const socialTitle = `${title} — ${siteConfig.name}`;
  const pageDescription = description ?? siteConfig.description;

  return {
    title,
    description: pageDescription,
    alternates: {
      canonical: path,
    },
    openGraph: {
      title: socialTitle,
      description: pageDescription,
      url: path,
      type: "article",
      locale: "en_US",
      siteName: siteConfig.name,
      images: [socialImage],
    },
    twitter: {
      card: "summary_large_image",
      title: socialTitle,
      description: pageDescription,
      images: [socialImage.url],
    },
  };
}
