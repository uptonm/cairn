import { loader } from "fumadocs-core/source";
import { icons } from "lucide-react";
import { createElement } from "react";
import { docs } from "../../.source/server";

export const source = loader({
  baseUrl: "/docs",
  source: docs.toFumadocsSource(),
  icon(iconName) {
    if (!iconName) return undefined;
    if (iconName in icons) {
      return createElement(icons[iconName as keyof typeof icons]);
    }
    return undefined;
  },
});
