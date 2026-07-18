import { CheckCircle2, CircleDashed, CircleDot } from "lucide-react";
import { cn } from "@/lib/utils";

export type PhaseStatus = "shipped" | "in-progress" | "planned";

const statusConfig: Record<
  PhaseStatus,
  { label: string; icon: typeof CheckCircle2; className: string }
> = {
  shipped: {
    label: "Shipped",
    icon: CheckCircle2,
    className:
      "border-[color-mix(in_oklch,var(--viz-good),transparent_55%)] bg-[color-mix(in_oklch,var(--viz-good),transparent_88%)] text-[color-mix(in_oklch,var(--viz-good),var(--foreground)_20%)]",
  },
  "in-progress": {
    label: "In progress",
    icon: CircleDot,
    className:
      "border-primary/40 bg-primary/10 text-[color-mix(in_oklch,var(--primary),var(--foreground)_15%)]",
  },
  planned: {
    label: "Planned",
    icon: CircleDashed,
    className: "border-border bg-muted text-muted-foreground",
  },
};

export function StatusBadge({
  status,
  className,
}: {
  status: PhaseStatus;
  className?: string;
}) {
  const config = statusConfig[status];
  const Icon = config.icon;

  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 font-mono text-xs font-medium",
        config.className,
        className,
      )}
    >
      <Icon className="size-3.5" aria-hidden />
      {config.label}
    </span>
  );
}
