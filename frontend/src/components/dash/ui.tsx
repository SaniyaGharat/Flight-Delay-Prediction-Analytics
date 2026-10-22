import type { ReactNode } from "react";
import { Download } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";

export function PageHeader({ eyebrow, title, intro }: { eyebrow: string; title: string; intro: ReactNode }) {
  return (
    <header className="mb-8 max-w-3xl">
      <p className="text-xs uppercase tracking-[0.14em] text-muted-foreground">{eyebrow}</p>
      <h1 className="font-display mt-2 text-3xl leading-tight text-foreground md:text-4xl">{title}</h1>
      <p className="mt-3 text-[15px] leading-relaxed text-muted-foreground">{intro}</p>
    </header>
  );
}

export function Panel({
  title,
  takeaway,
  actions,
  children,
  className,
  footnote,
}: {
  title: string;
  takeaway?: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
  className?: string;
  footnote?: ReactNode;
}) {
  return (
    <section className={cn("rounded-xl border border-border bg-card p-5 shadow-card md:p-6", className)}>
      <div className="mb-4 flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <h2 className="text-[15px] font-medium text-foreground">{title}</h2>
          {takeaway && <p className="mt-1 text-sm leading-relaxed text-muted-foreground">{takeaway}</p>}
        </div>
        {actions}
      </div>
      {children}
      {footnote && <p className="mt-4 border-t border-border pt-3 text-xs text-muted-foreground">{footnote}</p>}
    </section>
  );
}

export function Kpi({ label, value, hint, emphasis }: { label: string; value: string; hint?: string | undefined; emphasis?: boolean | undefined }) {
  return (
    <div className="rounded-xl border border-border bg-card p-5 shadow-card">
      <p className="text-xs uppercase tracking-[0.12em] text-muted-foreground">{label}</p>
      <p className={cn("font-display mt-3 text-3xl", emphasis ? "text-primary" : "text-foreground")}>{value}</p>
      {hint && <p className="mt-1 text-xs text-muted-foreground">{hint}</p>}
    </div>
  );
}

export function KpiSkeleton() {
  return (
    <div className="rounded-xl border border-border bg-card p-5 shadow-card">
      <Skeleton className="h-3 w-24 bg-muted" />
      <Skeleton className="mt-4 h-8 w-28 bg-muted" />
    </div>
  );
}

export function ChartSkeleton({ height = 280 }: { height?: number }) {
  return (
    <div className="flex items-end gap-2" style={{ height }} aria-label="Loading chart">
      {Array.from({ length: 14 }).map((_, i) => (
        <Skeleton key={i} className="flex-1 bg-muted" style={{ height: `${30 + ((i * 37) % 60)}%` }} />
      ))}
    </div>
  );
}

export function ErrorState({ error }: { error: unknown }) {
  const msg = error instanceof Error ? error.message : String(error);
  return (
    <div className="rounded-xl border border-dashed border-blush bg-card p-8 text-center">
      <p className="font-display text-xl text-foreground">Data could not be loaded</p>
      <p className="mt-2 text-sm text-muted-foreground">{msg}</p>
      <p className="mt-1 text-xs text-muted-foreground">Expected files live in /public/data. Check the file name and try reloading.</p>
    </div>
  );
}

export function EmptyState({ children = "No flights match the current filters." }: { children?: ReactNode }) {
  return (
    <div className="flex min-h-40 items-center justify-center rounded-lg bg-muted/60 p-6 text-sm text-muted-foreground">
      {children}
    </div>
  );
}

export function downloadCsv(filename: string, rows: Record<string, string | number | null | undefined>[]) {
  if (!rows.length) return;
  const headers = Object.keys(rows[0]!);
  const esc = (v: unknown) => {
    const s = v == null ? "" : String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const csv = [headers.join(","), ...rows.map((r) => headers.map((h) => esc(r[h])).join(","))].join("\n");
  const url = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

export function DownloadButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      className="inline-flex items-center gap-1.5 rounded-md border border-border px-2.5 py-1.5 text-xs text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
    >
      <Download className="h-3.5 w-3.5" strokeWidth={1.5} />
      Download this table as CSV
    </button>
  );
}

/** Recharts tooltip in the house style. */
export function ChartTip({
  active,
  label,
  rows,
}: {
  active?: boolean | undefined;
  label?: ReactNode;
  rows: { name: string; value: string; color?: string | undefined }[];
}) {
  if (!active) return null;
  return (
    <div className="rounded-lg border border-border bg-card px-3 py-2 text-xs shadow-card">
      {label != null && <p className="mb-1 font-medium text-foreground">{label}</p>}
      {rows.map((r) => (
        <p key={r.name} className="flex items-center gap-2 text-muted-foreground">
          {r.color && <span className="h-2 w-2 rounded-full" style={{ background: r.color }} />}
          {r.name}: <span className="text-foreground">{r.value}</span>
        </p>
      ))}
    </div>
  );
}

export const axisProps = {
  stroke: "var(--muted-foreground)",
  tick: { fill: "var(--muted-foreground)", fontSize: 11 },
  tickLine: false,
  axisLine: { stroke: "var(--border)" },
} as const;

export const gridProps = { stroke: "var(--grid-line)", strokeDasharray: "2 4", vertical: false } as const;
