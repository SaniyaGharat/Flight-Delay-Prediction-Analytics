import { useState, type ReactNode } from "react";

export type HeatCell = { v: number | null; note?: string };

/** Generic matrix heatmap in the house style: rows x cols of pastel cells with a hover readout. */
export function MatrixHeatmap({
  colLabels,
  rows,
  cell,
  color,
  format,
  legend,
  corner,
  minWidth = 520,
  colWidth = 34,
}: {
  colLabels: string[];
  rows: { key: string; label: string }[];
  cell: (rowKey: string, colIndex: number) => HeatCell | undefined;
  color: (v: number | null) => string;
  format: (v: number | null) => string;
  legend?: ReactNode;
  corner?: string;
  minWidth?: number;
  colWidth?: number;
}) {
  const [hover, setHover] = useState<{ row: string; col: string; v: number | null; note?: string | undefined } | null>(null);
  return (
    <div>
      <div className="overflow-x-auto pb-1">
        <div
          className="grid gap-[3px] text-xs"
          style={{
            gridTemplateColumns: `minmax(92px, max-content) repeat(${colLabels.length}, minmax(${colWidth}px, 1fr))`,
            minWidth,
          }}
        >
          <div className="self-end pb-1 text-[10px] text-muted-foreground">{corner}</div>
          {colLabels.map((c) => (
            <div key={c} className="pb-1 text-center text-[10px] leading-tight text-muted-foreground">
              {c}
            </div>
          ))}
          {rows.map((r) => (
            <div key={r.key} className="contents">
              <div className="truncate pr-2 text-right text-[11px] leading-6 text-muted-foreground">{r.label}</div>
              {colLabels.map((c, ci) => {
                const v = cell(r.key, ci);
                const on = hover?.row === r.label && hover?.col === c;
                return (
                  <div
                    key={`${r.key}|${c}`}
                    className="h-6 rounded-[3px]"
                    style={{
                      background: color(v?.v ?? null),
                      boxShadow: on ? "0 0 0 1.5px var(--slate-accent)" : "none",
                    }}
                    onMouseEnter={() => setHover({ row: r.label, col: c, v: v?.v ?? null, note: v?.note })}
                    onMouseLeave={() => setHover(null)}
                  />
                );
              })}
            </div>
          ))}
        </div>
      </div>
      <div className="mt-4 flex flex-wrap items-center justify-between gap-3 text-xs text-muted-foreground">
        <div className="flex flex-wrap items-center gap-3">{legend}</div>
        <span className="min-h-4 text-foreground">
          {hover ? `${hover.row} · ${hover.col}: ${format(hover.v)}${hover.note ? ` (${hover.note})` : ""}` : "Hover a cell for details"}
        </span>
      </div>
    </div>
  );
}

export function BinLegend({ label, bins, extra }: { label: string; bins: { color: string; label: string }[]; extra?: ReactNode }) {
  return (
    <>
      <span>{label}</span>
      {bins.map((b) => (
        <span key={b.label} className="flex items-center gap-1.5">
          <span className="h-2.5 w-2.5 rounded-sm" style={{ background: b.color }} />
          {b.label}
        </span>
      ))}
      {extra}
    </>
  );
}
