import { useState } from "react";
import { MONTHS } from "@/lib/filters";
import { fmtInt, fmtPct } from "@/lib/metrics";

export type DayCell = { date: string; rate: number | null; flown: number; included: boolean };

export const DELAY_BINS = [
  { max: 0.12, color: "var(--sage)", label: "< 12%" },
  { max: 0.18, color: "var(--dusty)", label: "12–18%" },
  { max: 0.24, color: "var(--butter)", label: "18–24%" },
  { max: 0.3, color: "var(--peach)", label: "24–30%" },
  { max: Infinity, color: "var(--blush)", label: "30%+" },
];
export const delayColor = (r: number | null) =>
  r == null ? "var(--muted)" : DELAY_BINS.find((b) => r < b.max)!.color;

const DOW = ["Mon", "", "Wed", "", "Fri", "", "Sun"];

export function CalendarHeatmap({ days, year = 2015 }: { days: Map<string, DayCell>; year?: number }) {
  const [hover, setHover] = useState<DayCell | null>(null);
  const cell = 13;
  const gap = 3;
  const jan1 = new Date(Date.UTC(year, 0, 1));
  const offset = (jan1.getUTCDay() + 6) % 7; // Monday = 0
  const cells: { x: number; y: number; key: string; d: DayCell | undefined }[] = [];
  const monthCols: { m: number; col: number }[] = [];
  for (let i = 0; i < 365; i++) {
    const dt = new Date(jan1.getTime() + i * 864e5);
    const key = dt.toISOString().slice(0, 10);
    const dow = (dt.getUTCDay() + 6) % 7;
    const col = Math.floor((i + offset) / 7);
    if (dt.getUTCDate() === 1) monthCols.push({ m: dt.getUTCMonth(), col });
    cells.push({ x: col, y: dow, key, d: days.get(key) });
  }
  const left = 30;
  const top = 18;
  const width = left + 53 * (cell + gap);
  const height = top + 7 * (cell + gap);

  return (
    <div>
      <div className="overflow-x-auto">
        <svg width={width} height={height} className="block" role="img" aria-label="Daily delay rate calendar">
          {monthCols.map(({ m, col }) => (
            <text key={m} x={left + col * (cell + gap)} y={11} fontSize={10} fill="var(--muted-foreground)">
              {MONTHS[m]}
            </text>
          ))}
          {DOW.map((d, i) => (
            <text key={i} x={0} y={top + i * (cell + gap) + 10} fontSize={10} fill="var(--muted-foreground)">
              {d}
            </text>
          ))}
          {cells.map(({ x, y, key, d }) => {
            const included = d?.included ?? false;
            return (
              <rect
                key={key}
                x={left + x * (cell + gap)}
                y={top + y * (cell + gap)}
                width={cell}
                height={cell}
                rx={3}
                fill={included ? delayColor(d!.rate) : "var(--muted)"}
                opacity={included ? 1 : 0.6}
                stroke={hover?.date === key ? "var(--slate-accent)" : "none"}
                onMouseEnter={() => setHover(d ?? { date: key, rate: null, flown: 0, included: false })}
                onMouseLeave={() => setHover(null)}
              />
            );
          })}
        </svg>
      </div>
      <div className="mt-4 flex flex-wrap items-center justify-between gap-3 text-xs text-muted-foreground">
        <div className="flex flex-wrap items-center gap-3">
          <span>Delay rate</span>
          {DELAY_BINS.map((b) => (
            <span key={b.label} className="flex items-center gap-1.5">
              <span className="h-2.5 w-2.5 rounded-sm" style={{ background: b.color }} />
              {b.label}
            </span>
          ))}
          <span className="flex items-center gap-1.5">
            <span className="h-2.5 w-2.5 rounded-sm bg-muted" /> filtered out
          </span>
        </div>
        <span className="min-h-4 text-foreground">
          {hover
            ? `${new Date(hover.date + "T00:00:00Z").toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric", timeZone: "UTC" })} · ${
                hover.included ? `${fmtPct(hover.rate)} delayed of ${fmtInt(hover.flown)} flown` : "outside filters"
              }`
            : "Hover a day for details"}
        </span>
      </div>
    </div>
  );
}
