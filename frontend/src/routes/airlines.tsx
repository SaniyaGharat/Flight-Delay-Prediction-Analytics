import { createFileRoute } from "@tanstack/react-router";
import { useMemo, useState } from "react";
import { Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { useAirlines, useAirlineDowHour, useDailyAirline, useDimDate } from "@/lib/data";
import { matchesAirline, matchesDate, useFilters } from "@/lib/filters";
import {
  addInto, avgArrDelay, cancelRate, delayRate, emptyCounts, fmtInt, fmtMin, fmtPct, onTimeRate, type Counts,
} from "@/lib/metrics";
import {
  ChartSkeleton, ChartTip, DownloadButton, EmptyState, ErrorState, Kpi, KpiSkeleton, PageHeader, Panel, axisProps, downloadCsv, gridProps,
} from "@/components/dash/ui";
import { DELAY_BINS, delayColor } from "@/components/dash/CalendarHeatmap";
import { BinLegend, MatrixHeatmap } from "@/components/dash/Heatmap";

export const Route = createFileRoute("/airlines")({
  head: () => ({
    meta: [
      { title: "Airlines · Flight Delay Analytics" },
      { name: "description", content: "On-time ranking, carrier comparison table, airline by month and hour-of-day patterns." },
      { property: "og:title", content: "Airlines · Flight Delay Analytics" },
      { property: "og:description", content: "On-time ranking, carrier comparison table, airline by month and hour-of-day patterns." },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary_large_image" },
    ],
  }),
  component: AirlinesPage,
});

type CarrierRow = {
  code: string;
  name: string;
  c: Counts;
  onTime: number | null;
  delay: number | null;
  cancel: number | null;
  avg: number | null;
};

type SortKey = "name" | "flights" | "cancel" | "delay" | "avg";

const DOW_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

function AirlinesPage() {
  const dates = useDimDate();
  const daily = useDailyAirline();
  const airlines = useAirlines();
  const dowHour = useAirlineDowHour();
  const { filters } = useFilters();
  const [sort, setSort] = useState<{ key: SortKey; dir: 1 | -1 }>({ key: "name", dir: 1 });

  const model = useMemo(() => {
    if (!dates.data || !daily.data) return null;
    const byAirline = new Map<string, Counts>();
    const byAirlineMonth = new Map<string, Counts>();
    const total = emptyCounts();
    for (const r of daily.data) {
      const d = dates.data.get(r.date);
      if (!matchesDate(filters, d)) continue;
      if (!byAirline.has(r.airline)) byAirline.set(r.airline, emptyCounts());
      addInto(byAirline.get(r.airline)!, r);
      const mk = `${r.airline}|${d!.month}`;
      if (!byAirlineMonth.has(mk)) byAirlineMonth.set(mk, emptyCounts());
      addInto(byAirlineMonth.get(mk)!, r);
      addInto(total, r);
    }
    return { byAirline, byAirlineMonth, total };
  }, [dates.data, daily.data, filters]);

  const ranking = useMemo<CarrierRow[] | null>(() => {
    if (!model || !airlines.data) return null;
    return airlines.data
      .map((a) => {
        const c = model.byAirline.get(a.code) ?? emptyCounts();
        return { code: a.code, name: a.name, c, onTime: onTimeRate(c), delay: delayRate(c), cancel: cancelRate(c), avg: avgArrDelay(c) };
      })
      .filter((r) => r.c.total > 0);
  }, [model, airlines.data]);

  const hourModel = useMemo(() => {
    if (!dowHour.data) return null;
    const byCell = new Map<string, Counts>();
    const hours = new Set<number>();
    for (const r of dowHour.data) {
      if (!matchesAirline(filters, r.airline)) continue;
      const k = `${r.dow}|${r.hour}`;
      if (!byCell.has(k)) byCell.set(k, emptyCounts());
      addInto(byCell.get(k)!, r);
      if (r.flown > 0) hours.add(r.hour);
    }
    return { byCell, hours: [...hours].sort((a, b) => a - b) };
  }, [dowHour.data, filters]);

  const error = dates.error ?? daily.error ?? airlines.error;
  const loading = !model || !ranking;
  const ranked = ranking ? [...ranking].sort((a, b) => (b.onTime ?? -1) - (a.onTime ?? -1)) : null;
  const best = ranked?.[0];
  const worst = ranked?.length ? ranked[ranked.length - 1] : undefined;
  const selectedAirline = filters.airlines.length === 1 ? filters.airlines[0] : null;

  const tableRows = useMemo(() => {
    if (!ranking) return [];
    const val = (r: CarrierRow, key: SortKey): number | string =>
      key === "name" ? r.name : key === "flights" ? r.c.total : (r[key === "cancel" ? "cancel" : key === "delay" ? "delay" : "avg"] ?? -1);
    return [...ranking].sort((a, b) => {
      const va = val(a, sort.key);
      const vb = val(b, sort.key);
      if (typeof va === "string" || typeof vb === "string") return String(va).localeCompare(String(vb)) * sort.dir;
      return (va - vb) * sort.dir;
    });
  }, [ranking, sort]);

  const setSortKey = (key: SortKey) =>
    setSort((s) => (s.key === key ? { key, dir: (s.dir * -1) as 1 | -1 } : { key, dir: key === "name" ? 1 : -1 }));

  const SortTh = ({ k, label, right }: { k: SortKey; label: string; right?: boolean }) => (
    <th className={`pb-2 font-normal ${right ? "text-right" : "text-left"}`}>
      <button className="hover:text-foreground" onClick={() => setSortKey(k)}>
        {label}
        {sort.key === k ? (sort.dir === 1 ? " ↑" : " ↓") : ""}
      </button>
    </th>
  );

  return (
    <div className="mx-auto max-w-6xl">
      <PageHeader
        eyebrow="02 · Airlines"
        title="Which carriers ran on time?"
        intro="Carriers ranked by on-time arrivals under the current filters, with their monthly patterns and the delay rate by scheduled hour of departure. A flight counts as delayed when it arrives 15 minutes or more after schedule."
      />

      {error ? (
        <ErrorState error={error} />
      ) : (
        <div className="flex flex-col gap-6">
          <div className="grid grid-cols-2 gap-4 md:grid-cols-3 xl:grid-cols-4">
            {loading ? (
              Array.from({ length: 4 }).map((_, i) => <KpiSkeleton key={i} />)
            ) : (
              <>
                <Kpi label="Total flights" value={fmtInt(model!.total.total)} hint={`${fmtInt(model!.total.flown)} flown`} emphasis />
                <Kpi label="Delay rate" value={fmtPct(delayRate(model!.total))} hint="Arrival ≥ 15 min, of flown" />
                <Kpi label="Cancel rate" value={fmtPct(cancelRate(model!.total))} hint={`${fmtInt(model!.total.cancelled)} cancelled`} />
                <Kpi label="Avg arrival delay" value={fmtMin(avgArrDelay(model!.total))} hint="Per flown flight, early = negative" />
              </>
            )}
          </div>

          <Panel
            title="On-time rate by airline"
            takeaway={
              best && worst
                ? `${best.name} was the most punctual at ${fmtPct(best.onTime)}; ${worst.name} trailed at ${fmtPct(worst.onTime)}.`
                : "Airlines ranked by the share of flights that arrived within 15 minutes of schedule."
            }
            footnote="All carriers are shown regardless of the airline filter; the month, weekday and holiday filters apply."
          >
            {loading ? (
              <ChartSkeleton height={Math.min(420, 300)} />
            ) : !ranked?.length ? (
              <EmptyState />
            ) : (
              <ResponsiveContainer width="100%" height={ranked.length * 28 + 40}>
                <BarChart data={ranked} layout="vertical" margin={{ top: 4, right: 40, left: 8, bottom: 4 }}>
                  <CartesianGrid {...gridProps} horizontal={false} />
                  <XAxis type="number" domain={[0, 1]} tickFormatter={(v) => `${Math.round(v * 100)}%`} {...axisProps} />
                  <YAxis type="category" dataKey="code" width={48} {...axisProps} />
                  <Tooltip
                    cursor={{ fill: "var(--muted)", opacity: 0.5 }}
                    content={({ active, payload }) => {
                      const p = payload?.[0]?.payload as CarrierRow | undefined;
                      return (
                        <ChartTip
                          active={active && !!p}
                          label={p ? `${p.code} · ${p.name}` : ""}
                          rows={p
                            ? [
                                { name: "On-time rate", value: fmtPct(p.onTime), color: "var(--slate-accent)" },
                                { name: "Flights", value: fmtInt(p.c.flown) },
                                { name: "Delay rate", value: fmtPct(p.delay) },
                                { name: "Avg arrival delay", value: fmtMin(p.avg) },
                              ]
                            : []}
                        />
                      );
                    }}
                  />
                  <Bar dataKey="onTime" barSize={14} radius={[0, 4, 4, 0]}>
                    {ranked.map((r) => (
                      <Cell key={r.code} fill={r.code === best?.code ? "var(--slate-accent)" : "var(--dusty)"} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            )}
          </Panel>

          <Panel
            title="Carrier comparison"
            takeaway="The figures behind the ranking. Click a column header to sort."
            actions={
              ranking && (
                <DownloadButton
                  onClick={() =>
                    downloadCsv(
                      `airlines_comparison.csv`,
                      tableRows.map((r) => ({
                        code: r.code,
                        airline: r.name,
                        total_flights: r.c.total,
                        flown_flights: r.c.flown,
                        cancelled_flights: r.c.cancelled,
                        on_time_rate: r.onTime,
                        delay_rate: r.delay,
                        cancel_rate: r.cancel,
                        avg_arr_delay_min: r.avg,
                      })),
                    )
                  }
                />
              )
            }
          >
            {loading ? (
              <ChartSkeleton height={200} />
            ) : !tableRows.length ? (
              <EmptyState />
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-xs text-muted-foreground">
                      <SortTh k="name" label="Airline" />
                      <SortTh k="flights" label="Flights" right />
                      <SortTh k="cancel" label="Cancel rate" right />
                      <SortTh k="delay" label="Delay rate" right />
                      <SortTh k="avg" label="Avg arrival delay" right />
                    </tr>
                  </thead>
                  <tbody>
                    {tableRows.map((r) => (
                      <tr key={r.code} className="border-t border-border">
                        <td className="py-2">
                          <span className="mr-2 text-xs text-muted-foreground">{r.code}</span>
                          {r.name}
                        </td>
                        <td className="py-2 text-right">{fmtInt(r.c.total)}</td>
                        <td className="py-2 text-right">{fmtPct(r.cancel)}</td>
                        <td className="py-2 text-right">{fmtPct(r.delay)}</td>
                        <td className="py-2 text-right">{fmtMin(r.avg)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Panel>

          <Panel
            title="Delay rate by airline and month"
            takeaway="Rows are carriers, columns are months in the selected range. Darker warm colours mark the most delay-prone airline-month pairs."
            footnote="All carriers are shown regardless of the airline filter; the month, weekday and holiday filters apply."
          >
            {loading ? (
              <ChartSkeleton height={280} />
            ) : !ranking?.length ? (
              <EmptyState />
            ) : (
              <MatrixHeatmap
                corner="Airline ↓ · Month →"
                colLabels={Array.from({ length: filters.monthTo - filters.monthFrom + 1 }, (_, i) => MONTHS_SHORT[filters.monthFrom - 1 + i]!)}
                rows={ranking.map((r) => ({ key: r.code, label: `${r.code} ${r.name}` }))}
                cell={(code, ci) => {
                  const c = model!.byAirlineMonth.get(`${code}|${filters.monthFrom + ci}`);
                  if (!c || c.flown === 0) return { v: null };
                  return { v: delayRate(c), note: `${fmtInt(c.flown)} flown` };
                }}
                color={delayColor}
                format={fmtPct}
                legend={<BinLegend label="Delay rate" bins={DELAY_BINS} />}
              />
            )}
          </Panel>

          <Panel
            title="Delay rate by scheduled departure hour and weekday"
            takeaway={
              selectedAirline
                ? `Pattern for ${selectedAirline} at the selected airlines' scheduled departure hours. Early-evening departures typically carry the highest delay risk.`
                : "All airlines combined at their scheduled departure hours. Early-evening departures typically carry the highest delay risk."
            }
            footnote="This chart comes from a schedule-hour table, so the month and holiday filters do not apply to it; the airline selection does."
          >
            {dowHour.isLoading ? (
              <ChartSkeleton height={260} />
            ) : dowHour.error ? (
              <ErrorState error={dowHour.error} />
            ) : !hourModel || !hourModel.hours.length ? (
              <EmptyState>No schedule-hour flights match the current filters.</EmptyState>
            ) : (
              <MatrixHeatmap
                corner="Day ↓ · Hour →"
                colLabels={hourModel.hours.map((h) => `${String(h).padStart(2, "0")}`)}
                rows={DOW_NAMES.map((name, i) => ({ key: String(i + 1), label: name }))}
                minWidth={720}
                cell={(dow, ci) => {
                  const c = hourModel!.byCell.get(`${dow}|${hourModel!.hours[ci]}`);
                  if (!c || c.flown === 0) return { v: null };
                  return { v: delayRate(c), note: `${fmtInt(c.flown)} flown` };
                }}
                color={delayColor}
                format={fmtPct}
                legend={<BinLegend label="Delay rate" bins={DELAY_BINS} />}
              />
            )}
          </Panel>
        </div>
      )}
    </div>
  );
}

const MONTHS_SHORT = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
