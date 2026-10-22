import { createFileRoute } from "@tanstack/react-router";
import { useMemo } from "react";
import { Bar, BarChart, CartesianGrid, Cell, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { useAirlines, useCancelReasons, useDailyAirline, useDimDate } from "@/lib/data";
import { matchesAirline, matchesDate, useFilters } from "@/lib/filters";
import { cancelRate, delayRate, emptyCounts, fmtCompact, fmtInt, fmtPct, type Counts } from "@/lib/metrics";
import {
  ChartSkeleton, ChartTip, DownloadButton, EmptyState, ErrorState, Kpi, KpiSkeleton, PageHeader, Panel, axisProps, downloadCsv, gridProps,
} from "@/components/dash/ui";

export const Route = createFileRoute("/causes")({
  head: () => ({
    meta: [
      { title: "Delay causes · Flight Delay Analytics" },
      { name: "description", content: "Cause share, monthly cause minutes and cancellation reasons by month and airline." },
      { property: "og:title", content: "Delay causes · Flight Delay Analytics" },
      { property: "og:description", content: "Cause share, monthly cause minutes and cancellation reasons by month and airline." },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary_large_image" },
    ],
  }),
  component: CausesPage,
});

const CAUSES = [
  { key: "airline", label: "Air carrier", color: "var(--peach)" },
  { key: "lateAircraft", label: "Late aircraft", color: "var(--dusty)" },
  { key: "airSystem", label: "National aviation system", color: "var(--sage)" },
  { key: "weather", label: "Weather", color: "var(--butter)" },
  { key: "security", label: "Security", color: "var(--lavender)" },
] as const;

const REASONS = [
  { key: "A", label: "Air carrier", color: "var(--peach)" },
  { key: "B", label: "Weather", color: "var(--sage)" },
  { key: "C", label: "National aviation system", color: "var(--butter)" },
  { key: "D", label: "Security", color: "var(--lavender)" },
] as const;

type ReasonTally = { A: number; B: number; C: number; D: number };
const emptyReasons = (): ReasonTally => ({ A: 0, B: 0, C: 0, D: 0 });

function CausesPage() {
  const dates = useDimDate();
  const daily = useDailyAirline();
  const cancel = useCancelReasons();
  const airlines = useAirlines();
  const { filters } = useFilters();

  const model = useMemo(() => {
    if (!dates.data || !daily.data) return null;
    const causeTotals = { airSystem: 0, security: 0, airline: 0, lateAircraft: 0, weather: 0 };
    const causeByMonth = Array.from({ length: 12 }, () => ({ ...causeTotals }));
    const total = emptyCounts();
    for (const r of daily.data) {
      const d = dates.data.get(r.date);
      if (!matchesDate(filters, d)) continue;
      const m = d!.month - 1;
      for (const c of CAUSES) {
        causeTotals[c.key] += r.causes[c.key];
        causeByMonth[m]![c.key] += r.causes[c.key];
      }
      total.cancelled += r.cancelled;
      total.total += r.total;
      total.flown += r.flown;
    }
    const cancelByMonth = Array.from({ length: 12 }, () => emptyReasons());
    const cancelByAirline = new Map<string, ReasonTally>();
    if (cancel.data && dates.data) {
      for (const r of cancel.data) {
        if (!matchesAirline(filters, r.airline)) continue;
        const d = dates.data.get(r.date);
        if (!matchesDate(filters, d)) continue;
        cancelByMonth[d!.month - 1]![r.reason] += r.cancelled;
        if (!cancelByAirline.has(r.airline)) cancelByAirline.set(r.airline, emptyReasons());
        cancelByAirline.get(r.airline)![r.reason] += r.cancelled;
      }
    }
    return { causeTotals, causeByMonth, cancelByMonth, cancelByAirline, total };
  }, [dates.data, daily.data, cancel.data, filters]);

  const error = dates.error ?? daily.error ?? cancel.error ?? airlines.error;
  const loading = !model || !airlines.data;

  const totalCauseMinutes = model ? CAUSES.reduce((s, c) => s + model.causeTotals[c.key], 0) : 0;
  const donut = model
    ? CAUSES.map((c) => ({ label: c.label, color: c.color, minutes: model.causeTotals[c.key], share: totalCauseMinutes > 0 ? model.causeTotals[c.key] / totalCauseMinutes : null }))
    : [];
  const topCause = [...donut].sort((a, b) => b.minutes - a.minutes)[0];

  const monthRange = Array.from({ length: filters.monthTo - filters.monthFrom + 1 }, (_, i) => filters.monthFrom + i - 1);
  const monthly = model ? monthRange.map((m) => {
    const row: Record<string, string | number> = { month: MONTHS[m]! };
    for (const c of CAUSES) row[c.key] = model.causeByMonth[m]![c.key];
    return row;
  }) : [];

  const cancelMonthly = model ? monthRange.map((m) => {
    const row: Record<string, string | number> = { month: MONTHS[m]! };
    for (const r of REASONS) row[r.key] = model.cancelByMonth[m]![r.key];
    return row;
  }) : [];

  const airlineNames = airlines.data ? new Map(airlines.data.map((a) => [a.code, a.name])) : new Map<string, string>();
  const byAirline = model
    ? [...model.cancelByAirline.entries()]
      .map(([code, t]) => ({
        airline: code,
        name: airlineNames.get(code) ?? code,
        ...t,
        total: t.A + t.B + t.C + t.D,
      }))
      .filter((r) => r.total > 0)
      .sort((a, b) => b.total - a.total)
    : [];
  const totalCancels = byAirline.reduce((s, r) => s + r.total, 0);

  return (
    <div className="mx-auto max-w-6xl">
      <PageHeader
        eyebrow="04 · Delay causes and cancellations"
        title="What caused delays and cancellations?"
        intro="Where the late minutes came from, and why flights were called off. Cause minutes are recorded only for flights that arrived 15 minutes or more late, so they describe the worst delays rather than every late arrival."
      />

      {error ? (
        <ErrorState error={error} />
      ) : (
        <div className="flex flex-col gap-6">
          <div className="grid grid-cols-2 gap-4 md:grid-cols-3 xl:grid-cols-5">
            {loading ? (
              Array.from({ length: 5 }).map((_, i) => <KpiSkeleton key={i} />)
            ) : (
              <>
                <Kpi label="Cancelled flights" value={fmtInt(model!.total.cancelled)} hint={`${fmtPct(cancelRate(model!.total))} of scheduled`} emphasis />
                <Kpi label="Delay rate" value={fmtPct(delayRate(model!.total))} hint="Arrival ≥ 15 min, of flown" />
                <Kpi label="Cause minutes" value={fmtCompact(totalCauseMinutes)} hint="Recorded for flights delayed 15+ min" />
                <Kpi label="Top cause" value={topCause ? `${fmtPct(topCause.share)}` : "–"} hint={topCause?.label} />
                <Kpi label="Weather cancels" value={fmtInt(model.cancelByMonth.reduce((s, m) => s + m.B, 0))} hint="Reason B of all cancellations" />
              </>
            )}
          </div>

          <div className="grid gap-6 lg:grid-cols-5">
            <Panel
              className="lg:col-span-2"
              title="Share of delay minutes by cause"
              takeaway={
                topCause
                  ? `${topCause.label} accounted for ${fmtPct(topCause.share)} of the ${fmtCompact(totalCauseMinutes)} attributed delay minutes.`
                  : "Attributed minutes split across the five DOT causes."
              }
              footnote="Cause minutes are recorded only for flights that arrived 15 minutes or more late."
            >
              {loading ? (
                <ChartSkeleton height={240} />
              ) : totalCauseMinutes === 0 ? (
                <EmptyState />
              ) : (
                <div className="flex flex-col items-center gap-4 sm:flex-row">
                  <ResponsiveContainer width="100%" height={220}>
                    <PieChart>
                      <Pie data={donut} dataKey="minutes" nameKey="label" innerRadius={58} outerRadius={92} paddingAngle={2} stroke="var(--card)" strokeWidth={2}>
                        {donut.map((d) => (
                          <Cell key={d.label} fill={d.color} />
                        ))}
                      </Pie>
                      <Tooltip
                        content={({ active, payload }) => {
                          const p = payload?.[0]?.payload as (typeof donut)[number] | undefined;
                          return (
                            <ChartTip
                              active={active && !!p}
                              label={p?.label}
                              rows={p ? [{ name: "Cause minutes", value: fmtInt(p.minutes), color: p.color }, { name: "Share", value: fmtPct(p.share) }] : []}
                            />
                          );
                        }}
                      />
                    </PieChart>
                  </ResponsiveContainer>
                  <ul className="w-full space-y-2 text-sm sm:w-auto">
                    {donut.map((d) => (
                      <li key={d.label} className="flex items-center gap-2">
                        <span className="h-2.5 w-2.5 shrink-0 rounded-full" style={{ background: d.color }} />
                        <span className="text-muted-foreground">{d.label}</span>
                        <span className="ml-auto pl-4 tabular-nums">{fmtPct(d.share)}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </Panel>

            <Panel
              className="lg:col-span-3"
              title="Cause minutes by month"
              takeaway="The five causes stacked over the year; summer thunderstorms and winter system strain show up as taller columns."
              footnote="Cause minutes are recorded only for flights that arrived 15 minutes or more late."
            >
              {loading ? (
                <ChartSkeleton height={240} />
              ) : totalCauseMinutes === 0 ? (
                <EmptyState />
              ) : (
                <ResponsiveContainer width="100%" height={260}>
                  <BarChart data={monthly} margin={{ top: 8, right: 10, left: 10, bottom: 8 }}>
                    <CartesianGrid {...gridProps} />
                    <XAxis dataKey="month" {...axisProps} />
                    <YAxis {...axisProps} tickFormatter={fmtCompact} label={{ value: "Cause minutes", angle: -90, position: "insideLeft", fill: "var(--muted-foreground)", fontSize: 11, dx: -2 }} />
                    <Tooltip
                      cursor={{ fill: "var(--muted)", opacity: 0.5 }}
                      content={({ active, payload, label }) => (
                        <ChartTip
                          active={active}
                          label={label}
                          rows={(payload ?? []).map((p) => {
                            const c = CAUSES.find((x) => x.key === p.dataKey);
                            return { name: c?.label ?? String(p.dataKey), value: fmtInt(p.value as number), color: c?.color };
                          })}
                        />
                      )}
                    />
                    {CAUSES.map((c) => (
                      <Bar key={c.key} dataKey={c.key} stackId="cause" fill={c.color} />
                    ))}
                  </BarChart>
                </ResponsiveContainer>
              )}
            </Panel>
          </div>

          <Panel
            title="Cancellation reasons by month"
            takeaway="Airline-controlled cancellations dominate most months; weather spikes in the winter and storm months."
            actions={
              model && (
                <DownloadButton
                  onClick={() =>
                    downloadCsv("cancellations_by_month.csv", cancelMonthly.map((r) => ({
                      month: r["month"],
                      air_carrier_A: r["A"],
                      weather_B: r["B"],
                      national_aviation_system_C: r["C"],
                      security_D: r["D"],
                      total: (r["A"] as number) + (r["B"] as number) + (r["C"] as number) + (r["D"] as number),
                    })))
                  }
                />
              )
            }
            footnote="Reason codes: A air carrier, B weather, C national aviation system, D security."
          >
            {loading ? (
              <ChartSkeleton height={240} />
            ) : totalCancels === 0 ? (
              <EmptyState />
            ) : (
              <ResponsiveContainer width="100%" height={260}>
                <BarChart data={cancelMonthly} margin={{ top: 8, right: 10, left: 10, bottom: 8 }}>
                  <CartesianGrid {...gridProps} />
                  <XAxis dataKey="month" {...axisProps} />
                  <YAxis {...axisProps} tickFormatter={fmtCompact} label={{ value: "Cancelled flights", angle: -90, position: "insideLeft", fill: "var(--muted-foreground)", fontSize: 11, dx: -2 }} />
                  <Tooltip
                    cursor={{ fill: "var(--muted)", opacity: 0.5 }}
                    content={({ active, payload, label }) => (
                      <ChartTip
                        active={active}
                        label={label}
                        rows={(payload ?? []).map((p) => {
                          const r = REASONS.find((x) => x.key === p.dataKey);
                          return { name: r ? `${r.label} (${r.key})` : String(p.dataKey), value: fmtInt(p.value as number), color: r?.color };
                        })}
                      />
                    )}
                  />
                  {REASONS.map((r) => (
                    <Bar key={r.key} dataKey={r.key} stackId="reason" fill={r.color} />
                  ))}
                </BarChart>
              </ResponsiveContainer>
            )}
          </Panel>

          <Panel
            title="Cancellation reasons by airline"
            takeaway={byAirline.length ? `${byAirline[0]!.name} called off the most flights under the current filters, ${fmtInt(byAirline[0]!.total)} in total.` : "Cancellations per carrier, split by reason."}
            footnote="Reason codes: A air carrier, B weather, C national aviation system, D security. Sorted by total cancellations."
          >
            {loading ? (
              <ChartSkeleton height={260} />
            ) : !byAirline.length ? (
              <EmptyState />
            ) : (
              <ResponsiveContainer width="100%" height={byAirline.length * 30 + 40}>
                <BarChart data={byAirline} layout="vertical" margin={{ top: 4, right: 20, left: 8, bottom: 4 }}>
                  <CartesianGrid {...gridProps} horizontal={false} />
                  <XAxis type="number" {...axisProps} tickFormatter={fmtCompact} />
                  <YAxis type="category" dataKey="airline" width={48} {...axisProps} />
                  <Tooltip
                    cursor={{ fill: "var(--muted)", opacity: 0.5 }}
                    content={({ active, payload }) => {
                      const p = payload?.[0]?.payload as (typeof byAirline)[number] | undefined;
                      return (
                        <ChartTip
                          active={active && !!p}
                          label={p ? `${p.airline} · ${p.name}` : ""}
                          rows={p
                            ? [
                                ...REASONS.map((r) => ({ name: `${r.label} (${r.key})`, value: fmtInt(p[r.key]), color: r.color })),
                                { name: "Total cancelled", value: fmtInt(p.total) },
                              ]
                            : []}
                        />
                      );
                    }}
                  />
                  {REASONS.map((r) => (
                    <Bar key={r.key} dataKey={r.key} stackId="reason" fill={r.color} barSize={14} radius={r.key === "D" ? [0, 4, 4, 0] : [0, 0, 0, 0]} />
                  ))}
                </BarChart>
              </ResponsiveContainer>
            )}
          </Panel>
        </div>
      )}
    </div>
  );
}

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
