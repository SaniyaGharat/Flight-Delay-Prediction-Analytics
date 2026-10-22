import { createFileRoute } from "@tanstack/react-router";
import { useMemo } from "react";
import { Bar, BarChart, CartesianGrid, ComposedChart, Line, ResponsiveContainer, Tooltip, XAxis, YAxis, LabelList } from "recharts";
import { useDailyAirline, useDimDate } from "@/lib/data";
import { MONTHS, matchesAirline, matchesDate, useFilters } from "@/lib/filters";
import {
  addInto, avgArrDelay, cancelRate, delayRate, emptyCounts, fmtCompact, fmtInt, fmtMin, fmtPct, onTimeRate, type Counts,
} from "@/lib/metrics";
import {
  ChartSkeleton, ChartTip, DownloadButton, EmptyState, ErrorState, Kpi, KpiSkeleton, PageHeader, Panel, axisProps, downloadCsv, gridProps,
} from "@/components/dash/ui";
import { CalendarHeatmap, type DayCell } from "@/components/dash/CalendarHeatmap";

export const Route = createFileRoute("/")({
  head: () => ({
    meta: [
      { title: "Overview · Flight Delay Analytics" },
      { name: "description", content: "Headline delay, cancellation and on-time rates for US domestic flights in 2015, by month and by day." },
      { property: "og:title", content: "Overview · Flight Delay Analytics" },
      { property: "og:description", content: "Headline delay and cancellation rates for 5.8M US domestic flights in 2015." },
    ],
  }),
  component: Overview,
});

function Overview() {
  const dates = useDimDate();
  const daily = useDailyAirline();
  const { filters } = useFilters();

  const model = useMemo(() => {
    if (!dates.data || !daily.data) return null;
    const total = emptyCounts();
    const byMonth = Array.from({ length: 12 }, () => emptyCounts());
    const byDay = new Map<string, Counts>();
    const holiday = emptyCounts();
    const normal = emptyCounts();
    for (const r of daily.data) {
      if (!matchesAirline(filters, r.airline)) continue;
      const d = dates.data.get(r.date);
      if (matchesDate(filters, d, { ignoreHoliday: true })) addInto(d!.isHoliday ? holiday : normal, r);
      if (!matchesDate(filters, d)) continue;
      addInto(total, r);
      addInto(byMonth[d!.month - 1]!, r);
      if (!byDay.has(r.date)) byDay.set(r.date, emptyCounts());
      addInto(byDay.get(r.date)!, r);
    }
    const months = byMonth
      .map((c, i) => ({ month: MONTHS[i], m: i + 1, flights: c.total, flown: c.flown, delay: delayRate(c), cancel: cancelRate(c), avg: avgArrDelay(c) }))
      .filter((r) => r.m >= filters.monthFrom && r.m <= filters.monthTo);
    const days = new Map<string, DayCell>();
    for (const [k, c] of byDay) days.set(k, { date: k, rate: delayRate(c), flown: c.flown, included: c.total > 0 });
    let worst: DayCell | null = null;
    for (const d of days.values()) if (d.flown >= 1000 && (d.rate ?? -1) > (worst?.rate ?? -1)) worst = d;
    return { total, months, days, holiday, normal, worst };
  }, [dates.data, daily.data, filters]);

  const error = dates.error ?? daily.error;
  const loading = !model && !error;

  const peak = model?.months.reduce<(typeof model.months)[number] | null>((a, b) => ((b.delay ?? -1) > (a?.delay ?? -1) ? b : a), null);
  const low = model?.months.reduce<(typeof model.months)[number] | null>((a, b) => ((b.delay ?? 2) < (a?.delay ?? 2) ? b : a), null);

  const hRate = model ? delayRate(model.holiday) : null;
  const nRate = model ? delayRate(model.normal) : null;
  const holidayData = model
    ? [
        { metric: "Delay rate", Holiday: hRate, Normal: nRate },
        { metric: "Cancel rate", Holiday: cancelRate(model.holiday), Normal: cancelRate(model.normal) },
      ]
    : [];

  return (
    <div className="mx-auto max-w-6xl">
      <PageHeader
        eyebrow="01 · Overview"
        title="How often did US domestic flights run late in 2015?"
        intro="Headline rates for the current filter selection. A flight counts as delayed when it arrives 15 minutes or more after schedule; cancelled flights are excluded from delay figures."
      />

      {error ? (
        <ErrorState error={error} />
      ) : (
        <div className="flex flex-col gap-6">
          <div className="grid grid-cols-2 gap-4 md:grid-cols-3 xl:grid-cols-5">
            {loading || !model ? (
              Array.from({ length: 5 }).map((_, i) => <KpiSkeleton key={i} />)
            ) : (
              <>
                <Kpi label="Total flights" value={fmtInt(model.total.total)} hint={`${fmtInt(model.total.flown)} flown`} emphasis />
                <Kpi label="Cancel rate" value={fmtPct(cancelRate(model.total))} hint={`${fmtInt(model.total.cancelled)} cancelled`} />
                <Kpi label="Delay rate" value={fmtPct(delayRate(model.total))} hint="Arrival ≥ 15 min, of flown" />
                <Kpi label="On-time rate" value={fmtPct(onTimeRate(model.total))} hint="1 − delay rate" />
                <Kpi label="Avg arrival delay" value={fmtMin(avgArrDelay(model.total))} hint="Per flown flight, early = negative" />
              </>
            )}
          </div>

          <Panel
            title="Monthly delay rate and flight volume"
            takeaway={
              peak && low && peak.delay != null
                ? `Delays peaked in ${peak.month} at ${fmtPct(peak.delay)} and were lowest in ${low.month} at ${fmtPct(low.delay)}.`
                : "Delay rate by month, with scheduled flights shown behind."
            }
          >
            {loading || !model ? (
              <ChartSkeleton />
            ) : model.total.total === 0 ? (
              <EmptyState />
            ) : (
              <ResponsiveContainer width="100%" height={300}>
                <ComposedChart data={model.months} margin={{ top: 10, right: 10, left: 10, bottom: 10 }}>
                  <CartesianGrid {...gridProps} />
                  <XAxis dataKey="month" {...axisProps} />
                  <YAxis
                    yAxisId="rate"
                    {...axisProps}
                    tickFormatter={(v) => `${Math.round(v * 100)}%`}
                    label={{ value: "Delay rate (%)", angle: -90, position: "insideLeft", fill: "var(--muted-foreground)", fontSize: 11, dx: -2 }}
                  />
                  <YAxis
                    yAxisId="flights"
                    orientation="right"
                    {...axisProps}
                    tickFormatter={fmtCompact}
                    label={{ value: "Flights", angle: 90, position: "insideRight", fill: "var(--muted-foreground)", fontSize: 11 }}
                  />
                  <Tooltip
                    cursor={{ fill: "var(--muted)", opacity: 0.5 }}
                    content={({ active, payload, label }) => {
                      const p = payload?.[0]?.payload;
                      return (
                        <ChartTip
                          active={active && !!p}
                          label={label}
                          rows={p ? [
                            { name: "Delay rate", value: fmtPct(p.delay), color: "var(--slate-accent)" },
                            { name: "Flights", value: fmtInt(p.flights), color: "var(--stone)" },
                            { name: "Avg arrival delay", value: fmtMin(p.avg) },
                          ] : []}
                        />
                      );
                    }}
                  />
                  <Bar yAxisId="flights" dataKey="flights" fill="var(--stone)" opacity={0.55} radius={[6, 6, 0, 0]} barSize={28} />
                  <Line yAxisId="rate" dataKey="delay" stroke="var(--slate-accent)" strokeWidth={1.5} dot={{ r: 3, fill: "var(--card)", strokeWidth: 1.5 }} />
                </ComposedChart>
              </ResponsiveContainer>
            )}
          </Panel>

          <Panel
            title="Daily delay rate, 2015"
            takeaway={
              model?.worst
                ? `The worst day in the selection was ${new Date(model.worst.date + "T00:00:00Z").toLocaleDateString("en-US", { month: "long", day: "numeric", timeZone: "UTC" })}, when ${fmtPct(model.worst.rate)} of flown flights arrived late.`
                : "Each square is one day, coloured by the share of flown flights that arrived 15+ minutes late."
            }
          >
            {loading || !model ? <ChartSkeleton height={140} /> : <CalendarHeatmap days={model.days} />}
          </Panel>

          <div className="grid gap-6 lg:grid-cols-5">
            <Panel
              className="lg:col-span-3"
              title="Holiday windows vs normal days"
              takeaway={
                hRate != null && nRate != null
                  ? `Flights in holiday windows were delayed ${fmtPct(hRate)} of the time, against ${fmtPct(nRate)} on normal days.`
                  : "Delay and cancellation rates inside and outside holiday travel windows."
              }
              footnote="This comparison ignores the holiday toggle in the filter bar; all other filters apply."
            >
              {loading || !model ? (
                <ChartSkeleton height={220} />
              ) : model.holiday.total + model.normal.total === 0 ? (
                <EmptyState />
              ) : (
                <ResponsiveContainer width="100%" height={230}>
                  <BarChart data={holidayData} margin={{ top: 20, right: 10, left: 10, bottom: 0 }} barGap={6}>
                    <CartesianGrid {...gridProps} />
                    <XAxis dataKey="metric" {...axisProps} />
                    <YAxis {...axisProps} tickFormatter={(v) => `${Math.round(v * 100)}%`} label={{ value: "Share of flights (%)", angle: -90, position: "insideLeft", fill: "var(--muted-foreground)", fontSize: 11 }} />
                    <Tooltip
                      cursor={{ fill: "var(--muted)", opacity: 0.5 }}
                      content={({ active, payload, label }) => (
                        <ChartTip active={active} label={label} rows={(payload ?? []).map((p) => ({ name: String(p.name), value: fmtPct(p.value as number), color: p.color }))} />
                      )}
                    />
                    <Bar dataKey="Holiday" fill="var(--peach)" radius={[6, 6, 0, 0]} barSize={36}>
                      <LabelList dataKey="Holiday" position="top" formatter={(v: number) => fmtPct(v)} fontSize={11} fill="var(--muted-foreground)" />
                    </Bar>
                    <Bar dataKey="Normal" fill="var(--dusty)" radius={[6, 6, 0, 0]} barSize={36}>
                      <LabelList dataKey="Normal" position="top" formatter={(v: number) => fmtPct(v)} fontSize={11} fill="var(--muted-foreground)" />
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              )}
            </Panel>
            <Panel className="lg:col-span-2" title="Side by side" takeaway="Counts behind the comparison.">
              {loading || !model ? (
                <ChartSkeleton height={220} />
              ) : (
                <dl className="grid grid-cols-3 gap-y-3 text-sm">
                  <dt className="text-muted-foreground" />
                  <dt className="text-right text-xs text-muted-foreground">Holiday</dt>
                  <dt className="text-right text-xs text-muted-foreground">Normal</dt>
                  {[
                    ["Flights", fmtInt(model.holiday.total), fmtInt(model.normal.total)],
                    ["Delay rate", fmtPct(hRate), fmtPct(nRate)],
                    ["Cancel rate", fmtPct(cancelRate(model.holiday)), fmtPct(cancelRate(model.normal))],
                    ["Avg arr. delay", fmtMin(avgArrDelay(model.holiday)), fmtMin(avgArrDelay(model.normal))],
                  ].map(([k, a, b]) => (
                    <div key={k} className="contents">
                      <dd className="border-t border-border pt-3 text-muted-foreground">{k}</dd>
                      <dd className="border-t border-border pt-3 text-right">{a}</dd>
                      <dd className="border-t border-border pt-3 text-right">{b}</dd>
                    </div>
                  ))}
                </dl>
              )}
            </Panel>
          </div>

          <Panel
            title="Monthly summary"
            takeaway="The figures behind the monthly chart."
            actions={
              model && (
                <DownloadButton
                  onClick={() =>
                    downloadCsv("monthly_summary.csv", model.months.map((m) => ({
                      month: m.month, total_flights: m.flights, flown_flights: m.flown,
                      cancel_rate: m.cancel, delay_rate: m.delay, avg_arr_delay_min: m.avg,
                    })))
                  }
                />
              )
            }
          >
            {loading || !model ? (
              <ChartSkeleton height={200} />
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left text-xs text-muted-foreground">
                      <th className="pb-2 font-normal">Month</th>
                      <th className="pb-2 text-right font-normal">Flights</th>
                      <th className="pb-2 text-right font-normal">Cancel rate</th>
                      <th className="pb-2 text-right font-normal">Delay rate</th>
                      <th className="pb-2 text-right font-normal">Avg arrival delay</th>
                    </tr>
                  </thead>
                  <tbody>
                    {model.months.map((m) => (
                      <tr key={m.month} className="border-t border-border">
                        <td className="py-2">{m.month}</td>
                        <td className="py-2 text-right">{fmtInt(m.flights)}</td>
                        <td className="py-2 text-right">{fmtPct(m.cancel)}</td>
                        <td className="py-2 text-right">{fmtPct(m.delay)}</td>
                        <td className="py-2 text-right">{fmtMin(m.avg)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Panel>
        </div>
      )}
    </div>
  );
}
