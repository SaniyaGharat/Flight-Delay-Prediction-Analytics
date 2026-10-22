import { createFileRoute } from "@tanstack/react-router";
import { useMemo, useState } from "react";
import { Search } from "lucide-react";
import { useAirportTotals, useRouteMonth } from "@/lib/data";
import { useFilters } from "@/lib/filters";
import { avgArrDelay, cancelRate, delayRate, emptyCounts, addInto, fmtInt, fmtMin, fmtPct, type Counts } from "@/lib/metrics";
import { isMapped, rankableAirports, rankableRoutes } from "@/lib/rankings";
import {
  ChartSkeleton, DownloadButton, EmptyState, ErrorState, Kpi, KpiSkeleton, PageHeader, Panel, downloadCsv,
} from "@/components/dash/ui";
import { UsaBubbleMap, type MapAirport } from "@/components/dash/UsaBubbleMap";

export const Route = createFileRoute("/airports")({
  head: () => ({
    meta: [
      { title: "Airports and routes · Flight Delay Analytics" },
      { name: "description", content: "Airport map, top and bottom airports, and searchable route table for US domestic flights in 2015." },
      { property: "og:title", content: "Airports and routes · Flight Delay Analytics" },
      { property: "og:description", content: "Airport map, top and bottom airports, and searchable route table for US domestic flights in 2015." },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary_large_image" },
    ],
  }),
  component: AirportsPage,
});

type AirportRow = {
  code: string;
  name: string;
  city: string;
  state: string;
  total: number;
  flown: number;
  delay: number | null;
  cancel: number | null;
  avg: number | null;
};

const toRow = (a: { code: string; name: string; city: string; state: string } & Counts): AirportRow => ({
  code: a.code,
  name: a.name,
  city: a.city,
  state: a.state,
  total: a.total,
  flown: a.flown,
  delay: delayRate(a),
  cancel: cancelRate(a),
  avg: avgArrDelay(a),
});

function AirportTable({ title, takeaway, rows, filename }: { title: string; takeaway: string; rows: AirportRow[]; filename: string }) {
  const download = () =>
    downloadCsv(
      filename,
      rows.map((r) => ({
        code: r.code, airport: r.name, city: r.city, state: r.state, total_flights: r.total, flown_flights: r.flown,
        delay_rate: r.delay, cancel_rate: r.cancel, avg_arr_delay_min: r.avg,
      })),
    );
  return (
    <Panel title={title} takeaway={takeaway} actions={<DownloadButton onClick={download} />}>
      {rows.length === 0 ? (
        <EmptyState />
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-xs text-muted-foreground">
                <th className="pb-2 font-normal">Airport</th>
                <th className="pb-2 text-right font-normal">Flights</th>
                <th className="pb-2 text-right font-normal">Delay rate</th>
                <th className="pb-2 text-right font-normal">Avg delay</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.code} className="border-t border-border">
                  <td className="py-2">
                    <span className="mr-2 font-mono text-xs text-muted-foreground">{r.code}</span>
                    <span className="text-muted-foreground">{r.city || r.name}{r.state ? `, ${r.state}` : ""}</span>
                  </td>
                  <td className="py-2 text-right">{fmtInt(r.total)}</td>
                  <td className="py-2 text-right">{fmtPct(r.delay)}</td>
                  <td className="py-2 text-right">{fmtMin(r.avg)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  );
}

function AirportsPage() {
  const [kind, setKind] = useState<"origin" | "dest">("origin");
  const [query, setQuery] = useState("");
  const [routeSort, setRouteSort] = useState<{ key: "route" | "flights" | "delay" | "avg"; dir: 1 | -1 }>({ key: "flights", dir: -1 });
  const origin = useAirportTotals("origin");
  const dest = useAirportTotals("dest");
  const routes = useRouteMonth();
  const { filters } = useFilters();

  const totals = kind === "origin" ? origin : dest;

  const airportModel = useMemo(() => {
    const rows = totals.data;
    if (!rows) return null;
    const rankable = rankableAirports(rows).map(toRow);
    const mapAirports: MapAirport[] = rows
      .filter((a) => isMapped(a.code) && a.hasCoords && a.flown > 0)
      .map((a) => ({ code: a.code, name: a.name, lat: a.lat, lon: a.lon, flights: a.flown, rate: delayRate(a) }));
    const busiest = [...rankable].sort((a, b) => b.total - a.total)[0];
    const slowest = [...rankable].sort((a, b) => (b.delay ?? -1) - (a.delay ?? -1))[0];
    return { rankable, mapAirports, busiest, slowest, count: rankable.length };
  }, [totals.data, kind]);

  const routeModel = useMemo(() => {
    if (!routes.data) return null;
    const acc = new Map<string, Counts & { origin: string; dest: string }>();
    for (const r of routes.data) {
      if (r.month < filters.monthFrom || r.month > filters.monthTo) continue;
      const k = `${r.origin}|${r.dest}`;
      if (!acc.has(k)) acc.set(k, { ...emptyCounts(), origin: r.origin, dest: r.dest });
      addInto(acc.get(k)!, r);
    }
    const rankable = rankableRoutes([...acc.values()]).map((r) => ({
      origin: r.origin,
      dest: r.dest,
      total: r.total,
      flown: r.flown,
      delay: delayRate(r),
      cancel: cancelRate(r),
      avg: avgArrDelay(r),
    }));
    return { rankable, count: acc.size };
  }, [routes.data, filters.monthFrom, filters.monthTo]);

  const routeRows = useMemo(() => {
    if (!routeModel) return [];
    const q = query.trim().toUpperCase();
    const matched = q ? routeModel.rankable.filter((r) => r.origin.includes(q) || r.dest.includes(q)) : routeModel.rankable;
    const val = (r: (typeof matched)[number]) =>
      routeSort.key === "route" ? `${r.origin}${r.dest}` : routeSort.key === "flights" ? r.total : (r[routeSort.key] ?? -1);
    return [...matched].sort((a, b) => {
      const va = val(a);
      const vb = val(b);
      const cmp = typeof va === "string" || typeof vb === "string" ? String(va).localeCompare(String(vb)) : va - vb;
      return cmp * routeSort.dir;
    });
  }, [routeModel, query, routeSort]);

  const error = origin.error ?? dest.error ?? routes.error;
  const loading = !airportModel || !routeModel;

  const bestRows = useMemo(
    () => [...(airportModel?.rankable ?? [])].sort((a, b) => (a.delay ?? 2) - (b.delay ?? 2)).slice(0, 10),
    [airportModel],
  );
  const worstRows = useMemo(
    () => [...(airportModel?.rankable ?? [])].sort((a, b) => (b.delay ?? -1) - (a.delay ?? -1)).slice(0, 10),
    [airportModel],
  );
  const toggleCls = (k: "origin" | "dest") =>
    `rounded-md px-3 py-1.5 text-xs transition-colors ${kind === k ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:text-foreground"}`;

  return (
    <div className="mx-auto max-w-6xl">
      <PageHeader
        eyebrow="03 · Airports and routes"
        title="Where did delays concentrate?"
        intro="Every mapped US airport as a bubble sized by flights and coloured by delay rate, the ten most and least punctual airports, and a searchable table of routes. Airport figures are pre-aggregated totals; route figures respect the month range."
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
                <Kpi label="Airports ranked" value={fmtInt(airportModel!.count)} hint="Flown ≥ 1,000, mapped codes" />
                <Kpi
                  label="Busiest airport"
                  value={airportModel!.busiest?.code ?? "–"}
                  hint={airportModel!.busiest ? `${fmtInt(airportModel!.busiest.total)} flights` : undefined}
                  emphasis
                />
                <Kpi
                  label="Highest delay rate"
                  value={fmtPct(airportModel!.slowest?.delay ?? null)}
                  hint={airportModel!.slowest ? `${airportModel!.slowest.code} ${airportModel!.slowest.name}` : undefined}
                />
                <Kpi label="Routes ranked" value={fmtInt(routeModel!.rankable.length)} hint={`of ${fmtInt(routeModel!.count)} flown, flown ≥ 500`} />
              </>
            )}
          </div>

          <Panel
            title={kind === "origin" ? "Departure airports" : "Arrival airports"}
            takeaway={
              airportModel?.busiest
                ? `${airportModel.busiest.code} ${airportModel.busiest.name} handled the most traffic as an ${kind}; bubble colour shows the share of flights arriving 15+ minutes late.`
                : "Bubble area is proportional to flights; colour shows the delay rate."
            }
            actions={
              <div className="inline-flex rounded-md border border-border bg-card p-0.5">
                <button className={toggleCls("origin")} onClick={() => setKind("origin")}>Origin</button>
                <button className={toggleCls("dest")} onClick={() => setKind("dest")}>Destination</button>
              </div>
            }
            footnote="Airport totals are pre-aggregated, so the filter bar does not change them. UNMAPPED airport codes — October 2015 codes that could not be recovered — are left out of the map and rankings."
          >
            {loading ? (
              <div className="h-[420px] animate-pulse rounded-lg bg-muted/60" aria-label="Loading map" />
            ) : !airportModel?.mapAirports.length ? (
              <EmptyState>No mapped airports with coordinates were found.</EmptyState>
            ) : (
              <UsaBubbleMap airports={airportModel.mapAirports} />
            )}
          </Panel>

          <div className="grid gap-6 lg:grid-cols-2">
            <AirportTable
              title="The ten most punctual airports"
              takeaway={`Lowest delay rate among the ${fmtInt(airportModel?.count ?? 0)} ranked airports, as ${kind === "origin" ? "origins" : "destinations"}.`}
              rows={bestRows}
              filename={`${kind}_airports_best10.csv`}
            />
            <AirportTable
              title="The ten most delayed airports"
              takeaway="Highest delay rate among the ranked airports — often weather-exposed or hub-congested fields."
              rows={worstRows}
              filename={`${kind}_airports_worst10.csv`}
            />
          </div>

          <Panel
            title="Route table"
            takeaway="Search by airport code; click a column header to sort. The download includes every matching route, not just the visible rows."
            actions={
              <DownloadButton
                onClick={() =>
                  downloadCsv(
                    `routes_${filters.monthFrom}_${filters.monthTo}.csv`,
                    routeRows.map((r) => ({
                      origin: r.origin, dest: r.dest, total_flights: r.total, flown_flights: r.flown,
                      cancel_rate: r.cancel, delay_rate: r.delay, avg_arr_delay_min: r.avg,
                    })),
                  )
                }
              />
            }
            footnote="Routes need at least 500 flown flights to rank. Route figures respect the month range; weekday and holiday filters do not apply, since the route table is aggregated by month."
          >
            {loading ? (
              <ChartSkeleton height={260} />
            ) : !routeModel ? (
              <EmptyState />
            ) : (
              <>
                <div className="mb-3 flex items-center gap-2">
                  <Search className="h-3.5 w-3.5 text-muted-foreground" strokeWidth={1.5} />
                  <input
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    placeholder="Search origin or destination code, e.g. ATL"
                    className="h-8 w-full max-w-xs rounded-md border border-border bg-card px-2 text-sm focus:outline-none focus:ring-1 focus:ring-ring"
                  />
                  <span className="text-xs text-muted-foreground">
                    {fmtInt(routeRows.length)} of {fmtInt(routeModel.rankable.length)} routes
                  </span>
                </div>
                {routeRows.length === 0 ? (
                  <EmptyState>No routes match the search.</EmptyState>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="text-left text-xs text-muted-foreground">
                          {([["route", "Route"], ["flights", "Flights"], ["delay", "Delay rate"], ["avg", "Avg arrival delay"]] as const).map(([k, label]) => (
                            <th key={k} className={`pb-2 font-normal ${k === "route" ? "text-left" : "text-right"}`}>
                              <button
                                className="hover:text-foreground"
                                onClick={() =>
                                  setRouteSort((s) => (s.key === k ? { key: k, dir: (s.dir * -1) as 1 | -1 } : { key: k, dir: k === "route" ? 1 : -1 }))
                                }
                              >
                                {label}
                                {routeSort.key === k ? (routeSort.dir === 1 ? " ↑" : " ↓") : ""}
                              </button>
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {routeRows.slice(0, 150).map((r) => (
                          <tr key={`${r.origin}-${r.dest}`} className="border-t border-border">
                            <td className="py-2 font-mono text-xs">
                              {r.origin} → {r.dest}
                            </td>
                            <td className="py-2 text-right">{fmtInt(r.total)}</td>
                            <td className="py-2 text-right">{fmtPct(r.delay)}</td>
                            <td className="py-2 text-right">{fmtMin(r.avg)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    {routeRows.length > 150 && (
                      <p className="mt-3 text-xs text-muted-foreground">Showing the first 150 rows; the CSV download includes all {fmtInt(routeRows.length)}.</p>
                    )}
                  </div>
                )}
              </>
            )}
          </Panel>
        </div>
      )}
    </div>
  );
}
