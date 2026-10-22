import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { geoAlbersUsa, geoPath, type GeoPermissibleObjects } from "d3-geo";
import { feature } from "topojson-client";
import { DELAY_BINS, delayColor } from "./CalendarHeatmap";
import { fmtInt, fmtPct } from "@/lib/metrics";

const W = 900;
const H = 540;

export type MapAirport = {
  code: string;
  name: string;
  lat: number | null;
  lon: number | null;
  flights: number;
  rate: number | null;
};

/** Bubble map of US airports: bubble area = flights, colour = delay rate. Albers USA projection, no external tiles. */
export function UsaBubbleMap({ airports }: { airports: MapAirport[] }) {
  const [hover, setHover] = useState<MapAirport | null>(null);
  const geo = useQuery({
    queryKey: ["us_states_path"],
    staleTime: Infinity,
    gcTime: Infinity,
    retry: 1,
    queryFn: async () => {
      const res = await fetch("/geo/us-states.json");
      if (!res.ok) throw new Error(`Could not load the US state outlines (HTTP ${res.status})`);
      const topo = (await res.json()) as Parameters<typeof feature>[0];
      const states = (topo.objects as { states: unknown })["states"] as Parameters<typeof feature>[1];
      const fc = feature(topo, states) as unknown as GeoPermissibleObjects;
      const projection = geoAlbersUsa().fitExtent(
        [
          [8, 8],
          [W - 8, H - 8],
        ],
        fc,
      );
      const path = geoPath(projection);
      return { d: path(fc) ?? "", projection };
    },
  });

  const maxFlights = Math.max(1, ...airports.map((a) => a.flights));
  const points = useMemo(() => {
    if (!geo.data) return [];
    return airports.flatMap((a) => {
      if (a.lat == null || a.lon == null || a.flights <= 0) return [];
      const p = geo.data!.projection([a.lon, a.lat]);
      if (!p) return [];
      return [{ ...a, x: p[0], y: p[1] }];
    });
  }, [geo.data, airports]);

  if (geo.isError) {
    return (
      <p className="py-10 text-center text-sm text-muted-foreground">
        The US state outlines could not be loaded from /geo/us-states.json, so the map is unavailable. The tables below still work.
      </p>
    );
  }

  return (
    <div>
      <div className="overflow-hidden rounded-lg bg-muted/40">
        {geo.isLoading || !geo.data ? (
          <div className="h-[420px] animate-pulse rounded-lg bg-muted/60" aria-label="Loading map" />
        ) : (
          <svg viewBox={`0 0 ${W} ${H}`} className="block h-auto w-full" role="img" aria-label="Bubble map of US airports">
            <path d={geo.data.d} fill="var(--stone)" fillOpacity={0.45} stroke="var(--border)" strokeWidth={0.7} />
            {points.map((p) => {
              const r = Math.max(1.6, Math.sqrt(p.flights / maxFlights) * 26);
              const on = hover?.code === p.code;
              return (
                <circle
                  key={p.code}
                  cx={p.x}
                  cy={p.y}
                  r={on ? r + 1 : r}
                  fill={delayColor(p.rate)}
                  opacity={on ? 1 : 0.85}
                  stroke="var(--card)"
                  strokeWidth={0.8}
                  onMouseEnter={() => setHover(p)}
                  onMouseLeave={() => setHover(null)}
                />
              );
            })}
          </svg>
        )}
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
        </div>
        <span className="min-h-4 text-foreground">
          {hover
            ? `${hover.code} ${hover.name} · ${fmtInt(hover.flights)} flights · ${fmtPct(hover.rate)} delayed`
            : "Bubble area is proportional to flights. Hover a bubble for details."}
        </span>
      </div>
    </div>
  );
}
