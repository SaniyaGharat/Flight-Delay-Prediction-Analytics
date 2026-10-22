import Papa from "papaparse";
import { useQuery } from "@tanstack/react-query";
import { bool, dateKey, num, type Counts } from "./metrics";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function loadCsv(file: string): Promise<any[]> {
  const res = await fetch(`/data/${file}`);
  if (!res.ok) throw new Error(`Could not load ${file} (HTTP ${res.status})`);
  const text = await res.text();
  if (text.trim().startsWith("<")) throw new Error(`Could not load ${file} (file not found)`);
  const out = Papa.parse<Record<string, string>>(text, {
    header: true,
    skipEmptyLines: true,
    transformHeader: (h) => h.trim(),
  });
  return out.data;
}

export type DimDate = {
  date: string;
  month: number;
  monthName: string;
  day: number;
  dow: number;
  dowName: string;
  week: number;
  isWeekend: boolean;
  isHoliday: boolean;
};

export type Airline = { code: string; name: string };

export type DailyAirline = Counts & {
  date: string;
  airline: string;
  delayedDep: number;
  causes: { airSystem: number; security: number; airline: number; lateAircraft: number; weather: number };
};

const staleForever = { staleTime: Infinity, gcTime: Infinity, retry: 1 } as const;

export function useDimDate() {
  return useQuery({
    queryKey: ["dim_date"],
    ...staleForever,
    queryFn: async () => {
      const rows = await loadCsv("dim_date.csv");
      const map = new Map<string, DimDate>();
      for (const r of rows) {
        const date = dateKey(r.DATE);
        if (!date) continue;
        map.set(date, {
          date,
          month: num(r.MONTH),
          monthName: r.MONTH_NAME ?? "",
          day: num(r.DAY),
          dow: num(r.DAY_OF_WEEK),
          dowName: r.DAY_OF_WEEK_NAME ?? "",
          week: num(r.WEEK_OF_YEAR),
          isWeekend: bool(r.IS_WEEKEND),
          isHoliday: bool(r.IS_HOLIDAY_WINDOW),
        });
      }
      return map;
    },
  });
}

export function useAirlines() {
  return useQuery({
    queryKey: ["dim_airline"],
    ...staleForever,
    queryFn: async () =>
      (await loadCsv("dim_airline.csv"))
        .map((r) => ({ code: (r.code ?? "").trim(), name: (r.name ?? "").trim() }))
        .filter((a) => a.code)
        .sort((a, b) => a.name.localeCompare(b.name)) as Airline[],
  });
}

export function useDailyAirline() {
  return useQuery({
    queryKey: ["fact_daily_airline"],
    ...staleForever,
    queryFn: async () => {
      const rows = await loadCsv("fact_daily_airline.csv");
      const out: DailyAirline[] = [];
      for (const r of rows) {
        const date = dateKey(r.FLIGHT_DATE);
        if (!date) continue;
        out.push({
          date,
          airline: (r.AIRLINE ?? "").trim(),
          total: num(r.total_flights),
          flown: num(r.flown_flights),
          cancelled: num(r.cancelled_flights),
          diverted: num(r.diverted_flights),
          delayedArr: num(r.delayed_arr_15),
          delayedDep: num(r.delayed_dep_15),
          sumArrDelay: num(r.sum_arr_delay_min),
          causes: {
            airSystem: num(r.sum_cause_air_system),
            security: num(r.sum_cause_security),
            airline: num(r.sum_cause_airline),
            lateAircraft: num(r.sum_cause_late_aircraft),
            weather: num(r.sum_cause_weather),
          },
        });
      }
      return out;
    },
  });
}

export type DowHourRow = Counts & { airline: string; dow: number; hour: number; delayedDep: number; sumDepDelay: number };

export function useAirlineDowHour() {
  return useQuery({
    queryKey: ["fact_airline_dow_hour"],
    ...staleForever,
    queryFn: async () => {
      const rows = await loadCsv("fact_airline_dow_hour.csv");
      const out: DowHourRow[] = [];
      for (const r of rows) {
        const airline = (r.AIRLINE ?? "").trim();
        const dow = num(r.DAY_OF_WEEK);
        if (!airline || dow < 1 || dow > 7) continue;
        out.push({
          airline,
          dow,
          hour: num(r.SCHED_DEP_HOUR),
          total: num(r.total_flights),
          flown: num(r.flown_flights),
          cancelled: num(r.cancelled_flights),
          diverted: num(r.diverted_flights),
          delayedArr: num(r.delayed_arr_15),
          delayedDep: num(r.delayed_dep_15),
          sumArrDelay: num(r.sum_arr_delay_min),
          sumDepDelay: num(r.sum_dep_delay_min),
        });
      }
      return out;
    },
  });
}

export type RouteRow = Counts & { origin: string; dest: string; month: number; delayedDep: number };

export function useRouteMonth() {
  return useQuery({
    queryKey: ["fact_route_month"],
    ...staleForever,
    queryFn: async () => {
      const rows = await loadCsv("fact_route_month.csv");
      const out: RouteRow[] = [];
      for (const r of rows) {
        const origin = (r.ORIGIN ?? "").trim();
        const dest = (r.DEST ?? "").trim();
        const month = num(r.MONTH);
        if (!origin || !dest || month < 1 || month > 12) continue;
        out.push({
          origin,
          dest,
          month,
          total: num(r.total_flights),
          flown: num(r.flown_flights),
          cancelled: num(r.cancelled_flights),
          diverted: num(r.diverted_flights),
          delayedArr: num(r.delayed_arr_15),
          delayedDep: num(r.delayed_dep_15),
          sumArrDelay: num(r.sum_arr_delay_min),
        });
      }
      return out;
    },
  });
}

export type AirportTotals = Counts & {
  code: string;
  name: string;
  city: string;
  state: string;
  lat: number | null;
  lon: number | null;
  hasCoords: boolean;
  delayedDep: number;
};

export function useAirportTotals(kind: "origin" | "dest") {
  const file = kind === "origin" ? "airport_origin_totals.csv" : "airport_dest_totals.csv";
  return useQuery({
    queryKey: ["airport_totals", kind],
    ...staleForever,
    queryFn: async () => {
      const rows = await loadCsv(file);
      const out: AirportTotals[] = [];
      for (const r of rows) {
        const code = (r.code ?? "").trim();
        if (!code) continue;
        const lat = r.lat == null || r.lat === "" ? NaN : Number(r.lat);
        const lon = r.lon == null || r.lon === "" ? NaN : Number(r.lon);
        out.push({
          code,
          name: (r.name ?? "").trim(),
          city: (r.city ?? "").trim(),
          state: (r.state ?? "").trim(),
          lat: isFinite(lat) ? lat : null,
          lon: isFinite(lon) ? lon : null,
          hasCoords: bool(r.HAS_COORDS) || (isFinite(lat) && isFinite(lon)),
          total: num(r.total_flights),
          flown: num(r.flown_flights),
          cancelled: num(r.cancelled_flights),
          diverted: num(r.diverted_flights),
          delayedArr: num(r.delayed_arr_15),
          sumArrDelay: num(r.sum_arr_delay_min),
          delayedDep: num(r.delayed_dep_15),
        });
      }
      return out;
    },
  });
}

export type CancelReason = "A" | "B" | "C" | "D";
export type CancelReasonRow = { date: string; airline: string; reason: CancelReason; cancelled: number };

export function useCancelReasons() {
  return useQuery({
    queryKey: ["fact_cancel_daily_airline_reason"],
    ...staleForever,
    queryFn: async () => {
      const rows = await loadCsv("fact_cancel_daily_airline_reason.csv");
      const out: CancelReasonRow[] = [];
      for (const r of rows) {
        const date = dateKey(r.FLIGHT_DATE);
        const reason = (r.CANCELLATION_REASON ?? "").trim().toUpperCase();
        const airline = (r.AIRLINE ?? "").trim();
        if (!date || !airline || !(["A", "B", "C", "D"] as string[]).includes(reason)) continue;
        out.push({ date, airline, reason: reason as CancelReason, cancelled: num(r.CANCELLED_FLIGHTS) });
      }
      return out;
    },
  });
}

export type MlModel = { name: string; aucRoc: number | null; aucPr: number | null; tn: number; fp: number; fn: number; tp: number };
export type MlSummary = { split: Record<string, string>; models: MlModel[] };

const asNum = (v: unknown): number | null => {
  if (v == null || v === "") return null;
  const n = typeof v === "number" ? v : Number(String(v).trim());
  return isFinite(n) ? n : null;
};

/*
 * Expected shape of /data/ml_summary.json (key aliases are tolerated):
 * {
 *   "split": { "train": "January–September 2015", "validation": "October 2015", "test": "November–December 2015" },
 *   "models": [
 *     { "name": "Logistic regression", "auc_roc": 0.61, "auc_pr": 0.33, "tn": 100, "fp": 20, "fn": 30, "tp": 10 }
 *   ]
 * }
 */
export function useMlSummary() {
  return useQuery({
    queryKey: ["ml_summary"],
    ...staleForever,
    queryFn: async (): Promise<MlSummary> => {
      const res = await fetch("/data/ml_summary.json");
      if (!res.ok) throw new Error(`Could not load ml_summary.json (HTTP ${res.status})`);
      const raw = (await res.json()) as Record<string, unknown>;
      const modelsRaw = raw["models"] ?? raw["classifiers"] ?? raw["results"];
      const readModel = (o: Record<string, unknown>, name: string): MlModel => ({
        name,
        aucRoc: asNum(o["auc_roc"] ?? o["aucRoc"] ?? o["roc_auc"]),
        aucPr: asNum(o["auc_pr"] ?? o["aucPr"] ?? o["pr_auc"]),
        tn: num(o["tn"]),
        fp: num(o["fp"]),
        fn: num(o["fn"]),
        tp: num(o["tp"]),
      });
      const models: MlModel[] = Array.isArray(modelsRaw)
        ? modelsRaw.map((m) => readModel(m as Record<string, unknown>, String((m as Record<string, unknown>)["name"] ?? (m as Record<string, unknown>)["model"] ?? "Model")))
        : modelsRaw && typeof modelsRaw === "object"
          ? Object.entries(modelsRaw as Record<string, unknown>).map(([name, m]) => readModel((m ?? {}) as Record<string, unknown>, name))
          : [];
      if (!models.length) throw new Error("ml_summary.json does not contain a models list.");
      const splitRaw = raw["split"] ?? raw["splits"] ?? raw["chronological_split"];
      const split: Record<string, string> = {};
      if (splitRaw && typeof splitRaw === "object") {
        for (const [k, v] of Object.entries(splitRaw as Record<string, unknown>)) {
          if (v == null) continue;
          if (typeof v === "string" || typeof v === "number") split[k] = String(v);
          else if (typeof v === "object") {
            const o = v as Record<string, unknown>;
            const from = o["from"] ?? o["start"] ?? o["begin"];
            const to = o["to"] ?? o["end"];
            split[k] = from != null && to != null ? `${String(from)}–${String(to)}` : JSON.stringify(v);
          }
        }
      } else if (typeof splitRaw === "string" && splitRaw.trim()) {
        split["split"] = splitRaw.trim();
      }
      return { split, models };
    },
  });
}
