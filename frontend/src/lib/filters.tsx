import { createContext, useContext, useMemo, useState, type ReactNode } from "react";
import type { DimDate } from "./data";

export type DayType = "all" | "weekday" | "weekend";
export type HolidayMode = "all" | "holiday" | "normal";

export type Filters = {
  monthFrom: number;
  monthTo: number;
  airlines: string[]; // empty = all
  dayType: DayType;
  holiday: HolidayMode;
};

export const defaultFilters: Filters = { monthFrom: 1, monthTo: 12, airlines: [], dayType: "all", holiday: "all" };

type Ctx = { filters: Filters; setFilters: (f: Filters) => void; reset: () => void };
const FilterCtx = createContext<Ctx | null>(null);

export function FilterProvider({ children }: { children: ReactNode }) {
  const [filters, setFilters] = useState<Filters>(defaultFilters);
  const value = useMemo(() => ({ filters, setFilters, reset: () => setFilters(defaultFilters) }), [filters]);
  return <FilterCtx.Provider value={value}>{children}</FilterCtx.Provider>;
}

export function useFilters() {
  const c = useContext(FilterCtx);
  if (!c) throw new Error("useFilters outside FilterProvider");
  return c;
}

export function matchesAirline(f: Filters, code: string) {
  return f.airlines.length === 0 || f.airlines.includes(code);
}

/** Date-level filter. `ignoreHoliday` lets a chart compare holiday vs normal regardless of the toggle. */
export function matchesDate(f: Filters, d: DimDate | undefined, opts?: { ignoreHoliday?: boolean }) {
  if (!d) return false;
  if (d.month < f.monthFrom || d.month > f.monthTo) return false;
  if (f.dayType === "weekday" && d.isWeekend) return false;
  if (f.dayType === "weekend" && !d.isWeekend) return false;
  if (!opts?.ignoreHoliday) {
    if (f.holiday === "holiday" && !d.isHoliday) return false;
    if (f.holiday === "normal" && d.isHoliday) return false;
  }
  return true;
}

export const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
