// Ranking rules: airports qualify at flown >= 1000, routes at flown >= 500.
// UNMAPPED airport codes (October 2015 leftovers) never rank; pages add a footnote.

export const AIRPORT_MIN_FLOWN = 1000;
export const ROUTE_MIN_FLOWN = 500;

export const isMapped = (code: string) => code !== "UNMAPPED";

export function rankableAirports<T extends { code: string; flown: number }>(rows: T[]): T[] {
  return rows.filter((r) => isMapped(r.code) && r.flown >= AIRPORT_MIN_FLOWN);
}

export function rankableRoutes<T extends { origin: string; dest: string; flown: number }>(rows: T[]): T[] {
  return rows.filter((r) => isMapped(r.origin) && isMapped(r.dest) && r.flown >= ROUTE_MIN_FLOWN);
}
