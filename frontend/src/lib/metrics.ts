// Metric definitions. Rates are always computed from summed counts, never by averaging rates.

export type Counts = {
  total: number;
  flown: number;
  cancelled: number;
  diverted: number;
  delayedArr: number;
  sumArrDelay: number;
};

export const emptyCounts = (): Counts => ({
  total: 0,
  flown: 0,
  cancelled: 0,
  diverted: 0,
  delayedArr: 0,
  sumArrDelay: 0,
});

export function addInto(acc: Counts, c: Counts): Counts {
  acc.total += c.total;
  acc.flown += c.flown;
  acc.cancelled += c.cancelled;
  acc.diverted += c.diverted;
  acc.delayedArr += c.delayedArr;
  acc.sumArrDelay += c.sumArrDelay;
  return acc;
}

export const cancelRate = (c: Counts) => (c.total > 0 ? c.cancelled / c.total : null);
export const divertRate = (c: Counts) => (c.total > 0 ? c.diverted / c.total : null);
export const delayRate = (c: Counts) => (c.flown > 0 ? c.delayedArr / c.flown : null);
export const onTimeRate = (c: Counts) => {
  const d = delayRate(c);
  return d == null ? null : 1 - d;
};
export const avgArrDelay = (c: Counts) => (c.flown > 0 ? c.sumArrDelay / c.flown : null);
export const share = (part: number, whole: number) => (whole > 0 ? part / whole : null);

const intFmt = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
const dec1 = new Intl.NumberFormat("en-US", { minimumFractionDigits: 1, maximumFractionDigits: 1 });

export const fmtInt = (n: number | null | undefined) => (n == null || !isFinite(n) ? "–" : intFmt.format(n));
export const fmtPct = (r: number | null | undefined) => (r == null || !isFinite(r) ? "–" : `${dec1.format(r * 100)}%`);
export const fmtMin = (m: number | null | undefined) => (m == null || !isFinite(m) ? "–" : `${dec1.format(m)} min`);
export const fmtCompact = (n: number) =>
  new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 }).format(n);

/** Safe number parse: empty / invalid cells are 0. */
export function num(v: unknown): number {
  if (v == null || v === "") return 0;
  const n = typeof v === "number" ? v : Number(String(v).trim());
  return isFinite(n) ? n : 0;
}

/** Normalise "2015-01-05", "2015-01-05 00:00:00", "1/5/2015" to "2015-01-05". */
export function dateKey(v: unknown): string | null {
  const s = String(v ?? "").trim();
  let m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
  if (m) return `${m[1]}-${m[2]!.padStart(2, "0")}-${m[3]!.padStart(2, "0")}`;
  m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (m) return `${m[3]}-${m[1]!.padStart(2, "0")}-${m[2]!.padStart(2, "0")}`;
  return null;
}

export function bool(v: unknown): boolean {
  const s = String(v ?? "").trim().toLowerCase();
  return s === "1" || s === "true" || s === "yes" || s === "t" || s === "y";
}

// Classifier metrics, derived from confusion-matrix counts (never from averaged rates).
export const precision = (tp: number, fp: number) => (tp + fp > 0 ? tp / (tp + fp) : null);
export const recall = (tp: number, fn: number) => (tp + fn > 0 ? tp / (tp + fn) : null);
export const f1Score = (p: number | null, r: number | null) =>
  p != null && r != null && p + r > 0 ? (2 * p * r) / (p + r) : null;
export const accuracy = (tp: number, fp: number, fn: number, tn: number) => {
  const total = tp + fp + fn + tn;
  return total > 0 ? (tp + tn) / total : null;
};
