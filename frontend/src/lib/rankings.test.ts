import { describe, expect, it } from "vitest";
import { AIRPORT_MIN_FLOWN, ROUTE_MIN_FLOWN, isMapped, rankableAirports, rankableRoutes } from "./rankings";
import { accuracy, f1Score, precision, recall } from "./metrics";

describe("airport ranking threshold", () => {
  it("requires flown >= 1000 and excludes UNMAPPED", () => {
    expect(AIRPORT_MIN_FLOWN).toBe(1000);
    const kept = rankableAirports([
      { code: "ATL", flown: 1000 },
      { code: "SMALL", flown: 999 },
      { code: "UNMAPPED", flown: 5000 },
      { code: "ORD", flown: 1200 },
    ]);
    expect(kept.map((r) => r.code)).toEqual(["ATL", "ORD"]);
  });
});

describe("route ranking threshold", () => {
  it("requires flown >= 500 and excludes UNMAPPED endpoints", () => {
    expect(ROUTE_MIN_FLOWN).toBe(500);
    const kept = rankableRoutes([
      { origin: "ATL", dest: "LAX", flown: 500 },
      { origin: "ATL", dest: "TINY", flown: 499 },
      { origin: "UNMAPPED", dest: "LAX", flown: 5000 },
      { origin: "ORD", dest: "UNMAPPED", flown: 5000 },
    ]);
    expect(kept.map((r) => `${r.origin}-${r.dest}`)).toEqual(["ATL-LAX"]);
  });

  it("treats only the literal code UNMAPPED as unmapped", () => {
    expect(isMapped("ATL")).toBe(true);
    expect(isMapped("UNMAPPED")).toBe(false);
  });
});

describe("classifier metrics from confusion counts", () => {
  it("derives precision, recall, F1 and accuracy", () => {
    expect(precision(30, 10)).toBeCloseTo(0.75);
    expect(recall(30, 20)).toBeCloseTo(0.6);
    expect(f1Score(precision(30, 10), recall(30, 20))).toBeCloseTo((2 * 0.75 * 0.6) / 1.35);
    expect(accuracy(30, 10, 20, 40)).toBeCloseTo(0.7);
  });

  it("returns null instead of dividing by zero", () => {
    expect(precision(0, 0)).toBeNull();
    expect(recall(0, 0)).toBeNull();
    expect(f1Score(null, 0.5)).toBeNull();
    expect(accuracy(0, 0, 0, 0)).toBeNull();
  });
});
