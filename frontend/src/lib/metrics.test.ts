import { describe, expect, it } from "vitest";
import { addInto, avgArrDelay, cancelRate, dateKey, delayRate, emptyCounts, fmtPct, num, onTimeRate } from "./metrics";

describe("metrics", () => {
  it("delay rate uses summed counts, not averaged rates", () => {
    const a = { ...emptyCounts(), total: 10, flown: 10, delayedArr: 5 };
    const b = { ...emptyCounts(), total: 90, flown: 90, delayedArr: 9 };
    const sum = addInto(addInto(emptyCounts(), a), b);
    expect(delayRate(sum)).toBeCloseTo(0.14);
  });
  it("cancel rate divides by total; delay rate divides by flown", () => {
    const c = { ...emptyCounts(), total: 100, flown: 80, cancelled: 20, delayedArr: 20, sumArrDelay: 400 };
    expect(cancelRate(c)).toBe(0.2);
    expect(delayRate(c)).toBe(0.25);
    expect(avgArrDelay(c)).toBe(5);
  });
  it("on-time rate is blank when nothing flew", () => {
    expect(onTimeRate({ ...emptyCounts(), total: 3, cancelled: 3 })).toBeNull();
  });
  it("empty cells parse as 0 and dates normalise", () => {
    expect(num("")).toBe(0);
    expect(dateKey("1/5/2015")).toBe("2015-01-05");
  });
  it("percentages use one decimal", () => {
    expect(fmtPct(0.18234)).toBe("18.2%");
  });
});
