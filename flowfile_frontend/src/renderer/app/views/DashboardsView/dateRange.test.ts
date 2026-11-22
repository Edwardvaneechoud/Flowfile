import { describe, expect, it } from "vitest";
import { DATE_PRESETS, isDatePreset, presetOf, resolveDateRange } from "./dateRange";

// A Tuesday afternoon, in the runner's own time zone like the windows themselves.
const NOW = new Date(2026, 10, 17, 15, 30);
const local = (y: number, m: number, d: number, h = 0, min = 0, s = 0, ms = 0) =>
  new Date(y, m, d, h, min, s, ms).toISOString();
const dayStart = (y: number, m: number, d: number) => local(y, m, d);
const dayEnd = (y: number, m: number, d: number) => local(y, m, d, 23, 59, 59, 999);

describe("resolveDateRange", () => {
  it.each([
    ["today", dayStart(2026, 10, 17), dayEnd(2026, 10, 17)],
    ["yesterday", dayStart(2026, 10, 16), dayEnd(2026, 10, 16)],
    ["last_7_days", dayStart(2026, 10, 11), dayEnd(2026, 10, 17)],
    ["last_30_days", dayStart(2026, 9, 19), dayEnd(2026, 10, 17)],
    ["last_90_days", dayStart(2026, 7, 20), dayEnd(2026, 10, 17)],
    ["this_month", dayStart(2026, 10, 1), dayEnd(2026, 10, 30)],
    ["last_month", dayStart(2026, 9, 1), dayEnd(2026, 9, 31)],
    ["this_quarter", dayStart(2026, 9, 1), dayEnd(2026, 11, 31)],
    ["this_year", dayStart(2026, 0, 1), dayEnd(2026, 11, 31)],
    ["year_to_date", dayStart(2026, 0, 1), dayEnd(2026, 10, 17)],
  ])("resolves %s against today", (preset, start, end) => {
    expect(resolveDateRange({ preset }, NOW)).toEqual({ start, end });
  });

  it("offers every preset it can resolve", () => {
    for (const { value } of DATE_PRESETS) {
      expect(resolveDateRange({ preset: value }, NOW)?.start).toBeTruthy();
    }
  });

  it("moves with the day it is asked on", () => {
    const later = new Date(2026, 11, 17, 9, 0);
    expect(resolveDateRange({ preset: "last_7_days" }, later)).toEqual({
      start: dayStart(2026, 11, 11),
      end: dayEnd(2026, 11, 17),
    });
  });

  it("returns a fixed window as stored, open ends included", () => {
    expect(resolveDateRange({ start: "2026-07-01T00:00:00.000Z", end: null }, NOW)).toEqual({
      start: "2026-07-01T00:00:00.000Z",
      end: null,
    });
  });

  it("lets a preset win over leftover fixed bounds", () => {
    const state = { preset: "today", start: "2020-01-01T00:00:00.000Z", end: null };
    expect(resolveDateRange(state, NOW)?.start).toBe(dayStart(2026, 10, 17));
  });

  it("is null when nothing is set, and ignores a preset it does not know", () => {
    expect(resolveDateRange({ start: null, end: null }, NOW)).toBeNull();
    expect(resolveDateRange({ preset: "next_decade", start: null, end: null }, NOW)).toBeNull();
    expect(resolveDateRange(undefined, NOW)).toBeNull();
  });
});

describe("presetOf", () => {
  it("names a known preset only", () => {
    expect(presetOf({ preset: "last_30_days" })).toBe("last_30_days");
    expect(presetOf({ preset: null })).toBeNull();
    expect(presetOf({ preset: "toString" })).toBeNull();
    expect(isDatePreset("this_month")).toBe(true);
  });
});
