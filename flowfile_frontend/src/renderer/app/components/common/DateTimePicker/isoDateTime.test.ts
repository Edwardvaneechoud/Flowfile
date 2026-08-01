import { describe, it, expect } from "vitest";
import { isoToLocalDate, localDateToIso, formatUtcHint } from "./isoDateTime";

describe("isoDateTime", () => {
  it("round-trips a UTC instant through isoToLocalDate/localDateToIso, ending in Z", () => {
    const iso = "2026-07-31T12:34:56Z";
    const d = isoToLocalDate(iso);
    expect(d).toBeInstanceOf(Date);
    expect(d?.getTime()).toBe(new Date(iso).getTime());
    const back = localDateToIso(d);
    expect(back).toBe(iso);
    expect(back?.endsWith("Z")).toBe(true);
  });

  it("clearing (null/undefined input) yields null, never '' or undefined", () => {
    expect(isoToLocalDate(null)).toBeNull();
    expect(isoToLocalDate(undefined)).toBeNull();
    expect(localDateToIso(null)).toBeNull();
    expect(localDateToIso(undefined)).toBeNull();
  });

  it("treats corrupt/unparseable input as null and never throws", () => {
    expect(() => isoToLocalDate("not-a-date")).not.toThrow();
    expect(isoToLocalDate("not-a-date")).toBeNull();
    expect(() => localDateToIso(new Date("not-a-date"))).not.toThrow();
    expect(localDateToIso(new Date("not-a-date"))).toBeNull();
  });

  it("formats the UTC hint independent of the local timezone", () => {
    expect(formatUtcHint("2026-07-31T12:34:56Z")).toBe("2026-07-31 12:34 UTC");
    expect(formatUtcHint(null)).toBe("");
    expect(formatUtcHint("not-a-date")).toBe("");
  });
});
