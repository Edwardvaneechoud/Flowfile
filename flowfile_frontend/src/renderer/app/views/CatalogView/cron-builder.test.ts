// Unit tests for the friendly schedule builder <-> cron mapping.
// These guard the exact cron strings the backend receives and the
// round-trip used to re-open an existing schedule in the right state.

import { describe, it, expect } from "vitest";
import {
  buildCron,
  parseCron,
  describeCron,
  defaultCronState,
  type CronBuilderState,
} from "./cron-builder";

describe("buildCron", () => {
  it("builds every-N-minutes", () => {
    expect(buildCron({ ...defaultCronState(), frequency: "minutes", everyN: 15 })).toBe(
      "*/15 * * * *",
    );
    expect(buildCron({ ...defaultCronState(), frequency: "minutes", everyN: 1 })).toBe("* * * * *");
  });

  it("builds hourly", () => {
    expect(buildCron({ ...defaultCronState(), frequency: "hourly", everyN: 1 })).toBe("0 * * * *");
    expect(buildCron({ ...defaultCronState(), frequency: "hourly", everyN: 3 })).toBe(
      "0 */3 * * *",
    );
  });

  it("builds daily at a time", () => {
    expect(buildCron({ ...defaultCronState(), frequency: "daily", time: "02:00" })).toBe(
      "0 2 * * *",
    );
    expect(buildCron({ ...defaultCronState(), frequency: "daily", time: "09:30" })).toBe(
      "30 9 * * *",
    );
  });

  it("builds weekly on selected days, sorted", () => {
    expect(
      buildCron({ ...defaultCronState(), frequency: "weekly", time: "09:00", weekdays: [4, 1] }),
    ).toBe("0 9 * * 1,4");
  });

  it("builds monthly on a day", () => {
    expect(
      buildCron({ ...defaultCronState(), frequency: "monthly", time: "06:00", dayOfMonth: 1 }),
    ).toBe("0 6 1 * *");
  });

  it("passes custom expressions through verbatim", () => {
    expect(buildCron({ ...defaultCronState(), frequency: "custom", expression: "5 4 * * 0" })).toBe(
      "5 4 * * 0",
    );
  });
});

describe("parseCron round-trips buildCron output", () => {
  const cases: Partial<CronBuilderState>[] = [
    { frequency: "minutes", everyN: 15 },
    { frequency: "hourly", everyN: 3 },
    { frequency: "daily", time: "02:00" },
    { frequency: "weekly", time: "09:00", weekdays: [1, 4] },
    { frequency: "monthly", time: "06:00", dayOfMonth: 12 },
  ];

  it.each(cases)("round-trips %o", (override) => {
    const state = { ...defaultCronState(), ...override };
    const expr = buildCron(state);
    const parsed = parseCron(expr);
    expect(parsed.frequency).toBe(override.frequency);
    expect(buildCron(parsed)).toBe(expr);
  });
});

describe("parseCron fallbacks", () => {
  it("falls back to custom for cron shapes the builder doesn't emit", () => {
    const parsed = parseCron("0 9 1-5 * *");
    expect(parsed.frequency).toBe("custom");
    expect(parsed.expression).toBe("0 9 1-5 * *");
  });

  it("falls back to custom for non-5-field input", () => {
    expect(parseCron("0 9 * *").frequency).toBe("custom");
  });
});

describe("describeCron", () => {
  it("renders plain English for known expressions", () => {
    expect(describeCron("*/15 * * * *")).toMatch(/15 minutes/i);
    expect(describeCron("0 2 * * *").length).toBeGreaterThan(0);
  });

  it("appends the timezone when provided", () => {
    expect(describeCron("0 2 * * *", "Europe/Amsterdam")).toContain("(Europe/Amsterdam)");
  });

  it("returns an empty string for invalid or blank expressions", () => {
    expect(describeCron("not a cron")).toBe("");
    expect(describeCron("")).toBe("");
    expect(describeCron(null)).toBe("");
  });
});
