import { describe, expect, it } from "vitest";

import {
  belongsToDashboard,
  chartsForDashboard,
  libraryCharts,
  ownerLabel,
  readIncludeOwned,
  writeIncludeOwned,
} from "./chartLibraryFilter";

interface Chart {
  id: number;
  dashboard_id?: number | null;
  dashboard_name?: string | null;
}

const chart = (id: number, dashboard_id: number | null = null, dashboard_name?: string): Chart => ({
  id,
  dashboard_id,
  dashboard_name: dashboard_name ?? null,
});

// The last one is a chart from a core that sends no owner fields.
const CHARTS: Chart[] = [chart(1), chart(2, 7, "Sales"), chart(3, 8, "Ops"), { id: 4 }];

describe("libraryCharts", () => {
  it("shows only the charts of their own by default", () => {
    expect(libraryCharts(CHARTS, false).map((c) => c.id)).toEqual([1, 4]);
  });

  it("shows every chart when the dashboards' charts are asked for", () => {
    expect(libraryCharts(CHARTS, true).map((c) => c.id)).toEqual([1, 2, 3, 4]);
  });
});

describe("chartsForDashboard", () => {
  it("offers the charts of their own and the dashboard's, never another dashboard's", () => {
    expect(chartsForDashboard(CHARTS, 7).map((c) => c.id)).toEqual([1, 2, 4]);
  });

  it("offers only the charts of their own to a dashboard that is not saved yet", () => {
    expect(chartsForDashboard(CHARTS, null).map((c) => c.id)).toEqual([1, 4]);
    expect(chartsForDashboard(CHARTS, 0).map((c) => c.id)).toEqual([1, 4]);
  });
});

describe("ownerLabel", () => {
  it("names the dashboard, and says so when the name is not known", () => {
    expect(ownerLabel(chart(2, 7, "Sales"))).toBe("On Sales");
    expect(ownerLabel(chart(2, 7))).toBe("On a dashboard");
    expect(ownerLabel(chart(1))).toBeNull();
    expect(belongsToDashboard({})).toBe(false);
  });
});

describe("the remembered choice", () => {
  it("round-trips through storage and defaults to off", () => {
    const held = new Map<string, string>();
    const storage = {
      getItem: (key: string) => held.get(key) ?? null,
      setItem: (key: string, value: string) => void held.set(key, value),
    };
    expect(readIncludeOwned(storage)).toBe(false);
    writeIncludeOwned(storage, true);
    expect(readIncludeOwned(storage)).toBe(true);
    writeIncludeOwned(storage, false);
    expect(readIncludeOwned(storage)).toBe(false);
  });

  it("is off when storage is missing or refuses", () => {
    const refusing = {
      getItem: () => {
        throw new Error("blocked");
      },
      setItem: () => {
        throw new Error("blocked");
      },
    };
    expect(readIncludeOwned(undefined)).toBe(false);
    expect(readIncludeOwned(refusing)).toBe(false);
    expect(() => writeIncludeOwned(refusing, true)).not.toThrow();
  });
});
