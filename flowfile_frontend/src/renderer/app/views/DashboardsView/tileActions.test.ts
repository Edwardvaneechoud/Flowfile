import { describe, expect, it } from "vitest";
import { downloadName, resultColumns, tileActions, type TileActionContext } from "./tileActions";

const ctx = (over: Partial<TileActionContext>): TileActionContext => ({
  type: "viz",
  configured: true,
  missing: false,
  hasChart: true,
  ...over,
});

describe("tileActions", () => {
  it("gives a drawn chart every entry", () => {
    expect(tileActions(ctx({}))).toEqual(["expand", "see-rows", "download-csv", "download-image"]);
  });

  it("gives a KPI its rows but no direct download, and a table its own download", () => {
    expect(tileActions(ctx({ type: "kpi", hasChart: false }))).toEqual(["expand", "see-rows"]);
    expect(tileActions(ctx({ type: "table", hasChart: false }))).toEqual([
      "expand",
      "download-csv",
    ]);
  });

  it("offers nothing on text, separator and filter tiles", () => {
    for (const type of ["text", "separator", "filter"] as const) {
      expect(tileActions(ctx({ type }))).toEqual([]);
    }
  });

  it("offers nothing on a tile still to be set up, one whose source is gone, or a chart not drawn", () => {
    for (const type of ["viz", "kpi", "table"] as const) {
      expect(tileActions(ctx({ type, configured: false }))).toEqual([]);
      expect(tileActions(ctx({ type, missing: true }))).toEqual([]);
    }
    expect(tileActions(ctx({ hasChart: false }))).toEqual([]);
  });
});

describe("downloadName", () => {
  it("makes the title safe and falls back when it is empty", () => {
    expect(downloadName("Revenue by region / 2026", "png", "chart")).toBe(
      "Revenue_by_region_2026.png",
    );
    expect(downloadName("  ", "csv", "chart")).toBe("chart.csv");
    expect(downloadName(null, "png", "chart")).toBe("chart.png");
  });
});

describe("resultColumns", () => {
  it("lists every key in the order it first appears", () => {
    expect(
      resultColumns([
        { region: "West", amount_sum: 150 },
        { region: "East", extra: 1 },
      ]),
    ).toEqual([{ field: "region" }, { field: "amount_sum" }, { field: "extra" }]);
    expect(resultColumns([])).toEqual([]);
  });
});
