import { describe, expect, it } from "vitest";

import type { IChart } from "@kanaries/graphic-walker/interfaces";
import {
  CUSTOM_FORMAT,
  DATE_FORMAT_PRESETS,
  NUMBER_FORMAT_PRESETS,
  SAMPLE_DATE,
  SAMPLE_NUMBER,
  SAMPLE_SHARE,
  applyTilePresentation,
  formatSampleDate,
  formatSampleNumber,
  normalizeChartFormat,
  numberFormatError,
  presetIdFor,
  timeFormatError,
} from "./chartPresentation";

const field = (fid: string, name: string, extra: Record<string, unknown> = {}) => ({
  fid,
  name,
  analyticType: "dimension",
  semanticType: "nominal",
  ...extra,
});

const chart = (): IChart =>
  ({
    visId: "v1",
    name: "Sales",
    encodings: {
      dimensions: [field("region", "region")],
      measures: [field("amount", "amount", { analyticType: "measure" })],
      rows: [field("region", "region")],
      columns: [
        field("year", "year"),
        field("amount", "amount", { analyticType: "measure", aggName: "sum" }),
      ],
      color: [field("region", "region")],
      opacity: [],
      size: [],
      shape: [],
      radius: [],
      theta: [],
      longitude: [],
      latitude: [],
      geoId: [],
      details: [],
      filters: [],
      text: [],
    },
    config: { defaultAggregated: true, geoms: ["bar"], limit: -1 },
    layout: { format: { normalizedNumberFormat: ".0%" }, background: "#fff" },
  }) as unknown as IChart;

describe("presets", () => {
  it("writes the sample value the way each preset says", () => {
    const written = Object.fromEntries(
      NUMBER_FORMAT_PRESETS.map((p) => [p.id, formatSampleNumber(p.format, SAMPLE_NUMBER)]),
    );
    expect(written).toEqual({
      auto: null,
      thousands: "1,234,567.891",
      whole: "1,234,568",
      decimals2: "1,234,567.89",
      compact: "1.23M",
      percent: "123456789.1%",
    });
    expect(formatSampleNumber(".1%", SAMPLE_SHARE)).toBe("12.3%");
  });

  it("writes the sample date the way each preset says", () => {
    const written = Object.fromEntries(
      DATE_FORMAT_PRESETS.map((p) => [p.id, formatSampleDate(p.format, SAMPLE_DATE)]),
    );
    expect(written).toEqual({
      auto: null,
      iso: "2024-03-31",
      dmy: "31 Mar 2024",
      my: "Mar 2024",
      year: "2024",
    });
  });

  it("finds the preset of a saved string, else custom", () => {
    expect(presetIdFor(null, NUMBER_FORMAT_PRESETS)).toBe("auto");
    expect(presetIdFor("  ", NUMBER_FORMAT_PRESETS)).toBe("auto");
    expect(presetIdFor(".3~s", NUMBER_FORMAT_PRESETS)).toBe("compact");
    expect(presetIdFor("$,.2f", NUMBER_FORMAT_PRESETS)).toBe(CUSTOM_FORMAT);
    expect(presetIdFor("%b %Y", DATE_FORMAT_PRESETS)).toBe("my");
  });
});

describe("validation", () => {
  it("accepts what d3-format reads and names what it does not", () => {
    expect(numberFormatError(null)).toBeNull();
    expect(numberFormatError("$,.2f")).toBeNull();
    expect(numberFormatError("not a format")).toMatch(/not a d3 number format/);
    expect(formatSampleNumber("not a format", 1)).toBeNull();
  });

  it("refuses a date format with no directive", () => {
    expect(timeFormatError("%d/%m/%Y")).toBeNull();
    expect(timeFormatError("year")).toMatch(/no d3 directive/);
    expect(formatSampleDate("year", SAMPLE_DATE)).toBeNull();
  });
});

describe("normalizeChartFormat", () => {
  it("trims, turns blanks into null and drops an empty override", () => {
    expect(normalizeChartFormat({ number_format: " ,.2f ", x_title: "  " })).toEqual({
      number_format: ",.2f",
      time_format: null,
      x_title: null,
      y_title: null,
    });
    expect(normalizeChartFormat({ number_format: "", y_title: " " })).toBeNull();
    expect(normalizeChartFormat(null)).toBeNull();
  });
});

describe("applyTilePresentation", () => {
  it("hands back the identical chart without overrides", () => {
    const c = chart();
    expect(applyTilePresentation(c, null)).toBe(c);
    expect(applyTilePresentation(c, undefined)).toBe(c);
    expect(applyTilePresentation(c, { number_format: " ", x_title: "" })).toBe(c);
  });

  it("sets the formats on a copy and keeps the chart's other settings", () => {
    const c = chart();
    const before = JSON.stringify(c);
    const drawn = applyTilePresentation(c, { number_format: ".3~s", time_format: "%Y" });
    expect(drawn.layout.format).toEqual({
      normalizedNumberFormat: ".0%",
      numberFormat: ".3~s",
      timeFormat: "%Y",
    });
    expect(drawn.layout.background).toBe("#fff");
    expect(JSON.stringify(c)).toBe(before);
  });

  it("leaves out a format d3 cannot read", () => {
    const c = chart();
    expect(applyTilePresentation(c, { number_format: "not a format" })).toBe(c);
    const drawn = applyTilePresentation(c, { number_format: "bad one", time_format: "%Y" });
    expect(drawn.layout.format).toEqual({ normalizedNumberFormat: ".0%", timeFormat: "%Y" });
  });

  it("renames the field ending each shelf and nothing else", () => {
    const c = chart();
    const drawn = applyTilePresentation(c, { x_title: "Revenue", y_title: "Region" });
    expect(drawn.encodings.columns.map((f) => [f.fid, f.name])).toEqual([
      ["year", "year"],
      ["amount", "Revenue"],
    ]);
    expect(drawn.encodings.rows.map((f) => f.name)).toEqual(["Region"]);
    expect(drawn.encodings.color[0].name).toBe("region");
    expect(drawn.encodings.dimensions[0].name).toBe("region");
    expect(c.encodings.columns[1].name).toBe("amount");
  });

  it("skips a title for an empty shelf", () => {
    const c = chart();
    c.encodings.rows = [];
    const drawn = applyTilePresentation(c, { y_title: "Region" });
    expect(drawn.encodings.rows).toEqual([]);
  });
});
