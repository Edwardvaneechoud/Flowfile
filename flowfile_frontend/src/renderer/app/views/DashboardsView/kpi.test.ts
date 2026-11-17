import { describe, expect, it, vi } from "vitest";

vi.mock("../../api/catalog.api", () => ({ CatalogApi: {} }));
vi.mock("../../composables/useGraphicWalkerCompute", () => ({ useGraphicWalkerCompute: vi.fn() }));

import {
  DEFAULT_KPI,
  KPI_TREND_MAX_BUCKETS,
  autoKpiLabel,
  autoTrendGrain,
  buildKpiPayload,
  buildKpiTrendPayload,
  formatTrendBucket,
  kpiTrendOf,
  parseBucket,
  readKpiSpan,
  readKpiTrend,
  resolveTrendGrain,
  trendDescription,
  trendNeedsSpan,
  computeDelta,
  fieldEligible,
  findDateRangeFilter,
  formatDeltaAbs,
  formatDeltaPct,
  formatKpiValue,
  isKpiComplete,
  pickComparisonFilter,
  previousPeriodFilters,
  previousPeriodLabel,
  readKpiValue,
  shiftDateRangeBack,
  valueSizePx,
} from "./kpi";
import type { DashboardFilter, DashboardKpi } from "../../types";

const kpiOf = (over: Partial<DashboardKpi>): DashboardKpi => ({ ...DEFAULT_KPI, ...over });

const measureOf = (kpi: DashboardKpi) =>
  (buildKpiPayload(kpi) as any).workflow[0].query[0].measures[0];

const filterOf = (over: Partial<DashboardFilter>): DashboardFilter => ({
  id: "f",
  field_name: "order_date",
  kind: "date_range",
  state: { start: "2026-07-01T00:00:00.000Z", end: "2026-07-07T00:00:00.000Z" },
  target: "all",
  target_tile_ids: [],
  datasource_id: 5,
  ...over,
});

describe("buildKpiPayload", () => {
  it("builds a one-row empty-groupBy aggregate with a row cap", () => {
    expect(buildKpiPayload(kpiOf({ field: "amount", agg: "sum" }))).toEqual({
      workflow: [
        {
          type: "view",
          query: [
            {
              op: "aggregate",
              groupBy: [],
              measures: [{ field: "amount", agg: "sum", asFieldKey: "kpi_value" }],
            },
          ],
        },
      ],
      limit: 2,
    });
  });

  it("counts rows with the '*' field and non-null values with a field", () => {
    expect(measureOf(kpiOf({ agg: "count", field: null }))).toEqual({
      field: "*",
      agg: "count",
      asFieldKey: "kpi_value",
    });
    expect(measureOf(kpiOf({ agg: "count", field: "region" }))).toEqual({
      field: "region",
      agg: "count",
      asFieldKey: "kpi_value",
    });
  });

  it("sends distinct count as a SQL expression with escaped quotes", () => {
    expect(measureOf(kpiOf({ agg: "distinctCount", field: 'odd"name' }))).toEqual({
      agg: "expr",
      expression: 'COUNT(DISTINCT "odd""name")',
      asFieldKey: "kpi_value",
    });
  });

  it("returns null until a required field is picked", () => {
    expect(buildKpiPayload(kpiOf({ agg: "sum", field: null }))).toBeNull();
    expect(buildKpiPayload(kpiOf({ agg: "distinctCount", field: null }))).toBeNull();
    expect(isKpiComplete(null)).toBe(false);
    expect(isKpiComplete(kpiOf({ agg: "count", field: null }))).toBe(true);
  });
});

describe("readKpiValue", () => {
  it("reads the single aggregate row", () => {
    expect(readKpiValue([{ kpi_value: 12.5 }], "amount")).toEqual({ value: 12.5 });
    expect(readKpiValue([{ kpi_value: null }], "amount")).toEqual({ value: null });
    expect(readKpiValue([{ kpi_value: "7" }], "amount")).toEqual({ value: 7 });
  });

  it("treats raw passthrough rows or no rows as a missing field", () => {
    const err = { error: "Field 'amount' not found in the source" };
    expect(readKpiValue([{ a: 1 }, { a: 2 }], "amount")).toEqual(err);
    expect(readKpiValue([{ a: 1 }], "amount")).toEqual(err);
    expect(readKpiValue([], "amount")).toEqual(err);
  });

  it("rejects non-numeric values", () => {
    expect(readKpiValue([{ kpi_value: "2026-05-29" }], "d")).toEqual({
      error: "The aggregate did not return a number",
    });
  });
});

describe("fieldEligible", () => {
  it("numeric aggregates need a quantitative column", () => {
    for (const agg of ["sum", "mean", "median", "min", "max"] as const) {
      expect(fieldEligible(agg, "quantitative")).toBe(true);
      expect(fieldEligible(agg, "nominal")).toBe(false);
      expect(fieldEligible(agg, "temporal")).toBe(false);
      expect(fieldEligible(agg, undefined)).toBe(false);
    }
  });

  it("counts take any column", () => {
    expect(fieldEligible("count", "nominal")).toBe(true);
    expect(fieldEligible("distinctCount", "temporal")).toBe(true);
  });
});

describe("pickComparisonFilter", () => {
  const ds = (id: string) => (id === "k" ? "table:5" : null);

  it("picks the first targeting date range with both bounds", () => {
    const open = filterOf({ id: "open", state: { start: "2026-07-01T00:00:00.000Z", end: null } });
    const region = filterOf({ id: "r", kind: "categorical", state: { selected: ["EU"] } });
    const full = filterOf({ id: "full" });
    expect(pickComparisonFilter([region, open, full], "k", ds, null)?.id).toBe("full");
  });

  it("ignores date ranges that do not reach the tile", () => {
    const otherTable = filterOf({ datasource_id: 9 });
    const otherTile = filterOf({ target: "tiles", target_tile_ids: ["x"] });
    expect(pickComparisonFilter([otherTable, otherTile], "k", ds, null)).toBeNull();
  });
});

describe("findDateRangeFilter", () => {
  it("finds the targeting date range even without bounds", () => {
    const ds = (id: string) => (id === "k" ? "table:5" : null);
    const region = filterOf({ id: "r", kind: "categorical", state: { selected: ["EU"] } });
    const otherTable = filterOf({ id: "x", datasource_id: 9 });
    const empty = filterOf({ id: "empty", state: {} });
    expect(findDateRangeFilter([region, otherTable, empty], "k", ds, null)?.id).toBe("empty");
    expect(pickComparisonFilter([region, otherTable, empty], "k", ds, null)).toBeNull();
    expect(findDateRangeFilter([region, otherTable], "k", ds, null)).toBeNull();
  });
});

describe("shiftDateRangeBack", () => {
  it("moves the window back by its own inclusive length without overlap", () => {
    const start = new Date(2026, 6, 1);
    const end = new Date(2026, 6, 7);
    const shifted = shiftDateRangeBack({ start: start.toISOString(), end: end.toISOString() });
    expect(shifted).toEqual({
      start: new Date(2026, 5, 24).toISOString(),
      end: new Date(2026, 5, 30).toISOString(),
    });
  });

  it("returns null for an open range", () => {
    expect(shiftDateRangeBack({ start: "2026-07-01", end: null })).toBeNull();
  });

  it("shifts a named window by its own length, counted from today", () => {
    const now = new Date(2026, 10, 17, 15, 30);
    expect(shiftDateRangeBack({ preset: "last_7_days" }, now)).toEqual({
      start: new Date(2026, 10, 4).toISOString(),
      end: new Date(2026, 10, 10, 23, 59, 59, 999).toISOString(),
    });
    expect(previousPeriodLabel({ preset: "last_30_days" }, now)).toBe("vs previous 30 days");
  });

  it("labels the comparison window", () => {
    const d = (day: number) => new Date(2026, 6, day).toISOString();
    expect(previousPeriodLabel({ start: d(1), end: d(7) })).toBe("vs previous 7 days");
    expect(previousPeriodLabel({ start: d(3), end: d(3) })).toBe("vs previous day");
  });
});

describe("previousPeriodFilters", () => {
  it("shifts only the comparison filter", () => {
    const date = filterOf({ id: "d" });
    const region = filterOf({ id: "r", kind: "categorical", state: { selected: ["EU"] } });
    const out = previousPeriodFilters([date, region], "d");
    expect(out[1]).toBe(region);
    expect(out[0].state).toEqual({ ...date.state, ...shiftDateRangeBack(date.state) });
    expect(date.state.start).toBe("2026-07-01T00:00:00.000Z");
  });

  it("is the identity without a comparison filter", () => {
    const filters = [filterOf({})];
    expect(previousPeriodFilters(filters, null)).toBe(filters);
  });

  it("turns a named window into the fixed window before it", () => {
    const date = filterOf({ id: "d", state: { preset: "last_7_days", start: null, end: null } });
    const [shifted] = previousPeriodFilters([date], "d");
    expect(shifted.state).not.toHaveProperty("preset");
    expect(typeof shifted.state.start).toBe("string");
    expect(date.state.preset).toBe("last_7_days");
  });
});

describe("computeDelta", () => {
  it("computes absolute and relative change with tone", () => {
    expect(computeDelta(120, 100, true)).toEqual({
      abs: 20,
      pct: 0.2,
      direction: "up",
      tone: "good",
    });
    expect(computeDelta(80, 100, true)).toMatchObject({ direction: "down", tone: "bad" });
  });

  it("flips tone when lower is better", () => {
    expect(computeDelta(80, 100, false)).toMatchObject({ direction: "down", tone: "good" });
    expect(computeDelta(120, 100, false)).toMatchObject({ direction: "up", tone: "bad" });
  });

  it("is flat and neutral when unchanged", () => {
    expect(computeDelta(5, 5)).toEqual({ abs: 0, pct: 0, direction: "flat", tone: "neutral" });
  });

  it("has no percentage against a zero reference and no delta without both sides", () => {
    expect(computeDelta(5, 0)?.pct).toBeNull();
    expect(computeDelta(null, 5)).toBeNull();
    expect(computeDelta(5, null)).toBeNull();
  });
});

describe("formatting", () => {
  const compact = kpiOf({});
  const full = kpiOf({ compact: false });

  it("formats compact numbers by default", () => {
    expect(formatKpiValue(1_234_567, compact, "en-US")).toBe("1.2M");
    expect(formatKpiValue(416326.08, compact, "en-US")).toBe("416.3K");
    expect(formatKpiValue(12, compact, "en-US")).toBe("12");
  });

  it("formats full numbers with auto or fixed decimals", () => {
    expect(formatKpiValue(1234.5678, full, "en-US")).toBe("1,234.57");
    expect(formatKpiValue(1234, full, "en-US")).toBe("1,234");
    expect(formatKpiValue(1234.5, kpiOf({ compact: false, decimals: 0 }), "en-US")).toBe("1,235");
    expect(formatKpiValue(3, kpiOf({ compact: false, decimals: 2 }), "en-US")).toBe("3.00");
  });

  it("wraps prefix and suffix, keeping the sign in front", () => {
    const money = kpiOf({ prefix: "$", suffix: " USD" });
    expect(formatKpiValue(1500, money, "en-US")).toBe("$1.5K USD");
    expect(formatKpiValue(-1500, money, "en-US")).toBe("-$1.5K USD");
  });

  it("renders a dash for a missing value", () => {
    expect(formatKpiValue(null, compact, "en-US")).toBe("—");
  });

  it("signs deltas except zero", () => {
    expect(formatDeltaAbs(1200, kpiOf({ prefix: "$" }), "en-US")).toBe("+$1.2K");
    expect(formatDeltaAbs(-3, compact, "en-US")).toBe("-3");
    expect(formatDeltaAbs(0, compact, "en-US")).toBe("0");
    expect(formatDeltaPct(0.1234, "en-US")).toBe("+12.3%");
    expect(formatDeltaPct(-0.05, "en-US")).toBe("-5%");
    expect(formatDeltaPct(0, "en-US")).toBe("0%");
  });
});

describe("valueSizePx", () => {
  it("maps fixed sizes to px and leaves auto to the container fit", () => {
    expect(valueSizePx("auto")).toBeNull();
    expect(valueSizePx(undefined)).toBeNull();
    expect(valueSizePx("sm")).toBe(20);
    expect(valueSizePx("xl")).toBe(72);
  });
});

describe("autoKpiLabel", () => {
  it("describes the aggregate", () => {
    expect(autoKpiLabel({ agg: "sum", field: "amount" })).toBe("Sum of amount");
    expect(autoKpiLabel({ agg: "mean", field: "amount" })).toBe("Average amount");
    expect(autoKpiLabel({ agg: "count", field: null })).toBe("Row count");
    expect(autoKpiLabel({ agg: "count", field: "region" })).toBe("Count of region");
    expect(autoKpiLabel({ agg: "distinctCount", field: "region" })).toBe(
      "Distinct count of region",
    );
  });
});

describe("trend settings", () => {
  it("is off by default and needs a column to be on", () => {
    expect(DEFAULT_KPI.trend).toBeNull();
    expect(kpiTrendOf(kpiOf({ field: "amount" }))).toBeNull();
    expect(kpiTrendOf(kpiOf({ field: "amount", trend: { field: "", grain: "day" } }))).toBeNull();
    expect(kpiTrendOf(kpiOf({ trend: { field: "d", grain: "day" } }))).toEqual({
      field: "d",
      grain: "day",
    });
  });

  it("reads the span with the headline only for an auto grain", () => {
    expect(trendNeedsSpan(kpiOf({ trend: { field: "d", grain: "auto" } }))).toBe(true);
    expect(trendNeedsSpan(kpiOf({ trend: { field: "d", grain: "month" } }))).toBe(false);
    expect(trendNeedsSpan(kpiOf({}))).toBe(false);
  });
});

describe("buildKpiPayload with the trend span", () => {
  const kpi = kpiOf({ field: "amount", agg: "sum", trend: { field: "order_date", grain: "auto" } });

  it("adds the first and last date of the trend column after the value", () => {
    const measures = (buildKpiPayload(kpi, { withSpan: true }) as any).workflow[0].query[0]
      .measures;
    expect(measures).toEqual([
      { field: "amount", agg: "sum", asFieldKey: "kpi_value" },
      { field: "order_date", agg: "min", asFieldKey: "kpi_span_start" },
      { field: "order_date", agg: "max", asFieldKey: "kpi_span_end" },
    ]);
  });

  it("leaves the headline as it was without the option or a trend", () => {
    expect(buildKpiPayload(kpi)).toEqual(buildKpiPayload(kpiOf({ field: "amount", agg: "sum" })));
    expect(buildKpiPayload(kpiOf({ field: "amount" }), { withSpan: true })).toEqual(
      buildKpiPayload(kpiOf({ field: "amount" })),
    );
  });
});

describe("buildKpiTrendPayload", () => {
  it("truncates the date on the worker, aggregates per bucket and asks for the newest first", () => {
    const kpi = kpiOf({
      field: "amount",
      agg: "mean",
      trend: { field: "order_date", grain: "auto" },
    });
    expect(buildKpiTrendPayload(kpi, "week")).toEqual({
      workflow: [
        {
          type: "transform",
          transform: [
            {
              key: "kpi_bucket",
              expression: {
                op: "dateTimeDrill",
                as: "kpi_bucket",
                params: [
                  { type: "field", value: "order_date" },
                  { type: "value", value: "week" },
                ],
              },
            },
          ],
        },
        {
          type: "view",
          query: [
            {
              op: "aggregate",
              groupBy: ["kpi_bucket"],
              measures: [{ field: "amount", agg: "mean", asFieldKey: "kpi_value" }],
            },
          ],
        },
        { type: "sort", by: ["kpi_bucket"], sort: "descending" },
      ],
      limit: KPI_TREND_MAX_BUCKETS + 1,
    });
  });

  it("uses the KPI's own measure, a row count and a null-skipping distinct count included", () => {
    const measure = (kpi: DashboardKpi) =>
      (buildKpiTrendPayload(kpi, "month") as any).workflow[1].query[0].measures[0];
    const trend = { field: "d", grain: "month" as const };
    expect(measure(kpiOf({ agg: "count", field: null, trend }))).toEqual({
      field: "*",
      agg: "count",
      asFieldKey: "kpi_value",
    });
    expect(measure(kpiOf({ agg: "distinctCount", field: "region", trend }))).toEqual({
      agg: "expr",
      expression: 'COUNT(DISTINCT "region")',
      asFieldKey: "kpi_value",
    });
  });

  it("is null without a trend or a complete KPI", () => {
    expect(buildKpiTrendPayload(kpiOf({ field: "amount" }), "day")).toBeNull();
    expect(
      buildKpiTrendPayload(kpiOf({ field: null, trend: { field: "d", grain: "day" } }), "day"),
    ).toBeNull();
  });
});

describe("parseBucket", () => {
  it("reads a date and a datetime as the local time they name", () => {
    expect(parseBucket("2024-03-01")).toBe(new Date(2024, 2, 1).getTime());
    expect(parseBucket("2024-03-01 13:45:10.000000")).toBe(
      new Date(2024, 2, 1, 13, 45, 10).getTime(),
    );
    expect(parseBucket("2024-03-01T00:00:00+0000")).toBe(new Date(2024, 2, 1).getTime());
  });

  it("is null for anything that is no date", () => {
    expect(parseBucket(null)).toBeNull();
    expect(parseBucket("March")).toBeNull();
    expect(parseBucket(undefined)).toBeNull();
  });
});

describe("readKpiSpan", () => {
  it("reads the span off the one headline row", () => {
    expect(
      readKpiSpan([{ kpi_value: 1, kpi_span_start: "2024-01-03", kpi_span_end: "2024-05-15" }]),
    ).toEqual({ start: new Date(2024, 0, 3).getTime(), end: new Date(2024, 4, 15).getTime() });
  });

  it("is null when no dates are in view or the column is gone", () => {
    expect(readKpiSpan([{ kpi_value: 0, kpi_span_start: null, kpi_span_end: null }])).toBeNull();
    expect(readKpiSpan([{ kpi_value: 0 }])).toBeNull();
    expect(readKpiSpan([])).toBeNull();
  });
});

describe("autoTrendGrain", () => {
  const span = (days: number) => ({ start: 0, end: days * 86_400_000 });

  it("picks the finest grain whose buckets fit in the cap", () => {
    expect(autoTrendGrain(span(0))).toBe("day");
    expect(autoTrendGrain(span(59))).toBe("day");
    expect(autoTrendGrain(span(60))).toBe("week");
    expect(autoTrendGrain(span(365))).toBe("week");
    expect(autoTrendGrain(span(2 * 365))).toBe("month");
    expect(autoTrendGrain(span(10 * 365))).toBe("quarter");
    expect(autoTrendGrain(span(30 * 365))).toBe("year");
  });

  it("keeps a fixed grain and falls back to months without a span", () => {
    expect(autoTrendGrain(null)).toBe("month");
    expect(resolveTrendGrain("quarter", span(3))).toBe("quarter");
    expect(resolveTrendGrain("auto", span(3))).toBe("day");
  });
});

describe("readKpiTrend", () => {
  it("drops the null-date bucket and empty values and orders the points oldest first", () => {
    const points = readKpiTrend([
      { kpi_bucket: null, kpi_value: 5 },
      { kpi_bucket: "2024-03-01", kpi_value: 30 },
      { kpi_bucket: "2024-02-01", kpi_value: null },
      { kpi_bucket: "2024-01-01", kpi_value: "10" },
    ]);
    expect(points).toEqual([
      { t: new Date(2024, 0, 1).getTime(), value: 10 },
      { t: new Date(2024, 2, 1).getTime(), value: 30 },
    ]);
  });

  it("keeps only the most recent buckets", () => {
    const rows = Array.from({ length: KPI_TREND_MAX_BUCKETS + 1 }, (_, i) => ({
      kpi_bucket: `2024-01-${String((i % 28) + 1).padStart(2, "0")} ${String(Math.floor(i / 28)).padStart(2, "0")}:00:00`,
      kpi_value: i,
    }));
    const points = readKpiTrend(rows)!;
    expect(points).toHaveLength(KPI_TREND_MAX_BUCKETS);
    expect(points.map((p) => p.value)).not.toContain(0);
  });

  it("is null when the rows are not buckets (the date column is missing)", () => {
    expect(readKpiTrend([{ order_date: "2024-01-01", amount: 1 }])).toBeNull();
    expect(readKpiTrend(null)).toBeNull();
    expect(readKpiTrend([])).toEqual([]);
  });
});

describe("formatTrendBucket and trendDescription", () => {
  const t = (y: number, m: number, d = 1) => new Date(y, m, d).getTime();

  it("names a bucket by its grain", () => {
    expect(formatTrendBucket(t(2024, 2, 4), "day", "en-GB")).toBe("4 Mar 2024");
    expect(formatTrendBucket(t(2024, 2, 4), "week", "en-GB")).toBe("Week of 4 Mar 2024");
    expect(formatTrendBucket(t(2024, 2), "month", "en-GB")).toBe("Mar 2024");
    expect(formatTrendBucket(t(2024, 7), "quarter")).toBe("Q3 2024");
    expect(formatTrendBucket(t(2024, 7), "year")).toBe("2024");
  });

  it("says what the line shows, its span and its direction", () => {
    const points = [
      { t: t(2024, 0), value: 100 },
      { t: t(2024, 4), value: 20 },
    ];
    expect(trendDescription(points, "month", "Total amount", "en-GB")).toBe(
      "Total amount by month, Jan 2024 to May 2024: down 80%",
    );
    expect(
      trendDescription(
        [
          { t: t(2024, 0), value: 0 },
          { t: t(2024, 1), value: 5 },
        ],
        "month",
        "Orders",
        "en-GB",
      ),
    ).toBe("Orders by month, Jan 2024 to Feb 2024: up");
    expect(
      trendDescription(
        [
          { t: t(2024, 0), value: 5 },
          { t: t(2025, 0), value: 5 },
        ],
        "year",
        "Orders",
      ),
    ).toBe("Orders by year, 2024 to 2025: flat");
  });
});
