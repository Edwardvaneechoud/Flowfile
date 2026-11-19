import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from "vitest";
import type {
  IChart,
  IFilterField,
  IMutField,
  IViewField,
} from "@kanaries/graphic-walker/interfaces";
import { normalize } from "@kanaries/graphic-walker/models/normalize";
import {
  expandTerse,
  parseShorthand,
  projectTerse,
  terseComputedFid,
} from "@kanaries/graphic-walker/models/terse";
import { chartToWorkflow } from "@kanaries/graphic-walker/utils/workflow";
import { chartToWorksheet } from "@kanaries/graphic-walker/renderer/native/adapter";
import {
  fillChart,
  fromSnapshot,
  performers,
  undo,
} from "@kanaries/graphic-walker/models/visSpecHistory";
import {
  chartFromRequest,
  describeChart,
  drawnMark,
  type ChartContext,
  type ChartRequest,
  type GraphicWalkerFunctions,
} from "./chartRequest";

const gw: GraphicWalkerFunctions = {
  normalize,
  expandTerse,
  projectTerse,
  parseShorthand,
  terseComputedFid,
};

// Roles as polars-gw classifies them: a small-integer column is a dimension.
const FIELDS: IMutField[] = [
  { fid: "region", name: "region", semanticType: "nominal", analyticType: "dimension" },
  { fid: "category", name: "category", semanticType: "nominal", analyticType: "dimension" },
  { fid: "quantity", name: "quantity", semanticType: "quantitative", analyticType: "dimension" },
  { fid: "revenue", name: "revenue", semanticType: "quantitative", analyticType: "measure" },
  { fid: "price", name: "price", semanticType: "quantitative", analyticType: "measure" },
  { fid: "order_date", name: "order_date", semanticType: "temporal", analyticType: "dimension" },
];

const ctx = (current: IChart | null = null): ChartContext => ({ fields: FIELDS, current, gw });

let warn: MockInstance;
beforeEach(() => {
  warn = vi.spyOn(console, "warn");
});
afterEach(() => {
  expect(warn).not.toHaveBeenCalled();
  warn.mockRestore();
});

function build(request: ChartRequest, current: IChart | null = null): IChart {
  const result = chartFromRequest(request, ctx(current));
  if (!result.ok) throw new Error(`expected a chart, got: ${result.error}`);
  return result.chart;
}

const errorOf = (request: ChartRequest): string => {
  const result = chartFromRequest(request, ctx());
  if (result.ok) throw new Error("expected a failure");
  return result.error;
};

/** A chart as the store holds it: `applyChart` strips normalize's version stamp. */
const unstamped = (chart: IChart): IChart => {
  const copy = { ...chart };
  delete copy.$schema;
  return copy;
};

const fids = (fields: IViewField[]) => fields.map((f) => f.fid);
const query = (chart: IChart) => chartToWorkflow(chart);
const viewStep = (chart: IChart) => query(chart).workflow.find((s) => s.type === "view");
const MONTH_FID = terseComputedFid("month(order_date)");
const BIN_FID = terseComputedFid("bin10(price)");
const sumOf = (fid: string) => ({ field: fid, agg: "sum", asFieldKey: `${fid}_sum` });
const aggregate = (groupBy: string[], measures: unknown[]) => ({
  type: "view",
  query: [{ op: "aggregate", groupBy, measures }],
});

describe("chartFromRequest: marks", () => {
  it("bar: a sum on a small-integer column the fields call a dimension is a sum measure", () => {
    const chart = build({
      mark: "bar",
      x: { field: "region" },
      y: { field: "quantity", aggregate: "sum" },
    });
    expect(chart.config.geoms).toEqual(["bar"]);
    expect(fids(chart.encodings.columns)).toEqual(["region"]);
    expect(chart.encodings.rows[0]).toMatchObject({
      fid: "quantity",
      analyticType: "measure",
      aggName: "sum",
    });
    expect(query(chart).workflow).toEqual([aggregate(["region"], [sumOf("quantity")])]);
  });

  it("bar: horizontal and stacked by colour", () => {
    const chart = build({
      mark: "bar",
      y: { field: "region" },
      x: { field: "revenue", aggregate: "sum" },
      color: { field: "category" },
      stack: "normalize",
    });
    expect(fids(chart.encodings.rows)).toEqual(["region"]);
    expect(fids(chart.encodings.columns)).toEqual(["revenue"]);
    expect(fids(chart.encodings.color)).toEqual(["category"]);
    expect(chart.layout.stack).toBe("normalize");
    expect(viewStep(chart)).toEqual(aggregate(["region", "category"], [sumOf("revenue")]));
  });

  it("line: a month grain is a dateTimeDrill field grouped by", () => {
    const chart = build({
      mark: "line",
      x: { field: "order_date", timeUnit: "month" },
      y: { field: "revenue", aggregate: "mean" },
    });
    expect(chart.encodings.columns[0]).toMatchObject({ fid: MONTH_FID, name: "month(order_date)" });
    expect(chart.encodings.dimensions.some((f) => f.fid === MONTH_FID)).toBe(true);
    const { workflow } = query(chart);
    expect(workflow[0]).toMatchObject({
      type: "transform",
      transform: [
        {
          key: MONTH_FID,
          expression: {
            op: "dateTimeDrill",
            params: expect.arrayContaining([
              { type: "field", value: "order_date" },
              { type: "value", value: "month" },
            ]),
          },
        },
      ],
    });
    expect(workflow[1]).toEqual(
      aggregate([MONTH_FID], [{ field: "revenue", agg: "mean", asFieldKey: "revenue_mean" }]),
    );
  });

  it("area: stacked by colour", () => {
    const chart = build({
      mark: "area",
      x: { field: "order_date", timeUnit: "year" },
      y: { field: "revenue", aggregate: "sum" },
      color: { field: "region" },
      stack: "none",
    });
    expect(chart.config.geoms).toEqual(["area"]);
    expect(chart.layout.stack).toBe("none");
    expect(viewStep(chart)).toEqual(
      aggregate([terseComputedFid("year(order_date)"), "region"], [sumOf("revenue")]),
    );
  });

  it("point: raw rows, numbers on x, y and size are measures", () => {
    const chart = build({
      mark: "point",
      x: { field: "quantity" },
      y: { field: "revenue" },
      size: { field: "price" },
      color: { field: "region" },
      aggregate: false,
    });
    expect(chart.config.defaultAggregated).toBe(false);
    expect(chart.encodings.columns[0].analyticType).toBe("measure");
    expect(chart.encodings.size[0].analyticType).toBe("measure");
    expect(chart.encodings.color[0].analyticType).toBe("dimension");
    expect(query(chart).workflow).toEqual([
      { type: "view", query: [{ op: "raw", fields: ["region", "quantity", "revenue", "price"] }] },
    ]);
  });

  it("point: raw mode strips reference aggregates", () => {
    const chart = build({
      mark: "point",
      x: { field: "quantity", aggregate: "sum" },
      y: { field: "revenue", aggregate: "max" },
      aggregate: false,
    });
    expect(viewStep(chart)).toEqual({
      type: "view",
      query: [{ op: "raw", fields: ["quantity", "revenue"] }],
    });
  });

  it("arc: theta and colour, no axes", () => {
    const chart = build({
      mark: "arc",
      theta: { field: "revenue", aggregate: "sum" },
      color: { field: "region" },
    });
    expect(fids(chart.encodings.theta)).toEqual(["revenue"]);
    expect(chart.encodings.rows).toEqual([]);
    expect(chart.encodings.columns).toEqual([]);
    expect(viewStep(chart)).toEqual(aggregate(["region"], [sumOf("revenue")]));
  });

  it("rect: two categories and an aggregated colour", () => {
    const chart = build({
      mark: "rect",
      x: { field: "region" },
      y: { field: "quantity" },
      color: { field: "revenue", aggregate: "median" },
    });
    expect(viewStep(chart)).toEqual(
      aggregate(
        ["region", "quantity"],
        [{ field: "revenue", agg: "median", asFieldKey: "revenue_median" }],
      ),
    );
  });

  it("boxplot: a category against a plain number", () => {
    const chart = build({
      mark: "boxplot",
      x: { field: "region" },
      y: { field: "revenue" },
      aggregate: false,
    });
    expect(chart.config.geoms).toEqual(["boxplot"]);
    expect(viewStep(chart)).toEqual({
      type: "view",
      query: [{ op: "raw", fields: ["region", "revenue"] }],
    });
  });

  it("table: dimensions go to y and measures to x, wherever the request put them", () => {
    const chart = build({
      mark: "table",
      x: [{ field: "region" }, { field: "revenue", aggregate: "sum" }],
      y: [{ field: "category" }, { field: "*", aggregate: "count" }],
    });
    expect(fids(chart.encodings.rows)).toEqual(["category", "region"]);
    expect(fids(chart.encodings.columns)).toEqual(["gw_count_fid", "revenue"]);
    expect(viewStep(chart)).toEqual(
      aggregate(["category", "region"], [sumOf("gw_count_fid"), sumOf("revenue")]),
    );
  });
});

describe("chartFromRequest: conversion table", () => {
  it("a plain reference is grouped by, also on a column the fields call a measure", () => {
    const chart = build({
      mark: "bar",
      x: { field: "price" },
      y: { field: "*", aggregate: "count" },
    });
    expect(chart.encodings.columns[0].analyticType).toBe("dimension");
    expect(viewStep(chart)).toEqual(aggregate(["price"], [sumOf("gw_count_fid")]));
  });

  it("the row count is the count field, every aggregate is a measure", () => {
    const chart = build({
      mark: "bar",
      x: { field: "region" },
      y: [
        { field: "*", aggregate: "count" },
        { field: "category", aggregate: "distinctCount" },
      ],
    });
    expect(fids(chart.encodings.rows)).toEqual(["gw_count_fid", "category"]);
    expect(viewStep(chart)).toEqual(
      aggregate(
        ["region"],
        [
          sumOf("gw_count_fid"),
          { field: "category", agg: "distinctCount", asFieldKey: "category_distinctCount" },
        ],
      ),
    );
  });

  it("a top 5 is a descending sort by the measure with limit 5", () => {
    const chart = build({
      mark: "bar",
      x: { field: "region" },
      y: { field: "revenue", aggregate: "sum" },
      sort: "descending",
      limit: 5,
    });
    expect(chart.encodings.columns[0].sort).toBe("descending");
    expect(chart.encodings.rows[0].sort).toBeUndefined();
    expect(chart.config.limit).toBe(5);
    expect(query(chart)).toEqual({
      workflow: [
        aggregate(["region"], [sumOf("revenue")]),
        { type: "sort", by: ["revenue_sum"], sort: "descending" },
      ],
      limit: 5,
    });
  });

  it("the sort goes on the category axis of a horizontal bar", () => {
    const chart = build({
      mark: "bar",
      y: [{ field: "region" }, { field: "category" }],
      x: { field: "revenue", aggregate: "sum" },
      sort: "ascending",
      limit: 3,
    });
    expect(chart.encodings.rows.map((f) => f.sort)).toEqual([undefined, "ascending"]);
    expect(query(chart).workflow.at(-1)).toEqual({
      type: "sort",
      by: ["revenue_sum"],
      sort: "ascending",
    });
  });

  it("a date range is UTC epoch milliseconds, through the end of the last day", () => {
    const chart = build({
      mark: "bar",
      x: { field: "region" },
      y: { field: "revenue", aggregate: "sum" },
      filters: [{ field: "order_date", dateRange: ["2024-01-01", "2024-12-31"] }],
    });
    const end = Date.UTC(2024, 11, 31) + 86_399_999;
    expect(new Date(end).toISOString()).toBe("2024-12-31T23:59:59.999Z");
    expect(query(chart).workflow[0]).toEqual({
      type: "filter",
      filters: [
        { fid: "order_date", rule: { type: "temporal range", value: [Date.UTC(2024, 0, 1), end] } },
      ],
    });
  });

  it("an open date range keeps its null bound", () => {
    const chart = build({
      mark: "bar",
      x: { field: "region" },
      y: { field: "revenue", aggregate: "sum" },
      filters: [{ field: "order_date", dateRange: [null, "2023-06-30"] }],
    });
    expect(chart.encodings.filters[0].rule).toEqual({
      type: "temporal range",
      value: [null, Date.UTC(2023, 5, 30) + 86_399_999],
    });
  });

  it("oneOf, notIn and range are row filters on the column", () => {
    const chart = build({
      mark: "bar",
      x: { field: "region" },
      y: { field: "revenue", aggregate: "sum" },
      filters: [
        { field: "region", oneOf: ["West", "East"] },
        { field: "category", notIn: ["Toys"] },
        { field: "price", range: [10, null] },
      ],
    });
    expect(query(chart).workflow[0]).toEqual({
      type: "filter",
      filters: [
        { fid: "region", rule: { type: "one of", value: ["West", "East"] } },
        { fid: "category", rule: { type: "not in", value: ["Toys"] } },
        { fid: "price", rule: { type: "range", value: [10, null] } },
      ],
    });
  });

  it("a histogram is a bin plus a count", () => {
    const chart = build({
      mark: "bar",
      x: { field: "price", bin: 10 },
      y: { field: "*", aggregate: "count" },
    });
    expect(chart.encodings.columns[0]).toMatchObject({
      fid: BIN_FID,
      name: "bin10(price)",
      analyticType: "dimension",
    });
    const { workflow } = query(chart);
    expect(workflow[0]).toMatchObject({
      type: "transform",
      transform: expect.arrayContaining([
        {
          key: BIN_FID,
          expression: {
            op: "bin",
            as: BIN_FID,
            params: [{ type: "field", value: "price" }],
            num: 10,
          },
        },
      ]),
    });
    expect(workflow[1]).toEqual(aggregate([BIN_FID], [sumOf("gw_count_fid")]));
  });

  it("a bin and a sum of the same column live side by side", () => {
    const chart = build({
      mark: "bar",
      x: { field: "price", bin: 5 },
      y: { field: "price", aggregate: "sum" },
    });
    expect(viewStep(chart)).toEqual(aggregate([terseComputedFid("bin5(price)")], [sumOf("price")]));
  });

  it("stack is set on bars and areas only, and never inherited", () => {
    const current = build({
      mark: "bar",
      x: { field: "region" },
      y: { field: "revenue", aggregate: "sum" },
      stack: "none",
    });
    expect(current.layout.stack).toBe("none");
    const next = build(
      { mark: "bar", x: { field: "region" }, y: { field: "revenue", aggregate: "sum" } },
      current,
    );
    expect(next.layout.stack).toBe("stack");
    const line = build({
      mark: "line",
      x: { field: "region" },
      y: { field: "revenue", aggregate: "sum" },
      stack: "none",
    });
    expect(line.layout.stack).toBe("stack");
  });

  it("a limit and an aggregate default from the request, not from the current chart", () => {
    const current = build({
      mark: "bar",
      x: { field: "region" },
      y: { field: "revenue", aggregate: "sum" },
      sort: "descending",
      limit: 5,
    });
    const next = build(
      { mark: "line", x: { field: "region" }, y: { field: "revenue", aggregate: "sum" } },
      current,
    );
    expect(next.config.limit).toBe(-1);
    expect(next.config.defaultAggregated).toBe(true);
    expect(query(next).limit).toBeUndefined();
  });
});

describe("chartFromRequest: refusals", () => {
  it.each<[string, ChartRequest, RegExp]>([
    [
      "unknown column",
      { mark: "bar", x: { field: "regoin" }, y: { field: "revenue", aggregate: "sum" } },
      /"regoin" is not a column/,
    ],
    ["unknown mark", { mark: "tick" as never, x: { field: "region" } }, /not a chart type/],
    [
      "* without count",
      { mark: "bar", x: { field: "region" }, y: { field: "*", aggregate: "sum" } },
      /row count/,
    ],
    [
      "* in raw mode",
      {
        mark: "point",
        x: { field: "price" },
        y: { field: "*", aggregate: "count" },
        aggregate: false,
      },
      /aggregated chart/,
    ],
    [
      "grouped and aggregated",
      { mark: "bar", x: { field: "quantity" }, y: { field: "quantity", aggregate: "sum" } },
      /both grouped and aggregated/,
    ],
    [
      "timeUnit on text",
      {
        mark: "line",
        x: { field: "region", timeUnit: "month" },
        y: { field: "revenue", aggregate: "sum" },
      },
      /could not be built.*non-temporal/,
    ],
    [
      "timeUnit outside the six",
      {
        mark: "line",
        x: { field: "order_date", timeUnit: "iso_week" as never },
        y: { field: "revenue", aggregate: "sum" },
      },
      /not a date grain/,
    ],
    [
      "bin out of range",
      { mark: "bar", x: { field: "price", bin: 80 }, y: { field: "*", aggregate: "count" } },
      /between 2 and 50/,
    ],
    [
      "bin with aggregate",
      { mark: "bar", x: { field: "price", bin: 10, aggregate: "sum" } },
      /one of aggregate, timeUnit or bin/,
    ],
    [
      "table of raw rows",
      { mark: "table", y: { field: "region" }, aggregate: false },
      /A table shows aggregated values/,
    ],
    [
      "sort with no category axis",
      {
        mark: "arc",
        theta: { field: "revenue", aggregate: "sum" },
        color: { field: "region" },
        sort: "descending",
      },
      /sort needs a category axis/,
    ],
    [
      "empty oneOf",
      {
        mark: "bar",
        x: { field: "region" },
        y: { field: "revenue", aggregate: "sum" },
        filters: [{ field: "region", oneOf: [] }],
      },
      /at least one value/,
    ],
    [
      "filter with two rules",
      {
        mark: "bar",
        x: { field: "region" },
        filters: [{ field: "price", range: [1, 2], oneOf: [1] } as never],
      },
      /exactly one of/,
    ],
    [
      "bad date",
      {
        mark: "bar",
        x: { field: "region" },
        filters: [{ field: "order_date", dateRange: ["2024-02-30", null] }],
      },
      /"YYYY-MM-DD"/,
    ],
    [
      "empty range",
      { mark: "bar", x: { field: "region" }, filters: [{ field: "price", range: [null, null] }] },
      /at least one bound/,
    ],
    [
      "bad limit",
      {
        mark: "bar",
        x: { field: "region" },
        y: { field: "revenue", aggregate: "sum" },
        sort: "descending",
        limit: 0,
      },
      /limit needs/,
    ],
  ])("%s", (_name, request, message) => {
    expect(errorOf(request)).toMatch(message);
  });
});

describe("chartFromRequest: what carries over from the current chart", () => {
  const margin: IViewField = {
    fid: "user_margin",
    name: "margin",
    analyticType: "measure",
    semanticType: "quantitative",
    aggName: "sum",
    computed: true,
    expression: {
      op: "expr",
      as: "user_margin",
      params: [{ type: "sql", value: "revenue - price" }],
    },
  };

  const currentChart = (): IChart => {
    const base = unstamped(
      build({ mark: "bar", x: { field: "region" }, y: { field: "*", aggregate: "count" } }),
    );
    const named = (f: IViewField) => (f.fid === "gw_count_fid" ? { ...f, name: "Row count" } : f);
    return {
      ...base,
      name: "Revenue by region",
      encodings: {
        ...base.encodings,
        rows: base.encodings.rows.map(named),
        measures: [
          ...base.encodings.measures.filter((f) => f.fid !== "gw_count_fid"),
          margin,
          named(base.encodings.measures.find((f) => f.fid === "gw_count_fid")!),
        ],
      },
      config: { ...base.config, timezoneDisplayOffset: 60 },
      layout: {
        ...base.layout,
        background: "#fafafa",
        size: { mode: "fixed", width: 800, height: 420 },
        format: { numberFormat: ",.0f" },
      },
    };
  };

  it("keeps name, layout, config, the synthetic names and the user's computed field", () => {
    const current = currentChart();
    const chart = build(
      {
        mark: "line",
        x: { field: "order_date", timeUnit: "month" },
        y: { field: "*", aggregate: "count" },
      },
      current,
    );
    expect(chart.visId).toBe(current.visId);
    expect(chart.name).toBe("Revenue by region");
    expect(chart.layout).toMatchObject({
      background: "#fafafa",
      size: { mode: "fixed", width: 800, height: 420 },
      format: { numberFormat: ",.0f" },
      renderer: "native",
    });
    expect(chart.config).toMatchObject({
      timezoneDisplayOffset: 60,
      geoms: ["line"],
      coordSystem: "generic",
    });
    expect(chart.encodings.rows[0]).toMatchObject({ fid: "gw_count_fid", name: "Row count" });
    const measures = fids(chart.encodings.measures);
    expect(measures).toContain("user_margin");
    expect(measures.slice(-2)).toEqual(["gw_count_fid", "gw_mea_val_fid"]);
    expect(chart.encodings.measures.find((f) => f.fid === "gw_count_fid")?.name).toBe("Row count");
  });

  it("is a complete chart that Graphic Walker's own replaceChart applies as one undoable step", () => {
    const current = currentChart();
    const chart = build(
      { mark: "bar", x: { field: "category" }, y: { field: "revenue", aggregate: "sum" } },
      current,
    );
    const payload = unstamped(chart);
    expect(fillChart(payload)).toEqual(payload);

    const history = performers.replaceChart(fromSnapshot(current), payload);
    expect(history.now).toEqual(payload);
    expect(query(history.now).workflow).toEqual([aggregate(["category"], [sumOf("revenue")])]);
    expect(undo(history).now).toEqual(current);
  });

  it("keeps a renamed field's name, and the round trip finds the column by id", () => {
    const base = build({
      mark: "bar",
      x: { field: "region" },
      y: { field: "revenue", aggregate: "sum" },
    });
    const rename = (f: IViewField) => (f.fid === "revenue" ? { ...f, name: "Sales" } : f);
    const current: IChart = {
      ...base,
      encodings: {
        ...base.encodings,
        rows: base.encodings.rows.map(rename),
        measures: base.encodings.measures.map(rename),
      },
    };
    const described = describeChart(current, ctx());
    expect(described).toEqual({
      ok: true,
      request: { mark: "bar", x: { field: "region" }, y: { field: "revenue", aggregate: "sum" } },
    });
    const next = build(
      { mark: "line", x: { field: "region" }, y: { field: "revenue", aggregate: "mean" } },
      current,
    );
    expect(next.encodings.rows[0]).toMatchObject({ fid: "revenue", name: "Sales" });
    expect(next.encodings.measures.find((f) => f.fid === "revenue")?.name).toBe("Sales");
  });

  it("keeps the role the user gave a field the request does not use", () => {
    const base = build({
      mark: "bar",
      x: { field: "region" },
      y: { field: "revenue", aggregate: "sum" },
    });
    const price = {
      ...base.encodings.measures.find((f) => f.fid === "price")!,
      analyticType: "dimension" as const,
    };
    const current: IChart = {
      ...base,
      encodings: {
        ...base.encodings,
        dimensions: [...base.encodings.dimensions, price],
        measures: base.encodings.measures.filter((f) => f.fid !== "price"),
      },
    };
    const next = build(
      { mark: "bar", x: { field: "category" }, y: { field: "revenue", aggregate: "sum" } },
      current,
    );
    expect(fids(next.encodings.dimensions)).toContain("price");
    expect(fids(next.encodings.measures)).not.toContain("price");
  });

  it("without a current chart the chart is a fresh one", () => {
    const chart = build({
      mark: "bar",
      x: { field: "region" },
      y: { field: "revenue", aggregate: "sum" },
    });
    expect(chart.name).toBe("Chart");
    expect(chart.layout.size).toEqual({ mode: "auto", width: 320, height: 200 });
  });
});

describe("describeChart: round trip", () => {
  const supported: [string, ChartRequest][] = [
    ["bar", { mark: "bar", x: { field: "region" }, y: { field: "quantity", aggregate: "sum" } }],
    [
      "stacked bar",
      {
        mark: "bar",
        x: { field: "region" },
        y: { field: "revenue", aggregate: "sum" },
        color: { field: "category" },
        stack: "normalize",
      },
    ],
    [
      "horizontal top 5",
      {
        mark: "bar",
        y: { field: "region" },
        x: { field: "revenue", aggregate: "sum" },
        sort: "descending",
        limit: 5,
      },
    ],
    [
      "grouped bar",
      {
        mark: "bar",
        x: [{ field: "region" }, { field: "category" }],
        y: { field: "revenue", aggregate: "min" },
      },
    ],
    [
      "line",
      {
        mark: "line",
        x: { field: "order_date", timeUnit: "month" },
        y: { field: "revenue", aggregate: "mean" },
        color: { field: "region" },
      },
    ],
    [
      "area",
      {
        mark: "area",
        x: { field: "order_date", timeUnit: "quarter" },
        y: { field: "revenue", aggregate: "sum" },
        stack: "none",
      },
    ],
    [
      "scatter",
      {
        mark: "point",
        x: { field: "quantity" },
        y: { field: "revenue" },
        size: { field: "price" },
        aggregate: false,
      },
    ],
    [
      "pie",
      { mark: "arc", theta: { field: "revenue", aggregate: "sum" }, color: { field: "region" } },
    ],
    [
      "heatmap",
      {
        mark: "rect",
        x: { field: "region" },
        y: { field: "category" },
        color: { field: "*", aggregate: "count" },
      },
    ],
    [
      "box plot",
      { mark: "boxplot", x: { field: "region" }, y: { field: "revenue" }, aggregate: false },
    ],
    [
      "table",
      {
        mark: "table",
        x: [
          { field: "revenue", aggregate: "sum" },
          { field: "*", aggregate: "count" },
        ],
        y: [{ field: "region" }, { field: "category" }],
      },
    ],
    [
      "histogram",
      { mark: "bar", x: { field: "price", bin: 20 }, y: { field: "*", aggregate: "count" } },
    ],
    [
      "hourly",
      {
        mark: "line",
        x: { field: "order_date", timeUnit: "hour" },
        y: { field: "category", aggregate: "distinctCount" },
      },
    ],
    [
      "filters",
      {
        mark: "bar",
        x: { field: "region" },
        y: { field: "revenue", aggregate: "max" },
        filters: [
          { field: "region", oneOf: ["West"] },
          { field: "category", notIn: ["Toys", "Games"] },
          { field: "price", range: [null, 99.5] },
          { field: "order_date", dateRange: ["2025-01-01", "2025-12-31"] },
        ],
      },
    ],
  ];

  it.each(supported)("%s", (_name, request) => {
    const chart = build(request);
    expect(describeChart(chart, ctx())).toEqual({ ok: true, request });
  });

  it("describes the chart again identically after a rebuild from its own description", () => {
    for (const [, request] of supported) {
      const first = build(request);
      const described = describeChart(first, ctx());
      if (!described.ok) throw new Error(described.unsupported.join(", "));
      const second = build(described.request, first);
      expect(query(second)).toEqual(query(first));
    }
  });

  it("leaves name, layout and config out of the request", () => {
    const chart = { ...build(supported[0][1]), name: "My chart" };
    const described = describeChart(chart, ctx());
    expect(described.ok && Object.keys(described.request)).toEqual(["mark", "x", "y"]);
  });
});

describe("describeChart: parts a request cannot express", () => {
  const bar = () =>
    build({ mark: "bar", x: { field: "region" }, y: { field: "revenue", aggregate: "sum" } });
  const field = (chart: IChart, fid: string) =>
    [...chart.encodings.dimensions, ...chart.encodings.measures].find((f) => f.fid === fid)!;
  const withFilter = (filter: Partial<IFilterField>): IChart => {
    const chart = bar();
    const base = field(chart, filter.fid ?? "revenue");
    return {
      ...chart,
      encodings: {
        ...chart.encodings,
        filters: [{ ...base, rule: null, ...filter } as IFilterField],
      },
    };
  };
  const on = (channel: keyof IChart["encodings"], fields: IViewField[], chart = bar()): IChart => ({
    ...chart,
    encodings: { ...chart.encodings, [channel]: fields },
  });
  const computedField = (
    fid: string,
    expression: IViewField["expression"],
    extra: Partial<IViewField> = {},
  ): IViewField => ({
    fid,
    name: fid,
    analyticType: "measure",
    semanticType: "quantitative",
    aggName: "sum",
    computed: true,
    expression,
    ...extra,
  });
  const withPoolField = (chart: IChart, extra: IViewField): IChart => ({
    ...chart,
    encodings: {
      ...chart.encodings,
      [extra.analyticType === "measure" ? "measures" : "dimensions"]: [
        ...chart.encodings[extra.analyticType === "measure" ? "measures" : "dimensions"],
        extra,
      ],
    },
  });
  const unsupported = (chart: IChart) => {
    const result = describeChart(chart, ctx());
    if (result.ok) throw new Error("expected the chart to be refused");
    return result.unsupported;
  };

  it("a filter on an aggregated value (projectTerse keeps it as a row filter)", () => {
    expect(
      unsupported(
        withFilter({ rule: { type: "range", value: [0, 100] }, enableAgg: true, aggName: "sum" }),
      ),
    ).toEqual(["a filter on an aggregated value"]);
  });

  it("a date filter with an offset (projectTerse drops the offset)", () => {
    const chart = withFilter({
      fid: "order_date",
      rule: { type: "temporal range", value: [0, 1], offset: -60 },
    });
    expect(unsupported(chart)).toEqual(["a date filter with a time zone offset"]);
  });

  it("a pattern filter", () => {
    const chart = withFilter({
      fid: "region",
      rule: { type: "regexp", value: "^W", caseSensitive: false },
    });
    expect(unsupported(chart)).toEqual(["a pattern filter"]);
  });

  it("a filter on a computed field", () => {
    const chart = build({
      mark: "bar",
      x: { field: "price", bin: 10 },
      y: { field: "*", aggregate: "count" },
    });
    const bin = field(chart, BIN_FID);
    const filtered = {
      ...chart,
      encodings: {
        ...chart.encodings,
        filters: [{ ...bin, rule: { type: "one of", value: [1] } } as IFilterField],
      },
    };
    expect(unsupported(filtered)).toEqual(["a filter on a computed field"]);
  });

  it("a formula field", () => {
    const formula = computedField("f_margin", {
      op: "expr",
      as: "f_margin",
      params: [{ type: "sql", value: "revenue - price" }],
    });
    expect(unsupported(on("rows", [formula], withPoolField(bar(), formula)))).toEqual([
      "a formula field",
    ]);
  });

  it("an aggregate formula field", () => {
    const formula = computedField(
      "f_ratio",
      { op: "expr", as: "f_ratio", params: [{ type: "sql", value: "sum(revenue) / sum(price)" }] },
      { aggName: "expr" },
    );
    expect(unsupported(on("rows", [formula], withPoolField(bar(), formula)))).toEqual([
      "a formula field",
    ]);
  });

  it("a log field", () => {
    const log = computedField("f_log", {
      op: "log10",
      as: "f_log",
      params: [{ type: "field", value: "revenue" }],
      num: 10,
    });
    expect(unsupported(on("rows", [log], withPoolField(bar(), log)))).toEqual(["a log field"]);
  });

  it("a date part field", () => {
    const part = computedField(
      "f_wd",
      {
        op: "dateTimeFeature",
        as: "f_wd",
        params: [
          { type: "field", value: "order_date" },
          { type: "value", value: "weekday" },
        ],
      },
      { analyticType: "dimension", semanticType: "ordinal", aggName: undefined },
    );
    expect(unsupported(on("columns", [part], withPoolField(bar(), part)))).toEqual([
      "a date part field",
    ]);
  });

  it("a date shown by a unit it is not grouped by", () => {
    const chart = bar();
    const date = { ...field(chart, "order_date"), timeUnit: "month" as const };
    expect(unsupported(on("columns", [date], chart))).toEqual([
      "a date shown by a unit it is not grouped by",
    ]);
  });

  it("measure names and values", () => {
    const chart = bar();
    expect(unsupported(on("columns", [field(chart, "gw_mea_key_fid")], chart))).toEqual([
      "measure names or values",
    ]);
  });

  it("several fields on one colour channel", () => {
    const chart = bar();
    expect(
      unsupported(on("color", [field(chart, "region"), field(chart, "category")], chart)),
    ).toEqual(["several fields on color"]);
  });

  it.each([
    ["opacity", "a field on opacity"],
    ["shape", "a field on shape"],
    ["text", "a text label"],
    ["details", "a detail field"],
    ["radius", "a field on radius"],
  ] as const)("a field on %s", (channel, phrase) => {
    const chart = bar();
    expect(unsupported(on(channel, [field(chart, "category")], chart))).toEqual([phrase]);
  });

  it("a map", () => {
    const chart = bar();
    const geo: IChart = {
      ...on("longitude", [field(chart, "price")], chart),
      config: { ...chart.config, geoms: ["poi"], coordSystem: "geographic" },
    };
    expect(unsupported(geo)).toEqual(['the "poi" chart type', "a map"]);
  });

  it("a mark outside the eight (auto is resolved first, see below)", () => {
    const chart = bar();
    expect(unsupported({ ...chart, config: { ...chart.config, geoms: ["tick"] } })).toEqual([
      'the "tick" chart type',
    ]);
  });

  it("an aggregate outside the seven", () => {
    const chart = bar();
    const variance = { ...chart.encodings.rows[0], aggName: "variance" as const };
    expect(unsupported(on("rows", [variance], chart))).toEqual(["a variance aggregate"]);
  });

  it("a centred stack", () => {
    const chart = bar();
    expect(unsupported({ ...chart, layout: { ...chart.layout, stack: "center" } })).toEqual([
      'the "center" stack',
    ]);
  });

  it("a table of raw rows", () => {
    const chart = build({
      mark: "table",
      y: { field: "region" },
      x: { field: "revenue", aggregate: "sum" },
    });
    expect(
      unsupported({ ...chart, config: { ...chart.config, defaultAggregated: false } }),
    ).toEqual(["a table of raw rows"]);
  });
});

describe("describeChart: the automatic chart type", () => {
  const auto = (request: ChartRequest, layout: Partial<IChart["layout"]> = {}): IChart => {
    const chart = build(request);
    return {
      ...chart,
      config: { ...chart.config, geoms: ["auto"] },
      layout: { ...chart.layout, ...layout },
    };
  };
  const SHAPES: [string, ChartRequest, string | null][] = [
    [
      "category + number",
      { mark: "bar", x: { field: "region" }, y: { field: "revenue", aggregate: "sum" } },
      "bar",
    ],
    [
      "number + category",
      { mark: "bar", y: { field: "region" }, x: { field: "revenue", aggregate: "sum" } },
      "bar",
    ],
    [
      "number on a number axis",
      { mark: "bar", x: { field: "quantity" }, y: { field: "revenue", aggregate: "sum" } },
      "bar",
    ],
    [
      "binned number + count",
      { mark: "bar", x: { field: "price", bin: 10 }, y: { field: "*", aggregate: "count" } },
      "bar",
    ],
    [
      "date + number",
      { mark: "line", x: { field: "order_date" }, y: { field: "revenue", aggregate: "sum" } },
      "line",
    ],
    [
      "month + number",
      {
        mark: "line",
        x: { field: "order_date", timeUnit: "month" },
        y: { field: "revenue", aggregate: "mean" },
      },
      "line",
    ],
    [
      "number + month",
      {
        mark: "line",
        y: { field: "order_date", timeUnit: "year" },
        x: { field: "revenue", aggregate: "sum" },
      },
      "line",
    ],
    [
      "category then date + number",
      {
        mark: "line",
        x: [{ field: "region" }, { field: "order_date", timeUnit: "month" }],
        y: { field: "revenue", aggregate: "sum" },
      },
      "line",
    ],
    [
      "two numbers raw",
      { mark: "point", x: { field: "quantity" }, y: { field: "revenue" }, aggregate: false },
      "point",
    ],
    [
      "two sums",
      {
        mark: "point",
        x: { field: "price", aggregate: "sum" },
        y: { field: "revenue", aggregate: "sum" },
        color: { field: "region" },
      },
      "point",
    ],
    [
      "two categories + number",
      {
        mark: "rect",
        x: { field: "region" },
        y: { field: "category" },
        color: { field: "revenue", aggregate: "sum" },
      },
      "rect",
    ],
    [
      "two categories + count",
      {
        mark: "rect",
        x: { field: "region" },
        y: { field: "category" },
        color: { field: "*", aggregate: "count" },
      },
      "rect",
    ],
    [
      "two categories",
      {
        mark: "rect",
        x: { field: "region" },
        y: { field: "category" },
        color: { field: "region" },
      },
      "point",
    ],
    [
      "category + number raw",
      { mark: "boxplot", x: { field: "region" }, y: { field: "revenue" }, aggregate: false },
      "tick",
    ],
    [
      "date + number raw",
      { mark: "point", x: { field: "order_date" }, y: { field: "revenue" }, aggregate: false },
      "line",
    ],
    [
      "a pie's shelves",
      { mark: "arc", theta: { field: "revenue", aggregate: "sum" }, color: { field: "region" } },
      null,
    ],
  ];

  it.each(SHAPES)(
    "%s: the mark matches Graphic Walker's worksheet renderer",
    (_name, request, mark) => {
      const chart = auto(request);
      expect(drawnMark(chart)).toBe(mark);
      expect(drawnMark(chart)).toBe(chartToWorksheet(chart).spec?.mark ?? null);
    },
  );

  it("a chart's own mark is the one drawn", () => {
    const chart = build({
      mark: "area",
      x: { field: "region" },
      y: { field: "revenue", aggregate: "sum" },
    });
    expect(drawnMark(chart)).toBe("area");
    expect(chartToWorksheet(chart).spec?.mark).toBe("area");
  });

  it.each([
    ["category + number", SHAPES[0][1], "bar"],
    ["date + number", SHAPES[5][1], "line"],
    ["two numbers raw", SHAPES[8][1], "point"],
    ["two categories + number", SHAPES[10][1], "rect"],
  ] as const)(
    "%s on auto is described with the drawn mark, and the chart is left as it was",
    (_name, request, mark) => {
      const chart = auto(request);
      const before = JSON.stringify(chart);
      const described = describeChart(chart, ctx());
      expect(described).toEqual({ ok: true, request: { ...request, mark } });
      expect(JSON.stringify(chart)).toBe(before);
      if (!described.ok) return;
      const rebuilt = build(described.request, chart);
      expect(rebuilt.config.geoms).toEqual([mark]);
      expect(query(rebuilt)).toEqual(query(chart));
    },
  );

  it("a stacked auto bar keeps its stack in the description", () => {
    const chart = auto({
      mark: "bar",
      x: { field: "region" },
      y: { field: "revenue", aggregate: "sum" },
      color: { field: "category" },
      stack: "normalize",
    });
    expect(describeChart(chart, ctx())).toEqual({
      ok: true,
      request: {
        mark: "bar",
        x: { field: "region" },
        y: { field: "revenue", aggregate: "sum" },
        color: { field: "category" },
        stack: "normalize",
      },
    });
  });

  it.each([
    ["it draws ticks", SHAPES[13][1], {}],
    ["nothing is on rows or columns", SHAPES[15][1], {}],
    ["another renderer picks the mark", SHAPES[0][1], { renderer: "vega-lite" as const }],
  ])("is refused when %s", (_name, request, layout) => {
    expect(describeChart(auto(request, layout), ctx())).toEqual({
      ok: false,
      unsupported: ["the automatic chart type"],
    });
  });
});

describe("the shapes core sends and accepts", () => {
  it("a horizontal bar of counts: the row count on x", () => {
    const request: ChartRequest = {
      mark: "bar",
      y: { field: "region" },
      x: { field: "*", aggregate: "count" },
    };
    const chart = build(request);
    expect(fids(chart.encodings.columns)).toEqual(["gw_count_fid"]);
    expect(viewStep(chart)).toEqual(aggregate(["region"], [sumOf("gw_count_fid")]));
    expect(describeChart(chart, ctx())).toEqual({ ok: true, request });
  });

  it("a table with two measures on x and a dimension on y", () => {
    const request: ChartRequest = {
      mark: "table",
      y: { field: "region" },
      x: [
        { field: "revenue", aggregate: "sum" },
        { field: "price", aggregate: "mean" },
      ],
    };
    const chart = build(request);
    expect(fids(chart.encodings.rows)).toEqual(["region"]);
    expect(fids(chart.encodings.columns)).toEqual(["revenue", "price"]);
    expect(viewStep(chart)).toEqual(
      aggregate(
        ["region"],
        [sumOf("revenue"), { field: "price", agg: "mean", asFieldKey: "price_mean" }],
      ),
    );
    expect(describeChart(chart, ctx())).toEqual({ ok: true, request });
  });

  it("x and y as one object or as a list convert to the same chart", () => {
    const single = build({
      mark: "bar",
      x: { field: "region" },
      y: { field: "revenue", aggregate: "sum" },
    });
    const listed = build({
      mark: "bar",
      x: [{ field: "region" }],
      y: [{ field: "revenue", aggregate: "sum" }],
    });
    expect(query(listed)).toEqual(query(single));
    expect(unstamped({ ...listed, visId: single.visId })).toEqual(unstamped(single));
  });

  it("an answer with only the keys it sets converts, no null needed", () => {
    const chart = build({
      mark: "arc",
      theta: { field: "*", aggregate: "count" },
      color: { field: "category" },
    });
    expect(chart.config.limit).toBe(-1);
    expect(chart.layout.stack).toBe("stack");
    expect(chart.encodings.filters).toEqual([]);
  });

  it("an aggregated scatter of plain numbers is described with aggregate: true, so core keeps it grouped", () => {
    const request: ChartRequest = {
      mark: "point",
      x: { field: "quantity" },
      y: { field: "price" },
    };
    const chart = build(request);
    expect(chart.config.defaultAggregated).toBe(true);
    const described = describeChart(chart, ctx());
    expect(described).toEqual({ ok: true, request: { ...request, aggregate: true } });
    if (described.ok) expect(query(build(described.request, chart))).toEqual(query(chart));
  });
});

describe("describeChart: requests core would reject are refused", () => {
  const poolField = (chart: IChart, fid: string) =>
    [...chart.encodings.dimensions, ...chart.encodings.measures].find((f) => f.fid === fid)!;
  const refusal = (chart: IChart) => {
    const result = describeChart(chart, ctx());
    if (result.ok) throw new Error(`expected a refusal, got ${JSON.stringify(result.request)}`);
    return result.unsupported;
  };
  const bar = () =>
    build({ mark: "bar", x: { field: "region" }, y: { field: "revenue", aggregate: "sum" } });

  it.each<[string, ChartRequest, string]>([
    [
      "two measures on one axis",
      {
        mark: "bar",
        x: { field: "region" },
        y: [
          { field: "revenue", aggregate: "sum" },
          { field: "price", aggregate: "sum" },
        ],
      },
      "several measures on one axis",
    ],
    [
      "numbers on both axes",
      {
        mark: "bar",
        x: { field: "price", aggregate: "sum" },
        y: { field: "revenue", aggregate: "sum" },
      },
      "a bar chart with numbers on both axes",
    ],
    [
      "no number axis",
      { mark: "line", x: { field: "region" }, y: { field: "category" } },
      "a line chart without a number axis",
    ],
    [
      "a pie with an axis",
      {
        mark: "arc",
        x: { field: "category" },
        theta: { field: "revenue", aggregate: "sum" },
        color: { field: "region" },
      },
      "a pie that is not one angle and one category",
    ],
    [
      "a limited pie",
      {
        mark: "arc",
        theta: { field: "revenue", aggregate: "sum" },
        color: { field: "region" },
        limit: 3,
      },
      "a sorted or limited pie",
    ],
    [
      "a heatmap without an aggregated colour",
      {
        mark: "rect",
        x: { field: "region" },
        y: { field: "category" },
        color: { field: "region" },
      },
      "a heatmap that is not two categories and an aggregated colour",
    ],
    [
      "a scatter of one point",
      {
        mark: "point",
        x: { field: "price", aggregate: "sum" },
        y: { field: "revenue", aggregate: "sum" },
      },
      "a scatter that draws a single point",
    ],
    [
      "an aggregated box plot",
      { mark: "boxplot", x: { field: "region" }, y: { field: "revenue" } },
      "a box plot of aggregated values",
    ],
    [
      "a box plot of two categories",
      { mark: "boxplot", x: { field: "region" }, y: { field: "category" }, aggregate: false },
      "a box plot that is not categories against one number",
    ],
    [
      "a sort on a number axis",
      {
        mark: "bar",
        x: { field: "quantity" },
        y: { field: "revenue", aggregate: "sum" },
        sort: "descending",
      },
      "a sort on a time or number axis",
    ],
    [
      "a limit without a sort",
      { mark: "bar", x: { field: "region" }, y: { field: "revenue", aggregate: "sum" }, limit: 5 },
      "a limit without a sort",
    ],
    [
      "a limit over two categories",
      {
        mark: "bar",
        x: { field: "region" },
        y: { field: "revenue", aggregate: "sum" },
        color: { field: "category" },
        sort: "descending",
        limit: 5,
      },
      "a limit over several categories",
    ],
    [
      "size on a bar",
      {
        mark: "bar",
        x: { field: "region" },
        y: { field: "revenue", aggregate: "sum" },
        size: { field: "price", aggregate: "sum" },
      },
      "a field on size",
    ],
    [
      "angle on a bar",
      {
        mark: "bar",
        x: { field: "region" },
        y: { field: "revenue", aggregate: "sum" },
        theta: { field: "price", aggregate: "sum" },
      },
      "a field on angle",
    ],
    [
      "a value filter on a date",
      {
        mark: "bar",
        x: { field: "region" },
        y: { field: "revenue", aggregate: "sum" },
        filters: [{ field: "order_date", oneOf: [1] }],
      },
      "a value filter on a date column",
    ],
    [
      "a range on text",
      {
        mark: "bar",
        x: { field: "region" },
        y: { field: "revenue", aggregate: "sum" },
        filters: [{ field: "region", range: [1, 2] }],
      },
      "a range filter on a column that is not a number",
    ],
    [
      "a range with min above max",
      {
        mark: "bar",
        x: { field: "region" },
        y: { field: "revenue", aggregate: "sum" },
        filters: [{ field: "price", range: [9, 1] }],
      },
      "a range filter without a valid range",
    ],
    [
      "a date range on text",
      {
        mark: "bar",
        x: { field: "region" },
        y: { field: "revenue", aggregate: "sum" },
        filters: [{ field: "region", dateRange: ["2024-01-01", null] }],
      },
      "a date filter on a column that is not a date",
    ],
    [
      "a date range ending first",
      {
        mark: "bar",
        x: { field: "region" },
        y: { field: "revenue", aggregate: "sum" },
        filters: [{ field: "order_date", dateRange: ["2024-12-31", "2024-01-01"] }],
      },
      "a date filter without a valid range",
    ],
  ])("%s", (_name, request, phrase) => {
    expect(refusal(build(request))).toContain(phrase);
  });

  it("a sum of a text column", () => {
    const chart = bar();
    const region = {
      ...poolField(chart, "region"),
      analyticType: "measure" as const,
      aggName: "sum",
    };
    expect(refusal({ ...chart, encodings: { ...chart.encodings, rows: [region] } })).toContain(
      "a sum of a column that is not a number",
    );
  });

  it("a column both grouped and aggregated", () => {
    const chart = bar();
    const counted = {
      ...poolField(chart, "region"),
      analyticType: "measure" as const,
      aggName: "distinctCount",
    };
    expect(refusal({ ...chart, encodings: { ...chart.encodings, color: [counted] } })).toContain(
      "a column both grouped and aggregated",
    );
  });

  it("a row count in a chart of raw rows", () => {
    const chart = build({
      mark: "point",
      x: { field: "quantity" },
      y: { field: "revenue" },
      aggregate: false,
    });
    const count = poolField(chart, "gw_count_fid");
    expect(refusal({ ...chart, encodings: { ...chart.encodings, rows: [count] } })).toContain(
      "a row count in a chart of raw rows",
    );
  });

  it("an empty value filter", () => {
    const chart = build({
      mark: "bar",
      x: { field: "region" },
      y: { field: "revenue", aggregate: "sum" },
      filters: [{ field: "region", oneOf: ["West"] }],
    });
    const filters = [
      { ...chart.encodings.filters[0], rule: { type: "one of" as const, value: [] } },
    ];
    expect(refusal({ ...chart, encodings: { ...chart.encodings, filters } })).toContain(
      "a value filter on no values or on empty values",
    );
  });

  it("a chart with only a filter", () => {
    const chart = build({
      mark: "bar",
      x: { field: "region" },
      y: { field: "revenue", aggregate: "sum" },
      filters: [{ field: "region", oneOf: ["West"] }],
    });
    expect(
      refusal({ ...chart, encodings: { ...chart.encodings, rows: [], columns: [] } }),
    ).toContain("a chart with no fields");
  });
});
