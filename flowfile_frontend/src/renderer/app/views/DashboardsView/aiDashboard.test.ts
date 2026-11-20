// Unit tests for the AI dashboard builder's pure parts: the page's tiles and where they land.

import { describe, expect, it, vi } from "vitest";

vi.mock("../../api/catalog.api", () => ({ CatalogApi: {} }));
vi.mock("../../composables/useGraphicWalkerCompute", () => ({ useGraphicWalkerCompute: vi.fn() }));

import type { IChart } from "@kanaries/graphic-walker/interfaces";

import type { DashboardPlanResponse } from "../../api/ai.api";
import { EMPTY_DASHBOARD_LAYOUT, type DashboardLayout, type DashboardTile } from "../../types";
import {
  FULL_CHART_HEIGHT,
  HALF_CHART_HEIGHT,
  UNGROUNDED_LINE,
  degradedPlanMessage,
  landsOnTab,
  layoutPage,
  pageFilters,
  placeOnDashboard,
  planRequestFailure,
  storyFallbackWarning,
  summaryMarkdown,
  type PlacedChart,
} from "./aiDashboard";

const COLS = 48;
const chart = (id: string, width: "half" | "full", vizId: number): PlacedChart => ({
  id,
  tableId: 1,
  title: `Chart ${id}`,
  finding: `Finding ${id}`,
  width,
  chart: {} as IChart,
  vizId,
});
const ids = () => {
  let n = 0;
  return () => `t${(n += 1)}`;
};
const page = (kpis = 0) => ({
  title: "Sales",
  summaryMd: "Sales are **48,210**.",
  grounded: true,
  kpis: Array.from({ length: kpis }, (_, i) => ({
    id: `k${i + 1}`,
    table_id: 7,
    label: `KPI ${i + 1}`,
    field: i === 0 ? null : "revenue",
    agg: i === 0 ? ("count" as const) : ("sum" as const),
  })),
});
const box = (t: DashboardTile) => [t.type, t.x, t.y, t.w, t.h];

describe("summaryMarkdown", () => {
  it("heads the answer with the title and says when no data was read", () => {
    expect(summaryMarkdown(page())).toBe("## Sales\n\nSales are **48,210**.");
    expect(summaryMarkdown({ ...page(), grounded: false })).toBe(
      `## Sales\n\nSales are **48,210**.\n\n${UNGROUNDED_LINE}`,
    );
  });
});

describe("layoutPage", () => {
  it("stacks the summary, a KPI row and the charts two to a row", () => {
    const tiles = layoutPage(
      page(2),
      [chart("c1", "full", 11), chart("c2", "half", 12), chart("c3", "half", 13)],
      COLS,
      ids(),
    );
    expect(tiles.map(box)).toEqual([
      ["text", 0, 0, 48, 3],
      ["kpi", 0, 3, 12, 3],
      ["kpi", 12, 3, 12, 3],
      ["viz", 0, 6, 48, FULL_CHART_HEIGHT],
      ["viz", 0, 14, 24, HALF_CHART_HEIGHT],
      ["viz", 24, 14, 24, HALF_CHART_HEIGHT],
    ]);
    expect(tiles.map((t) => t.id)).toEqual(["t1", "t2", "t3", "t4", "t5", "t6"]);
  });

  it("gives a KPI its table, measure and label, and a chart its title and finding", () => {
    const [, rows, revenue, viz] = layoutPage(page(2), [chart("c1", "half", 11)], COLS, ids());
    expect(rows).toMatchObject({ table_id: 7, viz_id: null });
    expect(rows.kpi).toMatchObject({
      field: null,
      agg: "count",
      label: "KPI 1",
      comparison: "none",
    });
    expect(revenue.kpi).toMatchObject({ field: "revenue", agg: "sum", label: "KPI 2" });
    expect(viz).toMatchObject({
      viz_id: 11,
      chart_index: 0,
      title: "Chart c1",
      note_md: "Finding c1",
    });
  });

  it("widens a half-width chart left alone on its row", () => {
    const tiles = layoutPage(
      page(),
      [chart("c1", "half", 1), chart("c2", "full", 2), chart("c3", "half", 3)],
      COLS,
      ids(),
    );
    expect(tiles.slice(1).map(box)).toEqual([
      ["viz", 0, 3, 48, HALF_CHART_HEIGHT],
      ["viz", 0, 10, 48, FULL_CHART_HEIGHT],
      ["viz", 0, 18, 48, HALF_CHART_HEIGHT],
    ]);
  });

  it("wraps more than four KPIs and gives a long summary another row", () => {
    const long = { ...page(5), summaryMd: "x".repeat(400) };
    const tiles = layoutPage(long, [chart("c1", "full", 1)], COLS, ids());
    expect(tiles.map(box)).toEqual([
      ["text", 0, 0, 48, 4],
      ["kpi", 0, 4, 12, 3],
      ["kpi", 12, 4, 12, 3],
      ["kpi", 24, 4, 12, 3],
      ["kpi", 36, 4, 12, 3],
      ["kpi", 0, 7, 12, 3],
      ["viz", 0, 10, 48, FULL_CHART_HEIGHT],
    ]);
  });

  it("makes a page of the summary alone when no chart was kept", () => {
    expect(layoutPage(page(), [], COLS, ids()).map(box)).toEqual([["text", 0, 0, 48, 3]]);
  });
});

describe("placeOnDashboard", () => {
  const tiles = layoutPage(page(), [chart("c1", "full", 1)], COLS, ids());
  const tabIds = () => {
    let n = 0;
    return () => `tab-${(n += 1)}`;
  };
  const existing: DashboardTile = {
    id: "old",
    type: "text",
    viz_id: null,
    chart_index: 0,
    x: 0,
    y: 0,
    w: 48,
    h: 2,
  };

  it("fills an empty dashboard without making a tab", () => {
    const placed = placeOnDashboard(EMPTY_DASHBOARD_LAYOUT, tiles, "Sales", [], tabIds());
    expect(placed.tabId).toBeNull();
    expect(placed.layout.tabs ?? []).toEqual([]);
    expect(placed.layout.tiles.map((t) => t.id)).toEqual(["t1", "t2"]);
    expect(placed.layout.tiles.every((t) => t.tab_id == null)).toBe(true);
  });

  it("puts the page on a second tab named after it when the canvas holds tiles", () => {
    const layout: DashboardLayout = { ...EMPTY_DASHBOARD_LAYOUT, tiles: [existing] };
    const placed = placeOnDashboard(layout, tiles, "Sales", [], tabIds());
    expect(placed.layout.tabs).toEqual([
      { id: "tab-1", title: "Tab 1" },
      { id: "tab-2", title: "Sales" },
    ]);
    expect(placed.tabId).toBe("tab-2");
    expect(placed.layout.tiles.map((t) => [t.id, t.tab_id])).toEqual([
      ["old", "tab-1"],
      ["t1", "tab-2"],
      ["t2", "tab-2"],
    ]);
  });

  it("adds one more tab to a dashboard that has tabs, cutting a long title", () => {
    const layout: DashboardLayout = {
      ...EMPTY_DASHBOARD_LAYOUT,
      tabs: [{ id: "a", title: "Overview" }],
      tiles: [{ ...existing, tab_id: "a" }],
    };
    const placed = placeOnDashboard(layout, tiles, "T".repeat(80), [], tabIds());
    expect(placed.layout.tabs).toEqual([
      { id: "a", title: "Overview" },
      { id: "tab-1", title: "T".repeat(60) },
    ]);
    expect(placed.layout.tiles.filter((t) => t.tab_id === "tab-1")).toHaveLength(2);
  });

  it("uses a tab even on an empty dashboard that already has tabs", () => {
    const layout: DashboardLayout = { ...EMPTY_DASHBOARD_LAYOUT, tabs: [{ id: "a", title: "A" }] };
    expect(placeOnDashboard(layout, tiles, "Sales", [], tabIds()).tabId).toBe("tab-1");
  });
});

describe("pageFilters", () => {
  const charts = [chart("c1", "full", 11), { ...chart("c2", "half", 12), tableId: 9 }];
  const tiles = layoutPage(page(1), charts, COLS, ids());
  const planned = [
    {
      id: "f1",
      table_id: 1,
      field: "order_date",
      kind: "date_range" as const,
      label: "Order date",
    },
    { id: "f2", table_id: 7, field: "region", kind: "categorical" as const, label: null },
    { id: "f3", table_id: 9, field: "amount", kind: "numeric_range" as const, label: "Amount" },
    { id: "f4", table_id: 404, field: "x", kind: "categorical" as const, label: null },
  ];

  it("binds each filter to its table with an empty state, and leaves out one no tile reads", () => {
    const filters = pageFilters(planned, tiles, charts, [], false, ids());
    expect(filters).toEqual([
      {
        id: "t1",
        field_name: "order_date",
        label: "Order date",
        kind: "date_range",
        state: { start: null, end: null },
        target: "all",
        target_tile_ids: [],
        datasource_id: 1,
        scope: "datasource",
        multiple: true,
      },
      expect.objectContaining({ field_name: "region", datasource_id: 7, state: { selected: [] } }),
      expect.objectContaining({ field_name: "amount", state: { min: null, max: null } }),
    ]);
  });

  it("on a tab of its own targets only the page's tiles that read the table", () => {
    const filters = pageFilters(planned, tiles, charts, [], true, ids());
    const byField = Object.fromEntries(filters.map((f) => [f.field_name, f]));
    const idOf = (match: (t: DashboardTile) => boolean) => tiles.filter(match).map((t) => t.id);
    expect(byField.order_date).toMatchObject({
      target: "tiles",
      target_tile_ids: idOf((t) => t.viz_id === 11),
    });
    expect(byField.region.target_tile_ids).toEqual(idOf((t) => t.type === "kpi"));
    expect(byField.amount.target_tile_ids).toEqual(idOf((t) => t.viz_id === 12));
  });

  it("skips a filter the dashboard already applies to every tile on that table", () => {
    const existing = pageFilters(planned.slice(0, 2), tiles, charts, [], false, ids());
    const scoped = { ...existing[1], target: "tiles" as const };
    const added = pageFilters(planned, tiles, charts, [existing[0], scoped], true, ids());
    expect(added.map((f) => f.field_name)).toEqual(["region", "amount"]);
  });
});

describe("placing filters", () => {
  const tiles = layoutPage(page(1), [chart("c1", "full", 11)], COLS, ids());
  const filter = pageFilters(
    [{ id: "f1", table_id: 1, field: "region", kind: "categorical", label: null }],
    tiles,
    [chart("c1", "full", 11)],
    [],
    false,
    ids(),
  );

  it("adds them to the dashboard's filters, after the ones it has", () => {
    const kept = { ...filter[0], id: "kept", field_name: "category" };
    const layout: DashboardLayout = { ...EMPTY_DASHBOARD_LAYOUT, filters: [kept] };
    expect(landsOnTab(layout)).toBe(false);
    const placed = placeOnDashboard(layout, tiles, "Sales", filter);
    expect(placed.layout.filters.map((f) => f.field_name)).toEqual(["category", "region"]);
    expect(placed.tabId).toBeNull();
  });

  it("keeps them when the page goes on a tab", () => {
    const layout: DashboardLayout = { ...EMPTY_DASHBOARD_LAYOUT, tabs: [{ id: "a", title: "A" }] };
    expect(landsOnTab(layout)).toBe(true);
    const placed = placeOnDashboard(layout, tiles, "Sales", filter, () => "tab-new");
    expect(placed.layout.filters).toHaveLength(1);
    expect(placed.tabId).toBe("tab-new");
  });
});

describe("wording", () => {
  const degraded = (
    reason: DashboardPlanResponse["reason"],
    extra = {},
  ): DashboardPlanResponse => ({
    plan: null,
    explanation: null,
    warnings: [],
    degraded: true,
    reason,
    message: null,
    attempts: 1,
    ...extra,
  });

  it("has a message for every plan reason", () => {
    const reasons: DashboardPlanResponse["reason"][] = [
      "empty_prompt",
      "no_columns",
      "timeout",
      "provider_error",
      "empty_response",
      "parse_error",
      "validation_error",
      "no_plan",
      "invalid_plan",
    ];
    const texts = reasons.map((r) => degradedPlanMessage(degraded(r)).text);
    expect(texts.every((t) => t.length > 20)).toBe(true);
    expect(new Set(texts.slice(0, 5)).size).toBe(5);
  });

  it("gives the AI's own reason for declining, as a warning", () => {
    const message = degradedPlanMessage(
      degraded("no_plan", { explanation: "The tables hold sales." }),
    );
    expect(message).toEqual({
      tone: "warning",
      text: "The AI did not plan a page: The tables hold sales.",
    });
  });

  it("keeps core's problem list as the detail of an unusable plan", () => {
    const message = degradedPlanMessage(
      degraded("invalid_plan", { message: "The plan has 1 problem" }),
    );
    expect(message.detail).toBe("The plan has 1 problem");
  });

  it("names why the story was not written", () => {
    expect(storyFallbackWarning(null)).toContain("the AI's answer could not be used");
    expect(
      storyFallbackWarning({
        story: null,
        grounded: false,
        evidence: [],
        warnings: [],
        degraded: true,
        reason: "timeout",
        message: null,
        attempts: 1,
      }),
    ).toContain("the AI took too long");
  });

  it("words a refused plan request by its status", () => {
    expect(planRequestFailure({ response: { status: 403 } }).text).toContain("may not read");
    expect(planRequestFailure({ response: { status: 404 } }).text).toContain("not found");
    expect(planRequestFailure({ response: { status: 422 } }).text).toContain(
      "Shorten the question",
    );
    expect(planRequestFailure({ response: { status: 409 } }).text).toContain("No AI provider");
    expect(planRequestFailure(new Error("down")).text).toContain("Could not reach");
  });
});
