import { beforeEach, describe, expect, it, vi } from "vitest";
import { ref } from "vue";
import type { DashboardLayout } from "../types";

const {
  getVisualizationMock,
  getTableMock,
  getTableColumnStatsMock,
  getFieldsMock,
  getSourceFieldsMock,
  computeSavedMock,
  computeAdHocMock,
} = vi.hoisted(() => ({
  getVisualizationMock: vi.fn(),
  getTableMock: vi.fn(),
  getTableColumnStatsMock: vi.fn(),
  getFieldsMock: vi.fn(),
  getSourceFieldsMock: vi.fn(),
  computeSavedMock: vi.fn(),
  computeAdHocMock: vi.fn(),
}));

vi.mock("../api/catalog.api", () => ({
  CatalogApi: {
    getVisualization: getVisualizationMock,
    getTable: getTableMock,
    getTableColumnStats: getTableColumnStatsMock,
    getSavedVisualizationFields: getFieldsMock,
    getVisualizationFields: getSourceFieldsMock,
    computeSavedVisualization: computeSavedMock,
    computeAdHocVisualization: computeAdHocMock,
  },
}));

import { fieldsAsColumns, useDashboardDatasources } from "./useDashboardDatasources";
import type { DashboardFilter } from "../types";

const TILE_ID = "tile-a";

const layoutOf = (): DashboardLayout => ({
  tiles: [
    { id: TILE_ID, type: "viz", viz_id: 1, chart_index: 0, x: 0, y: 0, w: 6, h: 6 },
    { id: "tile-text", type: "text", viz_id: null, chart_index: 0, x: 0, y: 6, w: 12, h: 3 },
  ],
  grid: { cols: 12, row_height: 40, version: 1 },
  filters: [],
});

const vizResponse = { id: 1, name: "Revenue", source_type: "table", catalog_table_id: 5 };
const tableResponse = {
  id: 5,
  name: "sales",
  full_table_name: "main.sales",
  schema_columns: [{ name: "region", dtype: "String" }],
};
const statsResponse = { dtype: "String", values: ["EU"], min: null, max: null, truncated: false };

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

describe("useDashboardDatasources", () => {
  beforeEach(() => {
    getVisualizationMock.mockReset().mockResolvedValue(vizResponse);
    getTableMock.mockReset().mockResolvedValue(tableResponse);
    getTableColumnStatsMock.mockReset().mockResolvedValue(statsResponse);
    getFieldsMock.mockReset().mockResolvedValue({
      fields: [{ fid: "region", semanticType: "nominal" }],
      cache_hit: false,
      error: null,
    });
    getSourceFieldsMock.mockReset().mockResolvedValue({
      fields: [{ fid: "amount", semanticType: "quantitative" }],
      cache_hit: false,
      error: null,
    });
  });

  it("loads tile columns on demand, caches them, and drops them on a force refresh", async () => {
    const { tileFields, loadTileFields, refresh } = useDashboardDatasources(ref(layoutOf()));
    await flush();

    expect(getFieldsMock).not.toHaveBeenCalled();
    expect(tileFields(TILE_ID)).toBeNull();

    await loadTileFields();
    expect(tileFields(TILE_ID)).toEqual([{ fid: "region", semanticType: "nominal" }]);
    expect(tileFields("tile-text")).toBeNull();

    await loadTileFields();
    expect(getFieldsMock).toHaveBeenCalledTimes(1);

    await refresh(true);
    expect(tileFields(TILE_ID)).toBeNull();
  });

  it("resolves a KPI tile through its viz like a chart tile", async () => {
    const layout = layoutOf();
    layout.tiles.push(
      {
        id: "tile-kpi",
        type: "kpi",
        viz_id: 1,
        chart_index: 0,
        kpi: { field: "amount", agg: "sum" },
        x: 0,
        y: 9,
        w: 12,
        h: 3,
      },
      {
        id: "tile-kpi-labelled",
        type: "kpi",
        viz_id: 1,
        chart_index: 0,
        kpi: { field: "amount", agg: "sum", label: "Revenue total" },
        x: 12,
        y: 9,
        w: 12,
        h: 3,
      },
      {
        id: "tile-kpi-empty",
        type: "kpi",
        viz_id: null,
        chart_index: 0,
        kpi: null,
        x: 24,
        y: 9,
        w: 12,
        h: 3,
      },
    );
    const { tilesByDatasource, vizTileIds, tileDatasource, tileLabel, tileFields, loadTileFields } =
      useDashboardDatasources(ref(layout));
    await flush();

    expect(tilesByDatasource.value).toEqual({
      "table:5": [TILE_ID, "tile-kpi", "tile-kpi-labelled"],
    });
    expect(vizTileIds.value).toEqual([TILE_ID, "tile-kpi", "tile-kpi-labelled"]);
    expect(tileDatasource("tile-kpi")).toBe("table:5");
    expect(tileDatasource("tile-kpi-empty")).toBeNull();
    expect(tileLabel("tile-kpi")).toBe("KPI · Revenue");
    expect(tileLabel("tile-kpi-labelled")).toBe("Revenue total");
    expect(tileLabel("tile-kpi-empty")).toBe("KPI");

    await loadTileFields();
    expect(tileFields("tile-kpi")).toEqual([{ fid: "region", semanticType: "nominal" }]);
  });

  it("serves repeat refreshes from the cache", async () => {
    const { refresh } = useDashboardDatasources(ref(layoutOf()));
    await flush();

    expect(getVisualizationMock).toHaveBeenCalledTimes(1);
    expect(getTableMock).toHaveBeenCalledTimes(1);

    await refresh();

    expect(getVisualizationMock).toHaveBeenCalledTimes(1);
    expect(getTableMock).toHaveBeenCalledTimes(1);
  });

  it("refetches viz and table metadata on refresh(true)", async () => {
    const { refresh } = useDashboardDatasources(ref(layoutOf()));
    await flush();

    await refresh(true);

    expect(getVisualizationMock).toHaveBeenCalledTimes(2);
    expect(getTableMock).toHaveBeenCalledTimes(2);
  });

  it("caches column stats per (table, column) until a force refresh", async () => {
    const { refresh, getColumnStats } = useDashboardDatasources(ref(layoutOf()));
    await flush();

    await getColumnStats(5, "region");
    await getColumnStats(5, "region");
    expect(getTableColumnStatsMock).toHaveBeenCalledTimes(1);

    await refresh(true);
    await getColumnStats(5, "region");
    expect(getTableColumnStatsMock).toHaveBeenCalledTimes(2);
  });

  it("keeps a resolved tile's datasource warm during a force refresh", async () => {
    const { refresh, tileDatasource } = useDashboardDatasources(ref(layoutOf()));
    await flush();
    expect(tileDatasource(TILE_ID)).toBe("table:5");

    let resolveViz!: (value: typeof vizResponse) => void;
    getVisualizationMock.mockImplementation(() => new Promise((resolve) => (resolveViz = resolve)));

    const pending = refresh(true);
    expect(tileDatasource(TILE_ID)).toBe("table:5");
    await Promise.resolve();
    expect(tileDatasource(TILE_ID)).toBe("table:5");

    resolveViz(vizResponse);
    await pending;
    expect(tileDatasource(TILE_ID)).toBe("table:5");
  });

  it("keeps the previous mapping when a force refetch fails", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const { refresh, tileDatasource } = useDashboardDatasources(ref(layoutOf()));
    await flush();
    expect(tileDatasource(TILE_ID)).toBe("table:5");

    getVisualizationMock.mockRejectedValue(new Error("boom"));
    await refresh(true);

    expect(tileDatasource(TILE_ID)).toBe("table:5");
    warn.mockRestore();
  });

  it("resolves a table or KPI tile that names its table, without a chart", async () => {
    getTableMock.mockImplementation(async (id: number) =>
      id === 9 ? { id: 9, name: "orders", full_table_name: "main.orders" } : tableResponse,
    );
    const layout = layoutOf();
    layout.tiles.push(
      {
        id: "tbl",
        type: "table",
        viz_id: null,
        table_id: 9,
        chart_index: 0,
        x: 0,
        y: 9,
        w: 6,
        h: 6,
      },
      {
        id: "tbl-titled",
        type: "table",
        viz_id: 1,
        title: "Latest orders",
        chart_index: 0,
        x: 6,
        y: 9,
        w: 6,
        h: 6,
      },
      {
        id: "kpi-t",
        type: "kpi",
        viz_id: null,
        table_id: 9,
        kpi: { field: null, agg: "count" },
        chart_index: 0,
        x: 12,
        y: 9,
        w: 6,
        h: 3,
      },
      { id: "tbl-empty", type: "table", viz_id: null, chart_index: 0, x: 18, y: 9, w: 6, h: 6 },
    );
    const {
      datasourcesInUse,
      tilesByDatasource,
      vizTileIds,
      tileDatasource,
      tileLabel,
      tileFields,
      loadTileFields,
    } = useDashboardDatasources(ref(layout));
    await flush();

    expect(datasourcesInUse.value.map((d) => d.id)).toEqual([9, 5]);
    expect(tilesByDatasource.value).toEqual({
      "table:5": [TILE_ID, "tbl-titled"],
      "table:9": ["tbl", "kpi-t"],
    });
    expect(vizTileIds.value).toEqual([TILE_ID, "tbl", "tbl-titled", "kpi-t"]);
    expect(tileDatasource("tbl")).toBe("table:9");
    expect(tileDatasource("tbl-empty")).toBeNull();
    expect(tileLabel("tbl")).toBe("Table · main.orders");
    expect(tileLabel("tbl-titled")).toBe("Latest orders");
    expect(tileLabel("kpi-t")).toBe("KPI · main.orders");
    expect(tileLabel("tbl-empty")).toBe("Table");

    await loadTileFields();
    expect(getSourceFieldsMock).toHaveBeenCalledTimes(1);
    expect(getSourceFieldsMock).toHaveBeenCalledWith({ source_type: "table", table_id: 9 });
    expect(tileFields("tbl")).toEqual([{ fid: "amount", semanticType: "quantitative" }]);
    expect(tileFields("kpi-t")).toEqual([{ fid: "amount", semanticType: "quantitative" }]);
    expect(tileFields("tbl-titled")).toEqual([{ fid: "region", semanticType: "nominal" }]);
  });

  it("lists a SQL chart as a source of its own, reached by its chart, KPI and table tiles", async () => {
    getVisualizationMock.mockImplementation(async (id: number) =>
      id === 2
        ? { id: 2, name: "Orders query", source_type: "sql", catalog_table_id: null }
        : vizResponse,
    );
    getFieldsMock.mockImplementation(async (id: number) => ({
      fields:
        id === 2
          ? [
              { fid: "region", semanticType: "nominal" },
              { fid: "amount", semanticType: "quantitative" },
              { fid: "day", semanticType: "temporal" },
            ]
          : [{ fid: "region", semanticType: "nominal" }],
      cache_hit: false,
      error: null,
    }));
    const layout = layoutOf();
    layout.tiles.push(
      { id: "sql-chart", type: "viz", viz_id: 2, chart_index: 0, x: 0, y: 9, w: 6, h: 6 },
      {
        id: "sql-kpi",
        type: "kpi",
        viz_id: 2,
        kpi: { field: "amount", agg: "sum" },
        chart_index: 0,
        x: 6,
        y: 9,
        w: 6,
        h: 3,
      },
      { id: "sql-rows", type: "table", viz_id: 2, chart_index: 0, x: 12, y: 9, w: 6, h: 6 },
    );
    const { datasourcesInUse, tilesByDatasource, tileDatasource } = useDashboardDatasources(
      ref(layout),
    );
    await flush();
    await flush();

    expect(datasourcesInUse.value.map((d) => [d.key, d.kind, d.name])).toEqual([
      ["table:5", "table", "main.sales"],
      ["viz:2", "viz", "Orders query"],
    ]);
    expect(datasourcesInUse.value[1].schema_columns).toEqual([
      { name: "region", dtype: "text" },
      { name: "amount", dtype: "number" },
      { name: "day", dtype: "date" },
    ]);
    expect(tilesByDatasource.value["viz:2"]).toEqual(["sql-chart", "sql-kpi", "sql-rows"]);
    expect(tileDatasource("sql-kpi")).toBe("viz:2");
    expect(tileDatasource(TILE_ID)).toBe("table:5");
    // The chart tile reads the same fields; one request serves both.
    expect(getFieldsMock).toHaveBeenCalledTimes(1);
  });

  it("searches a filter's own source for values, once per text, until a force refresh", async () => {
    computeAdHocMock.mockResolvedValue({ rows: [{ region: "West" }], error: null });
    computeSavedMock.mockResolvedValue({
      rows: [{ region: "North" }, { region: null }],
      error: null,
    });
    const { searchFilterValues, refresh } = useDashboardDatasources(ref(layoutOf()));
    await flush();
    const onTable: DashboardFilter = {
      id: "f",
      field_name: "region",
      kind: "categorical",
      state: { selected: [] },
      target: "all",
      target_tile_ids: [],
      datasource_id: 5,
    };

    expect(await searchFilterValues(onTable, "we")).toEqual({ values: ["West"], capped: false });
    await searchFilterValues(onTable, "we");
    expect(computeAdHocMock).toHaveBeenCalledTimes(1);
    const [source, payload] = computeAdHocMock.mock.calls[0];
    expect(source).toEqual({ source_type: "table", table_id: 5 });
    expect(payload.workflow[0].filters[0].rule).toEqual({
      type: "regexp",
      value: "we",
      caseSensitive: false,
    });

    const onChart = { ...onTable, datasource_id: null, datasource_viz_id: 2 };
    expect(await searchFilterValues(onChart, "")).toEqual({ values: ["North"], capped: false });
    expect(computeSavedMock.mock.calls[0][0]).toBe(2);

    await refresh(true);
    await searchFilterValues(onTable, "we");
    expect(computeAdHocMock).toHaveBeenCalledTimes(2);
  });

  it("lists a chart's fields as columns by their kind", () => {
    expect(
      fieldsAsColumns([
        { fid: "n", semanticType: "quantitative" },
        { fid: "o", semanticType: "ordinal" },
        { fid: "x" },
      ]),
    ).toEqual([
      { name: "n", dtype: "number" },
      { name: "o", dtype: "text" },
      { name: "x", dtype: "text" },
    ]);
  });
});
