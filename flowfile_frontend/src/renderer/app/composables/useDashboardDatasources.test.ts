import { beforeEach, describe, expect, it, vi } from "vitest";
import { ref } from "vue";
import type { DashboardLayout } from "../types";

const { getVisualizationMock, getTableMock, getTableColumnStatsMock, getFieldsMock } = vi.hoisted(
  () => ({
    getVisualizationMock: vi.fn(),
    getTableMock: vi.fn(),
    getTableColumnStatsMock: vi.fn(),
    getFieldsMock: vi.fn(),
  }),
);

vi.mock("../api/catalog.api", () => ({
  CatalogApi: {
    getVisualization: getVisualizationMock,
    getTable: getTableMock,
    getTableColumnStats: getTableColumnStatsMock,
    getSavedVisualizationFields: getFieldsMock,
  },
}));

import { useDashboardDatasources } from "./useDashboardDatasources";

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

    expect(tilesByDatasource.value).toEqual({ 5: [TILE_ID, "tile-kpi", "tile-kpi-labelled"] });
    expect(vizTileIds.value).toEqual([TILE_ID, "tile-kpi", "tile-kpi-labelled"]);
    expect(tileDatasource("tile-kpi")).toBe(5);
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
    expect(tileDatasource(TILE_ID)).toBe(5);

    let resolveViz!: (value: typeof vizResponse) => void;
    getVisualizationMock.mockImplementation(() => new Promise((resolve) => (resolveViz = resolve)));

    const pending = refresh(true);
    expect(tileDatasource(TILE_ID)).toBe(5);
    await Promise.resolve();
    expect(tileDatasource(TILE_ID)).toBe(5);

    resolveViz(vizResponse);
    await pending;
    expect(tileDatasource(TILE_ID)).toBe(5);
  });

  it("keeps the previous mapping when a force refetch fails", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const { refresh, tileDatasource } = useDashboardDatasources(ref(layoutOf()));
    await flush();
    expect(tileDatasource(TILE_ID)).toBe(5);

    getVisualizationMock.mockRejectedValue(new Error("boom"));
    await refresh(true);

    expect(tileDatasource(TILE_ID)).toBe(5);
    warn.mockRestore();
  });
});
