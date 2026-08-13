import { beforeEach, describe, expect, it, vi } from "vitest";
import { ref } from "vue";
import type { DashboardLayout } from "../types";

const { getVisualizationMock, getTableMock, getTableColumnStatsMock } = vi.hoisted(() => ({
  getVisualizationMock: vi.fn(),
  getTableMock: vi.fn(),
  getTableColumnStatsMock: vi.fn(),
}));

vi.mock("../api/catalog.api", () => ({
  CatalogApi: {
    getVisualization: getVisualizationMock,
    getTable: getTableMock,
    getTableColumnStats: getTableColumnStatsMock,
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
    getVisualizationMock.mockImplementation(
      () => new Promise((resolve) => (resolveViz = resolve)),
    );

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
