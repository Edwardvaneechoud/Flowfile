import { describe, expect, it } from "vitest";
import { upgradeLayoutGrid } from "./gridVersion";
import { DASHBOARD_GRID_COLS, EMPTY_DASHBOARD_LAYOUT, type DashboardLayout } from "../../types";

const v1 = (): DashboardLayout => ({
  tiles: [
    { id: "a", type: "viz", viz_id: 1, chart_index: 0, x: 6, y: 0, w: 6, h: 6 },
    { id: "b", type: "text", viz_id: null, chart_index: 0, x: 0, y: 6, w: 12, h: 3 },
  ],
  grid: { cols: 12, row_height: 40, version: 1 },
  filters: [],
});

describe("upgradeLayoutGrid", () => {
  it("scales v1 tiles onto the 48-column grid without moving them", () => {
    const out = upgradeLayoutGrid(v1());
    expect(out.grid).toEqual({ cols: DASHBOARD_GRID_COLS, row_height: 40, version: 2 });
    expect(out.tiles[0]).toMatchObject({ x: 24, w: 24, y: 0, h: 6 });
    expect(out.tiles[1]).toMatchObject({ x: 0, w: 48, y: 6, h: 3 });
  });

  it("returns a current layout untouched", () => {
    const current = { ...EMPTY_DASHBOARD_LAYOUT, tiles: [] };
    expect(upgradeLayoutGrid(current)).toBe(current);
  });

  it("is idempotent", () => {
    const once = upgradeLayoutGrid(v1());
    expect(upgradeLayoutGrid(once)).toBe(once);
  });

  it("never shrinks a tile below one column", () => {
    const layout = v1();
    layout.grid = { cols: 96, row_height: 40, version: 1 };
    layout.tiles[0] = { ...layout.tiles[0], w: 1 };
    expect(upgradeLayoutGrid(layout).tiles[0].w).toBe(1);
  });
});
