import { describe, expect, it } from "vitest";
import { buildLayoutPreview } from "./layoutPreview";
import { EMPTY_DASHBOARD_LAYOUT, type DashboardLayout, type DashboardTile } from "../../types";

const tile = (partial: Partial<DashboardTile>): DashboardTile => ({
  id: "t",
  type: "viz",
  viz_id: 1,
  chart_index: 0,
  x: 0,
  y: 0,
  w: 12,
  h: 4,
  ...partial,
});

const layout = (tiles: DashboardTile[], grid = EMPTY_DASHBOARD_LAYOUT.grid): DashboardLayout => ({
  tiles,
  grid: { ...grid },
  filters: [],
});

describe("buildLayoutPreview", () => {
  it("returns nothing for an empty dashboard", () => {
    expect(buildLayoutPreview(layout([]))).toEqual([]);
  });

  it("maps grid units to percentages of the box", () => {
    const cols = EMPTY_DASHBOARD_LAYOUT.grid.cols;
    const half = cols / 2;
    const rects = buildLayoutPreview(
      layout([
        tile({ id: "a", x: 0, y: 0, w: half, h: 8 }),
        tile({ id: "b", type: "text", x: half, y: 8, w: half, h: 8 }),
      ]),
    );
    expect(rects[0]).toMatchObject({
      id: "a",
      type: "viz",
      left: 0,
      top: 0,
      width: 50,
      height: 50,
    });
    expect(rects[1]).toMatchObject({
      id: "b",
      type: "text",
      left: 50,
      top: 50,
      width: 50,
      height: 50,
    });
  });

  it("keeps a short layout from filling the whole box", () => {
    const [rect] = buildLayoutPreview(layout([tile({ x: 0, y: 0, w: 48, h: 2 })]));
    expect(rect.height).toBe(25);
  });

  it("upgrades legacy 12-column grids before measuring", () => {
    const legacy = layout([tile({ x: 6, y: 0, w: 6, h: 8 })], {
      cols: 12,
      row_height: 40,
      version: 1,
    });
    const [rect] = buildLayoutPreview(legacy);
    expect(rect).toMatchObject({ left: 50, width: 50 });
  });

  it("still draws tiles when the stored grid width is degenerate", () => {
    const broken = layout([tile({ x: 0, y: 0, w: 24, h: 8 })], {
      cols: 0,
      row_height: 40,
      version: 2,
    });
    const [rect] = buildLayoutPreview(broken);
    expect(rect).toMatchObject({ left: 0, width: 50 });
  });

  it("carries separator orientation with a horizontal default", () => {
    const rects = buildLayoutPreview(
      layout([
        tile({ id: "h", type: "separator", viz_id: null, h: 1 }),
        tile({ id: "v", type: "separator", viz_id: null, orientation: "vertical", w: 1, h: 8 }),
      ]),
    );
    expect(rects.map((r) => r.orientation)).toEqual(["horizontal", "vertical"]);
  });
});
