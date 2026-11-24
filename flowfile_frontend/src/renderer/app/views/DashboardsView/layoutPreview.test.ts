import { describe, expect, it } from "vitest";
import { buildLayoutPreview, previewRows } from "./layoutPreview";
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

const ROWS = previewRows(40);
const rectsOf = (l: DashboardLayout) => buildLayoutPreview(l).rects;

const layout = (tiles: DashboardTile[], grid = EMPTY_DASHBOARD_LAYOUT.grid): DashboardLayout => ({
  tiles,
  grid: { ...grid },
  filters: [],
});

describe("buildLayoutPreview", () => {
  it("marks an image tile that holds a picture, even with its bytes left out", () => {
    const rects = rectsOf(
      layout([
        tile({ id: "logo", type: "image", viz_id: null, image: { data_url: "" } }),
        tile({ id: "empty", type: "image", viz_id: null, image: null, x: 12 }),
        tile({ id: "chart", x: 24 }),
      ]),
    );
    expect(rects.map((r) => r.hasImage)).toEqual([true, false, false]);
  });

  it("returns nothing for an empty dashboard", () => {
    expect(buildLayoutPreview(layout([]))).toEqual({ rects: [], clipped: false });
  });

  it("maps columns to the box width and rows to the dashboard's first screen", () => {
    const cols = EMPTY_DASHBOARD_LAYOUT.grid.cols;
    const half = cols / 2;
    const rects = rectsOf(
      layout([
        tile({ id: "a", x: 0, y: 0, w: half, h: 5 }),
        tile({ id: "b", type: "text", x: half, y: 5, w: half, h: 5 }),
      ]),
    );
    expect(rects[0]).toMatchObject({ id: "a", type: "viz", left: 0, top: 0, width: 50 });
    expect(rects[0].height).toBeCloseTo((5 / ROWS) * 100);
    expect(rects[1]).toMatchObject({ id: "b", type: "text", left: 50, width: 50 });
    expect(rects[1].top).toBeCloseTo((5 / ROWS) * 100);
  });

  it("draws every dashboard at one scale, so a short layout keeps its shape", () => {
    const short = rectsOf(layout([tile({ h: 2 })]))[0];
    const long = rectsOf(layout([tile({ id: "x", h: 2 }), tile({ id: "y", y: 60, h: 8 })]))[0];
    expect(short.height).toBeCloseTo(long.height);
  });

  it("cuts a long dashboard at the box's bottom and says so", () => {
    const fits = buildLayoutPreview(layout([tile({ h: 4 })]));
    expect(fits.clipped).toBe(false);
    const long = buildLayoutPreview(
      layout([
        tile({ id: "top", h: 4 }),
        tile({ id: "fold", y: 10, h: 20 }),
        tile({ id: "below", y: 40 }),
      ]),
    );
    expect(long.clipped).toBe(true);
    expect(long.rects.map((r) => r.id)).toEqual(["top", "fold"]);
  });

  it("upgrades legacy 12-column grids before measuring", () => {
    const legacy = layout([tile({ x: 6, y: 0, w: 6, h: 8 })], {
      cols: 12,
      row_height: 40,
      version: 1,
    });
    const [rect] = rectsOf(legacy);
    expect(rect).toMatchObject({ left: 50, width: 50 });
  });

  it("still draws tiles when the stored grid width is degenerate", () => {
    const broken = layout([tile({ x: 0, y: 0, w: 24, h: 8 })], {
      cols: 0,
      row_height: 40,
      version: 2,
    });
    const [rect] = rectsOf(broken);
    expect(rect).toMatchObject({ left: 0, width: 50 });
  });

  it("carries separator orientation with a horizontal default", () => {
    const rects = rectsOf(
      layout([
        tile({ id: "h", type: "separator", viz_id: null, h: 1 }),
        tile({ id: "v", type: "separator", viz_id: null, orientation: "vertical", w: 1, h: 8 }),
      ]),
    );
    expect(rects.map((r) => r.orientation)).toEqual(["horizontal", "vertical"]);
  });

  it("carries a KPI's trend and a text or KPI tile's own colours", () => {
    const rects = rectsOf(
      layout([
        tile({
          id: "k",
          type: "kpi",
          kpi: { field: null, agg: "count", trend: { field: "d", grain: "auto" } },
          bg_color: "#000",
          text_color: "#fff",
        }),
        tile({ id: "c", bg_color: "#123" }),
      ]),
    );
    expect(rects[0]).toMatchObject({ hasTrend: true, bgColor: "#000", textColor: "#fff" });
    expect(rects[1]).toMatchObject({ hasTrend: false, bgColor: null, textColor: null });
  });

  it("previews the first tab of a dashboard with tabs", () => {
    const tabbed: DashboardLayout = {
      ...layout([tile({ id: "second", tab_id: "b", h: 20 }), tile({ id: "first", tab_id: "a" })]),
      tabs: [
        { id: "a", title: "Overview" },
        { id: "b", title: "Detail" },
      ],
    };
    expect(rectsOf(tabbed).map((r) => r.id)).toEqual(["first"]);
  });
});
