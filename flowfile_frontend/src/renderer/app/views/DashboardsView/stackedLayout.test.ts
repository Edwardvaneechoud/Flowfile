import { describe, expect, it, vi } from "vitest";

vi.mock("../../api/catalog.api", () => ({ CatalogApi: {} }));
vi.mock("../../composables/useGraphicWalkerCompute", () => ({ useGraphicWalkerCompute: vi.fn() }));

import type { DashboardFilter, DashboardTile } from "../../types";
import { imageRows, stackTiles, textRows } from "./stackedLayout";

const tile = (t: Partial<DashboardTile> & Pick<DashboardTile, "id" | "type">): DashboardTile => ({
  viz_id: null,
  chart_index: 0,
  x: 0,
  y: 0,
  w: 12,
  h: 4,
  ...t,
});

const filter = (id: string): DashboardFilter => ({
  id,
  field_name: "region",
  kind: "categorical",
  state: {},
  target: "all",
  target_tile_ids: [],
  datasource_id: null,
});

const kpi = { field: "amount", agg: "sum" as const, label: null, compact: false };
const logo = { data_url: "data:image/png;base64,iVBORw0KGgo=", alt: "Logo" };

describe("stackTiles", () => {
  it("orders tiles top to bottom, then left to right, each full width and one under the other", () => {
    const tiles = [
      tile({ id: "table", type: "table", table_id: 1, x: 0, y: 8, w: 24, h: 8 }),
      tile({ id: "kpi", type: "kpi", viz_id: 1, kpi, x: 24, y: 0, w: 12, h: 4 }),
      tile({ id: "chart", type: "viz", viz_id: 1, x: 0, y: 0, w: 24, h: 8 }),
      tile({ id: "text", type: "text", text_md: "## Notes", x: 24, y: 4, w: 24, h: 3 }),
    ];
    const stacked = stackTiles(tiles, 48);
    expect(stacked.map((s) => s.i)).toEqual(["chart", "kpi", "text", "table"]);
    expect(stacked.every((s) => s.x === 0 && s.w === 48)).toBe(true);
    let y = 0;
    for (const item of stacked) {
      expect(item.y).toBe(y);
      y += item.h;
    }
  });

  it("gives each type a height that suits it", () => {
    const filters = [filter("f1")];
    const rows = (t: DashboardTile) => stackTiles([t], 48, { filters })[0].h;
    expect(rows(tile({ id: "k", type: "kpi", viz_id: 1, kpi, h: 2 }))).toBe(3);
    expect(rows(tile({ id: "k", type: "kpi", viz_id: 1, kpi, h: 12 }))).toBe(4);
    expect(rows(tile({ id: "c", type: "viz", viz_id: 1, h: 3 }))).toBe(8);
    expect(rows(tile({ id: "c", type: "viz", viz_id: 1, h: 20 }))).toBe(8);
    expect(rows(tile({ id: "t", type: "table", table_id: 1, h: 4 }))).toBe(8);
    expect(rows(tile({ id: "t", type: "table", table_id: 1, h: 30 }))).toBe(12);
    expect(rows(tile({ id: "f", type: "filter", filter_id: "f1", h: 6 }))).toBe(2);
    expect(rows(tile({ id: "s", type: "separator", orientation: "horizontal", w: 48, h: 1 }))).toBe(
      1,
    );
    expect(rows(tile({ id: "i", type: "image", image: logo, h: 3 }))).toBe(3);
    expect(rows(tile({ id: "i", type: "image", image: logo, h: 20 }))).toBe(8);
  });

  it("sizes a text tile to its content", () => {
    const short = stackTiles([tile({ id: "t", type: "text", text_md: "## Notes", h: 10 })], 48)[0];
    const long = stackTiles(
      [tile({ id: "t", type: "text", text_md: `# Title\n\n${"word ".repeat(200)}`, h: 2 })],
      48,
    )[0];
    expect(short.h).toBeLessThanOrEqual(2);
    expect(long.h).toBeGreaterThan(5);
  });

  it("leaves out vertical separators and the tiles a viewer sees blank", () => {
    const filters = [filter("kept")];
    const tiles = [
      tile({ id: "vsep", type: "separator", orientation: "vertical", w: 2, h: 8 }),
      tile({ id: "hsep", type: "separator", y: 1, w: 48, h: 1 }),
      tile({ id: "kpi-unset", type: "kpi", viz_id: null, kpi: null, y: 2 }),
      tile({ id: "kpi-nofield", type: "kpi", viz_id: 1, kpi: { ...kpi, field: null }, y: 2 }),
      tile({ id: "table-unset", type: "table", y: 3 }),
      tile({ id: "text-empty", type: "text", text_md: "   ", y: 4 }),
      tile({ id: "filter-gone", type: "filter", filter_id: "gone", y: 5 }),
      tile({ id: "filter-kept", type: "filter", filter_id: "kept", y: 6 }),
      tile({ id: "image-unset", type: "image", image: null, y: 7 }),
      tile({ id: "image", type: "image", image: logo, y: 8 }),
    ];
    expect(stackTiles(tiles, 48, { filters }).map((s) => s.i)).toEqual([
      "hsep",
      "filter-kept",
      "image",
    ]);
  });

  it("never changes the saved tiles", () => {
    const tiles = [
      tile({ id: "b", type: "viz", viz_id: 1, x: 24, y: 0, w: 24, h: 6 }),
      tile({ id: "a", type: "viz", viz_id: 1, x: 0, y: 0, w: 24, h: 6 }),
    ];
    const before = JSON.parse(JSON.stringify(tiles));
    stackTiles(tiles, 48);
    expect(tiles).toEqual(before);
  });

  it("is empty for an empty dashboard", () => {
    expect(stackTiles([], 48)).toEqual([]);
  });
});

describe("imageRows", () => {
  // A 360 px canvas: a 336 px column inside the grid's margins and the tile's padding.
  const rowsOf = (t: DashboardTile, size?: { width: number; height: number }) =>
    imageRows(t, size, 360, 40, 8);

  it("gives a wide picture the height it needs at the column's width", () => {
    const wide = tile({ id: "w", type: "image", image: logo, h: 6 });
    // 900x150 drawn 336 px wide is 56 px tall: 64 px with padding, two rows.
    expect(rowsOf(wide, { width: 900, height: 150 })).toBe(2);
  });

  it("never draws a small raster past its own width, but fills with an SVG or Fill the tile", () => {
    const small = tile({ id: "s", type: "image", image: logo, h: 8 });
    expect(rowsOf(small, { width: 120, height: 120 })).toBe(3);
    const cover = tile({ id: "c", type: "image", image: { ...logo, fit: "cover" }, h: 8 });
    expect(rowsOf(cover, { width: 120, height: 120 })).toBe(8);
    const svg = { data_url: "data:image/svg+xml;base64,PHN2Zy8+" };
    expect(
      rowsOf(tile({ id: "v", type: "image", image: svg, h: 8 }), { width: 100, height: 50 }),
    ).toBe(4);
  });

  it("is never taller than the tile is now, nor before the picture's size is known", () => {
    const tall = tile({ id: "t", type: "image", image: logo, h: 6 });
    expect(rowsOf(tall, { width: 150, height: 600 })).toBe(6);
    expect(rowsOf(tall)).toBe(6);
    expect(rowsOf(tall, { width: 0, height: 0 })).toBe(6);
  });

  it("is what stackTiles uses when it is given the pictures' sizes", () => {
    const wide = tile({ id: "w", type: "image", image: logo, h: 6 });
    const stacked = stackTiles([wide], 48, {
      widthPx: 360,
      imageSizes: { w: { width: 900, height: 150 } },
    });
    expect(stacked[0].h).toBe(2);
  });
});

describe("textRows", () => {
  it("wraps more lines in a narrower column", () => {
    const text = "word ".repeat(80);
    expect(textRows(text, 300, 40, 8)).toBeGreaterThan(textRows(text, 600, 40, 8));
  });
});
