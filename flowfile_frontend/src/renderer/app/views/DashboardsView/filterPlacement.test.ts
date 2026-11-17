import { describe, expect, it } from "vitest";
import {
  EMPTY_DASHBOARD_LAYOUT,
  type DashboardFilter,
  type DashboardLayout,
  type DashboardTile,
} from "../../types";
import {
  FILTER_TILE_DEFAULT_SIZE,
  deleteFilter,
  filterOfTile,
  moveFilterToBar,
  placeFilter,
  placedFilterIds,
  unplacedFilters,
  withFilters,
} from "./filterPlacement";

const filter = (id: string): DashboardFilter => ({
  id,
  field_name: id,
  kind: "categorical",
  state: { selected: [] },
  target: "all",
  target_tile_ids: [],
  datasource_id: 1,
});

const chart: DashboardTile = {
  id: "t-chart",
  type: "viz",
  viz_id: 7,
  chart_index: 0,
  x: 0,
  y: 0,
  w: 24,
  h: 6,
};

const filterTile = (id: string, filterId: string, y = 6): DashboardTile => ({
  id,
  type: "filter",
  viz_id: null,
  chart_index: 0,
  filter_id: filterId,
  x: 0,
  y,
  ...FILTER_TILE_DEFAULT_SIZE,
});

const layout = (tiles: DashboardTile[], filters: DashboardFilter[]): DashboardLayout => ({
  ...EMPTY_DASHBOARD_LAYOUT,
  tiles,
  filters,
});

const ids = (items: { id: string }[]) => items.map((x) => x.id);

describe("placedFilterIds / unplacedFilters", () => {
  it("lists the filters a tile shows and leaves the rest to the strip, in order", () => {
    const l = layout([chart, filterTile("ft", "b")], [filter("a"), filter("b"), filter("c")]);
    expect([...placedFilterIds(l)]).toEqual(["b"]);
    expect(ids(unplacedFilters(l))).toEqual(["a", "c"]);
  });

  it("does not count a tile whose filter is gone", () => {
    const l = layout([filterTile("ft", "gone")], [filter("a")]);
    expect(placedFilterIds(l).size).toBe(0);
    expect(ids(unplacedFilters(l))).toEqual(["a"]);
  });

  it("finds a tile's filter, and none for a chart", () => {
    const filters = [filter("a")];
    expect(filterOfTile(filterTile("ft", "a"), filters)?.id).toBe("a");
    expect(filterOfTile(filterTile("ft", "gone"), filters)).toBeNull();
    expect(filterOfTile(chart, filters)).toBeNull();
  });
});

describe("placeFilter", () => {
  const base = layout([chart], [filter("a"), filter("b")]);

  it("adds one filter tile at the cell", () => {
    const next = placeFilter(base, "a", { x: 4, y: 2 }, "new");
    expect(next.tiles).toHaveLength(2);
    expect(next.tiles[1]).toMatchObject({
      id: "new",
      type: "filter",
      filter_id: "a",
      viz_id: null,
      x: 4,
      y: 2,
      ...FILTER_TILE_DEFAULT_SIZE,
    });
    expect(ids(unplacedFilters(next))).toEqual(["b"]);
    expect(base.tiles).toHaveLength(1);
  });

  it("takes the next free row for a negative y and keeps the tile inside the grid", () => {
    const next = placeFilter(base, "a", { x: 100, y: -1 }, "new");
    expect(next.tiles[1]).toMatchObject({ y: 6, x: base.grid.cols - FILTER_TILE_DEFAULT_SIZE.w });
    expect(placeFilter(layout([], [filter("a")]), "a", { x: 0, y: -1 }, "n").tiles[0].y).toBe(0);
  });

  it("places a filter at most once, and never an unknown one", () => {
    const once = placeFilter(base, "a", { x: 0, y: 0 }, "one");
    expect(placeFilter(once, "a", { x: 0, y: 4 }, "two")).toBe(once);
    expect(placeFilter(base, "gone", { x: 0, y: 0 }, "x")).toBe(base);
  });
});

describe("moveFilterToBar", () => {
  it("removes the tile and keeps the filter with its state", () => {
    const withState = { ...filter("a"), state: { selected: ["West"] } };
    const l = layout([chart, filterTile("ft", "a")], [withState]);
    const next = moveFilterToBar(l, "a");
    expect(ids(next.tiles)).toEqual(["t-chart"]);
    expect(next.filters).toEqual([withState]);
    expect(ids(unplacedFilters(next))).toEqual(["a"]);
  });

  it("leaves a layout with nothing to move as it is", () => {
    const l = layout([chart], [filter("a")]);
    expect(moveFilterToBar(l, "a")).toBe(l);
  });
});

describe("deleteFilter / withFilters", () => {
  it("deleting a placed filter removes its tile", () => {
    const l = layout(
      [chart, filterTile("ft-a", "a"), filterTile("ft-b", "b", 8)],
      [filter("a"), filter("b")],
    );
    const next = deleteFilter(l, "a");
    expect(ids(next.filters)).toEqual(["b"]);
    expect(ids(next.tiles)).toEqual(["t-chart", "ft-b"]);
  });

  it("deleting a filter from the strip leaves the canvas alone", () => {
    const l = layout([chart, filterTile("ft-b", "b")], [filter("a"), filter("b")]);
    const next = deleteFilter(l, "a");
    expect(next.tiles).toBe(l.tiles);
    expect(ids(next.filters)).toEqual(["b"]);
  });

  it("a new filter list drops the tiles of filters it no longer holds", () => {
    const l = layout([chart, filterTile("ft-a", "a")], [filter("a")]);
    const edited = { ...filter("a"), label: "Region" };
    expect(withFilters(l, [edited]).tiles).toBe(l.tiles);
    expect(ids(withFilters(l, []).tiles)).toEqual(["t-chart"]);
  });
});
