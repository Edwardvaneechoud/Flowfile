import { describe, expect, it } from "vitest";
import {
  EMPTY_DASHBOARD_LAYOUT,
  type DashboardFilter,
  type DashboardLayout,
  type DashboardTile,
} from "../../types";
import {
  FILTER_TILE_DEFAULT_SIZE,
  addPlacedFilter,
  deleteFilter,
  filtersWithoutSourceTiles,
  filterOfTile,
  moveFilterToBar,
  placeFilter,
  placedFilterIds,
  pruneFilterTargets,
  targetsNoTile,
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

describe("addPlacedFilter", () => {
  const base = layout([chart], [filter("a")]);

  it("adds the filter and its tile at a free cell in one change", () => {
    const next = addPlacedFilter(base, filter("new"), { x: 30, y: 0 }, "t-new");
    expect(ids(next.filters)).toEqual(["a", "new"]);
    expect(next.tiles[1]).toMatchObject({
      id: "t-new",
      type: "filter",
      filter_id: "new",
      x: 30,
      y: 0,
      ...FILTER_TILE_DEFAULT_SIZE,
    });
    expect(base.filters).toHaveLength(1);
  });

  it("moves a tile dropped on a taken cell under the tiles in its columns", () => {
    const next = addPlacedFilter(base, filter("new"), { x: 4, y: 2 }, "t-new");
    expect(next.tiles[1]).toMatchObject({ x: 4, y: chart.y + chart.h });
    expect(next.tiles[0]).toBe(chart);
  });

  it("takes the next free row for a negative y, also on an empty dashboard", () => {
    expect(addPlacedFilter(base, filter("new"), { x: 0, y: -1 }, "n").tiles[1].y).toBe(6);
    const empty = layout([], []);
    const next = addPlacedFilter(empty, filter("new"), { x: 0, y: -1 }, "n");
    expect(ids(next.filters)).toEqual(["new"]);
    expect(next.tiles[0]).toMatchObject({ x: 0, y: 0, filter_id: "new" });
  });

  it("keeps the tile inside the grid and never adds a filter id twice", () => {
    const next = addPlacedFilter(base, filter("new"), { x: 100, y: 20 }, "n");
    expect(next.tiles[1].x).toBe(base.grid.cols - FILTER_TILE_DEFAULT_SIZE.w);
    expect(addPlacedFilter(base, filter("a"), { x: 0, y: 0 }, "n")).toBe(base);
  });
});

describe("filtersWithoutSourceTiles", () => {
  const name = (key: string) => `<${key}>`;

  it("flags a filter on a source no tile reads, with the source's name", () => {
    const hints = filtersWithoutSourceTiles([filter("a")], ["table:2"], name);
    expect(Object.keys(hints)).toEqual(["a"]);
    expect(hints.a).toContain("<table:1>");
    expect(filtersWithoutSourceTiles([filter("a")], ["table:2", "table:1"], name)).toEqual({});
  });

  it("matches a SQL chart's filter on its chart, and flags any filter on an empty dashboard", () => {
    const onChart = { ...filter("q"), datasource_id: null, datasource_viz_id: 9 };
    expect(filtersWithoutSourceTiles([onChart], ["viz:9"], name)).toEqual({});
    expect(Object.keys(filtersWithoutSourceTiles([onChart, filter("a")], [], name))).toEqual([
      "q",
      "a",
    ]);
  });

  it("stays quiet while a tile's source is not known yet", () => {
    expect(filtersWithoutSourceTiles([filter("a")], ["table:2", null], name)).toEqual({});
  });

  it("flags a scope-all filter only when no tile reads data", () => {
    const all = { ...filter("a"), scope: "all" as const };
    expect(filtersWithoutSourceTiles([all], ["table:2"], name)).toEqual({});
    expect(filtersWithoutSourceTiles([all], [], name).a).toContain("reads data");
  });

  it("leaves untied filters and filters whose chosen tiles are gone to their own hints", () => {
    const untied = { ...filter("u"), datasource_id: null };
    const none = { ...filter("n"), target: "tiles" as const, target_tile_ids: [] };
    expect(filtersWithoutSourceTiles([untied, none], [], name)).toEqual({});
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

describe("pruneFilterTargets", () => {
  const kpi: DashboardTile = { ...chart, id: "t-kpi", type: "kpi", x: 24 };
  const targeting = (id: string, targets: string[]): DashboardFilter => ({
    ...filter(id),
    target: "tiles",
    target_tile_ids: targets,
  });

  it("drops the id of a removed tile from every filter that targets it", () => {
    const l = layout([kpi], [targeting("a", ["t-chart", "t-kpi"]), targeting("b", ["t-chart"])]);
    const next = pruneFilterTargets(l);
    expect(next.filters.map((f) => f.target_tile_ids)).toEqual([["t-kpi"], []]);
    expect(next.tiles).toBe(l.tiles);
  });

  it("leaves a filter whose last target went filtering no tile, not every tile", () => {
    const next = pruneFilterTargets(layout([kpi], [targeting("a", ["t-chart"])]));
    expect(next.filters[0]).toMatchObject({ target: "tiles", target_tile_ids: [] });
    expect(targetsNoTile(next.filters[0])).toBe(true);
    expect(targetsNoTile(filter("x"))).toBe(false);
  });

  it("returns the same layout and filters when every target is still there", () => {
    const l = layout([chart, kpi], [targeting("a", ["t-chart"]), filter("b")]);
    expect(pruneFilterTargets(l)).toBe(l);
  });

  it("heals a stale id with any later change, whatever removed the tile", () => {
    const stale = layout([chart], [targeting("a", ["gone", "t-chart"])]);
    const placed = placeFilter(stale, "a", { x: 0, y: -1 }, "ft");
    expect(pruneFilterTargets(placed).filters[0].target_tile_ids).toEqual(["t-chart"]);
  });
});
