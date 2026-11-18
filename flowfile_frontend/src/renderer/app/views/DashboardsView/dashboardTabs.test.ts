import { describe, expect, it } from "vitest";
import {
  EMPTY_DASHBOARD_LAYOUT,
  type DashboardFilter,
  type DashboardLayout,
  type DashboardTile,
} from "../../types";
import {
  addTab,
  deleteTab,
  dissolveTabs,
  firstFreeRow,
  healTabs,
  mergeTabCanvas,
  moveTab,
  moveTileToTab,
  renameTab,
  resolveTabId,
  tabCanvasLayout,
  tabOfTile,
  tabTitleOfTile,
  tilesOfTab,
} from "./dashboardTabs";

const tile = (id: string, extra: Partial<DashboardTile> = {}): DashboardTile => ({
  id,
  type: "text",
  viz_id: null,
  chart_index: 0,
  x: 0,
  y: 0,
  w: 24,
  h: 4,
  ...extra,
});

const filter = (id: string, targets: string[] = []): DashboardFilter => ({
  id,
  field_name: id,
  kind: "categorical",
  state: { selected: [] },
  target: targets.length ? "tiles" : "all",
  target_tile_ids: targets,
  datasource_id: null,
});

const flat = (tiles: DashboardTile[], filters: DashboardFilter[] = []): DashboardLayout => ({
  ...EMPTY_DASHBOARD_LAYOUT,
  tiles,
  filters,
});

const tabbed = (): DashboardLayout => ({
  ...EMPTY_DASHBOARD_LAYOUT,
  tabs: [
    { id: "a", title: "Overview" },
    { id: "b", title: "Sales" },
  ],
  tiles: [
    tile("a1", { tab_id: "a" }),
    tile("b1", { tab_id: "b" }),
    tile("a2", { tab_id: "a", y: 4 }),
  ],
  filters: [],
});

const ids = (tiles: DashboardTile[]) => tiles.map((t) => t.id);

describe("resolveTabId and tabOfTile", () => {
  it("is null without tabs", () => {
    expect(resolveTabId(flat([]), "a")).toBeNull();
    expect(tabOfTile(flat([tile("x", { tab_id: "a" })]), tile("x", { tab_id: "a" }))).toBeNull();
  });

  it("keeps a known tab and falls back to the first for an unknown or missing one", () => {
    const layout = tabbed();
    expect(resolveTabId(layout, "b")).toBe("b");
    expect(resolveTabId(layout, "gone")).toBe("a");
    expect(resolveTabId(layout, null)).toBe("a");
    expect(tabOfTile(layout, tile("s", { tab_id: "gone" }))).toBe("a");
  });
});

describe("tilesOfTab and tabCanvasLayout", () => {
  it("gives every tile on a layout without tabs, the same layout to draw", () => {
    const layout = flat([tile("x"), tile("y")]);
    expect(ids(tilesOfTab(layout, null))).toEqual(["x", "y"]);
    expect(tabCanvasLayout(layout, null)).toBe(layout);
  });

  it("gives only one tab's tiles, a stray tile on the first tab", () => {
    const layout = tabbed();
    layout.tiles.push(tile("stray", { tab_id: "gone" }));
    expect(ids(tilesOfTab(layout, "a"))).toEqual(["a1", "a2", "stray"]);
    expect(ids(tilesOfTab(layout, "b"))).toEqual(["b1"]);
    const canvas = tabCanvasLayout(layout, "b");
    expect(ids(canvas.tiles)).toEqual(["b1"]);
    expect(canvas.filters).toBe(layout.filters);
  });

  it("names the tab a tile is on", () => {
    expect(tabTitleOfTile(tabbed(), "b1")).toBe("Sales");
    expect(tabTitleOfTile(flat([tile("x")]), "x")).toBeNull();
  });
});

describe("mergeTabCanvas", () => {
  it("is the canvas itself without tabs", () => {
    const layout = flat([tile("x")]);
    const canvas = flat([tile("x", { y: 3 })]);
    expect(mergeTabCanvas(layout, null, canvas)).toBe(canvas);
  });

  it("keeps the other tabs' tiles and takes the tab's own from the canvas", () => {
    const layout = tabbed();
    const canvas = tabCanvasLayout(layout, "a");
    const moved = { ...canvas, tiles: [{ ...canvas.tiles[0], y: 9 }] };
    const merged = mergeTabCanvas(layout, "a", moved);
    expect(ids(merged.tiles)).toEqual(["a1", "b1"]);
    expect(merged.tiles[0].y).toBe(9);
    expect(merged.tiles[1]).toBe(layout.tiles[1]);
    expect(merged.tabs).toBe(layout.tabs);
  });

  it("puts a new canvas tile on the tab and takes the canvas's filters", () => {
    const layout = tabbed();
    const canvas = tabCanvasLayout(layout, "b");
    const next = {
      ...canvas,
      tiles: [...canvas.tiles, tile("new", { y: 4 })],
      filters: [filter("f")],
    };
    const merged = mergeTabCanvas(layout, "b", next);
    expect(merged.tiles.find((t) => t.id === "new")?.tab_id).toBe("b");
    expect(ids(merged.tiles)).toEqual(["a1", "b1", "a2", "new"]);
    expect(merged.filters.map((f) => f.id)).toEqual(["f"]);
  });

  it("heals a stray tile the first tab's canvas hands back", () => {
    const layout = tabbed();
    layout.tiles.push(tile("stray", { tab_id: "gone" }));
    const merged = mergeTabCanvas(layout, "a", tabCanvasLayout(layout, "a"));
    expect(merged.tiles.find((t) => t.id === "stray")?.tab_id).toBe("a");
  });
});

describe("firstFreeRow", () => {
  it("is the row below the tiles, in a column span when one is given", () => {
    const tiles = [tile("l", { x: 0, w: 24, h: 4 }), tile("r", { x: 24, w: 24, h: 10 })];
    expect(firstFreeRow([])).toBe(0);
    expect(firstFreeRow(tiles)).toBe(10);
    expect(firstFreeRow(tiles, { x: 0, w: 24 })).toBe(4);
    expect(firstFreeRow(tiles, { x: 20, w: 10 })).toBe(10);
  });
});

describe("healTabs", () => {
  it("leaves a sound layout as it is", () => {
    const layout = tabbed();
    expect(healTabs(layout)).toBe(layout);
    const plain = flat([tile("x")]);
    expect(healTabs(plain)).toBe(plain);
  });

  it("moves a tile on an unknown or no tab to the first tab's free rows", () => {
    const layout = tabbed();
    layout.tiles.push(tile("gone", { tab_id: "gone", y: 0 }), tile("none", { y: 0, h: 2 }));
    const healed = healTabs(layout);
    const byId = Object.fromEntries(healed.tiles.map((t) => [t.id, t]));
    expect([byId.gone.tab_id, byId.gone.y]).toEqual(["a", 8]);
    expect([byId.none.tab_id, byId.none.y]).toEqual(["a", 12]);
    expect(byId.b1).toBe(layout.tiles[1]);
  });

  it("drops a tab id from the tiles of a layout without tabs", () => {
    const healed = healTabs(flat([tile("x", { tab_id: "a" }), tile("y")]));
    expect(healed.tiles.map((t) => "tab_id" in t)).toEqual([false, false]);
  });
});

describe("addTab", () => {
  it("turns the canvas into the first tab and adds an empty second one", () => {
    let n = 0;
    const { layout, tabId } = addTab(flat([tile("x"), tile("y")]), () => `t${++n}`);
    expect(layout.tabs).toEqual([
      { id: "t1", title: "Tab 1" },
      { id: "t2", title: "Tab 2" },
    ]);
    expect(layout.tiles.map((t) => t.tab_id)).toEqual(["t1", "t1"]);
    expect(tabId).toBe("t2");
    expect(tilesOfTab(layout, "t2")).toEqual([]);
  });

  it("adds a tab at the end with a title not in use", () => {
    const layout = {
      ...tabbed(),
      tabs: [
        { id: "a", title: "Tab 3" },
        { id: "b", title: "B" },
      ],
    };
    const { layout: next, tabId } = addTab(layout, () => "c");
    expect(next.tabs?.map((t) => [t.id, t.title])).toEqual([
      ["a", "Tab 3"],
      ["b", "B"],
      ["c", "Tab 4"],
    ]);
    expect(tabId).toBe("c");
    expect(next.tiles).toBe(layout.tiles);
  });
});

describe("renameTab and moveTab", () => {
  it("renames with a trimmed title and ignores a blank or unchanged one", () => {
    const layout = tabbed();
    expect(renameTab(layout, "b", "  Operations ").tabs?.[1].title).toBe("Operations");
    expect(renameTab(layout, "b", "   ")).toBe(layout);
    expect(renameTab(layout, "b", "Sales")).toBe(layout);
    expect(renameTab(layout, "b", "x".repeat(80)).tabs?.[1].title).toHaveLength(60);
  });

  it("moves a tab in the order, clamped to the ends", () => {
    const layout = { ...tabbed(), tabs: [...(tabbed().tabs ?? []), { id: "c", title: "C" }] };
    expect(moveTab(layout, "c", 0).tabs?.map((t) => t.id)).toEqual(["c", "a", "b"]);
    expect(moveTab(layout, "a", 9).tabs?.map((t) => t.id)).toEqual(["b", "c", "a"]);
    expect(moveTab(layout, "a", 0)).toBe(layout);
    expect(moveTab(layout, "gone", 1)).toBe(layout);
  });
});

describe("deleteTab", () => {
  it("removes the tab with its tiles and the filter targets on them", () => {
    const layout = { ...tabbed(), filters: [filter("f", ["a1", "b1"])] };
    const next = deleteTab(layout, "a");
    expect(next.tabs?.map((t) => t.id)).toEqual(["b"]);
    expect(ids(next.tiles)).toEqual(["b1"]);
    expect(next.filters[0].target_tile_ids).toEqual(["b1"]);
    expect(deleteTab(layout, "gone")).toBe(layout);
  });
});

describe("dissolveTabs", () => {
  it("keeps every tile on one canvas, each tab below the one before", () => {
    const next = dissolveTabs(tabbed());
    expect(next.tabs).toEqual([]);
    expect(next.tiles.map((t) => [t.id, t.y, "tab_id" in t])).toEqual([
      ["a1", 0, false],
      ["a2", 4, false],
      ["b1", 8, false],
    ]);
  });

  it("keeps the last tab's tiles where they are", () => {
    const one = deleteTab(tabbed(), "a");
    expect(dissolveTabs(one).tiles.map((t) => [t.id, t.y])).toEqual([["b1", 0]]);
  });
});

describe("moveTileToTab", () => {
  it("moves a tile onto the first free row of the other tab in its columns", () => {
    const layout = tabbed();
    const next = moveTileToTab(layout, "a2", "b");
    const moved = next.tiles.find((t) => t.id === "a2");
    expect([moved?.tab_id, moved?.y]).toEqual(["b", 4]);
    expect(ids(tilesOfTab(next, "a"))).toEqual(["a1"]);
  });

  it("lands at the top of an empty tab and ignores a move to the same or an unknown tab", () => {
    const { layout } = addTab(tabbed(), () => "c");
    const moved = moveTileToTab(layout, "a2", "c").tiles.find((t) => t.id === "a2");
    expect([moved?.tab_id, moved?.y]).toEqual(["c", 0]);
    expect(moveTileToTab(layout, "a2", "a")).toBe(layout);
    expect(moveTileToTab(layout, "a2", "gone")).toBe(layout);
  });
});
