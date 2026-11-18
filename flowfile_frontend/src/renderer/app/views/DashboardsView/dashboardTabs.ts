import {
  DASHBOARD_TAB_TITLE_MAX,
  type DashboardLayout,
  type DashboardTab,
  type DashboardTile,
} from "../../types";
import { pruneFilterTargets } from "./filterPlacement";

/** A fresh layout-internal tab id. */
export const newTabId = (): string =>
  typeof crypto !== "undefined" && "randomUUID" in crypto
    ? `tab-${crypto.randomUUID()}`
    : `tab-${Math.random().toString(36).slice(2, 10)}`;

export const layoutTabs = (layout: DashboardLayout): DashboardTab[] => layout.tabs ?? [];

export const hasTabs = (layout: DashboardLayout): boolean => layoutTabs(layout).length > 0;

/** The tab to show: `wanted` when the layout has it, else the first tab; null without tabs. */
export const resolveTabId = (
  layout: DashboardLayout,
  wanted: string | null | undefined,
): string | null => {
  const tabs = layoutTabs(layout);
  if (!tabs.length) return null;
  return tabs.some((t) => t.id === wanted) ? (wanted as string) : tabs[0].id;
};

/** The tab a tile shows on: its own, else the first, so a tile on an unknown tab is never lost. */
export const tabOfTile = (layout: DashboardLayout, tile: DashboardTile): string | null =>
  resolveTabId(layout, tile.tab_id);

/** The tiles one tab shows; every tile on a layout without tabs. */
export const tilesOfTab = (layout: DashboardLayout, tabId: string | null): DashboardTile[] =>
  hasTabs(layout) ? layout.tiles.filter((t) => tabOfTile(layout, t) === tabId) : layout.tiles;

/** The title of the tab a tile is on, for lists that name tiles across tabs; null without tabs. */
export const tabTitleOfTile = (layout: DashboardLayout, tileId: string): string | null => {
  const tile = layout.tiles.find((t) => t.id === tileId);
  const tabId = tile ? tabOfTile(layout, tile) : null;
  return layoutTabs(layout).find((t) => t.id === tabId)?.title ?? null;
};

/** The layout a canvas draws for one tab: the whole layout with only that tab's tiles. */
export const tabCanvasLayout = (layout: DashboardLayout, tabId: string | null): DashboardLayout =>
  hasTabs(layout) ? { ...layout, tiles: tilesOfTab(layout, tabId) } : layout;

/**
 * Put a tab's canvas update back into the whole layout: the other tabs' tiles stay as they are, the
 * tab's own tiles are the canvas's (a tile it dropped is gone, a new one lands on the tab), and every
 * other field is the canvas's.
 */
export const mergeTabCanvas = (
  layout: DashboardLayout,
  tabId: string | null,
  canvas: DashboardLayout,
): DashboardLayout => {
  if (!hasTabs(layout) || tabId == null) return canvas;
  const fromCanvas = new Map(canvas.tiles.map((t) => [t.id, t]));
  const onTab = (t: DashboardTile): DashboardTile =>
    t.tab_id === tabId ? t : { ...t, tab_id: tabId };
  const tiles: DashboardTile[] = [];
  const kept = new Set<string>();
  for (const tile of layout.tiles) {
    if (tabOfTile(layout, tile) !== tabId) {
      tiles.push(tile);
      continue;
    }
    const next = fromCanvas.get(tile.id);
    if (next) {
      tiles.push(onTab(next));
      kept.add(tile.id);
    }
  }
  for (const tile of canvas.tiles) if (!kept.has(tile.id)) tiles.push(onTab(tile));
  return { ...canvas, tabs: layout.tabs, tiles };
};

/** The first row below the given tiles in columns `x` to `x + w`; below all of them without a span. */
export const firstFreeRow = (tiles: DashboardTile[], span?: { x: number; w: number }): number => {
  const under = span ? tiles.filter((t) => t.x < span.x + span.w && span.x < t.x + t.w) : tiles;
  return under.length ? Math.max(...under.map((t) => t.y + t.h)) : 0;
};

const withoutTab = (tile: DashboardTile): DashboardTile => {
  const copy = { ...tile };
  delete copy.tab_id;
  return copy;
};

/**
 * Store every tile on the tab it shows on: a tile on an unknown tab (or none) moves to the first free
 * rows of the first tab, and on a layout without tabs no tile names one. The same layout comes back
 * when nothing changed.
 */
export const healTabs = (layout: DashboardLayout): DashboardLayout => {
  const tabs = layoutTabs(layout);
  if (!tabs.length) {
    if (!layout.tiles.some((t) => t.tab_id != null)) return layout;
    return { ...layout, tiles: layout.tiles.map((t) => (t.tab_id != null ? withoutTab(t) : t)) };
  }
  const known = new Set(tabs.map((t) => t.id));
  const strays = layout.tiles.filter((t) => t.tab_id == null || !known.has(t.tab_id));
  if (!strays.length) return layout;
  const first = tabs[0].id;
  let row = firstFreeRow(layout.tiles.filter((t) => t.tab_id === first));
  const healed = new Map<string, DashboardTile>();
  for (const tile of strays) {
    healed.set(tile.id, { ...tile, tab_id: first, y: row });
    row += tile.h;
  }
  return { ...layout, tiles: layout.tiles.map((t) => healed.get(t.id) ?? t) };
};

const nextTabTitle = (tabs: DashboardTab[]): string => {
  const titles = new Set(tabs.map((t) => t.title));
  let n = tabs.length + 1;
  while (titles.has(`Tab ${n}`)) n += 1;
  return `Tab ${n}`;
};

/**
 * Add a tab at the end and return it with the layout. The first use turns the canvas into the first
 * tab (every tile moves onto it) and adds a second, empty one.
 */
export const addTab = (
  layout: DashboardLayout,
  makeId: () => string = newTabId,
): { layout: DashboardLayout; tabId: string } => {
  const tabs = layoutTabs(layout);
  if (!tabs.length) {
    const first: DashboardTab = { id: makeId(), title: "Tab 1" };
    const second: DashboardTab = { id: makeId(), title: "Tab 2" };
    return {
      layout: {
        ...layout,
        tabs: [first, second],
        tiles: layout.tiles.map((t) => ({ ...t, tab_id: first.id })),
      },
      tabId: second.id,
    };
  }
  const tab: DashboardTab = { id: makeId(), title: nextTabTitle(tabs) };
  return { layout: { ...layout, tabs: [...tabs, tab] }, tabId: tab.id };
};

/** Rename a tab; a blank title leaves it as it is. */
export const renameTab = (
  layout: DashboardLayout,
  tabId: string,
  title: string,
): DashboardLayout => {
  const next = title.trim().slice(0, DASHBOARD_TAB_TITLE_MAX);
  const tabs = layoutTabs(layout);
  const tab = tabs.find((t) => t.id === tabId);
  if (!next || !tab || tab.title === next) return layout;
  return { ...layout, tabs: tabs.map((t) => (t.id === tabId ? { ...t, title: next } : t)) };
};

/** Move a tab to another place in the order (clamped to the ends). */
export const moveTab = (
  layout: DashboardLayout,
  tabId: string,
  toIndex: number,
): DashboardLayout => {
  const tabs = layoutTabs(layout);
  const from = tabs.findIndex((t) => t.id === tabId);
  const to = Math.max(0, Math.min(toIndex, tabs.length - 1));
  if (from < 0 || from === to) return layout;
  const next = [...tabs];
  const [tab] = next.splice(from, 1);
  next.splice(to, 0, tab);
  return { ...layout, tabs: next };
};

/** Delete a tab and the tiles on it; filters stay, and a filter tile's filter goes back to the strip. */
export const deleteTab = (layout: DashboardLayout, tabId: string): DashboardLayout => {
  const tabs = layoutTabs(layout);
  if (!tabs.some((t) => t.id === tabId)) return layout;
  const tiles = layout.tiles.filter((t) => tabOfTile(layout, t) !== tabId);
  return pruneFilterTargets({ ...layout, tabs: tabs.filter((t) => t.id !== tabId), tiles });
};

/** Go back to one canvas, keeping every tile: each tab's tiles go below the tiles of the tabs before it. */
export const dissolveTabs = (layout: DashboardLayout): DashboardLayout => {
  const tabs = layoutTabs(layout);
  if (!tabs.length) return layout;
  const tiles: DashboardTile[] = [];
  for (const tab of tabs) {
    const offset = firstFreeRow(tiles);
    for (const tile of tilesOfTab(layout, tab.id)) {
      tiles.push({ ...withoutTab(tile), y: tile.y + offset });
    }
  }
  return { ...layout, tabs: [], tiles };
};

/** Move a tile to another tab, onto the first free row there in its own columns. */
export const moveTileToTab = (
  layout: DashboardLayout,
  tileId: string,
  tabId: string,
): DashboardLayout => {
  if (!layoutTabs(layout).some((t) => t.id === tabId)) return layout;
  const tile = layout.tiles.find((t) => t.id === tileId);
  if (!tile || tabOfTile(layout, tile) === tabId) return layout;
  const y = firstFreeRow(tilesOfTab(layout, tabId), tile);
  return {
    ...layout,
    tiles: layout.tiles.map((t) => (t.id === tileId ? { ...t, tab_id: tabId, y } : t)),
  };
};
