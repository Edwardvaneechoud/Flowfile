import type { DashboardFilter, DashboardTile } from "../../types";
import { dataUrlInfo, imageTileShows } from "./imageTile";
import { isKpiComplete } from "./kpi";

/** Below this canvas width (px) the viewer lays its tiles out in one column. */
export const STACK_BREAKPOINT_PX = 640;

/** One tile's place in the grid, as grid-layout-plus takes it. */
export interface StackedItem {
  i: string;
  x: number;
  y: number;
  w: number;
  h: number;
}

export interface StackOptions {
  /** The dashboard's filters, so a filter tile whose filter is gone (blank to viewers) is left out. */
  filters?: DashboardFilter[];
  /** The canvas width the text is measured against; a narrower column wraps more lines. */
  widthPx?: number;
  /** The grid's row height and margin (px), to turn a text's estimated height into rows. */
  rowHeight?: number;
  margin?: number;
  /** The drawn pictures' own sizes by tile id, so an image tile takes the height its picture needs. */
  imageSizes?: Record<string, ImageSize>;
}

/** A picture's own size in px (`naturalWidth`/`naturalHeight`). */
export interface ImageSize {
  width: number;
  height: number;
}

const KPI_ROWS = { min: 3, max: 4 };
const CHART_ROWS = 8;
const TABLE_ROWS = { min: 8, max: 12 };
const FILTER_ROWS = 2;

const hasSource = (tile: DashboardTile) => tile.viz_id != null || tile.table_id != null;

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));

/** Whether a tile shows anything in one column; blank ones would only leave a gap. */
function showsInColumn(tile: DashboardTile, filterIds: Set<string> | null): boolean {
  switch (tile.type) {
    case "separator":
      return tile.orientation !== "vertical";
    case "text":
      return !!tile.text_md?.trim();
    case "kpi":
      return hasSource(tile) && isKpiComplete(tile.kpi);
    case "table":
      return hasSource(tile);
    case "filter":
      return !!tile.filter_id && (filterIds == null || filterIds.has(tile.filter_id));
    case "image":
      return imageTileShows(tile);
    default:
      return true;
  }
}

/**
 * Rows an image tile needs to show its picture at the column's width: "Fit whole image" draws a
 * raster at most its own width, an SVG and "Fill the tile" at the column's. Never more rows than
 * the tile has in one column now, and that many while the picture's size is not known yet.
 */
export function imageRows(
  tile: DashboardTile,
  size: ImageSize | undefined,
  widthPx: number,
  rowHeight: number,
  margin: number,
): number {
  const most = clamp(tile.h, 1, CHART_ROWS);
  if (!size || !(size.width > 0) || !(size.height > 0)) return most;
  // The grid insets a full-width tile by its margin on both sides; the tile body pads 4px.
  const column = Math.max(1, widthPx - 2 * margin - 8);
  const fills =
    tile.image?.fit === "cover" ||
    dataUrlInfo(tile.image?.data_url ?? "")?.mime === "image/svg+xml";
  const drawnWidth = fills ? column : Math.min(size.width, column);
  const px = (drawnWidth * size.height) / size.width + 8;
  return clamp(Math.ceil((px + margin) / (rowHeight + margin)), 1, most);
}

/** Rows a text tile's rendered markdown needs at this width; an estimate, the tile scrolls past it. */
export function textRows(
  markdown: string,
  widthPx: number,
  rowHeight: number,
  margin: number,
): number {
  const charsPerLine = Math.max(20, Math.floor((widthPx - 28) / 7.5));
  let px = 20;
  for (const line of markdown.split("\n")) {
    const text = line.trim();
    if (!text) {
      px += 8;
      continue;
    }
    const heading = /^(#{1,6})\s/.exec(text);
    const lineHeight = heading ? (heading[1].length === 1 ? 33 : 28) : 21;
    px += Math.ceil(text.length / charsPerLine) * lineHeight;
  }
  return Math.max(1, Math.ceil((px + margin) / (rowHeight + margin)));
}

function stackedRows(
  tile: DashboardTile,
  options: Required<Omit<StackOptions, "filters" | "imageSizes">> & {
    imageSizes?: Record<string, ImageSize>;
  },
): number {
  switch (tile.type) {
    case "kpi":
      return clamp(tile.h, KPI_ROWS.min, KPI_ROWS.max);
    case "viz":
      return CHART_ROWS;
    case "table":
      return clamp(tile.h, TABLE_ROWS.min, TABLE_ROWS.max);
    case "filter":
      return FILTER_ROWS;
    case "separator":
      return 1;
    case "image":
      return imageRows(
        tile,
        options.imageSizes?.[tile.id],
        options.widthPx,
        options.rowHeight,
        options.margin,
      );
    case "text":
      return textRows(tile.text_md ?? "", options.widthPx, options.rowHeight, options.margin);
    default:
      return tile.h;
  }
}

/**
 * The saved tiles laid out in one full-width column for a narrow screen: in reading order (top to
 * bottom, then left to right), each with a height that suits its type. Vertical separators and
 * tiles a viewer sees blank are left out. The saved tiles are not changed.
 */
export function stackTiles(
  tiles: DashboardTile[],
  cols: number,
  options: StackOptions = {},
): StackedItem[] {
  const filterIds = options.filters ? new Set(options.filters.map((f) => f.id)) : null;
  const sizing = {
    widthPx: options.widthPx ?? 360,
    rowHeight: options.rowHeight ?? 40,
    margin: options.margin ?? 8,
    imageSizes: options.imageSizes,
  };
  const ordered = tiles
    .filter((t) => showsInColumn(t, filterIds))
    .sort((a, b) => a.y - b.y || a.x - b.x);
  let y = 0;
  return ordered.map((tile) => {
    const h = stackedRows(tile, sizing);
    const item = { i: tile.id, x: 0, y, w: cols, h };
    y += h;
    return item;
  });
}
