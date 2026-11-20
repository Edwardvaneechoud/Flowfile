// The AI dashboard builder's pure parts: the page a build produces, its tiles and where they land.

import type { IChart } from "@kanaries/graphic-walker/interfaces";

import type {
  DashboardPlanResponse,
  DashboardStoryResponse,
  PlannedFilter,
  PlannedKpi,
} from "../../api/ai.api";
import {
  DASHBOARD_TAB_TITLE_MAX,
  type DashboardFilter,
  type DashboardFilterKind,
  type DashboardLayout,
  type DashboardTile,
} from "../../types";
import { requestFailureMessage, type ChartPromptMessage } from "../CatalogView/chartPrompt";
import { addTab, hasTabs, newTabId, renameTab } from "./dashboardTabs";
import { DEFAULT_KPI, KPI_DEFAULT_SIZE } from "./kpi";

/** A chart the build kept, in reading order, ready to be saved. */
export interface BuiltChart {
  id: string;
  tableId: number;
  title: string;
  /** One sentence on what the chart shows; null when the story was not written. */
  finding: string | null;
  width: "half" | "full";
  chart: IChart;
}

export interface PageNote {
  title: string;
  reason: string;
}

/** What one build produced; nothing is saved or placed yet. */
export interface BuiltPage {
  title: string;
  summaryMd: string;
  /** False when the text was written without numbers read from the data. */
  grounded: boolean;
  kpis: PlannedKpi[];
  charts: BuiltChart[];
  /** Filters the question asked for, each on one column of a source. */
  filters: PlannedFilter[];
  /** Charts that were built and then left off the page, with the reason. */
  leftOut: PageNote[];
  /** Planned charts that could not be built. */
  failed: PageNote[];
  /** Relations between two number columns that were measured, in plain words. */
  relations: PageNote[];
  warnings: string[];
}

/** A built chart once it is saved to the catalog. */
export interface PlacedChart extends BuiltChart {
  vizId: number;
}

export const UNGROUNDED_LINE = "*Written without seeing the data values.*";
export const FULL_CHART_HEIGHT = 8;
export const HALF_CHART_HEIGHT = 7;
const SUMMARY_LONG_CHARS = 320;

const newTileId = (): string =>
  typeof crypto !== "undefined" && "randomUUID" in crypto
    ? crypto.randomUUID()
    : `tile-${Math.random().toString(36).slice(2, 10)}`;

/** The summary tile's Markdown: the title, the answer, and a line when no data was read. */
export const summaryMarkdown = (
  page: Pick<BuiltPage, "title" | "summaryMd" | "grounded">,
): string =>
  [`## ${page.title}`, page.summaryMd, ...(page.grounded ? [] : [UNGROUNDED_LINE])].join("\n\n");

/**
 * The page's tiles from the top: the summary across the grid, the KPIs four to a row, then the
 * charts two to a row. A full-width chart takes a row; a half-width one left alone on its row is
 * widened to fill it.
 */
export function layoutPage(
  page: Pick<BuiltPage, "title" | "summaryMd" | "grounded" | "kpis">,
  charts: readonly PlacedChart[],
  cols: number,
  makeId: () => string = newTileId,
): DashboardTile[] {
  const tiles: DashboardTile[] = [];
  let y = 0;
  const summaryHeight = page.summaryMd.length > SUMMARY_LONG_CHARS ? 4 : 3;
  tiles.push({
    id: makeId(),
    type: "text",
    viz_id: null,
    chart_index: 0,
    text_md: summaryMarkdown(page),
    x: 0,
    y,
    w: cols,
    h: summaryHeight,
  });
  y += summaryHeight;

  const perRow = Math.max(1, Math.floor(cols / KPI_DEFAULT_SIZE.w));
  page.kpis.forEach((kpi, i) => {
    tiles.push({
      id: makeId(),
      type: "kpi",
      viz_id: null,
      table_id: kpi.table_id,
      chart_index: 0,
      kpi: { ...DEFAULT_KPI, field: kpi.field, agg: kpi.agg, label: kpi.label },
      x: (i % perRow) * KPI_DEFAULT_SIZE.w,
      y: y + Math.floor(i / perRow) * KPI_DEFAULT_SIZE.h,
      ...KPI_DEFAULT_SIZE,
    });
  });
  y += Math.ceil(page.kpis.length / perRow) * KPI_DEFAULT_SIZE.h;

  const half = Math.floor(cols / 2);
  let i = 0;
  while (i < charts.length) {
    const chart = charts[i];
    const next = charts[i + 1];
    const pair = chart.width === "half" && next?.width === "half";
    const row = pair ? [chart, next] : [chart];
    const h = chart.width === "full" ? FULL_CHART_HEIGHT : HALF_CHART_HEIGHT;
    row.forEach((item, at) => {
      tiles.push({
        id: makeId(),
        type: "viz",
        viz_id: item.vizId,
        chart_index: 0,
        title: item.title,
        note_md: item.finding,
        x: at * half,
        y,
        w: pair ? half : cols,
        h,
      });
    });
    y += h;
    i += row.length;
  }
  return tiles;
}

/** Whether a page goes on a tab of its own: the dashboard has tabs, or its canvas holds tiles. */
export const landsOnTab = (layout: DashboardLayout): boolean =>
  hasTabs(layout) || layout.tiles.length > 0;

const initialFilterState = (kind: DashboardFilterKind): Record<string, unknown> => {
  if (kind === "categorical") return { selected: [] };
  if (kind === "numeric_range") return { min: null, max: null };
  return { start: null, end: null };
};

/**
 * The dashboard filters a page adds, each bound to its table and reaching the page's tiles that
 * read it. On a tab of its own a filter targets only the page's tiles, so the tiles already on
 * the dashboard keep their data; on an empty dashboard it applies to every tile on its table.
 * A filter no tile of the page reads is left out, and so is one the dashboard already applies
 * to every tile on that table.
 */
export function pageFilters(
  planned: readonly PlannedFilter[],
  tiles: readonly DashboardTile[],
  charts: readonly PlacedChart[],
  existing: readonly DashboardFilter[],
  ontoTab: boolean,
  makeId: () => string = newTileId,
): DashboardFilter[] {
  const tableOfViz = new Map(charts.map((c) => [c.vizId, c.tableId]));
  const tableOf = (tile: DashboardTile): number | null => {
    if (tile.type === "kpi") return tile.table_id ?? null;
    if (tile.type === "viz" && tile.viz_id != null) return tableOfViz.get(tile.viz_id) ?? null;
    return null;
  };
  const filters: DashboardFilter[] = [];
  for (const wanted of planned) {
    const readers = tiles.filter((t) => tableOf(t) === wanted.table_id).map((t) => t.id);
    const covered = existing.some(
      (f) =>
        f.datasource_id === wanted.table_id && f.field_name === wanted.field && f.target === "all",
    );
    if (!readers.length || covered) continue;
    filters.push({
      id: makeId(),
      field_name: wanted.field,
      label: wanted.label,
      kind: wanted.kind,
      state: initialFilterState(wanted.kind),
      target: ontoTab ? "tiles" : "all",
      target_tile_ids: ontoTab ? readers : [],
      datasource_id: wanted.table_id,
      scope: "datasource",
      multiple: true,
    });
  }
  return filters;
}

/**
 * Put the page's tiles and filters on the dashboard: straight onto an empty one without tabs,
 * else on a new tab named after the page. Returns the layout and the tab to show (null without
 * tabs).
 */
export function placeOnDashboard(
  layout: DashboardLayout,
  tiles: readonly DashboardTile[],
  title: string,
  filters: readonly DashboardFilter[] = [],
  makeTabId: () => string = newTabId,
): { layout: DashboardLayout; tabId: string | null } {
  const withFilters = { ...layout, filters: [...layout.filters, ...filters] };
  if (!landsOnTab(layout)) {
    return { layout: { ...withFilters, tiles: [...tiles] }, tabId: null };
  }
  const added = addTab(withFilters, makeTabId);
  const named = renameTab(added.layout, added.tabId, title.slice(0, DASHBOARD_TAB_TITLE_MAX));
  return {
    layout: {
      ...named,
      tiles: [...named.tiles, ...tiles.map((t) => ({ ...t, tab_id: added.tabId }))],
    },
    tabId: added.tabId,
  };
}

export const BUILD_CANCELLED_MESSAGE: ChartPromptMessage = {
  tone: "info",
  text: "Stopped. Nothing was added to the dashboard.",
};

const UNUSABLE_PLAN =
  "The AI's plan could not be used. Try again, or put the question another way.";

/** The message for a plan core marked degraded; nothing was built. */
export function degradedPlanMessage(response: DashboardPlanResponse): ChartPromptMessage {
  const serverMessage = response.message?.trim() || null;
  switch (response.reason) {
    case "empty_prompt":
      return { tone: "error", text: "Ask a question for the dashboard to answer first." };
    case "no_columns":
      return { tone: "error", text: "The selected tables have no columns the AI can chart." };
    case "timeout":
      return {
        tone: "error",
        text: "The AI took too long to plan the page. Try again, or select fewer tables.",
      };
    case "provider_error":
      return {
        tone: "error",
        text: "The AI provider could not be reached or returned an error. Check it under Settings → AI and try again.",
        ...(serverMessage ? { detail: serverMessage } : {}),
      };
    case "empty_response":
      return { tone: "error", text: "The AI sent back an empty answer. Try again." };
    case "no_plan": {
      const why = response.explanation?.trim() || serverMessage;
      return {
        tone: "warning",
        text: why
          ? `The AI did not plan a page: ${why}`
          : "The AI did not plan a page for this question. Try putting it another way.",
      };
    }
    default:
      return {
        tone: "error",
        text: UNUSABLE_PLAN,
        ...(serverMessage ? { detail: serverMessage } : {}),
      };
  }
}

/** Why the story was not written, as a warning on a page that is built without it. */
export function storyFallbackWarning(response: DashboardStoryResponse | null): string {
  const why =
    response?.reason === "timeout"
      ? "the AI took too long"
      : response?.reason === "provider_error"
        ? "the AI provider returned an error"
        : "the AI's answer could not be used";
  return `The summary and the findings were not written (${why}); the charts are in the planned order.`;
}

/** The message for a plan request that never got an answer from the route. */
export function planRequestFailure(error: unknown): ChartPromptMessage {
  const status = (error as { response?: { status?: number } } | null)?.response?.status;
  if (status === 403) {
    return {
      tone: "error",
      text: "You may not read one of the selected tables, so the AI cannot build from it.",
    };
  }
  if (status === 404) {
    return { tone: "error", text: "One of the selected tables or the AI provider was not found." };
  }
  if (status === 422) {
    return { tone: "error", text: "The request was refused. Shorten the question and try again." };
  }
  return requestFailureMessage(error);
}
