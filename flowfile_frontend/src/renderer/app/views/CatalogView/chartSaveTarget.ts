// Where a new chart is saved and what it keeps as its source there. Pure, so the editor only renders it.
import type { SaveTargetOption } from "../../composables/useWritableNamespaces";
import type { VizSourceDescriptor } from "../../types";
import {
  NO_HUB_SCHEMA_TO_SAVE_IN,
  chartSource,
  isHubRow,
  isOffered,
  queryOver,
  type ChartedTable,
} from "./hubRows";

export const LOCAL_TABLE_STAYS_LOCAL =
  "This chart reads a table kept on this machine, so it is saved in this machine's catalog: " +
  "nobody on the hub could draw it.";

export interface ChartTargets {
  /** Whether the editor asks where to save; without a hub in the tree a chart is saved as it always was. */
  asks: boolean;
  options: SaveTargetOption[];
  /** The schema picked at first; null is this machine's catalog with no schema, as a query chart had before. */
  defaultId: number | null;
  /** Why a chart of a table on this machine is offered no hub schema. */
  note: string | null;
  /** Why the chart cannot be saved at all. */
  blocked: string | null;
}

const isHubTable = (source: VizSourceDescriptor): boolean =>
  source.source_type === "table" && (source.table_id ?? 0) < 0;

const isLocalTable = (source: VizSourceDescriptor): boolean =>
  source.source_type === "table" && (source.table_id ?? 0) > 0;

/**
 * The schemas a new chart of `source` may be saved in, from the save targets the person has.
 *
 * A chart of a table on this machine stays in this machine's catalog. A chart of a hub table starts in that
 * table's own schema when the person may write it (`tableNamespaceId`), else in the first hub schema, so the table
 * lists it. A query chart starts on this machine where its catalog is offered, and in a hub schema on a server
 * that joined a hub.
 */
export function chartTargets(
  source: VizSourceDescriptor,
  targets: SaveTargetOption[],
  shown: { ownOffered: boolean; hubShown: boolean },
  tableNamespaceId: number | null = null,
): ChartTargets {
  const none: ChartTargets = {
    asks: false,
    options: [],
    defaultId: null,
    note: null,
    blocked: null,
  };
  if (!shown.hubShown) return none;
  if (isLocalTable(source)) return { ...none, note: LOCAL_TABLE_STAYS_LOCAL };
  const hub = targets.filter((o) => o.hub);
  const ownSchema = hub.find((o) => o.id === tableNamespaceId);
  const onHub = (ownSchema ?? hub[0])?.id ?? null;
  const startsOnHub = isHubTable(source) || !shown.ownOffered;
  const defaultId = startsOnHub ? (onHub ?? targets[0]?.id ?? null) : null;
  const blocked = !shown.ownOffered && hub.length === 0 ? NO_HUB_SCHEMA_TO_SAVE_IN : null;
  return { asks: true, options: targets, defaultId, note: null, blocked };
}

/**
 * What the saved chart keeps: in a hub schema a hub table's own id, so the table lists the chart; on this machine
 * a query over the hub table, since this machine's catalog holds no row for it.
 */
export function chartSourceForSave(
  source: VizSourceDescriptor,
  table: ChartedTable | null,
  savedOnHub: boolean,
): VizSourceDescriptor {
  if (!isHubTable(source) || savedOnHub || table === null) return source;
  return { source_type: "sql", sql_query: queryOver(table) };
}

/** A saved chart moves only between schemas of the catalog that keeps it. */
export function targetsInSameCatalog(
  targets: SaveTargetOption[],
  chart: { id: number; source?: string | null },
): SaveTargetOption[] {
  const onHub = isHubRow(chart) || chart.id < 0;
  return targets.filter((o) => o.hub === onHub);
}

/** The catalog a chart made inside a dashboard is saved in; undefined where the editor asks at save. */
export type SavedIn = "hub" | "local" | undefined;

/** The catalogs whose tables a new chart may read: a hub dashboard's chart reads the hub's tables only. */
export function pickableCatalogs<T extends { source?: string | null; offered?: boolean }>(
  tree: T[],
  savedIn?: SavedIn,
): T[] {
  const offered = tree.filter(isOffered);
  return savedIn === "hub" ? offered.filter(isHubRow) : offered;
}

/** What a new chart of `table` reads, given where it will be saved. */
export function pickedSource(table: ChartedTable, savedIn?: SavedIn): VizSourceDescriptor {
  return chartSourceForSave(chartSource(table), table, savedIn !== "local");
}
