// A connected hub's rows in the catalog tree: browsed here, managed on the hub.
import type { VizSourceDescriptor } from "../../types";

export const HUB_MANAGED_NOTE = "This catalog is managed on the hub.";

export const isHubRow = (row: { source?: string | null }): boolean => row.source === "hub";

interface ChartedTable {
  id: number;
  name: string;
  source?: string | null;
  qualified_name?: string | null;
  full_table_name?: string | null;
}

/** What a chart of `table` reads: the table, or for a hub table a query over it, since a saved visual keeps a
 * query and never a hub table's id. */
export function chartSource(table: ChartedTable): VizSourceDescriptor {
  if (!isHubRow(table)) return { source_type: "table", table_id: table.id };
  const reference = table.qualified_name ?? table.full_table_name ?? table.name;
  const quoted = reference
    .split(".")
    .map((part) => (/^[a-zA-Z_]\w*$/.test(part) ? part : `"${part}"`));
  return { source_type: "sql", sql_query: `SELECT * FROM ${quoted.join(".")}` };
}

interface PlacedRow {
  place?: string | null;
  offered?: boolean;
  unavailable_reason?: string | null;
}

/** Whether the Catalog panel and the catalog pickers offer a row; core says no for a joined server's own. */
export const isOffered = (row: PlacedRow): boolean => row.offered !== false;

/** Why this Flowfile cannot use a hub catalog's tables, as core words it; null when it can. */
export const unavailableReason = (row: PlacedRow): string | null => row.unavailable_reason ?? null;

export interface PlaceGroup<T> {
  place: string | null;
  rows: T[];
}

/** Consecutive rows that live in one place; core lists this machine's catalogs first, then the hub's. */
export function byPlace<T extends PlacedRow>(rows: T[]): PlaceGroup<T>[] {
  const groups: PlaceGroup<T>[] = [];
  for (const row of rows) {
    const place = row.place ?? null;
    const last = groups[groups.length - 1];
    if (last && last.place === place) last.rows.push(row);
    else groups.push({ place, rows: [row] });
  }
  return groups;
}

/** Headings only say something where catalogs of more than one place are shown side by side. */
export const showsPlaces = (groups: PlaceGroup<unknown>[]): boolean =>
  groups.length > 1 && groups.every((group) => group.place !== null);

/**
 * How the catalog tree shows a hub: not at all, only the hub's catalogs (a server that joined its hub, whose own are
 * not offered), or the hub's beside this machine's (a connected desktop).
 */
export type HubPresence = "none" | "only" | "beside";

export function hubPresence(rows: { source?: string | null; offered?: boolean }[]): HubPresence {
  const shown = rows.filter(isOffered);
  if (!shown.some(isHubRow)) return "none";
  return shown.every(isHubRow) ? "only" : "beside";
}

/**
 * Whether this machine's own catalog takes a person's flows. Core marks its rows not offered on a server that
 * joined a hub: there a flow is a file on the server until it is published to the hub's catalog.
 */
export const offersOwnCatalog = (rows: { source?: string | null; offered?: boolean }[]): boolean =>
  !rows.some((row) => !isHubRow(row) && !isOffered(row));

export const OWN_CATALOG_NOT_OFFERED =
  "Flows on this server are kept in your organisation's catalog on the hub. This saves a file on the server; " +
  "Publish to hub puts the flow in the catalog.";

/** The rows that live in this Flowfile; a hub catalog drops out with everything under it. */
export function withoutHubRows<T extends { source?: string | null }>(rows: T[]): T[] {
  return rows.filter((row) => !isHubRow(row));
}

/** The New menu makes this machine's catalogs, so it hides where only the hub's show or a hub catalog is selected. */
export const offersNew = (
  presence: HubPresence,
  selected: { source?: string | null } | null,
): boolean => presence !== "only" && !(selected && isHubRow(selected));

interface HubFlowRow {
  latest_version?: number | null;
  author_name?: string | null;
}

/** A hub flow's version chip title: the newest version and, when the hub says, who published it. */
export function hubFlowVersionTitle(flow: HubFlowRow): string {
  const version = `Version ${flow.latest_version ?? "?"} on the hub`;
  return flow.author_name ? `${version}, published by ${flow.author_name}` : version;
}

/** The root row (the catalog) that holds namespace `namespaceId`, itself included; null when none does. */
export function rootOf<R extends { id: number; children: R[] }>(
  rows: R[],
  namespaceId: number,
): R | null {
  const holds = (row: R): boolean => row.id === namespaceId || row.children.some(holds);
  return rows.find(holds) ?? null;
}

type Shareable = { access?: { is_owner: boolean; access_level: string } | null };

/** Whether Share is offered on a hub row: to its owner and to who manages it, as the hub says; the hub decides again. */
export const canShareHubRow = (row: Shareable): boolean =>
  !row.access || row.access.is_owner || row.access.access_level === "manage";

/** Whether the person may hand out manage on a hub row: its owner on the hub, whatever they are on this machine. */
export const ownsHubRow = (row: Shareable): boolean => row.access?.is_owner !== false;

export interface SchedulerBanners {
  local: boolean;
  localScoped: boolean;
  remote: boolean;
}

/**
 * Which scheduler statuses the Schedules page shows: this machine's only where it lists schedules of its own (or
 * none of the hub's) and its own catalog is offered, worded for this machine's schedules beside the hub's; the hub's
 * where its schedules are listed and the hub said how its scheduler is.
 */
export function schedulerBanners(
  schedules: { source?: string | null }[],
  ownOffered: boolean,
  remoteKnown: boolean,
): SchedulerBanners {
  const remoteListed = schedules.some(isHubRow);
  const ownListed = schedules.some((s) => !isHubRow(s));
  const local = ownOffered && (ownListed || !remoteListed);
  return { local, localScoped: local && remoteListed, remote: remoteListed && remoteKnown };
}
