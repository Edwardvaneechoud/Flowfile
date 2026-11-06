// A connected hub's rows in the catalog tree: browsed here, managed on the hub.

export const HUB_NAMESPACE_TITLE =
  "This catalog lives on the hub. Create, rename and delete it there.";
export const HUB_MANAGED_NOTE = "This catalog is managed on the hub.";

export const isHubRow = (row: { source?: string | null }): boolean => row.source === "hub";

/**
 * The right-click actions a hub table offers: its panel, and a new flow that reads it. SQL, the notebook and charts
 * find a table by its name in this machine's own catalog, where a hub table is not listed.
 */
export const HUB_TABLE_ACTIONS: ReadonlySet<string> = new Set(["view", "read"]);

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
 * not offered), or the hub's beside this machine's (a connected desktop). The tree marks hub catalogs with a badge
 * only beside this machine's, and with any hub rows shows counts only on collapsed catalogs and schemas.
 */
export type HubPresence = "none" | "only" | "beside";

export function hubPresence(rows: { source?: string | null; offered?: boolean }[]): HubPresence {
  const shown = rows.filter(isOffered);
  if (!shown.some(isHubRow)) return "none";
  return shown.every(isHubRow) ? "only" : "beside";
}

/** The rows that live in this Flowfile; a hub catalog drops out with everything under it. */
export function withoutHubRows<T extends { source?: string | null }>(rows: T[]): T[] {
  return rows.filter((row) => !isHubRow(row));
}

/** The New menu makes this machine's catalogs, so it hides where only the hub's show or a hub catalog is selected. */
export const offersNew = (
  presence: HubPresence,
  selected: { source?: string | null } | null,
): boolean => presence !== "only" && !(selected && isHubRow(selected));

/** An older hub's granter written as a team's or the organisation's account id, such as `team:sales`. */
const RAW_PRINCIPAL = /^(team|org):\S+$/;

/** A hub row's "Shared" title: the granter by the name the hub gives, never a team's or the organisation's raw account id. */
export function hubSharedTitle(access: { shared_by?: string | null } | null | undefined): string {
  const by = access?.shared_by;
  return by && !RAW_PRINCIPAL.test(by) ? `Shared by ${by}` : "Shared with you";
}
