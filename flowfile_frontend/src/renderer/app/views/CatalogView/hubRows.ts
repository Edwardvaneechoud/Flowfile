// A connected hub's rows in the catalog tree: browsed here, managed on the hub.

export const HUB_NAMESPACE_TITLE =
  "This catalog lives on the hub. Create, rename and delete it there.";
export const HUB_TABLE_TITLE = "This table lives on the hub.";
export const HUB_MANAGED_NOTE = "This catalog is managed on the hub.";
export const HUB_PREVIEW_NOTE =
  "Preview isn't available for tables on the hub. Read it with a catalog reader node.";

export const isHubRow = (row: { source?: string | null }): boolean => row.source === "hub";

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
