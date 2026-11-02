// A connected hub's rows in the catalog tree: browsed here, managed on the hub.
import type { CatalogTable } from "../../types";
import { formatNumber, formatSize } from "./catalog-formatters";

export const HUB_NAMESPACE_TITLE =
  "This catalog lives on the hub. Create, rename and delete it there.";
export const HUB_TABLE_TITLE = "This table lives on the hub.";
export const HUB_MANAGED_NOTE = "This catalog is managed on the hub.";
export const HUB_PREVIEW_NOTE =
  "Preview isn't available for tables on the hub. Read it with a catalog reader node.";

export interface HubTableFact {
  label: string;
  value: string;
}

export const isHubRow = (row: { source?: string | null }): boolean => row.source === "hub";

/** The rows that live in this Flowfile; a hub catalog drops out with everything under it. */
export function withoutHubRows<T extends { source?: string | null }>(rows: T[]): T[] {
  return rows.filter((row) => !isHubRow(row));
}

/** What the table panel shows for a hub table, from the listing alone. */
export function hubTableFacts(table: CatalogTable): HubTableFact[] {
  return [
    { label: "Rows", value: formatNumber(table.row_count) },
    { label: "Columns", value: String(table.column_count ?? "--") },
    { label: "Size", value: formatSize(table.size_bytes) },
    { label: "Storage", value: table.is_remote_storage ? "Object storage" : "Hub filesystem" },
  ];
}
