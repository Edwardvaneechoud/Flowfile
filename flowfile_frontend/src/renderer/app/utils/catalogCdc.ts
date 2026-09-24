import type {
  CatalogTable,
  CdcCursor,
  CdcReaderSettings,
  CdcStart,
  DeltaVersionCommit,
} from "../types";
import { timeAgoShort } from "./time";

/** Columns Delta's change feed adds to every row it returns. */
export const CDF_COLUMNS: { name: string; dtype: string }[] = [
  { name: "_change_type", dtype: "String" },
  { name: "_commit_version", dtype: "Int64" },
  { name: "_commit_timestamp", dtype: "Datetime" },
];

// Backend timestamps are UTC but not always suffixed; JS would read a bare one as local.
function asUtc(iso: string): string {
  return iso.endsWith("Z") || /[+-]\d{2}:\d{2}$/.test(iso) ? iso : `${iso}Z`;
}

/**
 * Consumer key a reader commits under, mirroring core's ``resolve_consumer_key``.
 *
 * Only the named form is resolvable here: the flow-scoped key embeds a flow uuid the
 * renderer never receives, which is why ``findCursor`` matches those by node suffix.
 */
export function consumerKeyFor(consumerName: string | null | undefined): string | null {
  const trimmed = consumerName?.trim();
  return trimmed ? `name:${trimmed}` : null;
}

export function findCursor(
  cursors: CdcCursor[],
  consumerName: string | null | undefined,
  nodeId: number,
): CdcCursor | null {
  const key = consumerKeyFor(consumerName);
  if (key) return cursors.find((c) => c.consumer_key === key) ?? null;
  const suffix = `:node:${nodeId}`;
  return (
    cursors.find((c) => c.consumer_key.startsWith("flow:") && c.consumer_key.endsWith(suffix)) ??
    null
  );
}

/** Human label for how far behind a cursor is. */
export function pendingLabel(pending: number | null): string {
  if (pending === null) return "--";
  if (pending <= 0) return "up to date";
  return `${pending} new commit${pending === 1 ? "" : "s"}`;
}

/** "Cursor v12 · advanced 2h ago · 3 new commits", or what a first run would do. */
export function cursorStatusLine(cursor: CdcCursor | null, start: CdcStart): string {
  if (!cursor) {
    return start === "beginning"
      ? "No cursor yet — the first run reads every tracked change."
      : "No cursor yet — the first run starts from now and reads nothing.";
  }
  const parts = [`Cursor v${cursor.last_version}`];
  const age = timeAgoShort(asUtc(cursor.updated_at));
  if (age === "now") parts.push("advanced just now");
  else if (age !== "—") parts.push(`advanced ${age} ago`);
  if (cursor.pending_commits !== null) parts.push(pendingLabel(cursor.pending_commits));
  return parts.join(" · ");
}

/** Why the Read selector is unavailable for this table, or null when changes can be read. */
export function readModeDisabledReason(
  table: CatalogTable | null,
  deltaVersion: number | null,
): string | null {
  if (!table) return "Pick a table first.";
  if (table.table_type === "virtual") return "Virtual tables have no change feed.";
  if (table.scd2) {
    return "SCD2 tables keep their history in columns — read valid_from / valid_to instead.";
  }
  if (deltaVersion !== null) {
    return "This reader is pinned to a table version — clear the version to read changes.";
  }
  return null;
}

/** Picker options for a Delta history: "v12 (MERGE) - 2026-09-22T10:00:00". */
export function deltaVersionOptions(
  history: DeltaVersionCommit[],
): { version: number; label: string }[] {
  return history.map((v) => ({
    version: v.version,
    label: `v${v.version}${v.operation ? ` (${v.operation})` : ""}${v.timestamp ? ` - ${v.timestamp}` : ""}`,
  }));
}

/** Per-field problems with a change-feed read configuration; null where a field is fine. */
export function cdcFieldErrors(settings: CdcReaderSettings): {
  version: string | null;
  timestamp: string | null;
  consumerName: string | null;
} {
  const name = settings.cdc_consumer_name?.trim();
  return {
    version:
      settings.cdc_mode === "since_version" && settings.cdc_from_version == null
        ? "Pick the version to read changes after"
        : null,
    timestamp:
      settings.cdc_mode === "since_timestamp" && !settings.cdc_from_timestamp
        ? "Pick the date and time to read changes from"
        : null,
    consumerName:
      name && !/^[A-Za-z0-9_.:-]+$/.test(name)
        ? "Cursor name: use letters, digits and _ . : - only"
        : null,
  };
}
