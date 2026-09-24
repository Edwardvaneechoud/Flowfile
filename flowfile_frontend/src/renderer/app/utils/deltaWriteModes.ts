import type { CatalogWriteMode } from "../types";

/** Every Delta write mode; the cloud storage writer offers a subset (no scd2, no virtual). */
export type DeltaWriteMode = CatalogWriteMode;

const MODE_DESCRIPTIONS: Partial<Record<DeltaWriteMode, string>> = {
  overwrite: "Replace all existing data in the table.",
  error: "Fail if the table already exists.",
  append: "Add rows to the existing table without modifying existing data.",
  upsert: "Update rows that match the key columns, insert rows that don't match.",
  update: "Update only rows that match the key columns. No new rows are inserted.",
  delete: "Remove rows from the target table that match the key columns in the source data.",
  scd2: "Track history: rows whose compared columns changed are end-dated and re-inserted as a new version. New keys are inserted as current.",
};

/** Whether the mode matches source rows to table rows on key columns. */
export function needsMergeKeys(mode: DeltaWriteMode): boolean {
  return mode === "upsert" || mode === "update" || mode === "delete" || mode === "scd2";
}

/** Whether the mode can create the table, which is the only time partitioning applies. */
export function canPartition(mode: DeltaWriteMode): boolean {
  return mode === "overwrite" || mode === "error" || mode === "append" || mode === "scd2";
}

/** Why "Track changes" is unavailable for this mode, or null when it can be turned on. */
export function trackChangesDisabledReason(
  mode: DeltaWriteMode,
  options: { virtual?: boolean } = {},
): string | null {
  if (options.virtual || mode === "virtual") return "Virtual tables have no change feed.";
  if (mode === "overwrite") {
    return "An overwrite replaces the whole table, so its change feed would be every row deleted and re-inserted.";
  }
  if (mode === "scd2") {
    return "SCD2 already keeps history in the table's own valid_from / valid_to columns.";
  }
  return null;
}

export function modeDescription(mode: DeltaWriteMode): string | null {
  return MODE_DESCRIPTIONS[mode] ?? null;
}
