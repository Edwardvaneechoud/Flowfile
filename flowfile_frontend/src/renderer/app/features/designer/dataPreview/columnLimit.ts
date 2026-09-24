// Caps how many columns the preview grid renders. AG Grid deep-watches its
// props and builds a header per column, so a 10k+ column result froze the app.

export const MAX_PREVIEW_COLUMNS = 1000;

export interface LimitedPreview<T> {
  columns: T[];
  rows: Record<string, unknown>[];
  hiddenColumnCount: number;
}

/** Keeps the first `max` columns and projects each row onto them; a no-op at or under the cap. */
export function limitPreviewColumns<T extends { name: string }>(
  columns: T[],
  rows: Record<string, unknown>[],
  max: number = MAX_PREVIEW_COLUMNS,
): LimitedPreview<T> {
  if (columns.length <= max) return { columns, rows, hiddenColumnCount: 0 };
  const kept = columns.slice(0, max);
  const names = kept.map((c) => c.name);
  return {
    columns: kept,
    rows: rows.map((row) => Object.fromEntries(names.map((name) => [name, row[name]]))),
    hiddenColumnCount: columns.length - max,
  };
}
