// Pure presentation helpers for the column-stats panel and the preview status
// bar. The backend ships exact counts on the ordinary FileColumn (the single
// truth); percentages and quality badges are derived here, in one place.
// No Vue, no axios — unit-tested in node env (columnQuality.test.ts).
import type { FileColumn } from "../../../types/node.types";

export interface QualityBadge {
  label: string;
  className: string;
}

/** Thousands-separated count; null/undefined (unknown) renders as "?". */
export function formatCount(value: number | null | undefined): string {
  if (value === null || value === undefined) return "?";
  return value.toLocaleString("en-US");
}

/** Exact row count: nulls + non-nulls; null while stats are uncomputed. */
export function totalRowCount(column: FileColumn): number | null {
  if (column.number_of_empty_values === null || column.number_of_filled_values === null)
    return null;
  return column.number_of_empty_values + column.number_of_filled_values;
}

/** Percentage of null values (0-100, 2 decimals); null while unknown. */
export function pctNull(column: FileColumn): number | null {
  const total = totalRowCount(column);
  if (total === null) return null;
  if (total === 0) return 0;
  return Math.round(((column.number_of_empty_values ?? 0) / total) * 10000) / 100;
}

/** Distinct share of the values that exist (unique / filled); null when unknown. */
export function pctUnique(column: FileColumn): number | null {
  if (column.number_of_unique_values === null || !column.number_of_filled_values) return null;
  return (
    Math.round((column.number_of_unique_values / column.number_of_filled_values) * 10000) / 100
  );
}

/**
 * Status-badge modifier for the null percentage: none at 0%, info up to 5%,
 * warning up to 50%, danger above.
 */
export function nullSeverityClass(pctNullValue: number): string {
  if (pctNullValue <= 0) return "";
  if (pctNullValue <= 5) return "status-badge--info";
  if (pctNullValue <= 50) return "status-badge--warning";
  return "status-badge--danger";
}

/**
 * At most one shape badge, in precedence order: empty → all null → constant →
 * unique key. The null fraction is shown separately, so nothing is hidden.
 * Empty until the counts have actually been computed.
 */
export function qualityBadges(column: FileColumn): QualityBadge[] {
  const total = totalRowCount(column);
  if (total === null) return [];
  if (total === 0) return [{ label: "Empty", className: "status-badge--info" }];
  if (column.number_of_filled_values === 0) {
    return [{ label: "All null", className: "status-badge--danger" }];
  }
  if (column.number_of_unique_values === null) return [];
  if (column.number_of_unique_values === 1) {
    return [{ label: "Constant", className: "status-badge--warning" }];
  }
  if (
    column.number_of_empty_values === 0 &&
    column.number_of_unique_values === column.number_of_filled_values
  ) {
    return [{ label: "Unique key", className: "status-badge--success" }];
  }
  return [];
}

/**
 * Bounds a min/max/avg value before it reaches the DOM; null → em dash.
 * This is a safety cap, not the visual truncation: .csp-bound clamps to two
 * lines with a CSS ellipsis, so this only fires for pathological cell values —
 * a serialized blob in a String column — that would otherwise become a
 * megabyte-sized text node. The hover title keeps the full value.
 */
export function truncateValue(value: string | null, maxLength = 200): string {
  if (value === null) return "—";
  if (value.length <= maxLength) return value;
  return `${value.slice(0, maxLength - 1)}…`;
}
