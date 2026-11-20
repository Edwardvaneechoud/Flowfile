import type { DashboardTile, VizSourceDescriptor } from "../../types";

/** Shown under the name field and on the disabled Save button while the name is blank. */
export const CHART_NAME_REQUIRED = "Enter a name to save";

/** Why `name` cannot be saved as a chart's name, or null when it can. */
export function chartNameError(name: string): string | null {
  return name.trim() ? null : CHART_NAME_REQUIRED;
}

const unquote = (part: string): string => part.replace(/^["`[]|["`\]]$/g, "");

/** The last part of the first table a query reads (`FROM a.b."orders"` → `orders`), or null. */
export function tableNameInSql(sql: string): string | null {
  const match =
    /\bfrom\s+((?:"[^"]+"|`[^`]+`|\[[^\]]+\]|[\w$]+)(?:\s*\.\s*(?:"[^"]+"|`[^`]+`|\[[^\]]+\]|[\w$]+))*)/i.exec(
      sql,
    );
  if (!match) return null;
  const parts = match[1].split(/\s*\.\s*/);
  const last = unquote(parts[parts.length - 1]).trim();
  return last || null;
}

/** `base`, or `base 2`, `base 3`, … for the first one no name in `taken` already uses (case-insensitive). */
export function distinctName(base: string, taken: Iterable<string>): string {
  const used = new Set<string>();
  for (const name of taken) used.add(name.trim().toLowerCase());
  if (!used.has(base.toLowerCase())) return base;
  let n = 2;
  while (used.has(`${base} ${n}`.toLowerCase())) n += 1;
  return `${base} ${n}`;
}

/** The names a dashboard's chart tiles show: each one's own title, and its chart's name. */
export function dashboardChartNames(
  tiles: readonly Pick<DashboardTile, "type" | "viz_id" | "title">[],
  chartName: (vizId: number) => string | null | undefined,
): string[] {
  const names: string[] = [];
  for (const tile of tiles) {
    if (tile.type !== "viz" || tile.viz_id == null) continue;
    if (tile.title?.trim()) names.push(tile.title);
    const name = chartName(tile.viz_id);
    if (name) names.push(name);
  }
  return names;
}

/**
 * The name a new chart starts with: its table's name, or the table its query reads, made distinct
 * from `taken` (the charts already on the dashboard it is made for).
 */
export function suggestChartName(
  source: VizSourceDescriptor,
  tableName: string | null | undefined,
  taken: Iterable<string> = [],
): string {
  const fromSource =
    source.source_type === "table"
      ? tableName?.trim()
      : source.sql_query
        ? tableNameInSql(source.sql_query)
        : null;
  const base = fromSource || (source.source_type === "sql" ? "Query chart" : "Table chart");
  return distinctName(base, taken);
}

const MAX_AI_TITLE = 120;

/** An AI answer's title as a new chart's name, made distinct from `taken`; null when it has none. */
export function nameFromAiTitle(
  title: string | null | undefined,
  taken: Iterable<string> = [],
): string | null {
  const base = (title ?? "").replace(/\s+/g, " ").trim().slice(0, MAX_AI_TITLE).trim();
  return base ? distinctName(base, taken) : null;
}
