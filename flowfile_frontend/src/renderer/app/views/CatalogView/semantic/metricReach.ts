import { useCatalogStore } from "../../../stores/catalog-store";
import type { SemanticModel } from "../../../types/semantic.types";
import { needsReview, shortName } from "./semanticFormat";

/**
 * The tables a metric on `base` can read: `base` first, then every table reached by following
 * joins from their many side to their one side, nearest first (as the compiler walks them).
 * Accepted joins always count; open ones only with `allowPending`.
 */
export function reachableTables(
  base: string,
  joins: SemanticModel["joins"],
  allowPending: boolean,
): string[] {
  const seen = [base];
  for (let i = 0; i < seen.length; i++) {
    for (const j of joins) {
      if (j.from_table !== seen[i] || seen.includes(j.to_table)) continue;
      if (j.status === "accepted" || (allowPending && needsReview(j))) seen.push(j.to_table);
    }
  }
  return seen;
}

/** A table's columns: the model's own, else (a table of another schema) the catalog tree's. */
export function tableColumns(
  model: SemanticModel,
  key: string,
): { name: string; dtype: string | null; meaning?: unknown }[] {
  const own = model.tables.find((t) => t.key === key);
  if (own) return own.fields.map((f) => ({ ...f, meaning: f.facts.description?.value }));
  for (const catalog of useCatalogStore().tree) {
    for (const schema of catalog.children) {
      const found = (schema.tables ?? []).find(
        (t) => (t.qualified_name ?? `${catalog.name}.${schema.name}.${t.name}`) === key,
      );
      if (found) return found.schema_columns ?? [];
    }
  }
  return [];
}

/** One line saying what a metric can use: "Columns of orders, and of customers (through joins)". */
export function reachLine(tables: string[]): string {
  if (tables.length === 0) return "";
  const [base, ...rest] = tables.map(shortName);
  if (rest.length === 0) return `Columns of ${base}`;
  return `Columns of ${base}, and of ${rest.join(", ")} (through joins)`;
}
