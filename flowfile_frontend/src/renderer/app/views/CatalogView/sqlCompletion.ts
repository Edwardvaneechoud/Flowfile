import { completeFromList, type Completion } from "@codemirror/autocomplete";
import { sql, StandardSQL, type SQLNamespace } from "@codemirror/lang-sql";
import { oneDark } from "@codemirror/theme-one-dark";
import type { Extension } from "@codemirror/state";
import type { CatalogTable } from "../../types/catalog.types";

/** The SQL editor's table → columns map: each table by its display name and its qualified name. */
export function catalogTableSchema(tables: CatalogTable[]): Record<string, string[]> {
  const schema: Record<string, string[]> = {};
  for (const t of tables) {
    const cols = (t.schema_columns ?? []).map((c) => c.name);
    schema[t.full_table_name ?? t.name] = cols;
    if (t.qualified_name) schema[t.qualified_name] = cols;
  }
  return schema;
}

/**
 * The SQL language and theme the catalog's SQL editors share. `defaultTable` makes that table's
 * columns complete bare, which is how a metric expression names its base table's columns.
 */
export function sqlEditorExtensions(schema: SQLNamespace, defaultTable?: string): Extension[] {
  return [sql({ schema, defaultTable, upperCaseKeywords: true }), oneDark];
}

const AGGREGATES = "SUM COUNT AVG MIN MAX MEDIAN STDDEV VARIANCE FIRST LAST";
const SCALARS =
  "COALESCE NULLIF ABS ROUND FLOOR CEIL LOWER UPPER LENGTH CONCAT SUBSTR DATE_TRUNC DATE_PART CAST";

const functions = (labels: string, detail: string): Completion[] =>
  labels.split(" ").map((label) => ({ label, type: "function", detail, apply: `${label}(` }));

/** The functions Polars SQL runs, offered beside the language's own keyword and schema sources. */
export const sqlFunctionCompletions: Extension = StandardSQL.language.data.of({
  autocomplete: completeFromList([
    ...functions(AGGREGATES, "aggregate"),
    ...functions(SCALARS, "scalar"),
  ]),
});
