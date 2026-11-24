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

const fn = (label: string, detail: string): Completion => ({
  label,
  type: "function",
  detail,
  apply: `${label}(`,
});

/** Aggregate and scalar functions Polars SQL runs, offered where an expression is written. */
export const SQL_FUNCTIONS: Completion[] = [
  fn("SUM", "aggregate"),
  fn("COUNT", "aggregate"),
  fn("AVG", "aggregate"),
  fn("MIN", "aggregate"),
  fn("MAX", "aggregate"),
  fn("MEDIAN", "aggregate"),
  fn("STDDEV", "aggregate"),
  fn("VARIANCE", "aggregate"),
  fn("FIRST", "aggregate"),
  fn("LAST", "aggregate"),
  fn("COALESCE", "scalar"),
  fn("NULLIF", "scalar"),
  fn("ABS", "scalar"),
  fn("ROUND", "scalar"),
  fn("FLOOR", "scalar"),
  fn("CEIL", "scalar"),
  fn("LOWER", "scalar"),
  fn("UPPER", "scalar"),
  fn("LENGTH", "scalar"),
  fn("CONCAT", "scalar"),
  fn("SUBSTR", "scalar"),
  fn("DATE_TRUNC", "scalar"),
  fn("DATE_PART", "scalar"),
  fn("CAST", "scalar"),
];

/** Completions for `SQL_FUNCTIONS`, added beside the language's own keyword and schema sources. */
export const sqlFunctionCompletions: Extension = StandardSQL.language.data.of({
  autocomplete: completeFromList(SQL_FUNCTIONS),
});
