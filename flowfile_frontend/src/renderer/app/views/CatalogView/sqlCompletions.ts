import { sql, type SQLConfig } from "@codemirror/lang-sql";
import type { SqlTableMetadata } from "../../types";

/**
 * Build a CodeMirror SQL language extension with autocomplete for catalog tables.
 */
export function buildSqlExtension(tables: SqlTableMetadata[]) {
  const schema: Record<string, string[]> = {};
  for (const t of tables) {
    schema[t.name] = t.columns.map((c) => c.name);
  }
  const config: SQLConfig = {
    schema,
    upperCaseKeywords: true,
  };
  return sql(config);
}
