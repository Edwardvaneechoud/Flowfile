// Shared shapes for dataframe-aware column completions: the parser, the catalog
// resolver and the completion source agree on these, so they live apart from all three.
export interface SchemaColumn {
  name: string;
  dtype: string;
}

export type FrameKind = "DataFrame" | "LazyFrame" | "unknown";

export interface CatalogRefLiteral {
  table: string;
  schema?: string;
  catalog?: string;
  namespaceId?: number;
}

export function catalogRefKey(ref: CatalogRefLiteral): string {
  if (ref.namespaceId != null) return `ns:${ref.namespaceId}:${ref.table}`;
  if (ref.catalog && ref.schema) return `${ref.catalog}.${ref.schema}.${ref.table}`;
  if (ref.schema) return `${ref.schema}.${ref.table}`;
  return ref.table;
}
