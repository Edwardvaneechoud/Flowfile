import type { FileColumn, SemanticType } from "../../../../types/node.types";

/**
 * What the selector actually reads off a column. Narrower than `FileColumn` so
 * callers can pass a schema they derived themselves (e.g. the columns a formula
 * entry sees) without inventing the statistics fields.
 */
export type EditorSchemaColumn = Pick<FileColumn, "name" | "data_type"> &
  Partial<Pick<FileColumn, "data_type_group" | "semantic_type">>;

export interface ColumnSelectorInterface {
  label: string;
  name: string;
  node_type: string;
  level?: number;
  hasAction?: boolean;
  data_type?: string;
  data_type_group?: string;
  semantic_type?: SemanticType | null;
}

export interface MenuContents {
  title: string;
  icon: string;
  children?: ColumnSelectorInterface[];
}
