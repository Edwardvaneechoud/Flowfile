import type { SemanticType } from "../../../../types/node.types";

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
