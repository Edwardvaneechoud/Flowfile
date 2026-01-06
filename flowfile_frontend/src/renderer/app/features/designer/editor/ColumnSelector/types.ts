export interface ColumnSelectorInterface {
  label: string;
  name: string;
  node_type: string;
  level?: number;
  hasAction?: boolean;
}

export interface MenuContents {
  title: string;
  icon: string;
  children?: ColumnSelectorInterface[];
}
