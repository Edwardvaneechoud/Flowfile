export interface ColumnSelectorInterface {
  label: string;
  name: string;
  node_type: string;
  level?: number;
  hasAction?: boolean;
  data_type?: string;
  data_type_group?: string;
}

export interface MenuContents {
  title: string;
  icon: string;
  children?: ColumnSelectorInterface[];
}
