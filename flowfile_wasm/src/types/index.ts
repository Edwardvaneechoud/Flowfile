// Node template definition
export interface NodeTemplate {
  name: string
  item: string
  input: number
  output: number
  image: string
  multi: boolean
  node_type: 'input' | 'output' | 'process'
  node_group: 'input' | 'transform' | 'combine' | 'aggregate' | 'output'
  drawer_title: string
  drawer_intro: string
}

// Node settings for different node types
export interface BaseNodeSettings {
  node_id: number
  is_setup: boolean
  cache_results: boolean
}

export interface ReadCsvSettings extends BaseNodeSettings {
  file_name: string
  has_headers: boolean
  delimiter: string
  skip_rows: number
}

export interface ManualInputSettings extends BaseNodeSettings {
  manual_input: {
    data: string
    columns: string[]
    has_headers: boolean
    delimiter: string
  }
}

export interface FilterSettings extends BaseNodeSettings {
  filter_input: {
    mode: 'basic' | 'advanced'
    basic_filter: {
      field: string
      operator: FilterOperator
      value: string
      value2?: string
    }
    advanced_filter: string
  }
}

export type FilterOperator =
  | 'equals'
  | 'not_equals'
  | 'greater_than'
  | 'greater_than_or_equals'
  | 'less_than'
  | 'less_than_or_equals'
  | 'contains'
  | 'not_contains'
  | 'starts_with'
  | 'ends_with'
  | 'is_null'
  | 'is_not_null'
  | 'in'
  | 'not_in'
  | 'between'

export const FILTER_OPERATOR_LABELS: Record<string, FilterOperator> = {
  'Equals': 'equals',
  'Not Equals': 'not_equals',
  'Greater Than': 'greater_than',
  'Greater Than or Equals': 'greater_than_or_equals',
  'Less Than': 'less_than',
  'Less Than or Equals': 'less_than_or_equals',
  'Contains': 'contains',
  'Not Contains': 'not_contains',
  'Starts With': 'starts_with',
  'Ends With': 'ends_with',
  'Is Null': 'is_null',
  'Is Not Null': 'is_not_null',
  'In': 'in',
  'Not In': 'not_in',
  'Between': 'between',
}

export interface SelectSettings extends BaseNodeSettings {
  select_input: SelectColumn[]
  keep_missing: boolean
}

export interface SelectColumn {
  old_name: string
  new_name: string
  keep: boolean
  position: number
  data_type: string
}

export interface GroupBySettings extends BaseNodeSettings {
  groupby_input: {
    agg_cols: AggColumn[]
  }
}

export interface AggColumn {
  old_name: string
  new_name: string
  agg: AggType
}

export type AggType = 'groupby' | 'sum' | 'max' | 'min' | 'count' | 'mean' | 'median' | 'first' | 'last' | 'n_unique' | 'concat'

export interface JoinSettings extends BaseNodeSettings {
  join_input: {
    join_type: JoinType
    join_mapping: JoinMapping[]
    left_suffix: string
    right_suffix: string
  }
}

export type JoinType = 'inner' | 'left' | 'right' | 'outer' | 'cross' | 'semi' | 'anti'

export interface JoinMapping {
  left_col: string
  right_col: string
}

export interface SortSettings extends BaseNodeSettings {
  sort_input: {
    sort_cols: SortColumn[]
  }
}

export interface SortColumn {
  column: string
  descending: boolean
}

export interface WithColumnsSettings extends BaseNodeSettings {
  with_columns_input: {
    columns: WithColumnDef[]
  }
}

export interface WithColumnDef {
  name: string
  expression: string
}

export interface UniqueSettings extends BaseNodeSettings {
  unique_input: {
    subset: string[]
    keep: 'first' | 'last' | 'any' | 'none'
    maintain_order: boolean
  }
}

export interface HeadSettings extends BaseNodeSettings {
  head_input: {
    n: number
  }
}

export type NodeSettings =
  | ReadCsvSettings
  | ManualInputSettings
  | FilterSettings
  | SelectSettings
  | GroupBySettings
  | JoinSettings
  | SortSettings
  | WithColumnsSettings
  | UniqueSettings
  | HeadSettings

// Flow node definition
export interface FlowNode {
  id: number
  type: string
  x: number
  y: number
  settings: NodeSettings
  inputIds: number[]
  leftInputId?: number
  rightInputId?: number
}

// Edge/Connection definition
export interface FlowEdge {
  id: string
  source: string
  target: string
  sourceHandle: string
  targetHandle: string
}

// Schema/Column definition
export interface ColumnSchema {
  name: string
  data_type: string
}

// Data preview
export interface DataPreview {
  columns: string[]
  data: any[][]
  total_rows: number
}

// Node result
export interface NodeResult {
  success: boolean
  error?: string
  data?: DataPreview
  schema?: ColumnSchema[]
  execution_time?: number
}

// Flow state
export interface FlowState {
  nodes: Map<number, FlowNode>
  edges: FlowEdge[]
  nodeResults: Map<number, NodeResult>
}
