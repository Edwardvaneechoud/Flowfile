/**
 * Flowfile WASM Types
 * Aligned with flowfile_core/schemas for compatibility
 */

// =============================================================================
// BASE NODE TYPES (matches flowfile_core/schemas/input_schema.py)
// =============================================================================

export interface NodeBase {
  flow_id?: number
  node_id: number
  cache_results: boolean
  pos_x: number
  pos_y: number
  is_setup: boolean
  description: string
}

export interface NodeSingleInput extends NodeBase {
  depending_on_id?: number
}

export interface NodeMultiInput extends NodeBase {
  depending_on_ids: number[]
}

// =============================================================================
// FILTER SCHEMAS (matches flowfile_core/schemas/transform_schema.py)
// =============================================================================

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

export interface BasicFilter {
  field: string
  operator: FilterOperator
  value: string
  value2?: string  // For BETWEEN operator
}

export interface FilterInput {
  mode: 'basic' | 'advanced'
  basic_filter?: BasicFilter  // Optional - can be None in flowfile_core
  advanced_filter: string  // Polars expression
}

// =============================================================================
// SELECT SCHEMAS
// =============================================================================

export interface SelectInput {
  old_name: string
  new_name: string
  data_type: string
  data_type_change?: boolean
  join_key?: boolean
  is_altered?: boolean
  position: number
  is_available?: boolean
  keep: boolean
}

// =============================================================================
// SORT SCHEMAS
// =============================================================================

export interface SortByInput {
  column: string
  how: 'asc' | 'desc'
}

// =============================================================================
// GROUP BY / AGGREGATION SCHEMAS
// =============================================================================

export type AggType =
  | 'groupby'
  | 'sum'
  | 'max'
  | 'min'
  | 'count'
  | 'mean'
  | 'median'
  | 'first'
  | 'last'
  | 'n_unique'
  | 'concat'

export interface AggCol {
  old_name: string
  agg: AggType
  new_name: string
  output_type?: string
}

export interface GroupByInput {
  agg_cols: AggCol[]
}

// =============================================================================
// JOIN SCHEMAS
// =============================================================================

export type JoinStrategy = 'inner' | 'left' | 'right' | 'full' | 'semi' | 'anti' | 'cross' | 'outer'

export interface JoinMap {
  left_col: string
  right_col: string
}

export interface JoinInputs {
  renames: SelectInput[]
}

export interface JoinInput {
  join_mapping: JoinMap[]
  left_select?: JoinInputs
  right_select?: JoinInputs
  how: JoinStrategy
}

// =============================================================================
// UNIQUE SCHEMAS
// =============================================================================

export interface UniqueInput {
  columns?: string[]  // Optional - can be None in flowfile_core (all columns)
  strategy: 'first' | 'last' | 'any' | 'none'
}

// =============================================================================
// PIVOT SCHEMAS (matches flowfile_core/schemas/transform_schema.py)
// =============================================================================

export interface PivotInput {
  index_columns: string[]    // Columns to keep stable (row identifiers)
  pivot_column: string       // Column whose values become new column names
  value_col: string          // Column containing values to aggregate
  aggregations: string[]     // Aggregation functions: 'sum', 'mean', 'count', 'min', 'max', etc.
}

// =============================================================================
// UNPIVOT SCHEMAS (matches flowfile_core/schemas/transform_schema.py)
// =============================================================================

export type UnpivotDataTypeSelector = 'float' | 'all' | 'date' | 'numeric' | 'string'
export type UnpivotSelectorMode = 'data_type' | 'column'

export interface UnpivotInput {
  index_columns: string[]                          // Columns to keep as identifiers
  value_columns: string[]                          // Columns to unpivot (melt)
  data_type_selector?: UnpivotDataTypeSelector     // Select columns by data type
  data_type_selector_mode: UnpivotSelectorMode     // How to select columns: by type or explicit list
}

// =============================================================================
// OUTPUT SCHEMAS (matches flowfile_core/schemas/input_schema.py)
// =============================================================================

export type OutputFileType = 'csv' | 'excel' | 'parquet'
export type OutputWriteMode = 'overwrite' | 'new file' | 'append'

export interface OutputCsvTable {
  file_type: 'csv'
  delimiter: string
  encoding: string
}

export interface OutputExcelTable {
  file_type: 'excel'
  sheet_name: string
}

export interface OutputParquetTable {
  file_type: 'parquet'
}

export type OutputTableSettings = OutputCsvTable | OutputExcelTable | OutputParquetTable

export interface OutputSettings {
  name: string                     // Filename (e.g., "output.csv")
  directory: string                // Target directory (not used in WASM, kept for compatibility)
  file_type: OutputFileType
  fields?: string[]
  write_mode: OutputWriteMode
  table_settings: OutputTableSettings
}

// =============================================================================
// FORMULA / WITH COLUMNS SCHEMAS
// =============================================================================

export interface FieldInput {
  name: string
  data_type: string
}

export interface FunctionInput {
  field: FieldInput  // Changed from 'field_input' to match flowfile_core
  function: string  // Polars expression
}

// =============================================================================
// HEAD/SAMPLE SCHEMAS
// =============================================================================

export interface SampleInput {
  sample_size: number
}

// =============================================================================
// MANUAL INPUT / RAW DATA SCHEMAS (matches flowfile_core/schemas/input_schema.py)
// =============================================================================

export interface MinimalFieldInfo {
  name: string
  data_type: string
}

export interface RawData {
  columns: MinimalFieldInfo[]  // Changed from 'fields' to match flowfile_core
  data: any[][]
}

// =============================================================================
// INPUT TABLE SCHEMAS (for CSV reading)
// =============================================================================

export interface InputCsvTable {
  file_type: 'csv'
  reference?: string
  starting_from_line?: number
  delimiter: string
  has_headers: boolean
  encoding?: string
  quote_char?: string
  infer_schema_length?: number
  truncate_ragged_lines?: boolean
  ignore_errors?: boolean
}

export interface ReceivedTable {
  id?: number
  name: string
  path?: string
  directory?: string
  fields?: MinimalFieldInfo[]
  file_type: 'csv' | 'json' | 'parquet' | 'excel'
  table_settings: InputCsvTable
}

// =============================================================================
// NODE SETTING TYPES (matches flowfile_core node structures)
// =============================================================================

export interface NodeReadSettings extends NodeBase {
  received_table?: ReceivedTable
  // Simplified for WASM - we store file content separately
  file_name?: string
}

export interface NodeManualInputSettings extends NodeBase {
  raw_data_format?: RawData  // Changed from 'raw_data' to match flowfile_core
}

export interface NodeFilterSettings extends NodeSingleInput {
  filter_input: FilterInput
}

export interface NodeSelectSettings extends NodeSingleInput {
  select_input: SelectInput[]
  keep_missing?: boolean
}

export interface NodeSortSettings extends NodeSingleInput {
  sort_input: SortByInput[]
}

export interface NodeGroupBySettings extends NodeSingleInput {
  groupby_input: GroupByInput
}

export interface NodeJoinSettings extends NodeMultiInput {
  join_input: JoinInput
  left_input_id?: number
  right_input_id?: number
}

export interface NodeUniqueSettings extends NodeSingleInput {
  unique_input: UniqueInput
}

export interface NodeFormulaSettings extends NodeSingleInput {
  function?: FunctionInput  // Changed from 'function_input: FunctionInput[]' to match flowfile_core
}

export interface NodeSampleSettings extends NodeSingleInput {
  sample_size: number
}

export interface NodePreviewSettings extends NodeSingleInput {
  // No additional settings needed
}

export interface NodePivotSettings extends NodeSingleInput {
  pivot_input: PivotInput
}

export interface NodeUnpivotSettings extends NodeSingleInput {
  unpivot_input: UnpivotInput
}

export interface NodeOutputSettings extends NodeSingleInput {
  output_settings: OutputSettings
}

// Union type for all node settings
export type NodeSettings =
  | NodeReadSettings
  | NodeManualInputSettings
  | NodeFilterSettings
  | NodeSelectSettings
  | NodeSortSettings
  | NodeGroupBySettings
  | NodeJoinSettings
  | NodeUniqueSettings
  | NodeFormulaSettings
  | NodeSampleSettings
  | NodePreviewSettings
  | NodePivotSettings
  | NodeUnpivotSettings
  | NodeOutputSettings

// =============================================================================
// FLOWFILE DATA STRUCTURE (for save/load - matches flowfile_core/schemas/schemas.py)
// =============================================================================

export interface FlowfileSettings {
  description: string
  execution_mode: 'Development' | 'Performance'
  execution_location: 'local' | 'remote'
  auto_save: boolean
  show_detailed_progress: boolean
}

export interface FlowfileNode {
  id: number
  type: string
  is_start_node: boolean
  description: string
  x_position: number
  y_position: number
  left_input_id?: number
  right_input_id?: number
  input_ids: number[]
  outputs: number[]
  setting_input: any  // Node-specific settings
}

export interface NodeConnection {
  from_node: number
  to_node: number
  from_handle: string
  to_handle: string
}

export interface FlowfileData {
  flowfile_version: string
  flowfile_id: number
  flowfile_name: string
  flowfile_settings: FlowfileSettings
  nodes: FlowfileNode[]
  connections?: NodeConnection[]  // Optional - flowfile_core derives connections from node relationships
}

// =============================================================================
// RUNTIME TYPES (for Vue Flow and execution)
// =============================================================================

export interface FlowNode {
  id: number
  type: string
  x: number
  y: number
  settings: NodeSettings
  inputIds: number[]
  leftInputId?: number
  rightInputId?: number
  description?: string
}

export interface FlowEdge {
  id: string
  source: string
  target: string
  sourceHandle: string
  targetHandle: string
}

export interface ColumnSchema {
  name: string
  data_type: string
  is_unique?: boolean
  max_value?: string
  min_value?: string
  number_of_empty_values?: number
  number_of_unique_values?: number
}

export interface DataPreview {
  columns: string[]
  data: any[][]
  total_rows: number
}

export interface NodeResult {
  success?: boolean  // undefined = not executed yet (shows grey), true = success (green), false = error (red)
  error?: string
  data?: DataPreview
  schema?: ColumnSchema[]
  execution_time?: number
}

export interface FlowState {
  nodes: Map<number, FlowNode>
  edges: FlowEdge[]
  nodeResults: Map<number, NodeResult>
}

// =============================================================================
// NODE TYPE MAPPING (matches NODE_TYPE_TO_SETTINGS_CLASS in flowfile_core)
// =============================================================================

export const NODE_TYPES = {
  // Input nodes
  read_csv: 'read_csv',
  manual_input: 'manual_input',

  // Transform nodes
  filter: 'filter',
  select: 'select',
  sort: 'sort',
  group_by: 'group_by',
  unique: 'unique',
  formula: 'formula',
  sample: 'sample',

  // Aggregate/reshape nodes
  pivot: 'pivot',
  unpivot: 'unpivot',

  // Combine nodes
  join: 'join',

  // Output nodes
  preview: 'preview',
  output: 'output',
} as const

export type NodeType = typeof NODE_TYPES[keyof typeof NODE_TYPES]

// =============================================================================
// LEGACY COMPATIBILITY (for existing component props)
// These types map to the new structure for backwards compatibility
// =============================================================================

// Legacy ReadCsvSettings - maps to NodeReadSettings
export interface ReadCsvSettings extends NodeBase {
  file_name: string
  has_headers: boolean
  delimiter: string
  skip_rows: number
}

// Legacy ManualInputSettings
export interface ManualInputSettings extends NodeBase {
  manual_input: {
    data: string
    columns: string[]
    has_headers: boolean
    delimiter: string
  }
}

// Legacy type aliases for backwards compatibility with existing components
// These types add flexibility for components that use different property names
export interface FilterSettings extends NodeBase {
  filter_input: FilterInput
  depending_on_id?: number
}

export interface SelectSettings extends NodeBase {
  select_input: SelectColumn[]
  keep_missing?: boolean
  depending_on_id?: number
}

export interface SortSettings extends NodeBase {
  sort_input: {
    sort_cols: SortColumn[]
  }
  depending_on_id?: number
}

export interface GroupBySettings extends NodeBase {
  groupby_input: {
    agg_cols: AggColumn[]
  }
  depending_on_id?: number
}

export interface JoinSettings extends NodeBase {
  join_input: {
    join_type: JoinStrategy
    join_mapping: JoinMapping[]
    left_suffix?: string
    right_suffix?: string
    how?: JoinStrategy
  }
  depending_on_ids?: number[]
  left_input_id?: number
  right_input_id?: number
}

export interface UniqueSettings extends NodeBase {
  unique_input: {
    subset: string[]
    keep: 'first' | 'last' | 'any' | 'none'
    maintain_order?: boolean
    columns?: string[]
    strategy?: 'first' | 'last' | 'any' | 'none'
  }
  depending_on_id?: number
}

export interface HeadSettings extends NodeBase {
  head_input: {
    n: number
  }
  sample_size?: number
  depending_on_id?: number
}

export interface WithColumnsSettings extends NodeBase {
  with_columns_input: {
    columns: WithColumnDef[]
  }
  function_input?: FunctionInput[]
  depending_on_id?: number
}

export interface PolarsCodeSettings extends NodeBase {
  polars_code_input: {
    polars_code: string
  }
  depending_on_id?: number
}

export interface PreviewSettings extends NodeBase {
  depending_on_id?: number
}

export interface PivotSettings extends NodeBase {
  pivot_input: PivotInput
  depending_on_id?: number
}

export interface UnpivotSettings extends NodeBase {
  unpivot_input: UnpivotInput
  depending_on_id?: number
}

export interface OutputNodeSettings extends NodeBase {
  output_settings: OutputSettings
  depending_on_id?: number
}

export type BaseNodeSettings = NodeSettings

// Legacy column types for existing components
export interface SelectColumn {
  old_name: string
  new_name: string
  keep: boolean
  data_type?: string
  position?: number
}

export interface SortColumn {
  column: string
  descending: boolean
}

export interface AggColumn {
  column?: string
  old_name: string
  new_name: string
  agg: AggType
  alias?: string
}

export interface JoinMapping {
  left_col: string
  right_col: string
}

export type JoinType = JoinStrategy

export interface WithColumnDef {
  name: string
  expression: string
}
