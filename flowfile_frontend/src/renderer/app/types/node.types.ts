// Node-related TypeScript interfaces and types
// Consolidated from features/designer/baseNode/nodeInterfaces.ts and nodeInput.ts

import type { AuthMethod } from "../views/CloudConnectionView/CloudConnectionTypes";

// ============================================================================
// Data Type Definitions
// ============================================================================

type DataTypeGroup = "String" | "Date" | "Numeric";

// ============================================================================
// Column and Table Types
// ============================================================================

export interface FileColumn {
  name: string;
  data_type: string;
  is_unique: boolean;
  max_value: string;
  min_value: string;
  number_of_empty_values: number;
  number_of_filled_values: number;
  number_of_unique_values: number;
  size: number;
  data_type_group: DataTypeGroup;
}

export interface TableExample {
  node_id: string | number;
  number_of_records: number;
  number_of_columns: number;
  name: string;
  table_schema: FileColumn[];
  columns: string[];
  data: Record<string, any>[];
  has_example_data: boolean;
  has_run_with_current_setup: boolean;
}

// ============================================================================
// Node Data Types
// ============================================================================

export interface NodeData {
  flow_id: string | number;
  node_id: string | number;
  flow_type: string;
  left_input?: TableExample | null;
  right_input?: TableExample | null;
  main_input?: TableExample | null;
  main_output?: TableExample;
  left_output?: TableExample | null;
  right_output?: TableExample | null;
  has_run: boolean;
  is_cached: boolean;
  setting_input?: any | null;
}

export interface NodeValidationInput {
  isValid: boolean;
  error?: string;
}

export interface NodeValidation extends NodeValidationInput {
  validationTime: number;
}

export interface NodeResult {
  node_id: number;
  node_name?: string;
  start_timestamp: number;
  end_timestamp: number;
  success?: boolean;
  error: string;
  run_time: number;
  is_running: boolean;
}

// ============================================================================
// Node Description Types
// ============================================================================

export interface NodeDescriptionDictionaryPerGraph {
  [node_id: number]: string;
}

export interface NodeDescriptionDictionary {
  [flow_id: number]: NodeDescriptionDictionaryPerGraph;
}

export interface NodeTitleInfo {
  title: string;
  intro: string;
}

// ============================================================================
// Expression Types
// ============================================================================

export interface ExpressionRef {
  name: string;
  doc: string;
}

export interface ExpressionsOverview {
  expression_type: string;
  expressions: ExpressionRef[];
}

// ============================================================================
// Select Input Types
// ============================================================================

export interface SelectInput {
  old_name: string;
  new_name: string;
  keep: boolean;
  data_type?: string;
  join_key?: boolean;
  is_altered: boolean;
  data_type_change: boolean;
  is_available: boolean;
  position: number;
  original_position: number;
}

export interface SelectInputs {
  renames: SelectInput[];
}

export const createSelectInputFromName = (columnName: string, keep = true): SelectInput => {
  return {
    old_name: columnName,
    new_name: columnName,
    keep: keep,
    is_altered: false,
    data_type_change: false,
    is_available: true,
    position: 0,
    original_position: 0,
  };
};

// ============================================================================
// Input Table Settings
// ============================================================================

export interface InputCsvTable {
  file_type: "csv";
  reference?: string;
  starting_from_line: number;
  delimiter: string;
  has_headers: boolean;
  encoding: string;
  row_delimiter: string;
  quote_char: string;
  infer_schema_length: number;
  truncate_ragged_lines: boolean;
  ignore_errors: boolean;
}

export interface InputJsonTable {
  file_type: "json";
  reference?: string;
  starting_from_line: number;
  delimiter: string;
  has_headers: boolean;
  encoding: string;
  row_delimiter: string;
  quote_char: string;
  infer_schema_length: number;
  truncate_ragged_lines: boolean;
  ignore_errors: boolean;
}

export interface InputParquetTable {
  file_type: "parquet";
}

export interface InputExcelTable {
  file_type: "excel";
  sheet_name?: string;
  start_row: number;
  start_column: number;
  end_row: number;
  end_column: number;
  has_headers: boolean;
  type_inference: boolean;
}

export type InputTableSettings =
  | InputCsvTable
  | InputJsonTable
  | InputParquetTable
  | InputExcelTable;

export function isInputCsvTable(settings: InputTableSettings): settings is InputCsvTable {
  return settings.file_type === "csv";
}

export function isInputExcelTable(settings: InputTableSettings): settings is InputExcelTable {
  return settings.file_type === "excel";
}

export function isInputParquetTable(settings: InputTableSettings): settings is InputParquetTable {
  return settings.file_type === "parquet";
}

// ============================================================================
// Output Table Settings
// ============================================================================

export interface OutputCsvTable {
  delimiter: string;
  encoding: string;
  file_type: string;
}

export interface OutputParquetTable {
  file_type: string;
}

export interface OutputExcelTable {
  sheet_name: string;
  file_type: string;
}

export type OutputTableSettings = OutputCsvTable | OutputParquetTable | OutputExcelTable;

export function isOutputCsvTable(settings: OutputTableSettings): settings is OutputCsvTable {
  return settings.file_type === "csv";
}

export function isOutputParquetTable(
  settings: OutputTableSettings,
): settings is OutputParquetTable {
  return settings.file_type === "parquet";
}

export function isOutputExcelTable(settings: OutputTableSettings): settings is OutputExcelTable {
  return settings.file_type === "excel";
}

export interface OutputSettings {
  name: string;
  directory: string;
  file_type: "parquet" | "csv" | "excel";
  fields?: string[];
  write_mode: "overwrite" | "append" | "error";
  table_settings: OutputTableSettings;
}

// ============================================================================
// Field Types
// ============================================================================

export interface MinimalFieldInput {
  name: string;
  data_type: string;
}

export interface FieldInput {
  name: string;
  data_type?: string;
}

export interface FormulaInput {
  field: FieldInput;
  function: string;
}

// ============================================================================
// Filter Types
// ============================================================================

/**
 * Supported filter comparison operators.
 */
export type FilterOperator =
  | "equals"
  | "not_equals"
  | "greater_than"
  | "greater_than_or_equals"
  | "less_than"
  | "less_than_or_equals"
  | "contains"
  | "not_contains"
  | "starts_with"
  | "ends_with"
  | "is_null"
  | "is_not_null"
  | "in"
  | "not_in"
  | "between";

/**
 * Mapping from UI-friendly labels to FilterOperator values.
 */
export const FILTER_OPERATOR_LABELS: Record<string, FilterOperator> = {
  Equals: "equals",
  "Does not equal": "not_equals",
  "Greater than": "greater_than",
  "Greater than or equals": "greater_than_or_equals",
  "Less than": "less_than",
  "Less than or equals": "less_than_or_equals",
  Contains: "contains",
  "Does not contain": "not_contains",
  "Starts with": "starts_with",
  "Ends with": "ends_with",
  "Is null": "is_null",
  "Is not null": "is_not_null",
  In: "in",
  "Not in": "not_in",
  Between: "between",
};

/**
 * Reverse mapping from FilterOperator to label.
 */
export const FILTER_OPERATOR_REVERSE_LABELS: Record<FilterOperator, string> = {
  equals: "Equals",
  not_equals: "Does not equal",
  greater_than: "Greater than",
  greater_than_or_equals: "Greater than or equals",
  less_than: "Less than",
  less_than_or_equals: "Less than or equals",
  contains: "Contains",
  not_contains: "Does not contain",
  starts_with: "Starts with",
  ends_with: "Ends with",
  is_null: "Is null",
  is_not_null: "Is not null",
  in: "In",
  not_in: "Not in",
  between: "Between",
};

/**
 * Get the label for a filter operator.
 */
export function getFilterOperatorLabel(operator: FilterOperator): string {
  return FILTER_OPERATOR_REVERSE_LABELS[operator] || operator;
}

/**
 * Operators that require a value input.
 */
export const OPERATORS_WITH_VALUE: FilterOperator[] = [
  "equals",
  "not_equals",
  "greater_than",
  "greater_than_or_equals",
  "less_than",
  "less_than_or_equals",
  "contains",
  "not_contains",
  "starts_with",
  "ends_with",
  "in",
  "not_in",
  "between",
];

/**
 * Operators that require a second value (value2).
 */
export const OPERATORS_WITH_VALUE2: FilterOperator[] = ["between"];

/**
 * Operators that don't require any value.
 */
export const OPERATORS_NO_VALUE: FilterOperator[] = ["is_null", "is_not_null"];

export interface BasicFilter {
  field: string;
  operator: FilterOperator | string;
  value: string;
  value2?: string; // For BETWEEN operator
  // Legacy fields for backward compatibility
  filter_type?: string;
  filter_value?: string;
}

export type FilterMode = "basic" | "advanced";

export interface FilterInput {
  mode: FilterMode;
  basic_filter?: BasicFilter;
  advanced_filter?: string;
  // Legacy field for backward compatibility
  filter_type?: string;
}

// ============================================================================
// Aggregation Types
// ============================================================================

export interface AggColl {
  old_name: string;
  agg: string;
  new_name?: string;
  output_type?: string;
}

export interface GroupByInput {
  agg_cols: AggColl[];
}

export type AggOption =
  | "sum"
  | "max"
  | "median"
  | "min"
  | "count"
  | "n_unique"
  | "mean"
  | "concat"
  | "first"
  | "last";
export type GroupByOption = "groupby";

export interface PivotInput {
  index_columns: string[];
  pivot_column?: string | null;
  value_col?: string | null;
  aggregations: AggOption[];
}

export type DataTypeSelector = "float" | "all" | "date" | "numeric" | "string";
export type DataSelectorMode = "data_type" | "column";

export interface UnpivotInput {
  index_columns: string[];
  value_columns: string[];
  data_type_selector?: DataTypeSelector | null;
  data_type_selector_mode: DataSelectorMode;
}

export interface UniqueInput {
  columns: string[];
  strategy: "first" | "last" | "any" | "none";
}

// ============================================================================
// Join Types
// ============================================================================

export interface JoinMap {
  left_col: string;
  right_col: string;
}

export interface CrossJoinInput {
  left_select: SelectInputs;
  right_select: SelectInputs;
  how: string;
}

export interface FuzzyMap {
  left_col: string;
  right_col: string;
  threshold_score: number;
  fuzzy_type: string;
  valid: boolean;
}

export interface FuzzyJoinSettings {
  join_mapping: FuzzyMap[];
  left_select: SelectInputs;
  right_select: SelectInputs;
  aggregate_output: boolean;
}

// ============================================================================
// Sort Types
// ============================================================================

export interface SortByInput {
  column: string;
  how: string;
}

// ============================================================================
// Text Operations Types
// ============================================================================

export interface TextToRowsInput {
  column_to_split: string;
  output_column_name?: string;
  split_by_fixed_value?: boolean;
  split_fixed_value?: string;
  split_by_column?: string;
}

export interface RecordIdInput {
  output_column_name: string;
  offset: number;
  group_by: boolean;
  group_by_columns: string[];
}

// ============================================================================
// Graph Solver Types
// ============================================================================

export interface GraphSolverInput {
  col_from: string;
  col_to: string;
  output_column_name: string;
}

// ============================================================================
// Polars Code Types
// ============================================================================

export interface PolarsCodeInput {
  polars_code: string;
}

// ============================================================================
// Union Types
// ============================================================================

export interface UnionInput {
  mode: "selective" | "relaxed";
}

// ============================================================================
// Raw Data Types
// ============================================================================

export interface RawDataFormat {
  columns?: MinimalFieldInput[] | null;
  data: unknown[][];
}

// ============================================================================
// Received/File Table Types
// ============================================================================

export interface ReceivedTable {
  id?: number;
  name?: string;
  path: string;
  directory?: string;
  analysis_file_available?: boolean;
  status?: string;
  fields?: MinimalFieldInput[];
  abs_file_path?: string;
  file_type: "csv" | "json" | "parquet" | "excel";
  table_settings: InputTableSettings;
}

// ============================================================================
// Database Types
// ============================================================================

export interface DatabaseConnection {
  database_type: "postgresql" | "mysql";
  username: string;
  password_ref: string;
  host?: string;
  port?: number;
  database?: string;
  url?: string;
}

export type ConnectionModeOption = "inline" | "reference";
export type IfExistAction = "append" | "replace" | "fail";

export interface DatabaseSettings {
  connection_mode: ConnectionModeOption;
  database_connection?: DatabaseConnection;
  database_connection_name?: string;
  query_mode: "query" | "table";
  schema_name?: string;
  table_name?: string;
  query: string;
}

export interface DatabaseWriteSettings {
  connection_mode: ConnectionModeOption;
  database_connection?: DatabaseConnection;
  database_connection_name?: string;
  schema_name?: string;
  table_name?: string;
  if_exists: IfExistAction;
}

// ============================================================================
// Cloud Storage Types
// ============================================================================

interface CloudStorageSettings {
  auth_mode: AuthMethod;
  connection_name?: string;
  resource_path: string;
}

export type FileFormat = "csv" | "parquet" | "json" | "delta" | "iceberg";
export type CsvEncoding = "utf8" | "utf8-lossy";
export type ParquetCompression = "snappy" | "gzip" | "brotli" | "lz4" | "zstd";
export type WriteMode = "overwrite" | "append";

export interface CloudStorageReadSettings extends CloudStorageSettings {
  scan_mode: "single_file" | "directory";
  file_format?: FileFormat | undefined;
  csv_has_header?: boolean;
  csv_delimiter?: string;
  csv_encoding?: CsvEncoding;
  delta_version?: number;
}

export interface CloudStorageWriteSettings extends CloudStorageSettings {
  write_mode: WriteMode;
  file_format: FileFormat;
  parquet_compression: ParquetCompression;
  csv_delimiter: string;
  csv_encoding: CsvEncoding;
}

// ============================================================================
// External Source Types
// ============================================================================

interface ExternalSource {
  orientation: string;
  fields?: MinimalFieldInput[];
}

export interface SampleUsers extends ExternalSource {
  SAMPLE_USERS: boolean;
  size: number;
}

// ============================================================================
// Node Base Types
// ============================================================================

export interface OutputFieldInfo {
  name: string;
  data_type: string;
  default_value?: string | null;
}

export interface OutputFieldConfig {
  enabled: boolean;
  validation_mode_behavior: "add_missing" | "raise_on_missing" | "select_only";
  fields: OutputFieldInfo[];
  validate_data_types: boolean;
}

export interface NodeBase {
  flow_id: string | number;
  node_id: number;
  cache_results: boolean;
  pos_x: number;
  pos_y: number;
  is_setup?: boolean;
  description?: string;
  is_user_defined?: boolean;
  output_field_config?: OutputFieldConfig | null;
}

export interface NodeSingleInput extends NodeBase {
  depending_on_id?: string | number;
}

export interface NodeMultiInput extends NodeBase {
  depending_on_ids: number[] | null;
}

// ============================================================================
// Specific Node Types
// ============================================================================

export interface NodeRead extends NodeBase {
  received_file: ReceivedTable;
}

export interface NodeOutput extends NodeBase {
  output_settings: OutputSettings;
}

export interface NodeInputData extends NodeBase {
  file_ref: string;
}

export interface NodeManualInput extends NodeBase {
  raw_data_format: RawDataFormat;
}

export interface NodeSelect extends NodeSingleInput {
  keep_missing: boolean;
  select_input: SelectInput[];
  sorted_by?: "none" | "asc" | "desc";
}

export interface NodeFilter extends NodeSingleInput {
  filter_input: FilterInput;
}

export interface NodeGroupBy extends NodeSingleInput {
  groupby_input: GroupByInput;
}

export interface NodePivot extends NodeSingleInput {
  pivot_input: PivotInput;
  output_fields: FieldInput[];
}

export interface NodeUnpivot extends NodeSingleInput {
  unpivot_input: UnpivotInput;
}

export interface NodeJoin extends NodeMultiInput {
  join_input: FuzzyJoinSettings;
}

export interface NodeCrossJoin extends NodeMultiInput {
  auto_generate_selection: boolean;
  verify_integrity: boolean;
  cross_join_input: CrossJoinInput;
}

export interface NodeSort extends NodeSingleInput {
  sort_input: SortByInput[];
}

export interface NodeTextToRows extends NodeSingleInput {
  text_to_rows_input: TextToRowsInput;
}

export interface NodeRecordId extends NodeSingleInput {
  record_id_input: RecordIdInput;
}

export interface NodeSample extends NodeBase {
  sample_size: number;
}

export interface NodePolarsCode extends NodeSingleInput {
  polars_code_input: PolarsCodeInput;
}

export interface NodeUnique extends NodeSingleInput {
  unique_input: UniqueInput;
}

export interface NodeGraphSolver extends NodeSingleInput {
  graph_solver_input: GraphSolverInput;
}

export interface NodeUserDefined extends NodeMultiInput {
  settings: any;
}

export interface NodeFormula extends NodeSingleInput {
  function: FormulaInput;
}

export interface NodeUnion extends NodeBase {
  union_input: UnionInput;
}

export interface NodeExternalSource extends NodeBase {
  identifier: string;
  source_settings: SampleUsers;
}

export interface NodeDatabaseReader extends NodeBase {
  database_settings: DatabaseSettings;
  fields?: MinimalFieldInput[];
}

export interface NodeDatabaseWriter extends NodeSingleInput {
  database_write_settings: DatabaseWriteSettings;
}

export interface NodeCloudStorageReader extends NodeBase {
  cloud_storage_settings: CloudStorageReadSettings;
  fields?: MinimalFieldInput[];
}

export interface NodeCloudStorageWriter extends NodeBase {
  cloud_storage_settings: CloudStorageWriteSettings;
}
