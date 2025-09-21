import { ref } from 'vue'
import { AuthMethod,  } from "../../../pages/cloudConnectionManager/CloudConnectionTypes"

export interface SelectInput {
  old_name: string
  new_name: string
  keep: boolean
  data_type?: string // Optional
  join_key?: boolean // Optional
  is_altered: boolean
  data_type_change: boolean
  is_available: boolean
  position: number
  original_position: number
}

export const createSelectInputFromName = (columnName: string, keep: boolean = true): SelectInput => {
  return {
    old_name: columnName,
    new_name: columnName,
    keep: keep,
    is_altered: false,
    data_type_change: false,
    is_available: true,
    position: 0,
    original_position: 0
  };
}

export interface MinimalFieldInput {
  name: string
  data_type: string
}

export interface UniqueInput {
  columns: string[]
  strategy: 'first' | 'last' | 'any' | 'none'
}

export interface FieldInput {
  name: string
  data_type?: string // Optional
}

export interface FormulaInput {
  field: FieldInput
  function: string
}

export interface BasicFilter {
  field: string
  filter_type: string
  filter_value: string
}

export interface FilterInput {
  advanced_filter?: string
  basic_filter?: BasicFilter
  filter_type?: string
}

export interface SelectInputs {
  renames: SelectInput[]
}

export interface UnionInput {
  mode: 'selective' | 'relaxed'
}

export interface NodeUnion extends NodeBase {
  union_input: UnionInput
}

export interface AggColl {
  old_name: string
  agg: string
  new_name?: string // Optional
  output_type?: string // Optional
}

export interface GroupByInput {
  agg_cols: AggColl[]
}

export interface GraphSolverInput {
  col_from: string
  col_to: string
  output_column_name: string
}

export type AggOption = 'sum' | 'max' | 'median' | 'min' | 'count' | 'n_unique' | 'mean' | 'concat' | 'first' | 'last'
export type GroupByOption = 'groupby';

export interface PivotInput {
  index_columns: string[]
  pivot_column?: string | null
  value_col?: string | null
  aggregations: AggOption[]
}

export type DataTypeSelector = 'float' | 'all' | 'date' | 'numeric' | 'string'
export type DataSelectorMode =  'data_type' | 'column'

export interface UnpivotInput {
  index_columns: string[]
  value_columns: string[]
  data_type_selector?: DataTypeSelector|null
  data_type_selector_mode: DataSelectorMode
}

export interface NodeBase {
  flow_id: string | number
  node_id: number
  cache_results: boolean
  pos_x: number
  pos_y: number
  is_setup?: boolean
  description?: string
  is_user_defined?: boolean
}

interface ExternalSource {
  orientation: string
  fields?: MinimalFieldInput[]
}

export interface SampleUsers extends ExternalSource {
  SAMPLE_USERS: boolean
  size: number
}


export interface NodeExternalSource extends NodeBase {
  identifier: string
  source_settings: SampleUsers
}

interface ReceivedTable {
  id?: number
  name: string
  path: string
  analysis_file_available?: boolean
  status?: string
  file_type?: string
}

export interface ReceivedCsvTable extends ReceivedTable {
  reference: string
  starting_from_line: number
  delimiter: string
  has_headers: boolean
  encoding: string
  row_delimiter: string
  quote_char: string
  infer_schema_length: number
  truncate_ragged_lines: boolean
  ignore_errors: boolean
}

export interface ReceivedParquetTable extends ReceivedTable {
  file_type: string
}

interface OutputTableBase {
  name: string
  path: string
  directory?: string
  file_type: string
  fields?: string[]
}

export interface OutputCsvTable {
  delimiter: string
  encoding: string
  file_type: string
}

export interface OutputParquetTable {
  file_type: string
}

export interface OutputExcelTable{
  sheet_name: string
  file_type: string
}

export interface ReceivedExcelTable extends ReceivedTable {
  sheet_name: string
  start_row: number
  start_column: number
  end_row: number // 0 can indicate reading until the last row
  end_column: number // 0 can indicate reading until the last column
  has_headers: boolean
  type_inference: boolean
}

export interface NodeRead extends NodeBase {
  received_file: ReceivedCsvTable | ReceivedParquetTable | ReceivedExcelTable
}


export interface OutputSettings {
  name: string
  directory: string
  file_type: 'parquet' | 'csv' | 'excel'
  fields?: string[]
  write_mode: 'overwrite' | 'append' | 'error'
  output_csv_table: OutputCsvTable
  output_parquet_table: OutputParquetTable
  output_excel_table: OutputExcelTable
}

export interface NodeOutput extends NodeBase {
  output_settings: OutputSettings
}

export interface NodeSingleInput extends NodeBase {
  depending_on_id?: string | number // Since it's Optional
}

export interface NodeInputData extends NodeBase {
  file_ref: string
}

export interface RawDataFormat {
  columns?: MinimalFieldInput[] | null
  data: unknown[][]
}

export interface NodeManualInput extends NodeBase {
  raw_data_format: RawDataFormat
}

export interface NodeSelect extends NodeSingleInput {
  keep_missing: boolean
  select_input: SelectInput[]
  sorted_by?: "none"| "asc" | "desc"
}

export interface NodeFilter extends NodeSingleInput {
  filter_input: FilterInput
}

export interface NodeGroupBy extends NodeSingleInput {
  groupby_input: GroupByInput
}

export interface NodePivot extends NodeSingleInput {
  pivot_input: PivotInput
  output_fields: FieldInput[]
}

export interface NodeUnpivot extends NodeSingleInput {
  unpivot_input: UnpivotInput
}

export const nodeSelect = ref<NodeSelect | null>(null)

export interface NodeMultiInput extends NodeBase {
  depending_on_ids: number[] | null
}

export interface NodeJoin extends NodeMultiInput {
  join_input: FuzzyJoinSettings
}

export interface NodeCrossJoin extends NodeMultiInput {
  auto_generate_selection: boolean
  verify_integrity: boolean
  cross_join_input: CrossJoinInput
}

export interface JoinMap {
  left_col: string
  right_col: string
}

export interface CrossJoinInput {
  left_select: SelectInputs
  right_select: SelectInputs
  how: string
}


export interface FuzzyMap {
  left_col: string
  right_col: string
  threshold_score: number
  fuzzy_type: string
  valid: boolean
}

export interface FuzzyJoinSettings {
  join_mapping: FuzzyMap[]
  left_select: SelectInputs
  right_select: SelectInputs
  aggregate_output: boolean
}

export interface SortByInput {
  column: string
  how: string
}

export interface NodeSort extends NodeSingleInput {
  sort_input: SortByInput[]
}

export interface NodeTextToRows extends NodeSingleInput {
  text_to_rows_input: TextToRowsInput
}

export interface TextToRowsInput {
  column_to_split: string
  output_column_name?: string
  split_by_fixed_value?: boolean
  split_fixed_value?: string
  split_by_column?: string
}

export interface RecordIdInput {
  output_column_name: string
  offset: number
  group_by: boolean
  group_by_columns: string[]
}

export interface NodeRecordId extends NodeSingleInput {
  record_id_input: RecordIdInput
}

export interface NodeSample extends NodeBase {
  sample_size: number
}

export interface PolarsCodeInput {
  polars_code: string
}

export interface NodePolarsCode extends NodeSingleInput {
  polars_code_input: PolarsCodeInput
}

export interface NodeUnique extends NodeSingleInput {
  unique_input: UniqueInput
}

export interface NodeGraphSolver extends NodeSingleInput {
  graph_solver_input: GraphSolverInput
}

export interface NodeUserDefined extends NodeMultiInput {
  settings: any
}

export interface NodeFormula extends NodeSingleInput {
  function: FormulaInput
}

export interface DatabaseConnection {
  database_type: "postgresql" | "mysql"
  username: string
  password_ref: string
  host?: string
  port?: number
  database?: string
  url?: string
}

export type ConnectionModeOption = "inline" | "reference"

export interface DatabaseSettings {
  connection_mode: ConnectionModeOption
  database_connection?: DatabaseConnection
  database_connection_name?: string
  query_mode: "query" | "table"
  schema_name?: string
  table_name?: string
  query: string
}

export interface NodeDatabaseReader extends NodeBase {
  database_settings: DatabaseSettings
  fields?: MinimalFieldInput[]
}

export type IfExistAction =  'append' | 'replace' | 'fail'

export interface DatabaseWriteSettings {
  connection_mode: ConnectionModeOption
  database_connection?: DatabaseConnection
  database_connection_name?: string
  schema_name?: string
  table_name?: string
  if_exists: IfExistAction
}

export interface NodeDatabaseWriter extends NodeSingleInput {
  database_write_settings: DatabaseWriteSettings
}

interface CloudStorageSettings {
  auth_mode: AuthMethod
  connection_name?: string
  resource_path: string
}

export type FileFormat = "csv" | "parquet" | "json" | "delta" | "iceberg"
export type CsvEncoding = "utf8" | "utf8-lossy"
export type ParquetCompression = "snappy" | "gzip" | "brotli" | "lz4" | "zstd"
export type WriteMode = "overwrite" | "append"

export interface CloudStorageReadSettings extends CloudStorageSettings {
  scan_mode: "single_file" | "directory"
  file_format?: FileFormat | undefined
  csv_has_header?: boolean
  csv_delimiter?: string
  csv_encoding?: CsvEncoding
  delta_version?: number
}

export interface CloudStorageWriteSettings extends CloudStorageSettings {
  write_mode: WriteMode
  file_format: FileFormat
  parquet_compression: ParquetCompression
  csv_delimiter: string
  csv_encoding: CsvEncoding
}

export interface NodeCloudStorageReader extends NodeBase {
  cloud_storage_settings: CloudStorageReadSettings
  fields?: MinimalFieldInput[]
}

export interface NodeCloudStorageWriter extends NodeBase {
  cloud_storage_settings: CloudStorageWriteSettings
}