import { ReceivedExcelTable, ReceivedCsvTable, ReceivedParquetTable } from '../../../baseNode/nodeInput'

export const defaultCsvTable: ReceivedCsvTable = {
  name: '',
  path: '',
  file_type: 'csv',
  reference: '',
  starting_from_line: 0,
  delimiter: ',',
  has_headers: true,
  encoding: 'utf-8',
  row_delimiter: '',
  quote_char: '',
  infer_schema_length: 0,
  truncate_ragged_lines: false,
  ignore_errors: false,
}

export const defaultParquetTable: ReceivedParquetTable = {
  name: '',
  path: '',
  file_type: 'parquet',
}
