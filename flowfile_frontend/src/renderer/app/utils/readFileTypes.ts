// File-type detection and default table settings for the Read node. Pure (no
// Vue/axios) so drop handlers, the settings drawer and unit tests share it.
import type {
  InputAvroTable,
  InputCsvTable,
  InputExcelTable,
  InputIpcTable,
  InputNdjsonTable,
  InputParquetTable,
  ReceivedTable,
} from "../types/node.types";

export type ReadFileType = "csv" | "excel" | "parquet" | "ipc" | "ndjson" | "avro";

export type ReadTableSettings =
  | InputCsvTable
  | InputExcelTable
  | InputParquetTable
  | InputIpcTable
  | InputNdjsonTable
  | InputAvroTable;

// ".json" is deliberately absent: ReceivedTable accepts file_type "json" but the engine has no handler.
export const READ_EXTENSION_MAP: Readonly<Record<string, ReadFileType>> = Object.freeze({
  csv: "csv",
  txt: "csv",
  tsv: "csv",
  xlsx: "excel",
  xls: "excel",
  parquet: "parquet",
  ipc: "ipc",
  arrow: "ipc",
  feather: "ipc",
  ndjson: "ndjson",
  jsonl: "ndjson",
  avro: "avro",
});

export const READ_EXTENSIONS: readonly string[] = Object.keys(READ_EXTENSION_MAP);

export function baseNameOf(nameOrPath: string): string {
  return nameOrPath.split(/[/\\]/).pop() ?? "";
}

export function extensionOf(nameOrPath: string): string | null {
  const base = baseNameOf(nameOrPath);
  const dot = base.lastIndexOf(".");
  return dot > 0 ? base.slice(dot + 1).toLowerCase() : null;
}

export function detectFileType(path: string): ReadFileType | null {
  const ext = extensionOf(path.replace(/\$\{[^}]*\}/g, ""));
  return ext ? (READ_EXTENSION_MAP[ext] ?? null) : null;
}

export function createDefaultCsvSettings(delimiter = ","): InputCsvTable {
  return {
    file_type: "csv",
    reference: "",
    starting_from_line: 0,
    delimiter,
    has_headers: true,
    encoding: "utf-8",
    row_delimiter: "\n",
    quote_char: '"',
    infer_schema_length: 10000,
    infer_schema: true,
    truncate_ragged_lines: false,
    ignore_errors: false,
  };
}

export function createDefaultExcelSettings(): InputExcelTable {
  return {
    file_type: "excel",
    sheet_name: "",
    start_row: 0,
    start_column: 0,
    end_row: 0,
    end_column: 0,
    has_headers: true,
    type_inference: false,
  };
}

export function createDefaultParquetSettings(): InputParquetTable {
  return { file_type: "parquet" };
}

export function createDefaultSettings(
  fileType: ReadFileType,
  ext?: string | null,
): ReadTableSettings {
  switch (fileType) {
    case "excel":
      return createDefaultExcelSettings();
    case "parquet":
      return createDefaultParquetSettings();
    case "ipc":
    case "ndjson":
    case "avro":
      return { file_type: fileType };
    default:
      return createDefaultCsvSettings(ext === "tsv" ? "\t" : ",");
  }
}

export interface ReceivedTableInput {
  name: string;
  path: string;
  fileType: ReadFileType;
  ext?: string | null;
}

export function buildReceivedTable({
  name,
  path,
  fileType,
  ext,
}: ReceivedTableInput): ReceivedTable {
  return {
    name,
    path,
    file_type: fileType,
    table_settings: createDefaultSettings(fileType, ext),
  };
}
