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
  ScanMode,
} from "../types/node.types";

export type ReadFileType = "csv" | "excel" | "parquet" | "ipc" | "ndjson" | "avro";

// TS mirror of shared/path_utils.py DIRECTORY_SCAN_FILE_TYPES; ndjson/avro are deliberately excluded.
export const DIRECTORY_CAPABLE_TYPES: ReadonlySet<ReadFileType> = new Set<ReadFileType>([
  "csv",
  "parquet",
  "ipc",
]);

// Predicate, not a plain boolean: ReceivedTable.file_type is the wider engine union.
export function isDirectoryCapable(fileType?: string | null): fileType is ReadFileType {
  return !!fileType && (DIRECTORY_CAPABLE_TYPES as ReadonlySet<string>).has(fileType);
}

// TS mirror of shared/path_utils.py _UTF8_ENCODINGS; a directory-mode csv read rejects the rest.
const UTF8_ENCODINGS: ReadonlySet<string> = new Set(["UTF-8", "UTF8", "UTF8-LOSSY", "UTF-8-LOSSY"]);

export function isUtf8Encoding(encoding?: string | null): boolean {
  return !!encoding && UTF8_ENCODINGS.has(encoding.toUpperCase());
}

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
  // ${...} collapses to a stand-in, not to nothing: "${date}.csv" must keep its extension.
  const ext = extensionOf(path.replace(/\$\{[^}]*\}/g, "_"));
  return ext ? (READ_EXTENSION_MAP[ext] ?? null) : null;
}

/** Best-effort seed only — the renderer cannot stat the path, so a bare directory reads as a file.
 *  Only `*`/`?` promote: `[` is legal in real names, so bracket patterns need the explicit select. */
export function inferScanModeFromPath(path: string): ScanMode {
  // Same ${...} masking as detectFileType: a parameter reference is not a glob.
  const masked = path.replace(/\$\{[^}]*\}/g, "_");
  if (/[*?]/.test(masked)) return "directory";
  return /[/\\]$/.test(masked) ? "directory" : "single_file";
}

function createDefaultCsvSettings(delimiter = ","): InputCsvTable {
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

function createDefaultExcelSettings(): InputExcelTable {
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

function createDefaultParquetSettings(): InputParquetTable {
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
    scan_mode: "single_file",
    include_file_paths: null,
  };
}
