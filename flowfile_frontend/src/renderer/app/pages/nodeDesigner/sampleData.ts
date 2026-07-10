// Sample-data helpers for the Test panel: row<->column conversion, CSV parsing,
// and per-column dtype coercion. Column-oriented is the wire shape
// (ExampleInput.data); the grid edits a row-oriented view. Unit-tested in
// sampleData.test.ts.

import type { FileColumn } from "@/types";

export type SampleDtype = "str" | "int" | "float" | "bool" | "date" | "datetime";

export const SAMPLE_DTYPES: SampleDtype[] = ["str", "int", "float", "bool", "date", "datetime"];

export const MAX_SAMPLE_ROWS = 100;
export const MAX_SAMPLE_COLS = 25;

export interface SampleColumn {
  name: string;
  dtype: SampleDtype;
}

export interface SampleTable {
  columns: SampleColumn[];
  // rows[r][colName] = cell string; grid holds strings, coerced on export.
  rows: Record<string, string>[];
}

/** Coerce a raw string cell to the JSON scalar implied by its column dtype. */
export function coerceCell(raw: string, dtype: SampleDtype): unknown {
  const value = raw ?? "";
  if (value === "") return null;
  switch (dtype) {
    case "int": {
      const n = parseInt(value, 10);
      return Number.isNaN(n) ? null : n;
    }
    case "float": {
      const n = Number(value);
      return Number.isNaN(n) ? null : n;
    }
    case "bool": {
      const lowered = value.trim().toLowerCase();
      if (["true", "1", "yes", "y"].includes(lowered)) return true;
      if (["false", "0", "no", "n"].includes(lowered)) return false;
      return null;
    }
    case "str":
    case "date":
    case "datetime":
    default:
      return value;
  }
}

/** Grid table -> column-oriented ExampleInput.data ({col: [values]}). */
export function tableToColumnData(table: SampleTable): Record<string, unknown[]> {
  const data: Record<string, unknown[]> = {};
  for (const col of table.columns) {
    data[col.name] = table.rows.map((row) => coerceCell(row[col.name] ?? "", col.dtype));
  }
  return data;
}

/** Wire shape of the backend's input_schema.RawData: typed schema + columnar values. */
export interface RawDataInput {
  columns: Array<{ name: string; data_type: string }>;
  data: unknown[][];
}

/** Grid table -> typed RawData so the backend casts cells to each column's chosen dtype. */
export function tableToRawData(table: SampleTable): RawDataInput {
  return {
    columns: table.columns.map((c) => ({
      name: c.name,
      data_type: SAMPLE_DTYPE_TO_POLARS[c.dtype],
    })),
    data: table.columns.map((col) =>
      table.rows.map((row) => coerceCell(row[col.name] ?? "", col.dtype)),
    ),
  };
}

function inferDtype(values: unknown[]): SampleDtype {
  const nonNull = values.filter((v) => v !== null && v !== undefined);
  if (nonNull.length === 0) return "str";
  if (nonNull.every((v) => typeof v === "boolean")) return "bool";
  if (nonNull.every((v) => typeof v === "number" && Number.isInteger(v))) return "int";
  if (nonNull.every((v) => typeof v === "number")) return "float";
  return "str";
}

/** Column-oriented data -> grid table (stringifies cells, infers dtypes). */
export function columnDataToTable(data: Record<string, unknown[]>): SampleTable {
  const names = Object.keys(data);
  const columns: SampleColumn[] = names.map((name) => ({ name, dtype: inferDtype(data[name]) }));
  const rowCount = names.reduce((max, name) => Math.max(max, data[name].length), 0);
  const rows: Record<string, string>[] = [];
  for (let r = 0; r < rowCount; r++) {
    const row: Record<string, string> = {};
    for (const name of names) {
      const cell = data[name][r];
      row[name] = cell === null || cell === undefined ? "" : String(cell);
    }
    rows.push(row);
  }
  return { columns, rows };
}

/** Parse CSV/TSV text into a grid table (first row = header). Auto-sniffs delimiter. */
export function parseCsv(text: string): SampleTable {
  const trimmed = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
  if (!trimmed) return { columns: [], rows: [] };
  const lines = trimmed.split("\n");
  const delimiter = sniffDelimiter(lines[0]);
  const header = splitLine(lines[0], delimiter);
  const columns: SampleColumn[] = header
    .slice(0, MAX_SAMPLE_COLS)
    .map((name, i) => ({ name: name.trim() || `col_${i + 1}`, dtype: "str" as SampleDtype }));
  const rows: Record<string, string>[] = [];
  for (let i = 1; i < lines.length && rows.length < MAX_SAMPLE_ROWS; i++) {
    const cells = splitLine(lines[i], delimiter);
    const row: Record<string, string> = {};
    columns.forEach((col, ci) => {
      row[col.name] = (cells[ci] ?? "").trim();
    });
    rows.push(row);
  }
  // Infer dtypes from parsed cell values.
  for (const col of columns) {
    col.dtype = inferDtype(rows.map((r) => guessScalar(r[col.name])));
  }
  return { columns, rows };
}

function sniffDelimiter(headerLine: string): string {
  const tabs = (headerLine.match(/\t/g) || []).length;
  const commas = (headerLine.match(/,/g) || []).length;
  const semis = (headerLine.match(/;/g) || []).length;
  if (tabs >= commas && tabs >= semis && tabs > 0) return "\t";
  if (semis > commas && semis > 0) return ";";
  return ",";
}

function splitLine(line: string, delimiter: string): string[] {
  return line.split(delimiter);
}

function guessScalar(raw: string): unknown {
  if (raw === "" || raw === undefined) return null;
  const lowered = raw.trim().toLowerCase();
  if (lowered === "true" || lowered === "false") return lowered === "true";
  const n = Number(raw);
  if (!Number.isNaN(n) && raw.trim() !== "") return Number.isInteger(n) ? n : n;
  return raw;
}

export function emptyTable(): SampleTable {
  return {
    columns: [{ name: "column_1", dtype: "str" }],
    rows: [{ column_1: "" }],
  };
}

// The grid's coarse dtype -> the Polars type-name the ColumnSelector data_types
// filter matches against (DataType.value in the node_designer SDK).
const SAMPLE_DTYPE_TO_POLARS: Record<SampleDtype, string> = {
  str: "String",
  int: "Int64",
  float: "Float64",
  bool: "Boolean",
  date: "Date",
  datetime: "Datetime",
};

// The grid's coarse dtype -> its readable data_type_group, mirroring the backend
// classifier so group-based data_types filters ("Numeric", …) match sample columns.
const SAMPLE_DTYPE_TO_GROUP: Record<SampleDtype, string> = {
  str: "String",
  int: "Numeric",
  float: "Numeric",
  bool: "Boolean",
  date: "Date",
  datetime: "Date",
};

/** SampleColumns -> typed FileColumns for column-driven controls (ColumnSelector, …). */
export function sampleColumnsToFileColumns(columns: SampleColumn[]): FileColumn[] {
  return columns.map(
    (c) =>
      ({
        name: c.name,
        data_type: SAMPLE_DTYPE_TO_POLARS[c.dtype],
        data_type_group: SAMPLE_DTYPE_TO_GROUP[c.dtype],
      }) as FileColumn,
  );
}
