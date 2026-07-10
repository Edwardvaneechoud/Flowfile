// Sample-data helpers: row<->column conversion, dtype coercion, CSV parsing, caps.
import { describe, expect, it } from "vitest";
import {
  MAX_SAMPLE_COLS,
  MAX_SAMPLE_ROWS,
  coerceCell,
  columnDataToTable,
  parseCsv,
  sampleColumnsToFileColumns,
  tableToColumnData,
  type SampleTable,
} from "./sampleData";

describe("coerceCell", () => {
  it("coerces per dtype and treats empty as null", () => {
    expect(coerceCell("42", "int")).toBe(42);
    expect(coerceCell("3.5", "float")).toBe(3.5);
    expect(coerceCell("true", "bool")).toBe(true);
    expect(coerceCell("no", "bool")).toBe(false);
    expect(coerceCell("hello", "str")).toBe("hello");
    expect(coerceCell("", "int")).toBeNull();
    expect(coerceCell("not-a-number", "int")).toBeNull();
  });

  it("keeps date/datetime as strings", () => {
    expect(coerceCell("2024-01-01", "date")).toBe("2024-01-01");
    expect(coerceCell("2024-01-01T00:00:00", "datetime")).toBe("2024-01-01T00:00:00");
  });
});

describe("tableToColumnData", () => {
  it("produces column-oriented JSON scalars", () => {
    const table: SampleTable = {
      columns: [
        { name: "city", dtype: "str" },
        { name: "sales", dtype: "int" },
      ],
      rows: [
        { city: "NY", sales: "10" },
        { city: "SF", sales: "20" },
      ],
    };
    expect(tableToColumnData(table)).toEqual({
      city: ["NY", "SF"],
      sales: [10, 20],
    });
  });
});

describe("columnDataToTable", () => {
  it("round-trips column data back into a grid, inferring dtypes", () => {
    const table = columnDataToTable({ city: ["NY", "SF"], sales: [10, 20] });
    expect(table.columns).toEqual([
      { name: "city", dtype: "str" },
      { name: "sales", dtype: "int" },
    ]);
    expect(table.rows).toEqual([
      { city: "NY", sales: "10" },
      { city: "SF", sales: "20" },
    ]);
  });

  it("round trips through tableToColumnData for the same values", () => {
    const original = { a: [1, 2, 3], b: ["x", "y", "z"] };
    expect(tableToColumnData(columnDataToTable(original))).toEqual(original);
  });
});

describe("sampleColumnsToFileColumns", () => {
  it("maps grid dtypes to the Polars type-names the ColumnSelector filters on", () => {
    const result = sampleColumnsToFileColumns([
      { name: "age", dtype: "int" },
      { name: "income", dtype: "float" },
      { name: "city", dtype: "str" },
      { name: "active", dtype: "bool" },
    ]);
    expect(result.map((c) => [c.name, c.data_type])).toEqual([
      ["age", "Int64"],
      ["income", "Float64"],
      ["city", "String"],
      ["active", "Boolean"],
    ]);
  });
});

describe("parseCsv", () => {
  it("parses comma-delimited text with a header row", () => {
    const table = parseCsv("city,sales\nNY,10\nSF,20");
    expect(table.columns.map((c) => c.name)).toEqual(["city", "sales"]);
    expect(table.columns[1].dtype).toBe("int");
    expect(table.rows).toEqual([
      { city: "NY", sales: "10" },
      { city: "SF", sales: "20" },
    ]);
  });

  it("sniffs tab delimiters", () => {
    const table = parseCsv("a\tb\n1\t2");
    expect(table.columns.map((c) => c.name)).toEqual(["a", "b"]);
    expect(table.rows).toEqual([{ a: "1", b: "2" }]);
  });

  it("returns an empty table for blank text", () => {
    expect(parseCsv("   ")).toEqual({ columns: [], rows: [] });
  });

  it("caps rows and columns", () => {
    const header = Array.from({ length: MAX_SAMPLE_COLS + 5 }, (_, i) => `c${i}`).join(",");
    const bodyRow = Array.from({ length: MAX_SAMPLE_COLS + 5 }, () => "1").join(",");
    const rows = Array.from({ length: MAX_SAMPLE_ROWS + 10 }, () => bodyRow).join("\n");
    const table = parseCsv(`${header}\n${rows}`);
    expect(table.columns.length).toBe(MAX_SAMPLE_COLS);
    expect(table.rows.length).toBe(MAX_SAMPLE_ROWS);
  });
});
