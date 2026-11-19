import { describe, expect, it, vi } from "vitest";

vi.mock("../../api/catalog.api", () => ({ CatalogApi: {} }));
vi.mock("../../composables/useGraphicWalkerCompute", () => ({ useGraphicWalkerCompute: vi.fn() }));

import {
  TABLE_CSV_ROW_CAP,
  buildTableCountPayload,
  buildTableCsvPayload,
  buildTablePagePayload,
  capRows,
  columnHeader,
  csvFileName,
  defaultSort,
  formatTableCell,
  headerSortDirection,
  nextHeaderSort,
  isSortableDtype,
  pageCount,
  readRowCount,
  tableConfigOf,
  toCsv,
  totalSort,
  visibleColumns,
} from "./tableTile";
import type { DashboardTableConfig } from "../../types";

const fields = [
  { fid: "region", semanticType: "nominal" },
  { fid: "amount", semanticType: "quantitative" },
  { fid: "day", semanticType: "temporal" },
];

const configOf = (over: Partial<DashboardTableConfig>): DashboardTableConfig => ({
  ...tableConfigOf(null),
  ...over,
});

describe("buildTablePagePayload", () => {
  it("sorts, selects the columns and slices one page on the worker", () => {
    expect(
      buildTablePagePayload({
        columns: ["region", "amount"],
        sort: { by: ["amount"], direction: "descending" },
        page: 2,
        pageSize: 25,
      }),
    ).toEqual({
      workflow: [
        { type: "sort", by: ["amount", "region"], sort: "descending" },
        { type: "view", query: [{ op: "raw", fields: ["region", "amount"] }] },
      ],
      limit: 25,
      offset: 50,
    });
  });

  it("leaves out the sort without one and the selection when every column shows", () => {
    expect(buildTablePagePayload({ columns: [], sort: null, page: 0, pageSize: 10 })).toEqual({
      workflow: [],
      limit: 10,
      offset: 0,
    });
  });

  it("breaks ties on the other shown columns that can be sorted", () => {
    expect(
      buildTablePagePayload({
        columns: ["region", "tags", "blob", "amount"],
        sort: { by: ["amount"], direction: "ascending" },
        dtypes: { tags: "List(String)", blob: "Object", amount: "Float64" },
        page: 0,
        pageSize: 5,
      }).workflow,
    ).toEqual([
      { type: "sort", by: ["amount", "region", "tags"], sort: "ascending" },
      { type: "view", query: [{ op: "raw", fields: ["region", "tags", "blob", "amount"] }] },
    ]);
  });

  it("never asks for a negative offset", () => {
    expect(buildTablePagePayload({ columns: [], sort: null, page: -1, pageSize: 10 }).offset).toBe(
      0,
    );
  });
});

describe("the count query", () => {
  it("is one empty-groupBy count", () => {
    const payload = buildTableCountPayload();
    expect(payload.limit).toBe(2);
    expect(payload.workflow).toEqual([
      {
        type: "view",
        query: [
          {
            op: "aggregate",
            groupBy: [],
            measures: [{ field: "*", agg: "count", asFieldKey: "table_row_count" }],
          },
        ],
      },
    ]);
  });

  it("reads the count, or null when the answer is not one count row", () => {
    expect(readRowCount([{ table_row_count: 270 }])).toBe(270);
    expect(readRowCount([{ table_row_count: "12" }])).toBe(12);
    expect(readRowCount([])).toBeNull();
    expect(readRowCount([{ other: 1 }])).toBeNull();
    expect(readRowCount([{ table_row_count: 1 }, { table_row_count: 2 }])).toBeNull();
  });
});

describe("the CSV download", () => {
  it("asks for one row past the cap, sorted and with the chosen columns", () => {
    expect(
      buildTableCsvPayload({
        columns: ["region"],
        sort: { by: ["region"], direction: "ascending" },
      }),
    ).toEqual({
      workflow: [
        { type: "sort", by: ["region"], sort: "ascending" },
        { type: "view", query: [{ op: "raw", fields: ["region"] }] },
      ],
      limit: TABLE_CSV_ROW_CAP + 1,
      offset: 0,
    });
    expect(buildTableCsvPayload({ columns: [], sort: null, cap: 10 }).limit).toBe(11);
  });

  it("tells when the cap was hit", () => {
    expect(capRows([1, 2, 3], 2)).toEqual({ rows: [1, 2], capped: true });
    expect(capRows([1, 2], 2)).toEqual({ rows: [1, 2], capped: false });
  });

  it("writes the headers and quotes cells that need it", () => {
    const csv = toCsv(
      [{ field: "region", label: "Region" }, { field: "note" }, { field: "amount" }],
      [
        { region: "West", note: 'said "hi", left', amount: 1234.5 },
        { region: "East", note: null, amount: 7 },
      ],
    );
    expect(csv).toBe(
      ["Region,note,amount", 'West,"said ""hi"", left",1234.5', "East,,7"].join("\n"),
    );
  });

  it("names the file after the title", () => {
    expect(csvFileName("Latest orders / EU")).toBe("Latest_orders_EU.csv");
    expect(csvFileName("  ")).toBe("table.csv");
    expect(csvFileName(null)).toBe("table.csv");
  });
});

describe("columns", () => {
  it("shows every column when none are chosen, else the chosen ones the source still has", () => {
    expect(visibleColumns(null, fields).map((c) => c.field)).toEqual(["region", "amount", "day"]);
    const config = configOf({
      columns: [{ field: "amount", label: "Amount" }, { field: "gone" }, { field: "region" }],
    });
    expect(visibleColumns(config, fields)).toEqual([
      { field: "amount", label: "Amount" },
      { field: "region" },
    ]);
  });

  it("heads a column with its label, else its name", () => {
    expect(columnHeader({ field: "amount", label: " Amount " })).toBe("Amount");
    expect(columnHeader({ field: "amount", label: "  " })).toBe("amount");
  });
});

describe("sorting", () => {
  it("keeps the default sort on the columns the source still has", () => {
    expect(defaultSort(configOf({ sort_by: ["gone", "amount"] }), fields)).toEqual({
      by: ["amount"],
      direction: "ascending",
    });
    expect(defaultSort(configOf({ sort_by: ["gone"] }), fields)).toBeNull();
    expect(defaultSort(null, fields)).toBeNull();
  });

  it("cycles a header through ascending, descending and back to the default", () => {
    const asc = nextHeaderSort(null, "amount");
    expect(asc).toEqual({ by: ["amount"], direction: "ascending" });
    const desc = nextHeaderSort(asc, "amount");
    expect(desc).toEqual({ by: ["amount"], direction: "descending" });
    expect(nextHeaderSort(desc, "amount")).toBeNull();
    expect(nextHeaderSort(desc, "region")).toEqual({ by: ["region"], direction: "ascending" });
    // A multi-column default sort is replaced by a click, not continued.
    expect(nextHeaderSort({ by: ["amount", "region"], direction: "ascending" }, "amount")).toEqual({
      by: ["amount"],
      direction: "ascending",
    });
  });

  it("makes the order total: chosen columns first, then the other shown columns in order", () => {
    expect(
      totalSort({ by: ["amount", "day"], direction: "descending" }, ["region", "day", "amount"]),
    ).toEqual({ by: ["amount", "day", "region"], direction: "descending" });
    expect(totalSort(null, ["region"])).toBeNull();
    expect(totalSort({ by: [], direction: "ascending" }, ["region"])).toBeNull();
  });

  it("leaves out tiebreakers Polars cannot sort by, and keeps a column whose dtype is unknown", () => {
    expect(
      totalSort({ by: ["amount"], direction: "ascending" }, ["amount", "obj", "s", "b", "x"], {
        obj: "Object",
        s: "Struct({'x': Int64})",
        b: "Binary",
      }),
    ).toEqual({ by: ["amount", "s", "b", "x"], direction: "ascending" });
    expect(isSortableDtype("Object")).toBe(false);
    expect(isSortableDtype("List(Int64)")).toBe(true);
    expect(isSortableDtype("Array(Int64, shape=(2,))")).toBe(true);
    expect(isSortableDtype(undefined)).toBe(true);
  });

  it("marks only the column the rows are sorted by first", () => {
    const sort = { by: ["amount", "region"], direction: "descending" as const };
    expect(headerSortDirection(sort, "amount")).toBe("descending");
    expect(headerSortDirection(sort, "region")).toBeNull();
    expect(headerSortDirection(null, "amount")).toBeNull();
  });
});

describe("formatTableCell", () => {
  it("formats numbers with the column's decimals and thousands separators", () => {
    expect(formatTableCell(1234567.891, { field: "a" }, "en-US")).toBe("1,234,567.89");
    expect(formatTableCell(1234.5, { field: "a", decimals: 2 }, "en-US")).toBe("1,234.50");
    expect(formatTableCell(1234.5, { field: "a", decimals: 0, thousands: false }, "en-US")).toBe(
      "1235",
    );
    expect(formatTableCell(-1234, { field: "a" }, "en-US")).toBe("-1,234");
  });

  it("shows text, nulls and nested values as text", () => {
    expect(formatTableCell("West", { field: "a", decimals: 2 })).toBe("West");
    expect(formatTableCell(null, { field: "a" })).toBe("");
    expect(formatTableCell({ k: 1 }, { field: "a" })).toBe('{"k":1}');
  });
});

describe("pageCount", () => {
  it("is at least one page", () => {
    expect(pageCount(0, 25)).toBe(1);
    expect(pageCount(null, 25)).toBe(1);
    expect(pageCount(51, 25)).toBe(3);
    expect(pageCount(50, 25)).toBe(2);
  });
});
