import { describe, expect, it } from "vitest";

import type { PivotInput } from "../../../baseNode/nodeInput";
import { missingPivotParts, rowsFromPivot, writePivotRows } from "./pivotLogic";

const input = (): PivotInput => ({
  index_columns: ["Country", "Region"],
  pivot_column: "quarter",
  value_col: "sales",
  aggregations: ["sum"],
});

describe("rowsFromPivot / writePivotRows", () => {
  it("lists index keys in order, then the pivot and value columns", () => {
    expect(rowsFromPivot(input()).map((row) => `${row.name}:${row.role}`)).toEqual([
      "Country:index",
      "Region:index",
      "quarter:pivot",
      "sales:value",
    ]);
  });

  it("round-trips and clears a role nobody holds", () => {
    const target = input();
    writePivotRows(
      target,
      rowsFromPivot(input()).filter((row) => row.role !== "value"),
    );
    expect(target).toMatchObject({
      index_columns: ["Country", "Region"],
      pivot_column: "quarter",
      value_col: null,
      aggregations: ["sum"],
    });
  });
});

describe("missingPivotParts", () => {
  it("names each missing requirement", () => {
    expect(missingPivotParts(input())).toEqual([]);
    expect(
      missingPivotParts({
        index_columns: [],
        pivot_column: null,
        value_col: null,
        aggregations: [],
      }),
    ).toEqual(["pivot column", "value column", "aggregation"]);
  });
});
