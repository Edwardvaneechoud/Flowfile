import { describe, expect, it } from "vitest";

import type { UnpivotInput } from "../../../baseNode/nodeInput";
import { missingUnpivotParts, rowsFromUnpivot, writeUnpivotRows } from "./unpivotLogic";

const input = (): UnpivotInput => ({
  index_columns: ["Country"],
  value_columns: ["q1", "q2"],
  data_type_selector: null,
  data_type_selector_mode: "column",
});

describe("rowsFromUnpivot / writeUnpivotRows", () => {
  it("lists index keys before value columns and round-trips", () => {
    const rows = rowsFromUnpivot(input());
    expect(rows.map((row) => `${row.name}:${row.role}`)).toEqual([
      "Country:index",
      "q1:value",
      "q2:value",
    ]);
    const target = input();
    writeUnpivotRows(target, [rows[1], rows[0]]);
    expect(target.index_columns).toEqual(["Country"]);
    expect(target.value_columns).toEqual(["q1"]);
  });
});

describe("missingUnpivotParts", () => {
  it("wants value columns in column mode and a selector in data-type mode", () => {
    expect(missingUnpivotParts(input())).toEqual([]);
    expect(missingUnpivotParts({ ...input(), value_columns: [] })).toEqual(["value columns"]);
    const byType = { ...input(), data_type_selector_mode: "data_type" as const };
    expect(missingUnpivotParts(byType)).toEqual(["data type"]);
    expect(missingUnpivotParts({ ...byType, data_type_selector: "numeric" })).toEqual([]);
  });
});
