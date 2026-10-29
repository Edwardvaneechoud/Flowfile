import { describe, expect, it } from "vitest";

import type { UnpivotInput } from "../../../baseNode/nodeInput";
import {
  dataTypeLabel,
  rowsFromUnpivot,
  valueColumnsLabel,
  writeUnpivotRows,
} from "./unpivotLogic";

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

describe("valueColumnsLabel", () => {
  it("counts the chosen columns and reads none as every other column, like the backend", () => {
    expect(valueColumnsLabel(["q1", "q2"])).toBe("2 columns");
    expect(valueColumnsLabel(["q1"])).toBe("1 column");
    expect(valueColumnsLabel([])).toBe("every other column");
  });
});

describe("dataTypeLabel", () => {
  it("reads a missing selector as every other column, like the backend", () => {
    expect(dataTypeLabel(null)).toBe("All other columns");
    expect(dataTypeLabel("numeric")).toBe("Numeric columns");
  });
});
