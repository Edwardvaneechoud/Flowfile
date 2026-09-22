import { describe, expect, it } from "vitest";

import {
  COLUMNS_MIN_PX,
  SETTINGS_STRIP_PX,
  addAggRows,
  clampSettingsHeight,
  defaultAggFor,
  dropZoneAt,
  duplicateOutputNames,
  isNumericType,
  outputNameFor,
  pluralize,
  renamedForAgg,
  shiftAfterRemoval,
  usageByColumn,
  withoutRows,
} from "./groupByLogic";

describe("defaultAggFor", () => {
  it("sums numbers and counts everything else", () => {
    for (const type of ["Int32", "Int64", "UInt8", "Float64", "Decimal(10,2)"]) {
      expect(isNumericType(type)).toBe(true);
      expect(defaultAggFor(type)).toBe("sum");
    }
    for (const type of ["String", "Date", "Datetime", "Boolean", "List(Int64)", undefined]) {
      expect(isNumericType(type)).toBe(false);
      expect(defaultAggFor(type)).toBe("count");
    }
  });
});

describe("addAggRows", () => {
  it("appends one row per column with the conventional output name", () => {
    const result = addAggRows([], ["a", "b"], "sum");
    expect(result.rows).toEqual([
      { old_name: "a", agg: "sum", new_name: "a_sum" },
      { old_name: "b", agg: "sum", new_name: "b_sum" },
    ]);
    expect(result.added).toEqual([0, 1]);
    expect(result.existing).toEqual([]);
  });

  it("keeps a group-by key's own name", () => {
    expect(addAggRows([], ["country"], "groupby").rows[0].new_name).toBe("country");
    expect(outputNameFor("country", "groupby")).toBe("country");
  });

  it("reports an existing (column, aggregation) pair instead of duplicating it", () => {
    const rows = [{ old_name: "a", agg: "sum", new_name: "total" }];
    const result = addAggRows(rows, ["a", "b"], "sum");
    expect(result.existing).toEqual([0]);
    expect(result.added).toEqual([1]);
    expect(result.rows).toHaveLength(2);
    expect(rows).toHaveLength(1);
  });

  it("allows the same column under a different aggregation", () => {
    const rows = [{ old_name: "a", agg: "sum", new_name: "a_sum" }];
    expect(addAggRows(rows, ["a"], "count").added).toEqual([1]);
  });
});

describe("renamedForAgg", () => {
  it("follows the aggregation while the name is still the generated one", () => {
    expect(renamedForAgg({ old_name: "a", agg: "sum", new_name: "a_sum" }, "mean")).toBe("a_mean");
    expect(renamedForAgg({ old_name: "a", agg: "groupby", new_name: "a" }, "count")).toBe(
      "a_count",
    );
  });

  it("leaves a hand-typed name alone", () => {
    expect(renamedForAgg({ old_name: "a", agg: "sum", new_name: "total" }, "mean")).toBe("total");
  });
});

describe("duplicateOutputNames", () => {
  it("lists names used more than once and ignores blanks", () => {
    const duplicates = duplicateOutputNames([
      { old_name: "a", agg: "sum", new_name: "x" },
      { old_name: "b", agg: "sum", new_name: "x" },
      { old_name: "c", agg: "sum", new_name: "y" },
      { old_name: "d", agg: "sum", new_name: "" },
      { old_name: "e", agg: "sum" },
    ]);
    expect([...duplicates]).toEqual(["x"]);
  });
});

describe("usageByColumn", () => {
  it("groups rows by source column with their table index", () => {
    const usage = usageByColumn([
      { old_name: "a", agg: "groupby", new_name: "a" },
      { old_name: "b", agg: "sum", new_name: "b_sum" },
      { old_name: "a", agg: "count", new_name: "a_count" },
    ]);
    expect(usage.get("a")).toEqual([
      { agg: "groupby", index: 0 },
      { agg: "count", index: 2 },
    ]);
    expect(usage.get("b")).toEqual([{ agg: "sum", index: 1 }]);
    expect(usage.has("c")).toBe(false);
  });
});

describe("dropZoneAt", () => {
  it("splits the pane down the middle", () => {
    const rect = { left: 100, width: 200 };
    expect(dropZoneAt(rect, 100)).toBe("groupby");
    expect(dropZoneAt(rect, 199)).toBe("groupby");
    expect(dropZoneAt(rect, 200)).toBe("aggregate");
    expect(dropZoneAt(rect, 300)).toBe("aggregate");
  });
});

describe("pluralize", () => {
  it("handles regular and irregular plurals", () => {
    expect(pluralize(1, "key")).toBe("1 key");
    expect(pluralize(2, "key")).toBe("2 keys");
    expect(pluralize(0, "aggregation")).toBe("0 aggregations");
  });
});

describe("clampSettingsHeight", () => {
  it("never goes below the bare strip", () => {
    expect(clampSettingsHeight(-50, 400)).toBe(SETTINGS_STRIP_PX);
    expect(clampSettingsHeight(10, 400)).toBe(SETTINGS_STRIP_PX);
  });

  it("leaves the column list its floor", () => {
    expect(clampSettingsHeight(1000, 400)).toBe(400 - COLUMNS_MIN_PX);
    expect(clampSettingsHeight(200, 400)).toBe(200);
  });

  it("keeps the strip even when the card is shorter than both floors", () => {
    expect(clampSettingsHeight(80, 100)).toBe(SETTINGS_STRIP_PX);
  });

  it("rounds to whole pixels so the flex basis never carries subpixel drift", () => {
    expect(clampSettingsHeight(150.6, 400)).toBe(151);
  });
});

describe("withoutRows", () => {
  it("drops the given indices and keeps the rest in order", () => {
    expect(withoutRows(["a", "b", "c", "d"], [1, 3])).toEqual(["a", "c"]);
    expect(withoutRows(["a", "b"], [])).toEqual(["a", "b"]);
  });
});

describe("shiftAfterRemoval", () => {
  it("drops removed indices and shifts the survivors down", () => {
    expect(shiftAfterRemoval([0, 2, 3], [1])).toEqual([0, 1, 2]);
    expect(shiftAfterRemoval([4], [0, 2])).toEqual([2]);
  });

  it("is empty when the selection itself was removed", () => {
    expect(shiftAfterRemoval([1, 2], [1, 2])).toEqual([]);
  });
});
