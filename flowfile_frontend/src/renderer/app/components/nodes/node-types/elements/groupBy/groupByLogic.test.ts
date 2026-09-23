import { describe, expect, it } from "vitest";

import {
  addAggRows,
  defaultAggFor,
  duplicateOutputNames,
  effectiveOutputName,
  isNumericType,
  outputNameFor,
  renamedForAgg,
  usageByColumn,
  usesByAgg,
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
  it("lists names used more than once, counting a blank by its default name", () => {
    const duplicates = duplicateOutputNames([
      { old_name: "a", agg: "sum", new_name: "x" },
      { old_name: "b", agg: "sum", new_name: "x" },
      { old_name: "c", agg: "sum", new_name: "y" },
      { old_name: "d", agg: "sum", new_name: "" },
      { old_name: "d", agg: "sum", new_name: "d_sum" },
      { old_name: "e", agg: "sum" },
    ]);
    expect([...duplicates]).toEqual(["x", "d_sum"]);
  });
});

describe("effectiveOutputName", () => {
  it("falls back to the placeholder name for a blank field", () => {
    expect(effectiveOutputName({ old_name: "a", agg: "sum", new_name: "" })).toBe("a_sum");
    expect(effectiveOutputName({ old_name: "a", agg: "groupby" })).toBe("a");
    expect(effectiveOutputName({ old_name: "a", agg: "sum", new_name: "total" })).toBe("total");
  });
});

describe("usesByAgg", () => {
  it("merges rows that aggregate the same way, keeping first-seen order", () => {
    expect(
      usesByAgg([
        { agg: "sum", index: 0 },
        { agg: "count", index: 1 },
        { agg: "sum", index: 3 },
      ]),
    ).toEqual([
      { agg: "sum", rows: [0, 3] },
      { agg: "count", rows: [1] },
    ]);
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
