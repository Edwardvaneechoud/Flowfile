import { describe, expect, it } from "vitest";

import type { FileColumn } from "../../../types/node.types";
import {
  formatCount,
  nullSeverityClass,
  pctNull,
  pctUnique,
  qualityBadges,
  totalRowCount,
  truncateValue,
} from "./columnQuality";

function makeColumn(overrides: Partial<FileColumn> = {}): FileColumn {
  return {
    name: "a",
    data_type: "Int64",
    data_type_group: "Numeric",
    is_unique: false,
    max_value: "9",
    min_value: "1",
    average_value: null,
    number_of_empty_values: 0,
    number_of_filled_values: 100,
    number_of_unique_values: 10,
    size: 100,
    ...overrides,
  };
}

describe("formatCount", () => {
  it("thousands-separates counts", () => {
    expect(formatCount(1000)).toBe("1,000");
    expect(formatCount(0)).toBe("0");
  });

  it("renders unknown counts as ?", () => {
    expect(formatCount(null)).toBe("?");
    expect(formatCount(undefined)).toBe("?");
  });
});

describe("totalRowCount", () => {
  it("is nulls plus non-nulls", () => {
    expect(totalRowCount(makeColumn({ number_of_empty_values: 3 }))).toBe(103);
  });

  it("is null while stats are uncomputed", () => {
    expect(totalRowCount(makeColumn({ number_of_empty_values: null }))).toBeNull();
    expect(totalRowCount(makeColumn({ number_of_filled_values: null }))).toBeNull();
  });
});

describe("pctNull", () => {
  it("derives the null percentage", () => {
    expect(pctNull(makeColumn({ number_of_empty_values: 1, number_of_filled_values: 3 }))).toBe(25);
  });

  it("is 0 for an empty column, without division errors", () => {
    expect(pctNull(makeColumn({ number_of_empty_values: 0, number_of_filled_values: 0 }))).toBe(0);
  });

  it("is null while unknown", () => {
    expect(pctNull(makeColumn({ number_of_empty_values: null }))).toBeNull();
  });
});

describe("pctUnique", () => {
  it("uses the non-null count as denominator", () => {
    // [1, 1, null]: one distinct value among two present ones.
    const column = makeColumn({
      number_of_empty_values: 1,
      number_of_filled_values: 2,
      number_of_unique_values: 1,
    });
    expect(pctUnique(column)).toBe(50);
  });

  it("is null when unknown or when nothing is filled", () => {
    expect(pctUnique(makeColumn({ number_of_unique_values: null }))).toBeNull();
    expect(pctUnique(makeColumn({ number_of_filled_values: 0 }))).toBeNull();
  });
});

describe("nullSeverityClass", () => {
  it("has no badge at exactly 0%", () => {
    expect(nullSeverityClass(0)).toBe("");
  });

  it("is info up to and including 5%", () => {
    expect(nullSeverityClass(0.01)).toBe("status-badge--info");
    expect(nullSeverityClass(5)).toBe("status-badge--info");
  });

  it("is warning above 5% up to and including 50%", () => {
    expect(nullSeverityClass(5.01)).toBe("status-badge--warning");
    expect(nullSeverityClass(50)).toBe("status-badge--warning");
  });

  it("is danger above 50%", () => {
    expect(nullSeverityClass(50.01)).toBe("status-badge--danger");
    expect(nullSeverityClass(100)).toBe("status-badge--danger");
  });
});

describe("qualityBadges", () => {
  it("returns no badge while stats are uncomputed", () => {
    expect(qualityBadges(makeColumn({ number_of_empty_values: null }))).toEqual([]);
  });

  it("returns no badge for an unremarkable column", () => {
    expect(qualityBadges(makeColumn())).toEqual([]);
  });

  it("flags an empty column", () => {
    const column = makeColumn({ number_of_empty_values: 0, number_of_filled_values: 0 });
    expect(qualityBadges(column)).toEqual([{ label: "Empty", className: "status-badge--info" }]);
  });

  it("flags an all-null column, taking precedence over constant", () => {
    const column = makeColumn({
      number_of_empty_values: 3,
      number_of_filled_values: 0,
      number_of_unique_values: 0,
    });
    expect(qualityBadges(column).map((b) => b.label)).toEqual(["All null"]);
  });

  it("flags a constant column even when nulls are present", () => {
    const column = makeColumn({
      number_of_empty_values: 1,
      number_of_filled_values: 2,
      number_of_unique_values: 1,
    });
    expect(qualityBadges(column).map((b) => b.label)).toEqual(["Constant"]);
  });

  it("flags a unique key only with zero nulls and all-distinct values", () => {
    const uniqueKey = makeColumn({
      number_of_empty_values: 0,
      number_of_filled_values: 4,
      number_of_unique_values: 4,
    });
    expect(qualityBadges(uniqueKey)).toEqual([
      { label: "Unique key", className: "status-badge--success" },
    ]);

    const uniqueButNullable = makeColumn({
      number_of_empty_values: 1,
      number_of_filled_values: 4,
      number_of_unique_values: 4,
    });
    expect(qualityBadges(uniqueButNullable)).toEqual([]);
  });
});

describe("truncateValue", () => {
  it("renders null as an em dash", () => {
    expect(truncateValue(null)).toBe("—");
  });

  it("passes short values through", () => {
    expect(truncateValue("hello")).toBe("hello");
  });

  it("truncates long values with an ellipsis", () => {
    const long = "x".repeat(500);
    const result = truncateValue(long);
    expect(result).toHaveLength(200);
    expect(result.endsWith("…")).toBe(true);
  });

  it("honours an explicit cap", () => {
    expect(truncateValue("abcdef", 3)).toBe("ab…");
  });
});
