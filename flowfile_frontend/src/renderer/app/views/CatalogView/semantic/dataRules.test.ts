import { describe, expect, it } from "vitest";
import type { DataRuleResult } from "../../../types/semantic.types";
import {
  buildRule,
  describeRule,
  describeRules,
  lastRun,
  ranText,
  resultText,
  ruleKey,
  ruleKindsFor,
  rulesOf,
  rulesSummary,
} from "./dataRules";
import { displayValue } from "./semanticFormat";

const result = (over: Partial<DataRuleResult>): DataRuleResult => ({
  table_id: 1,
  column: "amount",
  rule: { check: "not_null" },
  passed: true,
  failed_rows: 0,
  total_rows: 6,
  origin: null,
  error: null,
  ran_at: null,
  ...over,
});

describe("describeRule", () => {
  it("reads every kind as words", () => {
    expect(describeRule({ check: "not_null" })).toBe("not null");
    expect(describeRule({ check: "unique" })).toBe("unique");
    expect(describeRule({ check: "matches", pattern: "^[A-Z]{2}$" })).toBe("matches ^[A-Z]{2}$");
    expect(describeRule({ check: "between", min: 0, max: 100 })).toBe("0 to 100");
    expect(describeRule({ check: "between", min: 0 })).toBe("at least 0");
    expect(describeRule({ check: "length", max: 10 })).toBe("at most 10 characters");
    expect(describeRule({ check: "length", min: 2, max: 2 })).toBe("exactly 2 characters");
    expect(describeRule({ check: "formula", expression: "[amount] >= 0" })).toBe("[amount] >= 0");
  });

  it("cuts a long list of values and names a kind it does not know", () => {
    const values = ["a", "b", "c", "d", "e", "f"];
    expect(describeRule({ check: "one_of", values })).toBe("one of a, b, c, d +2 more");
    expect(describeRule({ check: "from_the_future", x: 1 })).toBe("from_the_future");
  });

  it("is what Review and History show for a column's rules", () => {
    const rules = [{ check: "not_null" }, { check: "between", max: 8 }];
    expect(describeRules(rules)).toBe("not null · at most 8");
    expect(displayValue("checks", rules)).toBe("not null · at most 8");
    expect(rulesOf("nonsense")).toEqual([]);
  });
});

describe("ruleKindsFor", () => {
  it("offers a range only for a number column", () => {
    expect(ruleKindsFor("Float64").map((k) => k.kind)).toContain("between");
    expect(ruleKindsFor("String").map((k) => k.kind)).not.toContain("between");
    expect(ruleKindsFor("String").map((k) => k.kind)).toContain("length");
  });

  it("knows the number types a kernel-written table stores", () => {
    for (const dtype of ["long", "double", "short", "integer"])
      expect(ruleKindsFor(dtype).map((k) => k.kind)).toContain("between");
    expect(buildRule("one_of", { values: "1, 2" }, "long")).toEqual({
      rule: { check: "one_of", values: [1, 2] },
    });
  });

  it("offers a list of values where one can be written", () => {
    for (const dtype of ["Date", "Datetime(time_unit='us', time_zone=None)", "timestamp"])
      expect(ruleKindsFor(dtype).map((k) => k.kind)).not.toContain("one_of");
    expect(ruleKindsFor("Boolean").map((k) => k.kind)).toContain("one_of");
  });
});

describe("buildRule", () => {
  it("builds each kind from its form", () => {
    expect(buildRule("not_null", {}, "String")).toEqual({ rule: { check: "not_null" } });
    expect(buildRule("between", { min: "0", max: "" }, "Float64")).toEqual({
      rule: { check: "between", min: 0 },
    });
    expect(buildRule("length", { min: "", max: "10" }, "String")).toEqual({
      rule: { check: "length", max: 10 },
    });
    expect(buildRule("one_of", { values: "West, East, West" }, "String")).toEqual({
      rule: { check: "one_of", values: ["West", "East"] },
    });
    expect(buildRule("one_of", { values: "1, 2" }, "Int64")).toEqual({
      rule: { check: "one_of", values: [1, 2] },
    });
    expect(buildRule("one_of", { values: "True, true" }, "Boolean")).toEqual({
      rule: { check: "one_of", values: [true] },
    });
    expect(buildRule("formula", { expression: " [amount] >= 0 " }, "Float64")).toEqual({
      rule: { check: "formula", expression: "[amount] >= 0" },
    });
  });

  it("says what is missing or wrong", () => {
    expect(buildRule("matches", { pattern: " " }, "String")).toEqual({ error: "Give a pattern." });
    expect(buildRule("between", {}, "Int64")).toEqual({ error: "Give a min or a max." });
    expect(buildRule("between", { min: "9", max: "1" }, "Int64")).toEqual({
      error: "Min must be at most max.",
    });
    expect(buildRule("length", { max: "1.5" }, "String")).toEqual({
      error: "Give whole numbers of 0 or more.",
    });
    expect(buildRule("one_of", { values: "a, 2" }, "Int64")).toEqual({
      error: "This column holds numbers; give numbers.",
    });
    expect(buildRule("one_of", { values: "yes" }, "boolean")).toEqual({
      error: "This column holds true or false; give true or false.",
    });
  });
});

describe("results", () => {
  it("finds a rule whatever order its keys came in", () => {
    expect(ruleKey("amount", { check: "between", min: 0, max: 8 })).toBe(
      ruleKey("amount", { max: 8, check: "between", min: 0 }),
    );
    expect(ruleKey("amount", { check: "not_null" })).not.toBe(
      ruleKey("region", { check: "not_null" }),
    );
  });

  it("says what a run found", () => {
    expect(resultText(result({}))).toBe("Passes on all 6 rows");
    expect(resultText(result({ passed: false, failed_rows: 1 }))).toBe("1 of 6 rows fail");
    expect(resultText(result({ passed: false, error: "no such column" }))).toBe(
      "Could not run: no such column",
    );
  });

  it("says when a table's results were measured", () => {
    const first = result({ ran_at: "2026-11-27T10:00:00+00:00" });
    const later = result({ ran_at: "2026-11-27T12:00:00+00:00" });
    expect(lastRun({ a: first, b: later, c: result({}) })).toBe("2026-11-27T12:00:00+00:00");
    expect(lastRun({ c: result({}) })).toBeNull();
    expect(lastRun(undefined)).toBeNull();
    expect(ranText(null)).toBe("");
    expect(ranText("2026-11-27T12:00:00+00:00")).toMatch(/^run .*2026/);
  });

  it("sums a table's rules up", () => {
    const rules = [
      { column: "amount", rule: { check: "not_null" } },
      { column: "amount", rule: { check: "between", max: 8 } },
    ];
    expect(rulesSummary([], {})).toBe("");
    expect(rulesSummary(rules, {})).toBe("2 rules · not run yet");
    const found = {
      [ruleKey("amount", rules[0].rule)]: result({}),
      [ruleKey("amount", rules[1].rule)]: result({ passed: false, failed_rows: 1 }),
    };
    expect(rulesSummary(rules, found)).toBe("1 of 2 rules pass");
    expect(rulesSummary(rules, { [ruleKey("amount", rules[0].rule)]: result({}) })).toBe(
      "1 of 2 rules pass · 1 not run yet",
    );
  });
});
