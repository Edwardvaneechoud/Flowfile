import { describe, expect, it } from "vitest";

import { assignRole, namesWithRole, type RoleSpec } from "./columnRoles";

const SPECS: RoleSpec[] = [
  { value: "index", label: "Index key" },
  { value: "pivot", label: "Pivot column", single: true },
  { value: "value", label: "Value column", single: true },
];

describe("assignRole", () => {
  it("appends new columns to a multi-holder role and reports their rows", () => {
    const result = assignRole([{ name: "a", role: "index" }], ["b", "c"], "index", SPECS);
    expect(result.rows).toEqual([
      { name: "a", role: "index" },
      { name: "b", role: "index" },
      { name: "c", role: "index" },
    ]);
    expect(result.touched).toEqual([1, 2]);
  });

  it("reports a column that already holds the role without duplicating it", () => {
    const result = assignRole([{ name: "a", role: "index" }], ["a"], "index", SPECS);
    expect(result.rows).toHaveLength(1);
    expect(result.touched).toEqual([0]);
  });

  it("moves a column between roles", () => {
    const result = assignRole([{ name: "a", role: "index" }], ["a"], "pivot", SPECS);
    expect(result.rows).toEqual([{ name: "a", role: "pivot" }]);
  });

  it("drops the previous holder when a new column takes a single-holder role", () => {
    const result = assignRole([{ name: "q", role: "pivot" }], ["x"], "pivot", SPECS);
    expect(result.rows).toEqual([{ name: "x", role: "pivot" }]);
  });

  it("swaps roles when a column already in the settings takes a single-holder role", () => {
    const rows = [
      { name: "q", role: "pivot" },
      { name: "v", role: "value" },
    ];
    const result = assignRole(rows, ["v"], "pivot", SPECS);
    expect(result.rows).toEqual([
      { name: "q", role: "value" },
      { name: "v", role: "pivot" },
    ]);
  });

  it("hands an index key to the displaced holder", () => {
    const rows = [
      { name: "k", role: "index" },
      { name: "q", role: "pivot" },
    ];
    const result = assignRole(rows, ["k"], "pivot", SPECS);
    expect(namesWithRole(result.rows, "index")).toEqual(["q"]);
    expect(namesWithRole(result.rows, "pivot")).toEqual(["k"]);
  });
});
