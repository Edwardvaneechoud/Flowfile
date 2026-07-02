// Unit tests for the RunFlow pure helpers: binding reconciliation after an
// interface refresh and dangling-edge detection after handles change.

import { describe, it, expect } from "vitest";
import type { FlowParameter } from "../../../../../types/flow.types";
import type { FileColumn, RunFlowParameterBinding } from "../../../../../types/node.types";
import {
  mergeBindingRows,
  reconcileBindings,
  findDanglingEdges,
  hasColumnBinding,
  columnMatchesParamType,
  matchingColumnNames,
} from "./runFlowLogic";

const param = (name: string, overrides: Partial<FlowParameter> = {}): FlowParameter => ({
  name,
  default_value: "",
  description: "",
  type: "string",
  enum_values: [],
  ...overrides,
});

const binding = (
  name: string,
  overrides: Partial<RunFlowParameterBinding> = {},
): RunFlowParameterBinding => ({
  parameter_name: name,
  source: "default",
  constant_value: null,
  column_name: null,
  ...overrides,
});

describe("reconcileBindings", () => {
  it("binds a new parameter to a column on a case-insensitive name match", () => {
    const result = reconcileBindings([], [param("Region")], ["region", "amount"]);
    expect(result).toEqual([
      {
        parameter_name: "Region",
        source: "column",
        constant_value: null,
        column_name: "region",
      },
    ]);
  });

  it("falls back to default when no column matches", () => {
    const result = reconcileBindings([], [param("threshold")], ["region"]);
    expect(result).toEqual([
      {
        parameter_name: "threshold",
        source: "default",
        constant_value: null,
        column_name: null,
      },
    ]);
  });

  it("keeps bindings for surviving parameters", () => {
    const existing = binding("threshold", { source: "constant", constant_value: "5" });
    const result = reconcileBindings([existing], [param("threshold")], ["threshold"]);
    expect(result).toHaveLength(1);
    expect(result[0].source).toBe("constant");
    expect(result[0].constant_value).toBe("5");
  });

  it("drops bindings for removed parameters and preserves spec order", () => {
    const result = reconcileBindings(
      [binding("gone", { source: "constant", constant_value: "x" }), binding("kept")],
      [param("new_one"), param("kept")],
      [],
    );
    expect(result.map((b) => b.parameter_name)).toEqual(["new_one", "kept"]);
  });
});

describe("mergeBindingRows", () => {
  it("pairs each spec with its binding, defaulting missing ones", () => {
    const kept = binding("a", { source: "column", column_name: "a_col" });
    const rows = mergeBindingRows([param("a"), param("b")], [kept]);
    expect(rows).toHaveLength(2);
    // Same reference so table edits mutate the saved binding.
    expect(rows[0].binding).toBe(kept);
    expect(rows[1].binding).toEqual(binding("b"));
  });
});

describe("hasColumnBinding", () => {
  it("is true when at least one parameter is fed from a column", () => {
    const rows = mergeBindingRows(
      [param("a"), param("b")],
      [
        binding("a", { source: "constant", constant_value: "1" }),
        binding("b", { source: "column", column_name: "b_col" }),
      ],
    );
    expect(hasColumnBinding(rows)).toBe(true);
  });

  it("is false when every parameter is default or constant", () => {
    const rows = mergeBindingRows(
      [param("a"), param("b")],
      [binding("a"), binding("b", { source: "constant", constant_value: "1" })],
    );
    expect(hasColumnBinding(rows)).toBe(false);
  });

  it("is false for no parameters", () => {
    expect(hasColumnBinding([])).toBe(false);
  });
});

const fileColumn = (
  name: string,
  dataType: string,
  group: FileColumn["data_type_group"],
): FileColumn =>
  ({
    name,
    data_type: dataType,
    data_type_group: group,
    is_unique: false,
    max_value: "",
    min_value: "",
    number_of_empty_values: 0,
    number_of_filled_values: 0,
    number_of_unique_values: 0,
    size: 0,
  }) as FileColumn;

describe("columnMatchesParamType", () => {
  const intCol = fileColumn("i", "Int64", "Numeric");
  const uintCol = fileColumn("u", "UInt32", "Numeric");
  const floatCol = fileColumn("f", "Float64", "Numeric");
  const decimalCol = fileColumn("d", "Decimal", "Numeric");
  const strCol = fileColumn("s", "String", "String");
  const catCol = fileColumn("c", "Categorical", "String");
  const boolCol = fileColumn("b", "Boolean", "Boolean");

  it("integer accepts int/uint columns but not float/decimal/string", () => {
    expect(columnMatchesParamType(intCol, "integer")).toBe(true);
    expect(columnMatchesParamType(uintCol, "integer")).toBe(true);
    expect(columnMatchesParamType(floatCol, "integer")).toBe(false);
    expect(columnMatchesParamType(decimalCol, "integer")).toBe(false);
    expect(columnMatchesParamType(strCol, "integer")).toBe(false);
  });

  it("float accepts any numeric column", () => {
    expect(columnMatchesParamType(intCol, "float")).toBe(true);
    expect(columnMatchesParamType(floatCol, "float")).toBe(true);
    expect(columnMatchesParamType(decimalCol, "float")).toBe(true);
    expect(columnMatchesParamType(strCol, "float")).toBe(false);
  });

  it("boolean accepts only boolean columns", () => {
    expect(columnMatchesParamType(boolCol, "boolean")).toBe(true);
    expect(columnMatchesParamType(intCol, "boolean")).toBe(false);
  });

  it("string and enum accept String-group columns (incl. categorical)", () => {
    expect(columnMatchesParamType(strCol, "string")).toBe(true);
    expect(columnMatchesParamType(catCol, "string")).toBe(true);
    expect(columnMatchesParamType(intCol, "string")).toBe(false);
    expect(columnMatchesParamType(strCol, "enum")).toBe(true);
    expect(columnMatchesParamType(intCol, "enum")).toBe(false);
  });
});

describe("matchingColumnNames", () => {
  it("returns only the names of type-matching columns", () => {
    const cols = [
      fileColumn("id", "Int64", "Numeric"),
      fileColumn("amount", "Float64", "Numeric"),
      fileColumn("label", "String", "String"),
    ];
    expect(matchingColumnNames(cols, "integer")).toEqual(["id"]);
    expect(matchingColumnNames(cols, "float")).toEqual(["id", "amount"]);
    expect(matchingColumnNames(cols, "string")).toEqual(["label"]);
  });
});

describe("findDanglingEdges", () => {
  const edges = [
    { id: "e1", source: "1", target: "5", sourceHandle: "output-0", targetHandle: "input-0" },
    { id: "e2", source: "1", target: "5", sourceHandle: "output-0", targetHandle: "input-2" },
    { id: "e3", source: "5", target: "9", sourceHandle: "output-1", targetHandle: "input-0" },
    { id: "e4", source: "2", target: "3", sourceHandle: "output-0", targetHandle: "input-7" },
  ];

  it("drops edges pointing at removed handles and ignores other nodes", () => {
    const dangling = findDanglingEdges(edges, "5", ["input-0", "input-1"], ["output-0"]);
    expect(dangling.map((e) => e.id)).toEqual(["e2", "e3"]);
  });

  it("keeps input-0 edges when input-0 is valid", () => {
    const dangling = findDanglingEdges([edges[0]], "5", ["input-0"], []);
    expect(dangling).toEqual([]);
  });

  it("treats missing handles as input-0/output-0", () => {
    const bare = [{ id: "e5", source: "5", target: "6" }];
    expect(findDanglingEdges(bare, "5", ["input-0"], ["output-0"])).toEqual([]);
    expect(findDanglingEdges(bare, "5", ["input-0"], [])).toHaveLength(1);
  });
});
