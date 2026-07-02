// Unit tests for the RunFlow pure helpers: binding reconciliation after an
// interface refresh and dangling-edge detection after handles change.

import { describe, it, expect } from "vitest";
import type { FlowParameter } from "../../../../../types/flow.types";
import type { RunFlowParameterBinding } from "../../../../../types/node.types";
import { mergeBindingRows, reconcileBindings, findDanglingEdges } from "./runFlowLogic";

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
    const dangling = findDanglingEdges(
      [edges[0]],
      "5",
      ["input-0"],
      [],
    );
    expect(dangling).toEqual([]);
  });

  it("treats missing handles as input-0/output-0", () => {
    const bare = [{ id: "e5", source: "5", target: "6" }];
    expect(findDanglingEdges(bare, "5", ["input-0"], ["output-0"])).toEqual([]);
    expect(findDanglingEdges(bare, "5", ["input-0"], [])).toHaveLength(1);
  });
});
