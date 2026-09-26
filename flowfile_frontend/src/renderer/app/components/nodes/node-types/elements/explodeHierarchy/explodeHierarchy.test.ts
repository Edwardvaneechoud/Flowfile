import { describe, expect, it } from "vitest";

import type { ExplodeHierarchyInput } from "../../../../../types/node.types";
import {
  HIERARCHY_OUTPUT_OPTIONS,
  createExplodeHierarchyInput,
  createExplodeHierarchyNode,
  hierarchyOutputColumns,
  hierarchyOutputOption,
  MAX_HIERARCHY_DEPTH,
  normalizeExplodeHierarchyInput,
  normalizeMaxDepth,
} from "./explodeHierarchy";

const names = (detail: ExplodeHierarchyInput["output_detail"]) =>
  hierarchyOutputColumns(detail).map((c) => c.name);

describe("createExplodeHierarchyInput", () => {
  it("mirrors the backend defaults", () => {
    expect(createExplodeHierarchyInput()).toEqual({
      parent_column: "",
      child_column: "",
      quantity_column: null,
      output_detail: "totals",
      top_level_only: false,
      include_self: false,
      max_depth: null,
    });
  });

  it("gives each node its own settings object", () => {
    const a = createExplodeHierarchyNode(1, 2);
    const b = createExplodeHierarchyNode(1, 3);
    a.explode_hierarchy_input.parent_column = "assembly";
    expect(b.explode_hierarchy_input.parent_column).toBe("");
    expect(a).toMatchObject({ flow_id: 1, node_id: 2, cache_results: false });
  });
});

describe("normalizeExplodeHierarchyInput", () => {
  it("fills a missing blob out to the defaults", () => {
    expect(normalizeExplodeHierarchyInput(undefined)).toEqual(createExplodeHierarchyInput());
    expect(normalizeExplodeHierarchyInput(null)).toEqual(createExplodeHierarchyInput());
  });

  it("keeps a complete configuration untouched", () => {
    const configured: ExplodeHierarchyInput = {
      parent_column: "assembly",
      child_column: "component",
      quantity_column: "qty",
      output_detail: "paths",
      top_level_only: true,
      include_self: true,
      max_depth: 3,
    };
    expect(normalizeExplodeHierarchyInput(configured)).toEqual(configured);
  });

  it("turns a cleared quantity select into null", () => {
    for (const cleared of ["", undefined, null]) {
      const settings = normalizeExplodeHierarchyInput({
        parent_column: "a",
        child_column: "b",
        quantity_column: cleared as unknown as string | null,
      });
      expect(settings.quantity_column).toBeNull();
    }
  });

  it("turns a cleared or invalid max depth into null and keeps 0", () => {
    const depth = (value: unknown) =>
      normalizeExplodeHierarchyInput({ max_depth: value as number | null }).max_depth;
    expect(depth(undefined)).toBeNull();
    expect(depth(null)).toBeNull();
    expect(depth("")).toBeNull();
    expect(depth(Number.NaN)).toBeNull();
    expect(depth(-1)).toBeNull();
    expect(depth(Number.POSITIVE_INFINITY)).toBeNull();
    expect(depth(0)).toBe(0);
    expect(depth(2)).toBe(2);
    expect(depth("4")).toBe(4);
    expect(depth(2.7)).toBe(2);
  });

  it("falls back to totals for an unknown output detail", () => {
    const settings = normalizeExplodeHierarchyInput({
      output_detail: "tree" as unknown as ExplodeHierarchyInput["output_detail"],
    });
    expect(settings.output_detail).toBe("totals");
  });
});

describe("normalizeMaxDepth", () => {
  it("never returns a negative or fractional depth", () => {
    for (const value of [-3, 0.5, 1.9, 10]) {
      const result = normalizeMaxDepth(value);
      expect(result === null || (Number.isInteger(result) && result >= 0)).toBe(true);
    }
  });

  it("caps the depth at the plugin's u32 limit", () => {
    expect(normalizeMaxDepth(99999999999)).toBe(MAX_HIERARCHY_DEPTH);
    expect(normalizeMaxDepth(MAX_HIERARCHY_DEPTH)).toBe(MAX_HIERARCHY_DEPTH);
  });
});

describe("hierarchyOutputColumns", () => {
  it("lists the fixed totals and levels schema", () => {
    const expected = ["ancestor", "descendant", "level", "quantity", "is_leaf"];
    expect(names("totals")).toEqual(expected);
    expect(names("levels")).toEqual(expected);
  });

  it("lists the paths schema in output order", () => {
    expect(names("paths")).toEqual([
      "ancestor",
      "descendant",
      "level",
      "parent",
      "quantity_per",
      "quantity",
      "is_leaf",
      "path",
    ]);
  });

  it("describes every column", () => {
    for (const option of HIERARCHY_OUTPUT_OPTIONS) {
      for (const col of hierarchyOutputColumns(option.value)) {
        expect(col.description.length, `${option.value}.${col.name}`).toBeGreaterThan(0);
      }
    }
  });
});

describe("hierarchyOutputOption", () => {
  it("offers Totals, Levels and Paths in that order", () => {
    expect(HIERARCHY_OUTPUT_OPTIONS.map((o) => o.value)).toEqual(["totals", "levels", "paths"]);
    expect(HIERARCHY_OUTPUT_OPTIONS.map((o) => o.label)).toEqual(["Totals", "Levels", "Paths"]);
  });

  it("resolves the selected option and falls back to totals", () => {
    expect(hierarchyOutputOption("paths").label).toBe("Paths");
    const unknown = "tree" as unknown as ExplodeHierarchyInput["output_detail"];
    expect(hierarchyOutputOption(unknown).value).toBe("totals");
  });
});
