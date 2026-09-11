import { describe, expect, it } from "vitest";

import type { MultiFieldFormulaInput } from "../../../../../types/node.types";
import {
  createMultiFieldFormulaInput,
  createMultiFieldFormulaNode,
  normalizeMultiFieldFormulaInput,
  previewOutputColumns,
  resolveTargetColumns,
  type MultiFieldSchemaColumn,
} from "./multiFieldFormula";

const schema: MultiFieldSchemaColumn[] = [
  { name: "id", data_type_group: "Numeric" },
  { name: "January", data_type_group: "Numeric" },
  { name: "name", data_type_group: "String" },
  { name: "created_at", data_type_group: "Date" },
];

const input = (overrides: Partial<MultiFieldFormulaInput> = {}): MultiFieldFormulaInput => ({
  ...createMultiFieldFormulaInput(),
  ...overrides,
});

describe("createMultiFieldFormulaInput", () => {
  it("defaults to replacing every column with an auto-typed result", () => {
    expect(createMultiFieldFormulaInput()).toEqual({
      formula: "",
      selection_mode: "all",
      selected_columns: [],
      selected_data_type: null,
      output_mode: "replace",
      output_prefix: "",
      output_suffix: "",
      output_data_type: "Auto",
    });
  });

  it("gives each node its own settings object", () => {
    const a = createMultiFieldFormulaNode(1, 2);
    const b = createMultiFieldFormulaNode(1, 3);
    a.multi_field_formula_input.formula = "uppercase([_CurrentField_])";
    expect(b.multi_field_formula_input.formula).toBe("");
  });
});

describe("resolveTargetColumns", () => {
  it("returns every column in schema order for 'all'", () => {
    expect(resolveTargetColumns(schema, input())).toEqual(["id", "January", "name", "created_at"]);
  });

  it("keeps the order the user picked in 'list' mode", () => {
    const settings = input({ selection_mode: "list", selected_columns: ["name", "id"] });
    expect(resolveTargetColumns(schema, settings)).toEqual(["name", "id"]);
  });

  it("silently drops listed columns that are no longer in the schema", () => {
    const settings = input({
      selection_mode: "list",
      selected_columns: ["name", "gone", "id"],
    });
    expect(resolveTargetColumns(schema, settings)).toEqual(["name", "id"]);
  });

  it("filters on the readable data type group in 'data_type' mode", () => {
    const settings = input({ selection_mode: "data_type", selected_data_type: "Numeric" });
    expect(resolveTargetColumns(schema, settings)).toEqual(["id", "January"]);
  });

  it("selects nothing when no data type group is chosen", () => {
    const settings = input({ selection_mode: "data_type", selected_data_type: null });
    expect(resolveTargetColumns(schema, settings)).toEqual([]);
  });

  it("selects nothing for a group no column belongs to", () => {
    const settings = input({ selection_mode: "data_type", selected_data_type: "Binary" });
    expect(resolveTargetColumns(schema, settings)).toEqual([]);
  });

  it("returns nothing for an empty schema", () => {
    expect(resolveTargetColumns([], input())).toEqual([]);
  });
});

describe("previewOutputColumns", () => {
  it("writes back onto the source column in replace mode", () => {
    expect(previewOutputColumns(["id", "name"], input())).toEqual([
      { source: "id", output: "id" },
      { source: "name", output: "name" },
    ]);
  });

  it("applies the prefix in new mode", () => {
    const settings = input({ output_mode: "new", output_prefix: "New_" });
    expect(previewOutputColumns(["name"], settings)).toEqual([
      { source: "name", output: "New_name" },
    ]);
  });

  it("keeps significant whitespace in a suffix", () => {
    const settings = input({ output_mode: "new", output_suffix: " % Total" });
    expect(previewOutputColumns(["January"], settings)).toEqual([
      { source: "January", output: "January % Total" },
    ]);
  });

  it("applies a prefix and a suffix together", () => {
    const settings = input({ output_mode: "new", output_prefix: "a_", output_suffix: "_b" });
    expect(previewOutputColumns(["x"], settings)).toEqual([{ source: "x", output: "a_x_b" }]);
  });

  it("ignores the affixes while the output mode is replace", () => {
    const settings = input({ output_prefix: "New_", output_suffix: "_z" });
    expect(previewOutputColumns(["x"], settings)).toEqual([{ source: "x", output: "x" }]);
  });

  it("has no rows when nothing is targeted", () => {
    expect(previewOutputColumns([], input({ output_mode: "new", output_prefix: "p" }))).toEqual([]);
  });
});

describe("normalizeMultiFieldFormulaInput", () => {
  it("falls back to the defaults for a missing settings object", () => {
    expect(normalizeMultiFieldFormulaInput(undefined)).toEqual(createMultiFieldFormulaInput());
    expect(normalizeMultiFieldFormulaInput(null)).toEqual(createMultiFieldFormulaInput());
  });

  it("normalises a persisted null output_data_type to Auto", () => {
    const raw = {
      ...createMultiFieldFormulaInput(),
      formula: "[_CurrentField_] * 2",
      output_data_type: null,
    } as unknown as Partial<MultiFieldFormulaInput>;
    const normalized = normalizeMultiFieldFormulaInput(raw);
    expect(normalized.output_data_type).toBe("Auto");
    expect(normalized.formula).toBe("[_CurrentField_] * 2");
  });

  it("normalises an absent output_data_type key to Auto", () => {
    const rest: Partial<MultiFieldFormulaInput> = { ...createMultiFieldFormulaInput() };
    delete rest.output_data_type;
    expect(normalizeMultiFieldFormulaInput(rest).output_data_type).toBe("Auto");
  });

  it("normalises an empty-string output_data_type to Auto", () => {
    expect(normalizeMultiFieldFormulaInput({ output_data_type: "" }).output_data_type).toBe("Auto");
  });

  it("keeps an explicitly chosen output data type", () => {
    expect(normalizeMultiFieldFormulaInput({ output_data_type: "Float64" }).output_data_type).toBe(
      "Float64",
    );
  });

  it("keeps a null selected_columns from breaking the list selector", () => {
    const raw = {
      selection_mode: "list",
      selected_columns: null,
    } as unknown as Partial<MultiFieldFormulaInput>;
    expect(normalizeMultiFieldFormulaInput(raw).selected_columns).toEqual([]);
  });

  it("back-fills only what is missing", () => {
    const normalized = normalizeMultiFieldFormulaInput({
      selection_mode: "data_type",
      selected_data_type: "Numeric",
    });
    expect(normalized).toEqual({
      ...createMultiFieldFormulaInput(),
      selection_mode: "data_type",
      selected_data_type: "Numeric",
    });
  });
});
