import { describe, expect, it } from "vitest";

import type { NodeFormula } from "@/types/node.types";
import { applyReorder } from "../../../baseNode/selectComponents/columnSelection";
import {
  accumulatedColumnsAt,
  createFormulaInput,
  createFormulaNode,
  overwrittenOutputs,
  entrySummary,
  entryUid,
  flattenExpression,
  isBlankEntry,
  normalizeNodeFormula,
  replaceColumnReference,
  toSavePayload,
  type FormulaColumn,
} from "./formula";

const base: FormulaColumn[] = [
  { name: "a", data_type: "Int64" },
  { name: "b", data_type: "Int64" },
];

describe("createFormulaNode", () => {
  it("starts with a single entry", () => {
    const node = createFormulaNode(1, 2);
    expect(node.functions).toHaveLength(1);
    expect(node.functions[0].field.name).toBe("output_field");
    expect(node.function).toBeUndefined();
  });

  it("gives every node its own entry objects", () => {
    const first = createFormulaNode(1, 2);
    const second = createFormulaNode(1, 3);
    first.functions[0].function = "[a]";
    expect(second.functions[0].function).toBe("");
    expect(first.functions[0]).not.toBe(second.functions[0]);
  });
});

describe("normalizeNodeFormula", () => {
  it("promotes a legacy single function to the list", () => {
    const node = {
      flow_id: 1,
      node_id: 2,
      pos_x: 0,
      pos_y: 0,
      cache_results: false,
      function: createFormulaInput("total", "Auto", "[a] + [b]"),
    } as unknown as NodeFormula;

    normalizeNodeFormula(node);

    expect(node.functions).toHaveLength(1);
    expect(node.functions[0].function).toBe("[a] + [b]");
  });

  it("keeps an existing list and fills missing pieces", () => {
    const node = {
      functions: [{ field: { name: "x" } }, { field: {}, function: "1" }],
    } as unknown as NodeFormula;

    normalizeNodeFormula(node);

    expect(node.functions).toHaveLength(2);
    expect(node.functions[0]).toEqual({ field: { name: "x", data_type: "Auto" }, function: "" });
    expect(node.functions[1].field.name).toBe("");
  });

  it("falls back to one blank entry when there is nothing to load", () => {
    const node = { functions: [] } as unknown as NodeFormula;
    normalizeNodeFormula(node);
    expect(node.functions).toHaveLength(1);
    expect(isBlankEntry(node.functions[0])).toBe(true);
  });
});

describe("accumulatedColumnsAt", () => {
  const entries = [
    createFormulaInput("total", "Auto", "[a] + [b]"),
    createFormulaInput("double", "Int64", "[total] * 2"),
  ];

  it("shows only the input columns to the first entry", () => {
    expect(accumulatedColumnsAt(base, entries, 0).map((c) => c.name)).toEqual(["a", "b"]);
  });

  it("adds the outputs of the preceding entries, in order", () => {
    expect(accumulatedColumnsAt(base, entries, 1).map((c) => c.name)).toEqual(["a", "b", "total"]);
    expect(accumulatedColumnsAt(base, entries, 2).map((c) => c.name)).toEqual([
      "a",
      "b",
      "total",
      "double",
    ]);
  });

  it("skips blank entries — they never run, so they create no column", () => {
    const withBlank = [
      createFormulaInput("skipped", "Auto", "   "),
      createFormulaInput("", "Auto", "[a]"),
      createFormulaInput("kept", "Auto", "[a]"),
    ];
    expect(accumulatedColumnsAt(base, withBlank, 3).map((c) => c.name)).toEqual(["a", "b", "kept"]);
  });

  it("does not duplicate a column an entry overwrites, and takes its data type", () => {
    const overwrite = [createFormulaInput("a", "Float64", "[a] * 1.0")];
    const columns = accumulatedColumnsAt(base, overwrite, 1);
    expect(columns.map((c) => c.name)).toEqual(["a", "b"]);
    expect(columns[0].data_type).toBe("Float64");
  });

  it("leaves the data type blank for an Auto entry", () => {
    expect(accumulatedColumnsAt(base, entries, 2)[2].data_type).toBe("");
    expect(accumulatedColumnsAt(base, entries, 2)[3].data_type).toBe("Int64");
  });
});

describe("overwrittenOutputs", () => {
  it("notes only the overwriting row, pointing at the first producer", () => {
    const entries = [
      createFormulaInput("x", "Auto", "1"),
      createFormulaInput("y", "Auto", "2"),
      createFormulaInput("x", "Auto", "3"),
      createFormulaInput("x", "Auto", "4"),
    ];
    expect([...overwrittenOutputs(entries)]).toEqual([
      [2, 1],
      [3, 1],
    ]);
  });

  it("ignores blank names", () => {
    const entries = [createFormulaInput("", "Auto", "1"), createFormulaInput("", "Auto", "2")];
    expect(overwrittenOutputs(entries).size).toBe(0);
  });
});

describe("replaceColumnReference", () => {
  it("swaps every bracketed reference and nothing else", () => {
    expect(replaceColumnReference("[regiOn] + [regiOn_code] + [regiOn]", "regiOn", "region")).toBe(
      "[region] + [regiOn_code] + [region]",
    );
  });

  it("leaves an expression without the reference untouched", () => {
    expect(replaceColumnReference("[a] + 1", "b", "c")).toBe("[a] + 1");
  });
});

describe("reordering", () => {
  it("allows a reorder that creates a forward reference", () => {
    const entries = [
      createFormulaInput("total", "Auto", "[a] + [b]"),
      createFormulaInput("double", "Auto", "[total] * 2"),
    ];
    const swapped = applyReorder(entries, [1], "up").items;
    // The now-first entry references a column it can no longer see; that is a
    // validation issue on the row, never a blocked reorder.
    expect(accumulatedColumnsAt(base, swapped, 0).map((c) => c.name)).toEqual(["a", "b"]);
  });
});

describe("entryUid", () => {
  it("is stable per object and unique across objects", () => {
    const one = createFormulaInput("one");
    const two = createFormulaInput("two");
    expect(entryUid(one)).toBe(entryUid(one));
    expect(entryUid(one)).not.toBe(entryUid(two));
  });
});

describe("entrySummary", () => {
  it("shows the expression alone — the name lives in its own field now", () => {
    expect(entrySummary(createFormulaInput("full_name_len", "Auto", "length([a])"))).toEqual({
      text: "length([a])",
      placeholder: false,
    });
  });

  it("flattens whitespace but never truncates — overflow is CSS's job", () => {
    const entry = createFormulaInput("x", "Auto", "concat(\n  [a],\n  [b]\n)");
    expect(entrySummary(entry).text).toBe("concat( [a], [b] )");
    expect(flattenExpression("  a   b  ")).toBe("a b");
  });

  it("marks a missing expression as a placeholder", () => {
    expect(entrySummary(createFormulaInput("x", "Auto", "   "))).toEqual({
      text: "skipped",
      placeholder: true,
    });
    expect(entrySummary(createFormulaInput())).toEqual({ text: "empty", placeholder: true });
  });
});

describe("toSavePayload", () => {
  it("drops the legacy key so only `functions` is sent", () => {
    const node = createFormulaNode(1, 2);
    node.function = node.functions[0];
    const payload = toSavePayload(node);
    expect("function" in payload).toBe(false);
    expect(payload.functions).toHaveLength(1);
  });
});
