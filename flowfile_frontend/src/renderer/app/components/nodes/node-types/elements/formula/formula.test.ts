import { describe, expect, it } from "vitest";

import type { NodeFormula } from "@/types/node.types";
import {
  accumulatedColumnsAt,
  collapsedUidsFor,
  createFormulaInput,
  createFormulaNode,
  duplicateOutputPositions,
  entrySummary,
  entryUid,
  flattenExpression,
  isBlankEntry,
  moveEntryByCommand,
  moveEntryTo,
  normalizeNodeFormula,
  resolveRailCollapsed,
  toChainEntries,
  toSavePayload,
  toggledUid,
  withoutUid,
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

describe("duplicateOutputPositions", () => {
  it("flags every row sharing an output name", () => {
    const entries = [
      createFormulaInput("x", "Auto", "1"),
      createFormulaInput("y", "Auto", "2"),
      createFormulaInput("x", "Auto", "3"),
    ];
    expect(duplicateOutputPositions(entries)).toEqual([0, 2]);
  });

  it("ignores blank names", () => {
    const entries = [createFormulaInput("", "Auto", "1"), createFormulaInput("", "Auto", "2")];
    expect(duplicateOutputPositions(entries)).toEqual([]);
  });
});

describe("reordering", () => {
  it("keeps the same objects so editor state travels with the row", () => {
    const entries = [
      createFormulaInput("one", "Auto", "1"),
      createFormulaInput("two", "Auto", "2"),
    ];
    const moved = moveEntryTo(entries, 1, 0);
    expect(moved[0]).toBe(entries[1]);
    expect(moved[1]).toBe(entries[0]);
  });

  it("moves a row up and down by command", () => {
    const entries = [
      createFormulaInput("one", "Auto", "1"),
      createFormulaInput("two", "Auto", "2"),
      createFormulaInput("three", "Auto", "3"),
    ];
    expect(moveEntryByCommand(entries, 2, "up").map((e) => e.field.name)).toEqual([
      "one",
      "three",
      "two",
    ]);
    expect(moveEntryByCommand(entries, 0, "down").map((e) => e.field.name)).toEqual([
      "two",
      "one",
      "three",
    ]);
  });

  it("is a no-op at the edges", () => {
    const entries = [createFormulaInput("one"), createFormulaInput("two")];
    expect(moveEntryByCommand(entries, 0, "up").map((e) => e.field.name)).toEqual(["one", "two"]);
    expect(moveEntryByCommand(entries, 1, "down").map((e) => e.field.name)).toEqual(["one", "two"]);
  });

  it("allows a reorder that creates a forward reference", () => {
    const entries = [
      createFormulaInput("total", "Auto", "[a] + [b]"),
      createFormulaInput("double", "Auto", "[total] * 2"),
    ];
    const swapped = moveEntryByCommand(entries, 1, "up");
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

describe("toChainEntries", () => {
  it("flattens entries into the validator's wire shape", () => {
    expect(toChainEntries([createFormulaInput("total", "Int64", "[a]")])).toEqual([
      { name: "total", data_type: "Int64", function: "[a]" },
    ]);
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

describe("collapsed-set helpers", () => {
  const entries = [
    createFormulaInput("one", "Auto", "1"),
    createFormulaInput("two", "Auto", "2"),
    createFormulaInput("three", "Auto", "3"),
  ];

  it("toggles a uid in and out without touching the input set", () => {
    const start = new Set(["a"]);
    const added = toggledUid(start, "b");
    expect([...added].sort()).toEqual(["a", "b"]);
    expect(toggledUid(added, "a")).toEqual(new Set(["b"]));
    expect(start).toEqual(new Set(["a"]));
  });

  it("expands idempotently", () => {
    expect(withoutUid(new Set(["a", "b"]), "a")).toEqual(new Set(["b"]));
    expect(withoutUid(new Set(["b"]), "a")).toEqual(new Set(["b"]));
  });

  it("collapses every row, optionally keeping one expanded", () => {
    expect(collapsedUidsFor(entries).size).toBe(3);
    const keep = entryUid(entries[1]);
    const collapsed = collapsedUidsFor(entries, keep);
    expect(collapsed.has(keep)).toBe(false);
    expect(collapsed.size).toBe(2);
  });

  it("follows the row, not the position, through a reorder", () => {
    const collapsed = collapsedUidsFor(entries, entryUid(entries[0]));
    const moved = moveEntryByCommand(entries, 2, "up");
    expect(collapsed.has(entryUid(moved[0]))).toBe(false);
    expect(collapsed.has(entryUid(moved[1]))).toBe(true);
  });
});

describe("resolveRailCollapsed", () => {
  it("folds the rail for a list and keeps it open for a lone formula", () => {
    expect(resolveRailCollapsed(null, 1)).toBe(false);
    expect(resolveRailCollapsed(null, 2)).toBe(true);
  });

  it("lets an explicit choice outrank the entry count", () => {
    expect(resolveRailCollapsed(false, 5)).toBe(false);
    expect(resolveRailCollapsed(true, 1)).toBe(true);
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
