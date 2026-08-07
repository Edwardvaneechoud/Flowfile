import { describe, expect, it } from "vitest";
import {
  applyCellEdit,
  buildEditsPayload,
  buildLabelClasses,
  distinctColumnValues,
  hasPendingChanges,
  isEditableDtype,
  labelingOrder,
  labelingProgress,
  sessionCounts,
  suggestKeyColumns,
  validateKeyValues,
  type EditRow,
  type EditSession,
} from "./tableEditing";

let uid = 0;

function makeRow(cells: Record<string, unknown>, added = false): EditRow {
  uid += 1;
  return {
    uid,
    cells: { __uid: uid, ...cells },
    original: added ? null : { ...cells },
    deleted: false,
    dirty: new Set<string>(),
  };
}

function makeSession(rows: EditRow[], keyColumns = ["id"]): EditSession {
  return {
    columns: [
      { name: "id", dtype: "int64", isNew: false, editable: true },
      { name: "text", dtype: "large_string", isNew: false, editable: true },
      { name: "score", dtype: "double", isNew: false, editable: true },
    ],
    rows,
    keyColumns,
    expectedVersion: 3,
    newColumns: [],
  };
}

describe("isEditableDtype", () => {
  it("accepts scalar arrow dtypes", () => {
    for (const dtype of [
      "int64",
      "uint8",
      "double",
      "float64",
      "string",
      "large_string",
      "bool",
      "date32[day]",
      "timestamp[us]",
      "decimal128(10, 2)",
    ]) {
      expect(isEditableDtype(dtype), dtype).toBe(true);
    }
  });

  it("rejects nested and binary dtypes", () => {
    for (const dtype of ["list<item: int64>", "struct<a: int64>", "binary", "large_binary", "map<...>"]) {
      expect(isEditableDtype(dtype), dtype).toBe(false);
    }
  });
});

describe("suggestKeyColumns", () => {
  it("prefers id-shaped names that are unique in the slice", () => {
    const columns = ["text", "customer_id", "score"];
    const rows = [
      ["a", 1, 0.5],
      ["b", 2, 0.5],
    ];
    expect(suggestKeyColumns(columns, rows)).toEqual(["customer_id"]);
  });

  it("falls back to any unique column", () => {
    const columns = ["text", "amount"];
    const rows = [
      ["a", 1],
      ["a", 2],
    ];
    expect(suggestKeyColumns(columns, rows)).toEqual(["amount"]);
  });

  it("returns empty when nothing is unique", () => {
    const columns = ["text"];
    const rows = [["a"], ["a"]];
    expect(suggestKeyColumns(columns, rows)).toEqual([]);
  });
});

describe("applyCellEdit / sessionCounts", () => {
  it("tracks dirty columns and reverts", () => {
    const row = makeRow({ id: 1, text: "a", score: 0.5 });
    applyCellEdit(row, "text", "b");
    expect(row.dirty.has("text")).toBe(true);
    applyCellEdit(row, "text", "a");
    expect(row.dirty.size).toBe(0);
  });

  it("treats grid string edits equal to original numbers as unchanged", () => {
    const row = makeRow({ id: 1, text: "a", score: 42 });
    applyCellEdit(row, "score", "42");
    expect(row.dirty.size).toBe(0);
  });

  it("counts edited/added/deleted correctly", () => {
    const edited = makeRow({ id: 1, text: "a", score: 1 });
    applyCellEdit(edited, "text", "changed");
    const added = makeRow({ id: 9, text: "new", score: null }, true);
    added.dirty.add("text");
    const deleted = makeRow({ id: 2, text: "b", score: 2 });
    deleted.deleted = true;
    const session = makeSession([edited, added, deleted]);
    expect(sessionCounts(session)).toEqual({ edited: 1, added: 1, deleted: 1 });
    expect(hasPendingChanges(session)).toBe(true);
  });
});

describe("validateKeyValues", () => {
  it("passes clean keys", () => {
    const session = makeSession([makeRow({ id: 1, text: "a", score: 1 })]);
    expect(validateKeyValues(session)).toBeNull();
  });

  it("rejects empty key columns", () => {
    const session = makeSession([makeRow({ id: 1, text: "a", score: 1 })], []);
    expect(validateKeyValues(session)).toMatch(/at least one key/i);
  });

  it("rejects null keys and duplicate keys", () => {
    const withNull = makeSession([makeRow({ id: null, text: "a", score: 1 })]);
    expect(validateKeyValues(withNull)).toMatch(/empty values/i);

    const withDupes = makeSession([
      makeRow({ id: 1, text: "a", score: 1 }),
      makeRow({ id: 1, text: "b", score: 2 }),
    ]);
    expect(validateKeyValues(withDupes)).toMatch(/not unique/i);
  });

  it("rejects integers beyond the JS safe range", () => {
    const session = makeSession([makeRow({ id: 9007199254740993, text: "a", score: 1 })]);
    expect(validateKeyValues(session)).toMatch(/safe range/i);
  });

  it("requires keys on added rows", () => {
    const added = makeRow({ id: null, text: "x", score: null }, true);
    const session = makeSession([added]);
    expect(validateKeyValues(session)).toMatch(/new rows need/i);
  });

  it("blocks saves that would ship unedited unsafe integers in a dirty column", () => {
    // score is dirty on row A, so row B's unedited (JSON-rounded) big int would ship too.
    const edited = makeRow({ id: 1, text: "a", score: 1 });
    applyCellEdit(edited, "score", 2);
    const collateral = makeRow({ id: 2, text: "b", score: 9007199254740993 });
    applyCellEdit(collateral, "text", "b-edited");
    const session = makeSession([edited, collateral]);
    expect(validateKeyValues(session)).toMatch(/rows you did not edit/i);
  });
});

describe("buildEditsPayload", () => {
  it("ships only key columns plus edited columns", () => {
    const edited = makeRow({ id: 1, text: "a", score: 1.5 });
    applyCellEdit(edited, "text", "a-fixed");
    const untouched = makeRow({ id: 2, text: "b", score: 2.5 });
    const session = makeSession([edited, untouched]);

    const payload = buildEditsPayload(session);
    expect(payload.key_columns).toEqual(["id"]);
    expect(payload.expected_version).toBe(3);
    expect(payload.upsert_columns).toEqual(["id", "text"]);
    expect(payload.upsert_rows).toEqual([[1, "a-fixed"]]);
    expect(payload.delete_keys).toBeUndefined();
  });

  it("includes added rows and deletes with original key values", () => {
    const added = makeRow({ id: null, text: null, score: null }, true);
    applyCellEdit(added, "id", 10);
    applyCellEdit(added, "text", "fresh");
    const doomed = makeRow({ id: 3, text: "c", score: 3.5 });
    doomed.deleted = true;
    const session = makeSession([added, doomed]);

    const payload = buildEditsPayload(session);
    expect(payload.upsert_columns).toEqual(["id", "text"]);
    expect(payload.upsert_rows).toEqual([[10, "fresh"]]);
    expect(payload.delete_keys).toEqual([[3]]);
  });

  it("normalizes empty strings to null and skips columns unknown to the session", () => {
    const edited = makeRow({ id: 1, text: "a", score: 1 });
    applyCellEdit(edited, "text", "");
    edited.dirty.add("ghost_column");
    const session = makeSession([edited]);

    const payload = buildEditsPayload(session);
    expect(payload.upsert_columns).toEqual(["id", "text"]);
    expect(payload.upsert_rows).toEqual([[1, null]]);
  });

  it("carries new columns even without row edits", () => {
    const session = makeSession([makeRow({ id: 1, text: "a", score: 1 })]);
    session.newColumns = [{ name: "label", dtype: "String" }];
    const payload = buildEditsPayload(session);
    expect(payload.new_columns).toEqual([{ name: "label", dtype: "String" }]);
    expect(payload.upsert_rows).toBeUndefined();
  });
});

describe("labelling helpers", () => {
  it("assigns shortcuts to the first nine classes", () => {
    const classes = buildLabelClasses(["a", "b"]);
    expect(classes[0]).toEqual({ value: "a", shortcut: 1 });
    expect(classes[1]).toEqual({ value: "b", shortcut: 2 });
  });

  it("collects distinct values, skipping deleted rows and blanks", () => {
    const rows = [
      makeRow({ id: 1, text: "spam", score: 1 }),
      makeRow({ id: 2, text: "", score: 1 }),
      makeRow({ id: 3, text: "ham", score: 1 }),
      makeRow({ id: 4, text: "spam", score: 1 }),
    ];
    rows[3].deleted = true;
    expect(distinctColumnValues(rows, "text")).toEqual(["spam", "ham"]);
  });

  it("orders unlabelled rows first and reports progress", () => {
    const rows = [
      makeRow({ id: 1, label: "spam" }),
      makeRow({ id: 2, label: null }),
      makeRow({ id: 3, label: "ham" }),
      makeRow({ id: 4, label: null }),
    ];
    expect(labelingOrder(rows, "label")).toEqual([1, 3, 0, 2]);
    const progress = labelingProgress(rows, "label");
    expect(progress).toEqual({
      labelled: 2,
      total: 4,
      perClass: { spam: 1, ham: 1 },
    });
  });
});
