import { describe, expect, it } from "vitest";
import {
  applyCellEdit,
  buildEditsPayload,
  buildJoinKeySpec,
  buildLabelClasses,
  buildSuggestionMap,
  distinctColumnValues,
  formatConfidence,
  hasPendingChanges,
  isEditableDtype,
  isContinuousDtype,
  isNumericDtype,
  joinKeyToken,
  labelingOrder,
  labelingProgress,
  rowSuggestion,
  sessionCounts,
  suggestKeyColumns,
  suggestionCoverage,
  suggestionDisagreement,
  validateKeyValues,
  type EditColumn,
  type EditRow,
  type EditSession,
  type JoinKeySpec,
  type SuggestionEntry,
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
    for (const dtype of [
      "list<item: int64>",
      "struct<a: int64>",
      "binary",
      "large_binary",
      "map<...>",
    ]) {
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
    const session = makeSession([
      makeRow({ id: Number.MAX_SAFE_INTEGER + 1, text: "a", score: 1 }),
    ]);
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
    const collateral = makeRow({ id: 2, text: "b", score: Number.MAX_SAFE_INTEGER + 1 });
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

describe("isNumericDtype", () => {
  it("accepts numeric arrow dtypes only", () => {
    for (const dtype of ["int64", "uint8", " float32 ", "float", "double", "decimal128(10, 2)"]) {
      expect(isNumericDtype(dtype), dtype).toBe(true);
    }
    for (const dtype of [
      "large_string",
      "string",
      "bool",
      "date32[day]",
      "",
      "list<item: int64>",
    ]) {
      expect(isNumericDtype(dtype), dtype).toBe(false);
    }
  });
});

const joinSessionColumns: EditColumn[] = [
  { name: "id", dtype: "int64", isNew: false, editable: true },
  { name: "region", dtype: "large_string", isNew: false, editable: true },
];

describe("buildJoinKeySpec", () => {
  it("coerces a numeric session key even against a string prediction column", () => {
    expect(
      buildJoinKeySpec(["id", "region"], joinSessionColumns, ["id", "region", "prediction"]),
    ).toEqual([
      { column: "id", coerceNumeric: true },
      { column: "region", coerceNumeric: false },
    ]);
  });

  it("does not coerce a string session key against a numeric prediction column", () => {
    expect(buildJoinKeySpec(["region"], joinSessionColumns, ["region"])).toEqual([
      { column: "region", coerceNumeric: false },
    ]);
  });

  it("keeps zero-padded string session keys distinct from their numeric form", () => {
    const spec = buildJoinKeySpec(["region"], joinSessionColumns, ["region"])!;
    expect(joinKeyToken(["007"], spec)).not.toBe(joinKeyToken(["7"], spec));
    // Ordinary values still join across the grid's string/number shape difference.
    expect(joinKeyToken(["7"], spec)).toBe(joinKeyToken([7], spec));
  });

  it("treats a dtype missing on both sides as non-numeric", () => {
    expect(buildJoinKeySpec(["ghost"], joinSessionColumns, ["ghost"])).toEqual([
      { column: "ghost", coerceNumeric: false },
    ]);
  });

  it("returns null when a key column is absent from the prediction table", () => {
    expect(buildJoinKeySpec(["id"], joinSessionColumns, ["region"])).toBeNull();
  });

  it("returns null without key columns", () => {
    expect(buildJoinKeySpec([], joinSessionColumns, ["id"])).toBeNull();
  });
});

describe("joinKeyToken", () => {
  const numeric: JoinKeySpec[] = [{ column: "id", coerceNumeric: true }];
  const strict: JoinKeySpec[] = [{ column: "id", coerceNumeric: false }];

  it("matches numbers with their grid string forms", () => {
    expect(joinKeyToken([42], numeric)).toBe(joinKeyToken(["42"], numeric));
    expect(joinKeyToken(["007"], numeric)).toBe(joinKeyToken([7], numeric));
    expect(joinKeyToken(["+7"], numeric)).toBe(joinKeyToken([7], numeric));
    expect(joinKeyToken(["-0"], numeric)).toBe(joinKeyToken([0], numeric));
    expect(joinKeyToken(["1.50"], numeric)).toBe(joinKeyToken([1.5], numeric));
    expect(joinKeyToken([" 42 "], numeric)).toBe(joinKeyToken([42], numeric));
  });

  it("keeps text-valued integers beyond 2^53 exact", () => {
    const big = joinKeyToken(["1234567890123456789"], numeric);
    const neighbour = joinKeyToken(["1234567890123456788"], numeric);
    expect(big).toBe("n:1234567890123456789");
    expect(big).not.toBe(neighbour);
    expect(joinKeyToken(["9007199254740993"], numeric)).toBe("n:9007199254740993");
  });

  it("cannot recover precision from a number-valued integer beyond 2^53", () => {
    // An int column ships as a JSON number, so the server's ...993 arrives as ...992.
    const fromNumber = joinKeyToken([Number.MAX_SAFE_INTEGER + 2], numeric);
    expect(fromNumber).toBe("n:9007199254740992");
    expect(fromNumber).toBe(joinKeyToken([Number.MAX_SAFE_INTEGER + 1], numeric));
    expect(fromNumber).not.toBe(joinKeyToken(["9007199254740993"], numeric));
  });

  it("stays strict for non-numeric key columns", () => {
    expect(joinKeyToken(["42"], strict)).toBe("s:42");
    expect(joinKeyToken(["42"], strict)).not.toBe(joinKeyToken(["042"], strict));
  });

  it("falls back to the raw string when a coerced value is not a number", () => {
    expect(joinKeyToken(["abc"], numeric)).toBe("s:abc");
  });

  it("returns null when any part is empty or blank", () => {
    const composite: JoinKeySpec[] = [...numeric, { column: "region", coerceNumeric: false }];
    expect(joinKeyToken([null], numeric)).toBeNull();
    expect(joinKeyToken([""], strict)).toBeNull();
    expect(joinKeyToken([1, undefined], composite)).toBeNull();
    expect(joinKeyToken([" "], numeric)).toBeNull();
    expect(joinKeyToken(["\t"], numeric)).toBeNull();
    expect(joinKeyToken([" "], strict)).toBeNull();
    expect(joinKeyToken([[]], numeric)).toBeNull();
  });

  it("never folds a blank or boolean key onto a genuine key of 0", () => {
    expect(joinKeyToken([0], numeric)).toBe("n:0");
    expect(joinKeyToken([" "], numeric)).not.toBe("n:0");
    expect(joinKeyToken([[]], numeric)).not.toBe("n:0");
    expect(joinKeyToken([false], numeric)).toBe("s:false");
  });

  it("joins composite keys on a separator no cell value can hold", () => {
    const composite: JoinKeySpec[] = [...numeric, { column: "region", coerceNumeric: false }];
    expect(joinKeyToken(["07", "eu"], composite)).toBe("n:7\u0000s:eu");
    expect(joinKeyToken([7, "eu"], composite)).toBe(joinKeyToken(["7", "eu"], composite));
    expect(joinKeyToken([7, "us"], composite)).not.toBe(joinKeyToken([7, "eu"], composite));
  });

  it("keeps composite keys distinct when a part looks like a joined token", () => {
    const twoStrings: JoinKeySpec[] = [
      { column: "a", coerceNumeric: false },
      { column: "b", coerceNumeric: false },
    ];
    expect(joinKeyToken(["x s:y", "z"], twoStrings)).not.toBe(
      joinKeyToken(["x", "y s:z"], twoStrings),
    );
  });
});

describe("buildSuggestionMap", () => {
  const keySpec: JoinKeySpec[] = [{ column: "id", coerceNumeric: true }];
  const predColumns = ["id", "label", "value"];

  it("takes the argmax across a key's class rows and reports the distribution", () => {
    const map = buildSuggestionMap(
      predColumns,
      [
        [19, "business", 0.7757],
        [19, "sports", 0.2242],
        [21, "business", 0.6523],
      ],
      keySpec,
      "label",
      "value",
    );
    expect(map.byKey.get("n:19")).toEqual({
      suggested: "business",
      confidence: 0.7757,
      probabilities: { business: 0.7757, sports: 0.2242 },
    });
    expect(map.byKey.get("n:21")).toEqual({
      suggested: "business",
      confidence: 0.6523,
      probabilities: { business: 0.6523 },
    });
    expect(map.duplicateKeys).toBe(0);
    expect(map.unjoinableRows).toBe(0);
  });

  it("is independent of row order", () => {
    const rows = [
      [19, "business", 0.7757],
      [19, "sports", 0.2242],
    ];
    const forward = buildSuggestionMap(predColumns, rows, keySpec, "label", "value");
    const reversed = buildSuggestionMap(predColumns, [...rows].reverse(), keySpec, "label", "value");
    expect(forward.byKey.get("n:19")).toEqual(reversed.byKey.get("n:19"));
    expect(forward.byKey.get("n:19")?.suggested).toBe("business");
  });

  it("keeps the first class on an exact tie", () => {
    const map = buildSuggestionMap(
      predColumns,
      [
        [7, "spam", 0.5],
        [7, "ham", 0.5],
      ],
      keySpec,
      "label",
      "value",
    );
    expect(map.byKey.get("n:7")?.suggested).toBe("spam");
    expect(map.byKey.get("n:7")?.confidence).toBe(0.5);
  });

  it("lets a class with a probability beat a first-seen class without one", () => {
    const rows = [
      [3, "business", null],
      [3, "sports", 0.62],
    ];
    const forward = buildSuggestionMap(predColumns, rows, keySpec, "label", "value");
    const reversed = buildSuggestionMap(predColumns, [...rows].reverse(), keySpec, "label", "value");
    expect(forward.byKey.get("n:3")).toEqual({
      suggested: "sports",
      confidence: 0.62,
      probabilities: { sports: 0.62 },
    });
    expect(reversed.byKey.get("n:3")).toEqual(forward.byKey.get("n:3"));
  });

  it("falls back to the first class when no row carries a probability", () => {
    const map = buildSuggestionMap(
      predColumns,
      [
        [4, "business", null],
        [4, "sports", null],
        [4, "politics", null],
      ],
      keySpec,
      "label",
      "value",
    );
    expect(map.byKey.get("n:4")).toEqual({
      suggested: "business",
      confidence: null,
      probabilities: null,
    });
    expect(map.duplicateKeys).toBe(0);
  });

  it("counts a repeated class for one key and keeps its highest probability", () => {
    const rows = [
      [5, "business", 0.9],
      [5, "sports", 0.3],
      [5, "business", 0.4],
    ];
    const forward = buildSuggestionMap(predColumns, rows, keySpec, "label", "value");
    const reversed = buildSuggestionMap(predColumns, [...rows].reverse(), keySpec, "label", "value");
    expect(forward.byKey.get("n:5")).toEqual({
      suggested: "business",
      confidence: 0.9,
      probabilities: { business: 0.9, sports: 0.3 },
    });
    expect(forward.duplicateKeys).toBe(1);
    expect(reversed.byKey.get("n:5")).toEqual(forward.byKey.get("n:5"));
    expect(reversed.duplicateKeys).toBe(1);
  });

  it("counts a repeated class even when both rows lack a probability", () => {
    const map = buildSuggestionMap(
      predColumns,
      [
        [6, "business", null],
        [6, "sports", 0.5],
        [6, "business", null],
      ],
      keySpec,
      "label",
      "value",
    );
    expect(map.duplicateKeys).toBe(1);
    expect(map.byKey.get("n:6")?.suggested).toBe("sports");
  });

  it("coerces string keys against numeric ones", () => {
    const map = buildSuggestionMap(
      predColumns,
      [["19", "business", "0.77"]],
      keySpec,
      "label",
      "value",
    );
    expect(map.byKey.get("n:19")?.confidence).toBe(0.77);
  });

  it("counts rows with an empty key as unjoinable", () => {
    const map = buildSuggestionMap(
      predColumns,
      [
        [null, "business", 0.9],
        ["", "sports", 0.9],
        [2, "sports", 0.9],
      ],
      keySpec,
      "label",
      "value",
    );
    expect(map.unjoinableRows).toBe(2);
    expect(map.byKey.size).toBe(1);
  });

  it("skips rows without a class without counting them", () => {
    const map = buildSuggestionMap(
      predColumns,
      [
        [1, null, 0.9],
        [2, "", 0.9],
      ],
      keySpec,
      "label",
      "value",
    );
    expect(map.byKey.size).toBe(0);
    expect(map.unjoinableRows).toBe(0);
    expect(map.duplicateKeys).toBe(0);
  });

  it("treats an unusable probability as absent", () => {
    const map = buildSuggestionMap(
      predColumns,
      [
        [1, "business", "not-a-number"],
        [2, "business", null],
      ],
      keySpec,
      "label",
      "value",
    );
    expect(map.byKey.get("n:1")?.confidence).toBeNull();
    expect(map.byKey.get("n:1")?.probabilities).toBeNull();
    expect(map.byKey.get("n:2")?.confidence).toBeNull();
  });

  it("yields nothing usable when the probability column is absent", () => {
    const map = buildSuggestionMap(predColumns, [[1, "business", 0.9]], keySpec, "label", "ghost");
    expect(map.byKey.get("n:1")).toEqual({
      suggested: "business",
      confidence: null,
      probabilities: null,
    });
  });
});

describe("isContinuousDtype", () => {
  it("accepts float, double and decimal dtypes", () => {
    for (const dtype of [
      "float32",
      "float64",
      "float16",
      "double",
      "Float64",
      "decimal128(10, 2)",
      " FLOAT64 ",
    ]) {
      expect(isContinuousDtype(dtype), dtype).toBe(true);
    }
  });

  it("rejects labels, keys and nested dtypes", () => {
    for (const dtype of [
      "int64",
      "Int32",
      "uint8",
      "large_string",
      "string",
      "bool",
      "date32[day]",
      "timestamp[us]",
      "list[f32]",
      "",
    ]) {
      expect(isContinuousDtype(dtype), dtype).toBe(false);
    }
  });
});

describe("rowSuggestion / coverage / disagreement", () => {
  const spec: JoinKeySpec[] = [{ column: "id", coerceNumeric: true }];
  const byKey = new Map<string, SuggestionEntry>([
    ["n:1", { suggested: "spam", confidence: 0.9, probabilities: null }],
    ["n:2", { suggested: "ham", confidence: 0.3, probabilities: null }],
  ]);

  it("joins a grid row to its suggestion", () => {
    expect(rowSuggestion(makeRow({ id: "1", label: null }), spec, byKey)?.suggested).toBe("spam");
  });

  it("returns null for a miss and for an added row without a key", () => {
    expect(rowSuggestion(makeRow({ id: 99, label: null }), spec, byKey)).toBeNull();
    expect(rowSuggestion(makeRow({ id: null, label: null }, true), spec, byKey)).toBeNull();
  });

  it("reports coverage over non-deleted rows", () => {
    const rows = [
      makeRow({ id: 1, label: null }),
      makeRow({ id: 99, label: null }),
      makeRow({ id: 2, label: null }),
    ];
    rows[2].deleted = true;
    expect(suggestionCoverage(rows, spec, byKey)).toEqual({ covered: 1, total: 2 });
  });

  it("counts only labelled rows that disagree with their suggestion", () => {
    const rows = [
      makeRow({ id: 1, label: "ham" }),
      makeRow({ id: 2, label: "ham" }),
      makeRow({ id: 1, label: null }),
      makeRow({ id: 99, label: "ham" }),
      makeRow({ id: 2, label: "spam" }),
    ];
    rows[4].deleted = true;
    expect(suggestionDisagreement(rows, "label", spec, byKey)).toBe(1);
  });
});

describe("labelingOrder with confidences", () => {
  const rows = [
    makeRow({ id: 1, label: "spam", conf: 0.5 }),
    makeRow({ id: 2, label: null, conf: 0.9 }),
    makeRow({ id: 3, label: null, conf: null }),
    makeRow({ id: 4, label: null, conf: 0.2 }),
    makeRow({ id: 5, label: "ham", conf: 0.1 }),
    makeRow({ id: 6, label: null, conf: 0.2 }),
  ];
  const confidenceOf = (row: EditRow) => (row.cells.conf as number | null) ?? null;

  it("puts least-confident unlabelled rows first, stably", () => {
    expect(labelingOrder(rows, "label", confidenceOf)).toEqual([3, 5, 1, 2, 0, 4]);
  });

  it("keeps the two-argument order untouched", () => {
    expect(labelingOrder(rows, "label")).toEqual([1, 2, 3, 5, 0, 4]);
  });

  it("skips deleted rows", () => {
    const copy = rows.map((row) => ({ ...row, cells: { ...row.cells } }));
    copy[3].deleted = true;
    expect(labelingOrder(copy, "label", confidenceOf)).toEqual([5, 1, 2, 0, 4]);
  });
});

describe("formatConfidence", () => {
  it("renders fractions as percentages", () => {
    expect(formatConfidence(0.824)).toBe("82%");
    expect(formatConfidence(0)).toBe("0%");
    expect(formatConfidence(1)).toBe("100%");
  });

  it("leaves already percent-scaled values alone", () => {
    expect(formatConfidence(82)).toBe("82%");
    expect(formatConfidence(99.6)).toBe("100%");
  });

  it("renders nothing for missing or non-finite values", () => {
    expect(formatConfidence(null)).toBe("");
    expect(formatConfidence(Number.NaN)).toBe("");
    expect(formatConfidence(Number.POSITIVE_INFINITY)).toBe("");
  });
});
