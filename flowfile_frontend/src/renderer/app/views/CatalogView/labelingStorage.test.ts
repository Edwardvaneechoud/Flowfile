// Unit tests for the catalog labeling persistence helpers.
//
// The helpers are pure (no Vue, no DOM) so hand-rolled `Storage` mocks are
// enough — no jsdom / happy-dom needed.

import { describe, expect, it } from "vitest";

import {
  LABELING_COLLAPSED_KEY_PREFIX,
  LABEL_SETTINGS_KEY_PREFIX,
  MAX_LABEL_CLASSES,
  type StoredLabelSettings,
  readCollapsedColumns,
  readLabelSettings,
  sanitizeLabelSettings,
  writeCollapsedColumns,
  writeLabelSettings,
} from "./labelingStorage";

interface MockStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
  _data: Map<string, string>;
}

const makeStorage = (): MockStorage => {
  const data = new Map<string, string>();
  return {
    _data: data,
    getItem: (key) => (data.has(key) ? data.get(key)! : null),
    setItem: (key, value) => {
      data.set(key, value);
    },
    removeItem: (key) => {
      data.delete(key);
    },
  };
};

const makeThrowingStorage = (): MockStorage => {
  const storage = makeStorage();
  storage.getItem = () => {
    throw new Error("SecurityError");
  };
  storage.setItem = () => {
    throw new Error("QuotaExceededError");
  };
  storage.removeItem = () => {
    throw new Error("SecurityError");
  };
  return storage;
};

const settingsKey = (tableId: number) => `${LABEL_SETTINGS_KEY_PREFIX}.${tableId}`;
const collapsedKey = (tableId: number) => `${LABELING_COLLAPSED_KEY_PREFIX}.${tableId}`;

const fullSettings = (): StoredLabelSettings => ({
  targetMode: "new",
  newColumnName: "sentiment",
  existingTarget: "",
  classes: ["positive", "negative"],
  suggestion: {
    predictionTableId: 12,
    predictionTableName: "preds",
    suggestionColumn: "prediction",
    confidenceColumn: "confidence",
    useProbabilityColumns: true,
    leastConfidentFirst: true,
  },
});

describe("label settings round-trip", () => {
  it("reads back exactly what was written", () => {
    const storage = makeStorage();
    const settings = fullSettings();
    writeLabelSettings(7, settings, storage);
    expect(readLabelSettings(7, storage)).toEqual(settings);
  });

  it("keys entries per table id so two tables do not collide", () => {
    const storage = makeStorage();
    const first = fullSettings();
    const second: StoredLabelSettings = {
      targetMode: "existing",
      newColumnName: "",
      existingTarget: "label",
      classes: ["a"],
      suggestion: null,
    };
    writeLabelSettings(7, first, storage);
    writeLabelSettings(8, second, storage);

    expect(readLabelSettings(7, storage)).toEqual(first);
    expect(readLabelSettings(8, storage)).toEqual(second);
    expect(storage._data.has(settingsKey(7))).toBe(true);
    expect(storage._data.has(settingsKey(8))).toBe(true);
  });

  it("caps the class list on write so hydration cannot silently discard it", () => {
    const storage = makeStorage();
    const classes = Array.from({ length: MAX_LABEL_CLASSES + 12 }, (_, i) => `c${i}`);
    writeLabelSettings(7, { ...fullSettings(), classes }, storage);

    const stored = JSON.parse(storage._data.get(settingsKey(7))!);
    expect(stored.classes).toHaveLength(MAX_LABEL_CLASSES);
    expect(stored.classes[MAX_LABEL_CLASSES - 1]).toBe(`c${MAX_LABEL_CLASSES - 1}`);
    expect(readLabelSettings(7, storage)?.classes).toEqual(stored.classes);
  });

  it("applies the read-path class sanitizer on write", () => {
    const storage = makeStorage();
    const dirty = { ...fullSettings(), classes: ["a", "", "a", "b"] } as StoredLabelSettings;
    writeLabelSettings(7, dirty, storage);
    expect(JSON.parse(storage._data.get(settingsKey(7))!).classes).toEqual(["a", "b"]);
  });

  it("returns null when nothing is stored for the table", () => {
    const storage = makeStorage();
    writeLabelSettings(7, fullSettings(), storage);
    expect(readLabelSettings(9, storage)).toBeNull();
  });

  it("returns null without storage", () => {
    expect(readLabelSettings(7, null)).toBeNull();
  });

  it("drops the entry and returns null on corrupt JSON", () => {
    const storage = makeStorage();
    storage.setItem(settingsKey(7), "{not json");
    storage.setItem(settingsKey(8), JSON.stringify(fullSettings()));

    expect(readLabelSettings(7, storage)).toBeNull();
    expect(storage.getItem(settingsKey(7))).toBeNull();
    expect(readLabelSettings(8, storage)).not.toBeNull();
  });
});

describe("sanitizeLabelSettings", () => {
  it("rejects non-object payloads", () => {
    expect(sanitizeLabelSettings(null)).toBeNull();
    expect(sanitizeLabelSettings("a string")).toBeNull();
    expect(sanitizeLabelSettings(42)).toBeNull();
    expect(sanitizeLabelSettings([{ targetMode: "new" }])).toBeNull();
  });

  it("rejects an unusable targetMode", () => {
    expect(sanitizeLabelSettings({ classes: ["a"] })).toBeNull();
    expect(sanitizeLabelSettings({ targetMode: "New", classes: ["a"] })).toBeNull();
    expect(sanitizeLabelSettings({ targetMode: 1, classes: ["a"] })).toBeNull();
  });

  it("defaults non-string name fields to empty strings", () => {
    const settings = sanitizeLabelSettings({
      targetMode: "existing",
      newColumnName: 5,
      existingTarget: null,
    });
    expect(settings).toEqual({
      targetMode: "existing",
      newColumnName: "",
      existingTarget: "",
      classes: [],
      suggestion: null,
    });
  });

  it("treats a non-array classes field as empty", () => {
    expect(sanitizeLabelSettings({ targetMode: "new", classes: "positive" })?.classes).toEqual([]);
    expect(sanitizeLabelSettings({ targetMode: "new", classes: { a: 1 } })?.classes).toEqual([]);
  });

  it("drops non-string and empty class entries and dedupes", () => {
    const settings = sanitizeLabelSettings({
      targetMode: "new",
      classes: ["a", "", 3, null, "b", "a", { name: "c" }],
    });
    expect(settings?.classes).toEqual(["a", "b"]);
  });

  it("caps the class list", () => {
    const classes = Array.from({ length: MAX_LABEL_CLASSES + 12 }, (_, i) => `c${i}`);
    const settings = sanitizeLabelSettings({ targetMode: "new", classes });
    expect(settings?.classes).toHaveLength(MAX_LABEL_CLASSES);
    expect(settings?.classes[0]).toBe("c0");
    expect(settings?.classes[MAX_LABEL_CLASSES - 1]).toBe(`c${MAX_LABEL_CLASSES - 1}`);
  });

  it("drops only the suggestion when suggestionColumn is missing", () => {
    const settings = sanitizeLabelSettings({
      targetMode: "new",
      newColumnName: "sentiment",
      classes: ["a", "b"],
      suggestion: { predictionTableId: 12, predictionTableName: "preds" },
    });
    expect(settings).toEqual({
      targetMode: "new",
      newColumnName: "sentiment",
      existingTarget: "",
      classes: ["a", "b"],
      suggestion: null,
    });
  });

  it("drops only the suggestion when the prediction table id is not numeric", () => {
    const base = {
      targetMode: "new" as const,
      newColumnName: "sentiment",
      classes: ["a"],
    };
    for (const predictionTableId of ["12", null, 0, -3, 1.5, Number.NaN, Infinity]) {
      const settings = sanitizeLabelSettings({
        ...base,
        suggestion: { predictionTableId, suggestionColumn: "prediction" },
      });
      expect(settings?.suggestion).toBeNull();
      expect(settings?.classes).toEqual(["a"]);
    }
  });

  it("drops the suggestion when it is not an object", () => {
    expect(
      sanitizeLabelSettings({ targetMode: "new", suggestion: "preds" })?.suggestion,
    ).toBeNull();
    expect(sanitizeLabelSettings({ targetMode: "new", suggestion: [] })?.suggestion).toBeNull();
  });

  it("fills suggestion defaults and coerces the flags", () => {
    const settings = sanitizeLabelSettings({
      targetMode: "new",
      suggestion: {
        predictionTableId: 4,
        suggestionColumn: "prediction",
        predictionTableName: 99,
        confidenceColumn: 0,
        useProbabilityColumns: "yes",
        leastConfidentFirst: undefined,
      },
    });
    expect(settings?.suggestion).toEqual({
      predictionTableId: 4,
      predictionTableName: "",
      suggestionColumn: "prediction",
      confidenceColumn: null,
      useProbabilityColumns: true,
      leastConfidentFirst: false,
    });
  });
});

describe("collapsed columns", () => {
  it("round-trips per table id", () => {
    const storage = makeStorage();
    writeCollapsedColumns(7, ["notes", "raw_text"], storage);
    writeCollapsedColumns(8, ["id"], storage);

    expect(readCollapsedColumns(7, storage)).toEqual(["notes", "raw_text"]);
    expect(readCollapsedColumns(8, storage)).toEqual(["id"]);
    expect(readCollapsedColumns(9, storage)).toEqual([]);
  });

  it("returns an empty array without storage", () => {
    expect(readCollapsedColumns(7, null)).toEqual([]);
  });

  it("drops the entry and returns an empty array on corrupt JSON", () => {
    const storage = makeStorage();
    storage.setItem(collapsedKey(7), "[not json");
    expect(readCollapsedColumns(7, storage)).toEqual([]);
    expect(storage.getItem(collapsedKey(7))).toBeNull();
  });

  it("ignores non-array payloads and non-string entries", () => {
    const storage = makeStorage();
    storage.setItem(collapsedKey(7), JSON.stringify({ notes: true }));
    expect(readCollapsedColumns(7, storage)).toEqual([]);
    storage.setItem(collapsedKey(7), JSON.stringify(["notes", "", 3, null, "raw_text"]));
    expect(readCollapsedColumns(7, storage)).toEqual(["notes", "raw_text"]);
  });
});

describe("hostile storage", () => {
  it("never propagates a throwing Storage from any exported function", () => {
    const storage = makeThrowingStorage();
    expect(() => sanitizeLabelSettings({ targetMode: "new" })).not.toThrow();
    expect(() => readLabelSettings(7, storage)).not.toThrow();
    expect(() => writeLabelSettings(7, fullSettings(), storage)).not.toThrow();
    expect(() => readCollapsedColumns(7, storage)).not.toThrow();
    expect(() => writeCollapsedColumns(7, ["notes"], storage)).not.toThrow();
    expect(readLabelSettings(7, storage)).toBeNull();
    expect(readCollapsedColumns(7, storage)).toEqual([]);
  });

  it("swallows a removeItem failure while self-healing corrupt JSON", () => {
    const storage = makeStorage();
    storage.setItem(settingsKey(7), "{not json");
    storage.removeItem = () => {
      throw new Error("SecurityError");
    };
    expect(() => readLabelSettings(7, storage)).not.toThrow();
    expect(readLabelSettings(7, storage)).toBeNull();
  });

  it("swallows a value that cannot be serialized", () => {
    const storage = makeStorage();
    const cyclic = fullSettings() as StoredLabelSettings & { self?: unknown };
    cyclic.self = cyclic;
    expect(() => writeLabelSettings(7, cyclic, storage)).not.toThrow();
    expect(storage._data.has(settingsKey(7))).toBe(false);
  });
});
