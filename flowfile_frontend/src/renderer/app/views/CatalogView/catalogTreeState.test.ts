// Unit tests for the catalog tree expansion persistence helpers.
//
// The helpers are pure (no Vue, no DOM) so a hand-rolled `Storage` mock
// is enough — no jsdom / happy-dom needed.

import { describe, expect, it } from "vitest";

import { TREE_EXPANSION_KEY, loadTreeExpansion, persistTreeExpansion } from "./catalogTreeState";

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

describe("loadTreeExpansion", () => {
  it("returns an empty record when storage is empty", () => {
    expect(loadTreeExpansion(makeStorage())).toEqual({});
  });

  it("returns an empty record when storage is unavailable", () => {
    expect(loadTreeExpansion(null)).toEqual({});
  });

  it("drops the entry and returns an empty record on corrupt JSON", () => {
    const storage = makeStorage();
    storage.setItem(TREE_EXPANSION_KEY, "{not json");
    expect(loadTreeExpansion(storage)).toEqual({});
    expect(storage.getItem(TREE_EXPANSION_KEY)).toBeNull();
  });

  it("returns an empty record for non-object payloads", () => {
    const storage = makeStorage();
    storage.setItem(TREE_EXPANSION_KEY, JSON.stringify("a string"));
    expect(loadTreeExpansion(storage)).toEqual({});
    storage.setItem(TREE_EXPANSION_KEY, JSON.stringify([true, false]));
    expect(loadTreeExpansion(storage)).toEqual({});
  });

  it("keeps only boolean values", () => {
    const storage = makeStorage();
    storage.setItem(
      TREE_EXPANSION_KEY,
      JSON.stringify({ "ns:1": true, "sec:1:flows": "yes", "sec:1:tables": 0, "ns:2": false }),
    );
    expect(loadTreeExpansion(storage)).toEqual({ "ns:1": true, "ns:2": false });
  });
});

describe("persistTreeExpansion", () => {
  it("round-trips a record under TREE_EXPANSION_KEY", () => {
    const storage = makeStorage();
    const record = { "ns:1": true, "sec:1:flows": false };
    persistTreeExpansion(record, storage);
    expect(storage._data.has(TREE_EXPANSION_KEY)).toBe(true);
    expect(loadTreeExpansion(storage)).toEqual(record);
  });

  it("is a no-op without storage", () => {
    expect(() => persistTreeExpansion({ "ns:1": true }, null)).not.toThrow();
  });

  it("swallows setItem failures", () => {
    const storage = makeStorage();
    storage.setItem = () => {
      throw new Error("quota exceeded");
    };
    expect(() => persistTreeExpansion({ "ns:1": true }, storage)).not.toThrow();
  });
});
