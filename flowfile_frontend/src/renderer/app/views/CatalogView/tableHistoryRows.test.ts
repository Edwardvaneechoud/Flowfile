import { describe, expect, it } from "vitest";

import { HISTORY_ROWS_KEY, recallHistoryRows, rememberHistoryRows } from "./tableHistoryRows";

const makeStorage = () => {
  const data = new Map<string, string>();
  return {
    _data: data,
    getItem: (key: string) => data.get(key) ?? null,
    setItem: (key: string, value: string) => {
      data.set(key, value);
    },
  };
};

describe("tableHistoryRows", () => {
  it("recalls the count a table listed, per table", () => {
    const storage = makeStorage();
    rememberHistoryRows(4, 12, storage);
    rememberHistoryRows(5, 1, storage);
    expect(recallHistoryRows(4, storage)).toBe(12);
    expect(recallHistoryRows(5, storage)).toBe(1);
    expect(recallHistoryRows(6, storage)).toBeNull();
  });

  it("ignores a corrupt or tampered entry", () => {
    const storage = makeStorage();
    storage.setItem(HISTORY_ROWS_KEY, "{not json");
    expect(recallHistoryRows(4, storage)).toBeNull();
    storage.setItem(HISTORY_ROWS_KEY, JSON.stringify({ 4: "many", 5: -1, 6: 2.5 }));
    expect(recallHistoryRows(4, storage)).toBeNull();
    expect(recallHistoryRows(5, storage)).toBeNull();
    expect(recallHistoryRows(6, storage)).toBeNull();
  });

  it("starts over once it holds too many tables", () => {
    const storage = makeStorage();
    for (let id = 1; id <= 500; id++) rememberHistoryRows(id, 3, storage);
    rememberHistoryRows(501, 9, storage);
    expect(recallHistoryRows(501, storage)).toBe(9);
    expect(recallHistoryRows(1, storage)).toBeNull();
  });

  it("survives a storage that throws", () => {
    const thrower = {
      getItem: () => {
        throw new Error("denied");
      },
      setItem: () => {
        throw new Error("denied");
      },
    };
    expect(recallHistoryRows(1, thrower)).toBeNull();
    expect(() => rememberHistoryRows(1, 2, thrower)).not.toThrow();
  });
});
