// Unit tests for the notebook browser-persistence helpers. Pure functions with
// an injected StorageLike — no jsdom needed.
import { describe, expect, it } from "vitest";
import {
  PERSISTENCE_KEY,
  MAX_PERSISTED_NOTEBOOKS,
  loadPersistedNotebooks,
  persistNotebooks,
  clearPersistedNotebooks,
  type PersistedNotebookState,
  type StorageLike,
} from "./notebook-store-persistence";

function makeStorage(
  initial: Record<string, string> = {},
): StorageLike & { _data: Record<string, string> } {
  const data: Record<string, string> = { ...initial };
  return {
    _data: data,
    getItem: (k) => (k in data ? data[k] : null),
    setItem: (k, v) => {
      data[k] = v;
    },
    removeItem: (k) => {
      delete data[k];
    },
  };
}

const sample = (): PersistedNotebookState => ({
  activeTabId: "t1",
  openNotebooks: [
    {
      tabId: "t1",
      persistedId: 3,
      name: "nb",
      description: null,
      namespaceId: null,
      cells: [
        { id: "c1", type: "python", source: "x=1", metadata: {} },
        { id: "c2", type: "markdown", source: "# hi", metadata: {} },
      ],
      kernelId: "k1",
      dirty: true,
    },
  ],
});

describe("notebook persistence round-trip", () => {
  it("persists and reloads open notebooks", () => {
    const storage = makeStorage();
    persistNotebooks(sample(), storage);
    const loaded = loadPersistedNotebooks(storage);
    expect(loaded.activeTabId).toBe("t1");
    expect(loaded.openNotebooks).toHaveLength(1);
    expect(loaded.openNotebooks[0].cells).toHaveLength(2);
    expect(loaded.openNotebooks[0].kernelId).toBe("k1");
    expect(loaded.openNotebooks[0].dirty).toBe(true);
  });

  it("returns empty state when nothing is stored", () => {
    expect(loadPersistedNotebooks(makeStorage())).toEqual({ openNotebooks: [], activeTabId: null });
  });

  it("coerces a legacy sql cell to python on load", () => {
    const storage = makeStorage({
      [PERSISTENCE_KEY]: JSON.stringify({
        activeTabId: "t1",
        openNotebooks: [
          {
            tabId: "t1",
            persistedId: null,
            name: "x",
            description: null,
            namespaceId: null,
            kernelId: null,
            dirty: false,
            cells: [{ id: "c1", type: "sql", source: "SELECT 1", metadata: {} }],
          },
        ],
      }),
    });
    const loaded = loadPersistedNotebooks(storage);
    expect(loaded.openNotebooks[0].cells[0].type).toBe("python");
    expect(loaded.openNotebooks[0].cells[0].source).toBe("SELECT 1");
  });

  it("drops corrupt JSON and clears the key", () => {
    const storage = makeStorage({ [PERSISTENCE_KEY]: "{not json" });
    expect(loadPersistedNotebooks(storage)).toEqual({ openNotebooks: [], activeTabId: null });
    expect(storage.getItem(PERSISTENCE_KEY)).toBeNull();
  });

  it("caps the number of persisted notebooks", () => {
    const many: PersistedNotebookState = {
      activeTabId: "t0",
      openNotebooks: Array.from({ length: MAX_PERSISTED_NOTEBOOKS + 5 }, (_, i) => ({
        tabId: `t${i}`,
        persistedId: null,
        name: `n${i}`,
        description: null,
        namespaceId: null,
        cells: [],
        kernelId: null,
        dirty: false,
      })),
    };
    const storage = makeStorage();
    persistNotebooks(many, storage);
    expect(loadPersistedNotebooks(storage).openNotebooks).toHaveLength(MAX_PERSISTED_NOTEBOOKS);
  });

  it("clearPersistedNotebooks removes the entry", () => {
    const storage = makeStorage();
    persistNotebooks(sample(), storage);
    clearPersistedNotebooks(storage);
    expect(storage.getItem(PERSISTENCE_KEY)).toBeNull();
  });

  it("no-ops gracefully when storage is null (no window)", () => {
    expect(() => persistNotebooks(sample(), null)).not.toThrow();
    expect(loadPersistedNotebooks(null)).toEqual({ openNotebooks: [], activeTabId: null });
  });
});
