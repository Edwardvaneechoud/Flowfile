// Unit tests for the "last catalog location" sanitiser + persistence helpers.
//
// sanitizeCatalogQuery is pure and the storage helpers take a `StorageLike` seam, so a
// hand-rolled mock is enough — no jsdom / happy-dom needed.

import { describe, expect, it } from "vitest";

import {
  LAST_BY_TAB_KEY,
  LAST_LOCATION_KEY,
  catalogTabLocation,
  forgetCatalogSelection,
  isSameLocation,
  lastCatalogQuery,
  lastCatalogQueryByTab,
  loadCatalogLocationsByTab,
  loadLastCatalogLocation,
  persistCatalogLocationsByTab,
  persistLastCatalogLocation,
  rememberCatalogLocation,
  sanitizeCatalogQuery,
  withoutSelection,
} from "./catalogLastLocation";

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

describe("sanitizeCatalogQuery", () => {
  it("keeps a known tab and the selection ids", () => {
    expect(sanitizeCatalogQuery({ tab: "catalog", tableId: "12" })).toEqual({
      tab: "catalog",
      tableId: "12",
    });
    expect(sanitizeCatalogQuery({ tab: "runs", flowId: "3", runId: "48" })).toEqual({
      tab: "runs",
      flowId: "3",
      runId: "48",
    });
  });

  it("rejects a query with no tab so a bare catalog push can't wipe the memory", () => {
    expect(sanitizeCatalogQuery({})).toBeNull();
    expect(sanitizeCatalogQuery({ tableId: "12" })).toBeNull();
  });

  it("rejects tabs that are not in CATALOG_TAB_KEYS", () => {
    // "dashboards" reaches the URL in the wild and "following" only exists in the drifted
    // CatalogTab union — neither renders a panel, so neither may be remembered.
    expect(sanitizeCatalogQuery({ tab: "dashboards" })).toBeNull();
    expect(sanitizeCatalogQuery({ tab: "following" })).toBeNull();
    expect(sanitizeCatalogQuery({ tab: "nonsense" })).toBeNull();
  });

  it("accepts every declared tab key", () => {
    for (const tab of ["catalog", "notebook", "sql", "community", "customNodes"]) {
      expect(sanitizeCatalogQuery({ tab })).toEqual({ tab });
    }
  });

  it("drops scheduleFlowId so the create-schedule modal can't re-pop", () => {
    expect(sanitizeCatalogQuery({ tab: "schedules", scheduleFlowId: "7" })).toEqual({
      tab: "schedules",
    });
  });

  it("drops unknown keys", () => {
    expect(sanitizeCatalogQuery({ tab: "catalog", openFile: "x.py", nope: "1" })).toEqual({
      tab: "catalog",
    });
  });

  it("drops non-numeric, overlong and non-string ids", () => {
    expect(sanitizeCatalogQuery({ tab: "catalog", tableId: "abc" })).toEqual({ tab: "catalog" });
    expect(sanitizeCatalogQuery({ tab: "catalog", tableId: "1234567890" })).toEqual({
      tab: "catalog",
    });
    expect(sanitizeCatalogQuery({ tab: "catalog", tableId: 12 })).toEqual({ tab: "catalog" });
    expect(sanitizeCatalogQuery({ tab: "catalog", tableId: null })).toEqual({ tab: "catalog" });
  });

  it("drops array-valued params, which already break the view", () => {
    expect(sanitizeCatalogQuery({ tab: ["catalog", "runs"] })).toBeNull();
    expect(sanitizeCatalogQuery({ tab: "catalog", tableId: ["1", "2"] })).toEqual({
      tab: "catalog",
    });
  });

  it("keeps only the declared enum values for kind and apiView", () => {
    expect(sanitizeCatalogQuery({ tab: "visuals", kind: "dashboards" })).toEqual({
      tab: "visuals",
      kind: "dashboards",
    });
    expect(sanitizeCatalogQuery({ tab: "visuals", kind: "sideways" })).toEqual({ tab: "visuals" });
    expect(sanitizeCatalogQuery({ tab: "apis", apiView: "consumers" })).toEqual({
      tab: "apis",
      apiView: "consumers",
    });
    expect(sanitizeCatalogQuery({ tab: "apis", apiView: "elsewhere" })).toEqual({ tab: "apis" });
  });

  it("rejects non-object input", () => {
    expect(sanitizeCatalogQuery(null)).toBeNull();
    expect(sanitizeCatalogQuery(undefined)).toBeNull();
    expect(sanitizeCatalogQuery("tab=catalog")).toBeNull();
    expect(sanitizeCatalogQuery([{ tab: "catalog" }])).toBeNull();
  });
});

describe("loadLastCatalogLocation / persistLastCatalogLocation", () => {
  it("round-trips a location", () => {
    const storage = makeStorage();
    persistLastCatalogLocation({ tab: "catalog", tableId: "9" }, storage);
    expect(loadLastCatalogLocation(storage)).toEqual({ tab: "catalog", tableId: "9" });
  });

  it("returns null when nothing is stored", () => {
    expect(loadLastCatalogLocation(makeStorage())).toBeNull();
  });

  it("returns null when storage is unavailable", () => {
    expect(loadLastCatalogLocation(null)).toBeNull();
    expect(() => persistLastCatalogLocation({ tab: "catalog" }, null)).not.toThrow();
  });

  it("drops the entry and returns null on corrupt JSON", () => {
    const storage = makeStorage();
    storage.setItem(LAST_LOCATION_KEY, "{not json");
    expect(loadLastCatalogLocation(storage)).toBeNull();
    expect(storage._data.has(LAST_LOCATION_KEY)).toBe(false);
  });

  it("re-validates on read, so a tampered entry is ignored", () => {
    const storage = makeStorage();
    storage.setItem(
      LAST_LOCATION_KEY,
      JSON.stringify({ tab: "catalog", tableId: "../../etc", scheduleFlowId: "7" }),
    );
    expect(loadLastCatalogLocation(storage)).toEqual({ tab: "catalog" });

    storage.setItem(LAST_LOCATION_KEY, JSON.stringify({ tab: "bogus" }));
    expect(loadLastCatalogLocation(storage)).toBeNull();
  });

  it("survives a throwing storage", () => {
    const thrower = {
      getItem: () => {
        throw new Error("denied");
      },
      setItem: () => {
        throw new Error("quota");
      },
      removeItem: () => {
        throw new Error("denied");
      },
    };
    expect(loadLastCatalogLocation(thrower)).toBeNull();
    expect(() => persistLastCatalogLocation({ tab: "catalog" }, thrower)).not.toThrow();
  });
});

describe("isSameLocation", () => {
  it("compares by content, not identity", () => {
    expect(isSameLocation({ tab: "catalog" }, { tab: "catalog" })).toBe(true);
    expect(isSameLocation({ tab: "catalog", tableId: "1" }, { tab: "catalog" })).toBe(false);
    expect(isSameLocation({ tab: "catalog" }, { tab: "catalog", tableId: "1" })).toBe(false);
    expect(isSameLocation({ tab: "catalog" }, { tab: "runs" })).toBe(false);
  });

  it("treats a missing previous value as different", () => {
    expect(isSameLocation(null, { tab: "catalog" })).toBe(false);
  });
});

describe("loadCatalogLocationsByTab / persistCatalogLocationsByTab", () => {
  it("round-trips where each tab was left", () => {
    const storage = makeStorage();
    const byTab = { catalog: { tab: "catalog", tableId: "9" }, runs: { tab: "runs", runId: "4" } };
    persistCatalogLocationsByTab(byTab, storage);
    expect(loadCatalogLocationsByTab(storage)).toEqual(byTab);
  });

  it("returns nothing when nothing is stored, storage is missing or the entry is corrupt", () => {
    expect(loadCatalogLocationsByTab(makeStorage())).toEqual({});
    expect(loadCatalogLocationsByTab(null)).toEqual({});
    const storage = makeStorage();
    storage.setItem(LAST_BY_TAB_KEY, "{not json");
    expect(loadCatalogLocationsByTab(storage)).toEqual({});
    storage.setItem(LAST_BY_TAB_KEY, "[]");
    expect(loadCatalogLocationsByTab(storage)).toEqual({});
  });

  it("re-validates each entry and drops one filed under another tab", () => {
    const storage = makeStorage();
    storage.setItem(
      LAST_BY_TAB_KEY,
      JSON.stringify({
        catalog: { tab: "catalog", tableId: "12", evil: "x" },
        runs: { tab: "catalog", tableId: "3" },
        nonsense: { tab: "nonsense" },
        sql: "sql",
      }),
    );
    expect(loadCatalogLocationsByTab(storage)).toEqual({
      catalog: { tab: "catalog", tableId: "12" },
    });
  });
});

describe("catalogTabLocation", () => {
  const byTab = { catalog: { tab: "catalog", flowId: "7" } };

  it("reopens a tab where it was left when coming from another tab or page", () => {
    expect(catalogTabLocation("catalog", "notebook", byTab)).toEqual({
      tab: "catalog",
      flowId: "7",
    });
    expect(catalogTabLocation("catalog", undefined, byTab)).toEqual({
      tab: "catalog",
      flowId: "7",
    });
  });

  it("goes to the tab's root when that tab is already shown", () => {
    expect(catalogTabLocation("catalog", "catalog", byTab)).toEqual({ tab: "catalog" });
  });

  it("goes to the root of a tab nothing is remembered for", () => {
    expect(catalogTabLocation("sql", "catalog", byTab)).toEqual({ tab: "sql" });
  });
});

describe("forgetting a deleted item", () => {
  it("sends a location naming the item back to its tab's root and leaves others alone", () => {
    const open = { tab: "favorites", flowId: "12" };
    expect(withoutSelection(open, "flowId", 12)).toEqual({ tab: "favorites" });
    expect(withoutSelection(open, "flowId", 13)).toBe(open);
    expect(withoutSelection(open, "tableId", 12)).toBe(open);
  });

  it("drops it from every tab's memory and from the last location", () => {
    rememberCatalogLocation({ tab: "favorites", flowId: "12" });
    rememberCatalogLocation({ tab: "runs", runId: "5" });
    rememberCatalogLocation({ tab: "catalog", flowId: "12" });

    forgetCatalogSelection("flowId", 12);

    expect(lastCatalogQueryByTab.value).toEqual({
      favorites: { tab: "favorites" },
      runs: { tab: "runs", runId: "5" },
      catalog: { tab: "catalog" },
    });
    expect(lastCatalogQuery.value).toEqual({ tab: "catalog" });
    expect(catalogTabLocation("favorites", "catalog", lastCatalogQueryByTab.value)).toEqual({
      tab: "favorites",
    });
  });
});
