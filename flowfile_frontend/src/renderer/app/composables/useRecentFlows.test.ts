// Unit tests for the recent-flows list backing the designer welcome screen.
// These guard the dedupe/cap behavior and the tolerance for malformed
// localStorage payloads (the list must never crash the welcome screen).

import { describe, it, expect, beforeEach, vi } from "vitest";

// refreshCatalogRefs lazily imports these, so the composable itself stays
// dependency-free; vi.mock intercepts dynamic imports too.
const mocks = vi.hoisted(() => ({ getFlows: vi.fn(), getNamespaceTree: vi.fn() }));

vi.mock("../api", () => ({
  CatalogApi: { getFlows: mocks.getFlows, getNamespaceTree: mocks.getNamespaceTree },
}));
vi.mock("../types", () => ({ findNamespacePath: () => ["General", "default"] }));

import {
  upsertRecent,
  removeRecent,
  renameRecent,
  parseStored,
  basenameNoExt,
  recentDisplayName,
  useRecentFlows,
  MAX_RECENT,
  type RecentFlow,
} from "./useRecentFlows";

const entry = (path: string, lastOpened = 0): RecentFlow => ({
  path,
  name: basenameNoExt(path),
  lastOpened,
});

describe("upsertRecent", () => {
  it("inserts a new entry at the front", () => {
    const list = [entry("/flows/a.flowfile", 1)];
    const result = upsertRecent(list, entry("/flows/b.flowfile", 2));
    expect(result.map((f) => f.path)).toEqual(["/flows/b.flowfile", "/flows/a.flowfile"]);
  });

  it("moves an existing path to the front and updates it", () => {
    const list = [entry("/flows/a.flowfile", 1), entry("/flows/b.flowfile", 2)];
    const result = upsertRecent(list, entry("/flows/b.flowfile", 9));
    expect(result.map((f) => f.path)).toEqual(["/flows/b.flowfile", "/flows/a.flowfile"]);
    expect(result[0].lastOpened).toBe(9);
    expect(result).toHaveLength(2);
  });

  it("caps the list at the max, dropping the oldest", () => {
    let list: RecentFlow[] = [];
    for (let i = 0; i < MAX_RECENT + 2; i++) {
      list = upsertRecent(list, entry(`/flows/f${i}.flowfile`, i));
    }
    expect(list).toHaveLength(MAX_RECENT);
    expect(list[0].path).toBe(`/flows/f${MAX_RECENT + 1}.flowfile`);
    expect(list.some((f) => f.path === "/flows/f0.flowfile")).toBe(false);
    expect(list.some((f) => f.path === "/flows/f1.flowfile")).toBe(false);
  });
});

describe("removeRecent", () => {
  it("removes the matching path", () => {
    const list = [entry("/flows/a.flowfile"), entry("/flows/b.flowfile")];
    expect(removeRecent(list, "/flows/a.flowfile").map((f) => f.path)).toEqual([
      "/flows/b.flowfile",
    ]);
  });

  it("is a no-op when the path is absent", () => {
    const list = [entry("/flows/a.flowfile")];
    expect(removeRecent(list, "/flows/missing.flowfile")).toEqual(list);
  });
});

describe("renameRecent", () => {
  it("renames the matching path only", () => {
    const list = [entry("/flows/a.yaml", 1), entry("/flows/b.yaml", 2)];
    const result = renameRecent(list, "/flows/a.yaml", "Q3 Sales");
    expect(result.map((f) => f.name)).toEqual(["Q3 Sales", "b"]);
  });

  it("keeps position and lastOpened — a rename is not an open", () => {
    const list = [entry("/flows/a.yaml", 1), entry("/flows/b.yaml", 2)];
    const result = renameRecent(list, "/flows/b.yaml", "Renamed");
    expect(result.map((f) => f.path)).toEqual(["/flows/a.yaml", "/flows/b.yaml"]);
    expect(result[1].lastOpened).toBe(2);
  });

  it("does not resurrect an entry for an unknown path", () => {
    const list = [entry("/flows/a.yaml")];
    const result = renameRecent(list, "/flows/missing.yaml", "Nope");
    expect(result).toEqual(list);
  });

  it("returns the same entry object when the name is unchanged", () => {
    const list = [entry("/flows/a.yaml")];
    expect(renameRecent(list, "/flows/a.yaml", "a")[0]).toBe(list[0]);
  });
});

describe("recentDisplayName", () => {
  it("strips flow file extensions", () => {
    expect(recentDisplayName({ name: "sales.yaml" })).toBe("sales");
    expect(recentDisplayName({ name: "sales.yml" })).toBe("sales");
    expect(recentDisplayName({ name: "sales.flowfile" })).toBe("sales");
  });

  it("leaves a catalog name containing a dot alone", () => {
    expect(recentDisplayName({ name: "Q3 report v1.2" })).toBe("Q3 report v1.2");
  });
});

describe("parseStored", () => {
  it("returns [] for null, invalid JSON, and non-arrays", () => {
    expect(parseStored(null)).toEqual([]);
    expect(parseStored("not json")).toEqual([]);
    expect(parseStored('{"path":"/a"}')).toEqual([]);
  });

  it("drops malformed entries and keeps valid ones", () => {
    const valid = entry("/flows/a.flowfile", 5);
    const raw = JSON.stringify([
      valid,
      null,
      "string",
      { path: "", name: "empty path", lastOpened: 1 },
      { path: "/flows/no-name.flowfile", lastOpened: 1 },
      { path: "/flows/no-time.flowfile", name: "x" },
    ]);
    expect(parseStored(raw)).toEqual([valid]);
  });
});

describe("basenameNoExt", () => {
  it("handles posix and windows separators", () => {
    expect(basenameNoExt("/home/user/flows/my_flow.flowfile")).toBe("my_flow");
    expect(basenameNoExt("C:\\Users\\me\\flows\\my_flow.yaml")).toBe("my_flow");
  });

  it("strips only the last extension", () => {
    expect(basenameNoExt("/flows/my.flow.yml")).toBe("my.flow");
  });

  it("handles names without an extension and dotfiles", () => {
    expect(basenameNoExt("/flows/noext")).toBe("noext");
    expect(basenameNoExt("/flows/.hidden")).toBe(".hidden");
  });
});

describe("useRecentFlows", () => {
  beforeEach(() => {
    const store = new Map<string, string>();
    vi.stubGlobal("localStorage", {
      getItem: (key: string) => store.get(key) ?? null,
      setItem: (key: string, value: string) => void store.set(key, value),
      removeItem: (key: string) => void store.delete(key),
    });
    useRecentFlows().loadRecentFlows();
  });

  it("records flows, derives names, and persists", () => {
    const { recentFlows, recordFlow, loadRecentFlows } = useRecentFlows();
    recordFlow({ path: "/flows/sales.flowfile" });
    recordFlow({ path: "/flows/orders.flowfile", name: "Orders pipeline" });

    expect(recentFlows.value.map((f) => f.name)).toEqual(["Orders pipeline", "sales"]);

    // Round-trips through the stubbed localStorage.
    loadRecentFlows();
    expect(recentFlows.value.map((f) => f.path)).toEqual([
      "/flows/orders.flowfile",
      "/flows/sales.flowfile",
    ]);
  });

  it("dedupes by path on re-open", () => {
    const { recentFlows, recordFlow } = useRecentFlows();
    recordFlow({ path: "/flows/a.flowfile" });
    recordFlow({ path: "/flows/b.flowfile" });
    recordFlow({ path: "/flows/a.flowfile" });

    expect(recentFlows.value.map((f) => f.path)).toEqual([
      "/flows/a.flowfile",
      "/flows/b.flowfile",
    ]);
  });

  it("removes entries and persists the removal", () => {
    const { recentFlows, recordFlow, removeFlow, loadRecentFlows } = useRecentFlows();
    recordFlow({ path: "/flows/gone.flowfile" });
    removeFlow("/flows/gone.flowfile");

    expect(recentFlows.value).toEqual([]);
    loadRecentFlows();
    expect(recentFlows.value).toEqual([]);
  });

  it("ignores records without a path", () => {
    const { recentFlows, recordFlow } = useRecentFlows();
    recordFlow({ path: "" });
    expect(recentFlows.value).toEqual([]);
  });

  it("stores and round-trips a catalog reference", () => {
    const { recentFlows, recordFlow, loadRecentFlows } = useRecentFlows();
    recordFlow({ path: "/flows/a.yaml", name: "a", catalogRef: "General.default.a" });

    loadRecentFlows();
    expect(recentFlows.value[0].catalogRef).toBe("General.default.a");
  });

  it("keeps name and catalogRef on re-records without explicit metadata", () => {
    const { recentFlows, recordFlow } = useRecentFlows();
    recordFlow({ path: "/flows/a.yaml", name: "My Flow", catalogRef: "General.default.a" });
    recordFlow({ path: "/flows/a.yaml" });

    expect(recentFlows.value[0].name).toBe("My Flow");
    expect(recentFlows.value[0].catalogRef).toBe("General.default.a");
  });

  it("overwrites catalogRef when a new one is provided", () => {
    const { recentFlows, recordFlow } = useRecentFlows();
    recordFlow({ path: "/flows/a.yaml", catalogRef: "General.default.a" });
    recordFlow({ path: "/flows/a.yaml", catalogRef: "General.marketing.a" });

    expect(recentFlows.value[0].catalogRef).toBe("General.marketing.a");
  });

  it("records a catalogId when provided", () => {
    const { recentFlows, recordFlow } = useRecentFlows();
    recordFlow({ path: "/flows/a.yaml", name: "a", catalogRef: "General.default.a", catalogId: 7 });

    expect(recentFlows.value[0].catalogId).toBe(7);
  });

  it("keeps catalogId on re-records without one", () => {
    const { recentFlows, recordFlow } = useRecentFlows();
    recordFlow({ path: "/flows/a.yaml", catalogId: 7 });
    recordFlow({ path: "/flows/a.yaml" });

    expect(recentFlows.value[0].catalogId).toBe(7);
  });

  it("renames an entry in place and persists it", () => {
    const { recentFlows, recordFlow, renameFlow, loadRecentFlows } = useRecentFlows();
    recordFlow({ path: "/flows/a.yaml", name: "Untitled flow 2026-07-28" });
    renameFlow("/flows/a.yaml", "Q3 Sales");

    expect(recentFlows.value[0].name).toBe("Q3 Sales");
    loadRecentFlows();
    expect(recentFlows.value[0].name).toBe("Q3 Sales");
  });

  it("ignores renames for unknown paths and empty names", () => {
    const { recentFlows, recordFlow, renameFlow } = useRecentFlows();
    recordFlow({ path: "/flows/a.yaml", name: "a" });
    renameFlow("/flows/missing.yaml", "Ghost");
    renameFlow("/flows/a.yaml", "");

    expect(recentFlows.value.map((f) => f.name)).toEqual(["a"]);
  });

  it("prefers the catalog display name when recording from flow settings", () => {
    const { recentFlows, recordFlowFromSettings } = useRecentFlows();
    recordFlowFromSettings({ path: "/flows/41_house.yaml", name: "41_house", display_name: "Q3 Sales" });

    expect(recentFlows.value[0].name).toBe("Q3 Sales");
  });
});

describe("useRecentFlows.refreshCatalogRefs", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    const store = new Map<string, string>();
    vi.stubGlobal("localStorage", {
      getItem: (key: string) => store.get(key) ?? null,
      setItem: (key: string, value: string) => void store.set(key, value),
      removeItem: (key: string) => void store.delete(key),
    });
    useRecentFlows().loadRecentFlows();
    mocks.getNamespaceTree.mockResolvedValue([]);
  });

  it("refreshes a stale name and the catalog ref from the registration", async () => {
    const { recentFlows, recordFlow, refreshCatalogRefs, loadRecentFlows } = useRecentFlows();
    recordFlow({ path: "/flows/a.yaml", name: "Untitled flow 2026-07-28" });
    mocks.getFlows.mockResolvedValue([
      { id: 3, name: "Q3 Sales", flow_path: "/flows/a.yaml", namespace_id: 2 },
    ]);

    await refreshCatalogRefs();

    expect(recentFlows.value[0].name).toBe("Q3 Sales");
    expect(recentFlows.value[0].catalogRef).toBe("General.default.Q3 Sales");
    expect(recentFlows.value[0].catalogId).toBe(3);
    loadRecentFlows();
    expect(recentFlows.value[0].name).toBe("Q3 Sales");
  });

  it("never clobbers the name of a flow with no registration", async () => {
    const { recentFlows, recordFlow, refreshCatalogRefs } = useRecentFlows();
    recordFlow({ path: "/flows/local.yaml", name: "local", catalogRef: "Gone.ref", catalogId: 9 });
    mocks.getFlows.mockResolvedValue([]);

    await refreshCatalogRefs();

    expect(recentFlows.value[0].name).toBe("local");
    expect(recentFlows.value[0].catalogRef).toBeUndefined();
    expect(recentFlows.value[0].catalogId).toBeUndefined();
  });

  it("leaves entries untouched when nothing changed", async () => {
    const { recentFlows, recordFlow, refreshCatalogRefs } = useRecentFlows();
    recordFlow({ path: "/flows/a.yaml", name: "Q3 Sales", catalogRef: "General.default.Q3 Sales", catalogId: 3 });
    const before = recentFlows.value[0];
    mocks.getFlows.mockResolvedValue([
      { id: 3, name: "Q3 Sales", flow_path: "/flows/a.yaml", namespace_id: 2 },
    ]);

    await refreshCatalogRefs();

    expect(recentFlows.value[0]).toBe(before);
  });

  it("swallows a catalog failure and keeps the list intact", async () => {
    const { recentFlows, recordFlow, refreshCatalogRefs } = useRecentFlows();
    recordFlow({ path: "/flows/a.yaml", name: "a" });
    mocks.getFlows.mockRejectedValue(new Error("offline"));

    await refreshCatalogRefs();

    expect(recentFlows.value.map((f) => f.name)).toEqual(["a"]);
  });

  it("makes no API call when there are no recents", async () => {
    const { refreshCatalogRefs } = useRecentFlows();
    await refreshCatalogRefs();
    expect(mocks.getFlows).not.toHaveBeenCalled();
  });
});
