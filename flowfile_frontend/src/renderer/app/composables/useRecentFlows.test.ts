// Unit tests for the recent-flows list backing the designer welcome screen.
// These guard the dedupe/cap behavior and the tolerance for malformed
// localStorage payloads (the list must never crash the welcome screen).

import { describe, it, expect, beforeEach, vi } from "vitest";
import {
  upsertRecent,
  removeRecent,
  parseStored,
  basenameNoExt,
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
});
