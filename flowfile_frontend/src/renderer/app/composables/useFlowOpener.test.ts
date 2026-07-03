// Guards the open-flow contract: record in recents on success, prune on
// failure (moved/deleted files must not keep resurfacing on the home screen).

import { describe, it, expect, beforeEach, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  importFlow: vi.fn(),
  getAllFlows: vi.fn(),
  setFlowId: vi.fn(),
  notify: vi.fn(),
}));

vi.mock("../api", () => ({
  FlowApi: { importFlow: mocks.importFlow, getAllFlows: mocks.getAllFlows },
}));
vi.mock("../stores/column-store", () => ({ useNodeStore: () => ({ setFlowId: mocks.setFlowId }) }));
vi.mock("element-plus", () => ({ ElNotification: mocks.notify }));

import { useFlowOpener } from "./useFlowOpener";
import { useRecentFlows } from "./useRecentFlows";

describe("useFlowOpener", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    const store = new Map<string, string>();
    vi.stubGlobal("localStorage", {
      getItem: (key: string) => store.get(key) ?? null,
      setItem: (key: string, value: string) => void store.set(key, value),
      removeItem: (key: string) => void store.delete(key),
    });
    useRecentFlows().loadRecentFlows();
  });

  it("sets the flow id and records the flow on success", async () => {
    mocks.importFlow.mockResolvedValue(7);
    const { openFlow } = useFlowOpener();
    const { recentFlows } = useRecentFlows();

    const result = await openFlow("/flows/a.yaml", {
      name: "My Flow",
      catalogRef: "General.default.a",
    });

    expect(result).toBe(7);
    expect(mocks.setFlowId).toHaveBeenCalledWith(7);
    expect(recentFlows.value[0]).toMatchObject({
      path: "/flows/a.yaml",
      name: "My Flow",
      catalogRef: "General.default.a",
    });
    expect(mocks.notify).not.toHaveBeenCalled();
  });

  it("prunes the recents entry and notifies when no flow id is returned", async () => {
    mocks.importFlow.mockResolvedValue(undefined);
    const { openFlow } = useFlowOpener();
    const { recentFlows, recordFlow } = useRecentFlows();
    recordFlow({ path: "/flows/gone.yaml" });

    const result = await openFlow("/flows/gone.yaml");

    expect(result).toBeNull();
    expect(mocks.setFlowId).not.toHaveBeenCalled();
    expect(recentFlows.value).toEqual([]);
    expect(mocks.notify).toHaveBeenCalledOnce();
  });

  it("prunes the recents entry and notifies when the import throws", async () => {
    mocks.importFlow.mockRejectedValue(new Error("boom"));
    const { openFlow } = useFlowOpener();
    const { recentFlows, recordFlow } = useRecentFlows();
    recordFlow({ path: "/flows/broken.yaml" });

    const result = await openFlow("/flows/broken.yaml");

    expect(result).toBeNull();
    expect(recentFlows.value).toEqual([]);
    expect(mocks.notify).toHaveBeenCalledOnce();
  });

  it("reuses an already-open session by catalog id instead of importing", async () => {
    mocks.getAllFlows.mockResolvedValue([
      { flow_id: 1, source_registration_id: 7 },
      { flow_id: 42, source_registration_id: 99 },
    ]);
    const { openFlow } = useFlowOpener();
    const { recentFlows } = useRecentFlows();

    const result = await openFlow("/flows/a.yaml", { name: "A", catalogId: 99 });

    expect(result).toBe(42);
    expect(mocks.setFlowId).toHaveBeenCalledWith(42);
    expect(mocks.importFlow).not.toHaveBeenCalled();
    expect(recentFlows.value[0]).toMatchObject({ path: "/flows/a.yaml", catalogId: 99 });
  });

  it("imports when no open session matches the catalog id", async () => {
    mocks.getAllFlows.mockResolvedValue([{ flow_id: 1, source_registration_id: 7 }]);
    mocks.importFlow.mockResolvedValue(50);
    const { openFlow } = useFlowOpener();

    const result = await openFlow("/flows/b.yaml", { catalogId: 99 });

    expect(result).toBe(50);
    expect(mocks.importFlow).toHaveBeenCalledWith("/flows/b.yaml");
    expect(mocks.setFlowId).toHaveBeenCalledWith(50);
  });

  it("skips sessions with a null source_registration_id", async () => {
    mocks.getAllFlows.mockResolvedValue([{ flow_id: 5, source_registration_id: null }]);
    mocks.importFlow.mockResolvedValue(60);
    const { openFlow } = useFlowOpener();

    const result = await openFlow("/flows/c.yaml", { catalogId: 99 });

    expect(result).toBe(60);
    expect(mocks.importFlow).toHaveBeenCalledWith("/flows/c.yaml");
  });

  it("falls back to import without pruning the recent when getAllFlows throws", async () => {
    mocks.getAllFlows.mockRejectedValue(new Error("listing down"));
    mocks.importFlow.mockResolvedValue(61);
    const { openFlow } = useFlowOpener();
    const { recentFlows, recordFlow } = useRecentFlows();
    recordFlow({ path: "/flows/d.yaml", catalogId: 99 });

    const result = await openFlow("/flows/d.yaml", { catalogId: 99 });

    expect(result).toBe(61);
    expect(mocks.importFlow).toHaveBeenCalledWith("/flows/d.yaml");
    expect(recentFlows.value.some((f) => f.path === "/flows/d.yaml")).toBe(true);
    expect(mocks.notify).not.toHaveBeenCalled();
  });
});
