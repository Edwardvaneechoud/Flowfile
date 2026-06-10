// Guards the open-flow contract: record in recents on success, prune on
// failure (moved/deleted files must not keep resurfacing on the home screen).

import { describe, it, expect, beforeEach, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  importFlow: vi.fn(),
  setFlowId: vi.fn(),
  notify: vi.fn(),
}));

vi.mock("../api", () => ({ FlowApi: { importFlow: mocks.importFlow } }));
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
});
