import { describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ get: vi.fn() }));
vi.mock("../services/axios.config", () => ({ default: { get: mocks.get } }));

import { FlowApi } from "./flow.api";

describe("canvas placement previews", () => {
  it("can explicitly check workers for a cold preview", async () => {
    mocks.get.mockResolvedValueOnce({ data: { unchecked_backend_ids: [] } });
    await FlowApi.getPlacement(83, true);
    expect(mocks.get).toHaveBeenLastCalledWith("/flow/placement", {
      params: { flow_id: 83, probe: true },
      headers: { accept: "application/json" },
    });
  });

  it("requests cached verdicts without probing workers and preserves cache metadata", async () => {
    const preview = {
      available: true,
      assignment: { 1: "remote" },
      cached_node_ids: [1],
      fallbacks: [],
      refusals: [],
      boundaries: [],
    };
    mocks.get.mockResolvedValueOnce({ data: preview });

    expect(await FlowApi.getPlacement(83)).toEqual(preview);
    expect(mocks.get).toHaveBeenCalledWith("/flow/placement", {
      params: { flow_id: 83, probe: false },
      headers: { accept: "application/json" },
    });
  });
});
