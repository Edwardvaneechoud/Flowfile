import { beforeEach, describe, expect, it, vi } from "vitest";

const { getVisualizationMock, getFieldsMock } = vi.hoisted(() => ({
  getVisualizationMock: vi.fn(),
  getFieldsMock: vi.fn(),
}));

vi.mock("../api/catalog.api", () => ({
  CatalogApi: {
    getVisualization: getVisualizationMock,
    getSavedVisualizationFields: getFieldsMock,
  },
}));

import { createDashboardVizCache } from "./useDashboardVizCache";

beforeEach(() => {
  getVisualizationMock.mockReset();
  getFieldsMock.mockReset();
});

describe("createDashboardVizCache", () => {
  it("asks core once per chart, for readers that overlap and readers that come later", async () => {
    getVisualizationMock.mockImplementation(async (id: number) => ({ id, name: `chart ${id}` }));
    getFieldsMock.mockResolvedValue({ fields: [{ fid: "amount" }] });
    const cache = createDashboardVizCache();

    const [a, b] = await Promise.all([cache.getViz(1), cache.getViz(1)]);
    await cache.getViz(1);
    await cache.getViz(2);
    await Promise.all([cache.getFields(1), cache.getFields(1)]);

    expect(a).toBe(b);
    expect(getVisualizationMock.mock.calls.map((c) => c[0])).toEqual([1, 2]);
    expect(getFieldsMock).toHaveBeenCalledTimes(1);
  });

  it("does not keep a request that failed", async () => {
    getVisualizationMock.mockRejectedValueOnce(new Error("down")).mockResolvedValue({ id: 1 });
    const cache = createDashboardVizCache();

    await expect(cache.getViz(1)).rejects.toThrow("down");
    await expect(cache.getViz(1)).resolves.toEqual({ id: 1 });
    expect(getVisualizationMock).toHaveBeenCalledTimes(2);
  });

  it("asks again after a chart, or every chart, is invalidated", async () => {
    getVisualizationMock.mockImplementation(async (id: number) => ({ id }));
    getFieldsMock.mockResolvedValue({ fields: [] });
    const cache = createDashboardVizCache();
    await Promise.all([cache.getViz(1), cache.getViz(2), cache.getFields(1)]);

    cache.invalidate(1);
    await Promise.all([cache.getViz(1), cache.getViz(2), cache.getFields(1)]);
    expect(getVisualizationMock.mock.calls.map((c) => c[0])).toEqual([1, 2, 1]);
    expect(getFieldsMock).toHaveBeenCalledTimes(2);

    cache.invalidate();
    await Promise.all([cache.getViz(1), cache.getViz(2)]);
    expect(getVisualizationMock).toHaveBeenCalledTimes(5);
  });
});
