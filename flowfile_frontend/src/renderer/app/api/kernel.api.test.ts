import { beforeEach, describe, expect, it, vi } from "vitest";
import type { DisplayOutput } from "../types";

const mocks = vi.hoisted(() => ({
  get: vi.fn(),
}));

vi.mock("../services/axios.config", () => ({
  default: { get: mocks.get },
}));

import { KernelApi } from "./kernel.api";

const payload: DisplayOutput = {
  mime_type: "image/png",
  data: "base64data",
  title: "Chart",
};

beforeEach(() => {
  mocks.get.mockReset();
});

describe("KernelApi.getArtifactPreview", () => {
  it("calls the exact URL (no trailing slash) with flow_id + name params", async () => {
    mocks.get.mockResolvedValue({ data: payload });

    await KernelApi.getArtifactPreview("k1", 5, "chart");

    expect(mocks.get).toHaveBeenCalledWith("/kernels/k1/artifact_preview", {
      params: { flow_id: 5, name: "chart" },
    });
  });

  it("returns the payload verbatim", async () => {
    mocks.get.mockResolvedValue({ data: payload });

    const result = await KernelApi.getArtifactPreview("k1", 5, "chart");

    expect(result).toEqual(payload);
  });

  it("returns null when axios rejects", async () => {
    mocks.get.mockRejectedValue(new Error("boom"));

    const result = await KernelApi.getArtifactPreview("k1", 5, "chart");

    expect(result).toBeNull();
  });

  it("returns null when the response data is null", async () => {
    mocks.get.mockResolvedValue({ data: null });

    const result = await KernelApi.getArtifactPreview("k1", 5, "chart");

    expect(result).toBeNull();
  });
});
