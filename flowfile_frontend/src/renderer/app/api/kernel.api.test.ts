import { beforeEach, describe, expect, it, vi } from "vitest";
import type { DisplayOutput } from "../types";

const mocks = vi.hoisted(() => ({
  get: vi.fn(),
  post: vi.fn(),
}));

vi.mock("../services/axios.config", () => ({
  default: { get: mocks.get, post: mocks.post },
}));

import { KernelApi } from "./kernel.api";

const payload: DisplayOutput = {
  mime_type: "image/png",
  data: "base64data",
  title: "Chart",
};

beforeEach(() => {
  mocks.get.mockReset();
  mocks.post.mockReset();
});

describe("KernelApi.matchKernels", () => {
  const response = {
    matches: [],
    suggestion: {
      config: {
        id: "node-kernel",
        name: "Node kernel",
        packages: [],
        cpu_cores: 2,
        memory_gb: 4,
        gpu: false,
        image_flavour: "base",
        custom_image: null,
      },
      covered_by_flavour: [],
      flavour_image_available: true,
    },
    docker_available: true,
  };

  it("posts exactly /kernels/match (no trailing slash) with dependencies + node_name", async () => {
    mocks.post.mockResolvedValue({ data: response });

    await KernelApi.matchKernels(["scikit-learn>=1.5"], "My Node");

    expect(mocks.post).toHaveBeenCalledWith("/kernels/match", {
      dependencies: ["scikit-learn>=1.5"],
      node_name: "My Node",
    });
  });

  it("sends node_name null when omitted and returns the payload verbatim", async () => {
    mocks.post.mockResolvedValue({ data: response });

    const result = await KernelApi.matchKernels(["numpy"]);

    expect(mocks.post).toHaveBeenCalledWith("/kernels/match", {
      dependencies: ["numpy"],
      node_name: null,
    });
    expect(result).toEqual(response);
  });

  it("propagates rejections raw so callers can fall back", async () => {
    const failure = new Error("503");
    mocks.post.mockRejectedValue(failure);

    await expect(KernelApi.matchKernels(["numpy"])).rejects.toBe(failure);
  });
});

describe("KernelApi.matchKernelsBatch", () => {
  it("posts /kernels/match/batch with the items map and returns the payload", async () => {
    const payload = {
      results: { a: { level: "full", best_kernel_id: "k1", best_kernel_name: "K1" } },
      docker_available: true,
    };
    mocks.post.mockResolvedValue({ data: payload });

    const result = await KernelApi.matchKernelsBatch({ a: ["numpy"] });

    expect(mocks.post).toHaveBeenCalledWith("/kernels/match/batch", { items: { a: ["numpy"] } });
    expect(result).toEqual(payload);
  });

  it("propagates rejections raw", async () => {
    const failure = new Error("down");
    mocks.post.mockRejectedValue(failure);

    await expect(KernelApi.matchKernelsBatch({})).rejects.toBe(failure);
  });
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
