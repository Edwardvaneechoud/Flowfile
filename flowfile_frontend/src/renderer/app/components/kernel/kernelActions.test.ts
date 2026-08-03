import { beforeEach, describe, expect, it, vi } from "vitest";

import type { KernelInfo } from "@/types";

const mocks = vi.hoisted(() => ({
  stop: vi.fn(),
  update: vi.fn(),
  start: vi.fn(),
}));

vi.mock("@/api/kernel.api", () => ({
  KernelApi: { stop: mocks.stop, update: mocks.update, start: mocks.start },
}));

import { addPackagesToKernel } from "./kernelActions";

function kernel(state: KernelInfo["state"], packages: string[] = []): KernelInfo {
  return {
    id: "k1",
    name: "Kernel",
    state,
    container_id: null,
    port: null,
    packages,
    resolved_packages: [],
    memory_gb: 4,
    cpu_cores: 2,
    gpu: false,
    image_flavour: "base",
    custom_image: null,
    image: null,
    created_at: "2026-01-01T00:00:00Z",
    error_message: null,
    kernel_version: null,
  };
}

beforeEach(() => {
  mocks.stop.mockReset().mockResolvedValue(undefined);
  mocks.update.mockReset().mockImplementation(async () => kernel("stopped"));
  mocks.start.mockReset().mockResolvedValue(kernel("idle"));
});

describe("addPackagesToKernel", () => {
  it("skips stop when the kernel is already stopped", async () => {
    await addPackagesToKernel(kernel("stopped"), ["numpy"]);
    expect(mocks.stop).not.toHaveBeenCalled();
  });

  it("stops a running kernel first", async () => {
    await addPackagesToKernel(kernel("idle"), ["numpy"]);
    expect(mocks.stop).toHaveBeenCalledWith("k1");
  });

  it("updates with the merged, deduped package list then starts", async () => {
    await addPackagesToKernel(kernel("stopped", ["pandas", "numpy"]), ["numpy", "xgboost"]);
    expect(mocks.update).toHaveBeenCalledWith("k1", {
      packages: ["pandas", "numpy", "xgboost"],
    });
    expect(mocks.start).toHaveBeenCalledWith("k1");
  });

  it("reports phases in order", async () => {
    const phases: string[] = [];
    await addPackagesToKernel(kernel("idle"), ["numpy"], (phase) => phases.push(phase));
    expect(phases).toEqual(["stopping", "rebuilding", "starting"]);
  });

  it("skips the stopping phase for a stopped kernel", async () => {
    const phases: string[] = [];
    await addPackagesToKernel(kernel("stopped"), ["numpy"], (phase) => phases.push(phase));
    expect(phases).toEqual(["rebuilding", "starting"]);
  });

  it("propagates update failure without starting (kernel left stopped)", async () => {
    const failure = new Error("bake failed");
    mocks.update.mockRejectedValue(failure);

    await expect(addPackagesToKernel(kernel("idle"), ["numpy"])).rejects.toBe(failure);
    expect(mocks.start).not.toHaveBeenCalled();
  });

  it("returns the started kernel", async () => {
    const started = kernel("idle");
    mocks.start.mockResolvedValue(started);
    await expect(addPackagesToKernel(kernel("stopped"), ["numpy"])).resolves.toBe(started);
  });
});
