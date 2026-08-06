import { beforeEach, describe, expect, it, vi } from "vitest";

import type { KernelConfig, KernelInfo } from "@/types";

const mocks = vi.hoisted(() => ({
  create: vi.fn(),
  start: vi.fn(),
  notify: vi.fn(),
}));

vi.mock("@/api/kernel.api", () => ({
  KernelApi: { create: mocks.create, start: mocks.start },
}));

vi.mock("element-plus", () => ({
  ElNotification: mocks.notify,
}));

import { _resetKernelCreationTracker, useKernelCreationTracker } from "./useKernelCreationTracker";

function config(overrides: Partial<KernelConfig> = {}): KernelConfig {
  return {
    id: "k1",
    name: "Kernel One",
    packages: ["xgboost"],
    cpu_cores: 2,
    memory_gb: 4,
    gpu: false,
    image_flavour: "base",
    custom_image: null,
    ...overrides,
  };
}

function info(overrides: Partial<KernelInfo> = {}): KernelInfo {
  return {
    id: "k1",
    name: "Kernel One",
    state: "stopped",
    container_id: null,
    port: null,
    packages: ["xgboost"],
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
    ...overrides,
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

beforeEach(() => {
  _resetKernelCreationTracker();
  mocks.create.mockReset();
  mocks.start.mockReset();
  mocks.notify.mockReset();
});

describe("createKernel", () => {
  it("registers a pending entry during create and removes it on success", async () => {
    const gate = deferred<KernelInfo>();
    mocks.create.mockReturnValue(gate.promise);

    const { pendingCreations, createKernel, isPending } = useKernelCreationTracker();
    const call = createKernel(config());

    expect(isPending("k1")).toBe(true);
    expect(pendingCreations.value).toHaveLength(1);
    expect(pendingCreations.value[0]).toMatchObject({
      id: "k1",
      name: "Kernel One",
      flavour: "base",
      packages: ["xgboost"],
      phase: "creating",
    });

    gate.resolve(info());
    await expect(call).resolves.toMatchObject({ id: "k1" });
    expect(pendingCreations.value).toHaveLength(0);
    expect(mocks.notify).toHaveBeenCalledWith(expect.objectContaining({ type: "success" }));
  });

  it("flips the phase to starting with autoStart and resolves with the started kernel", async () => {
    mocks.create.mockResolvedValue(info());
    const gate = deferred<KernelInfo>();
    mocks.start.mockReturnValue(gate.promise);

    const { pendingCreations, createKernel } = useKernelCreationTracker();
    const call = createKernel(config(), { autoStart: true });

    await vi.waitFor(() => expect(pendingCreations.value[0]?.phase).toBe("starting"));

    gate.resolve(info({ state: "idle" }));
    await expect(call).resolves.toMatchObject({ state: "idle" });
    expect(mocks.start).toHaveBeenCalledWith("k1");
    expect(pendingCreations.value).toHaveLength(0);
  });

  it("removes the entry, notifies, and rethrows when create fails", async () => {
    mocks.create.mockRejectedValue(new Error("build exploded"));

    const { pendingCreations, createKernel } = useKernelCreationTracker();
    await expect(createKernel(config())).rejects.toThrow("build exploded");

    expect(pendingCreations.value).toHaveLength(0);
    expect(mocks.notify).toHaveBeenCalledWith(
      expect.objectContaining({ type: "error", message: expect.stringContaining("build exploded") }),
    );
  });

  it("resolves with the created kernel and warns when only the start fails", async () => {
    mocks.create.mockResolvedValue(info());
    mocks.start.mockRejectedValue(new Error("no docker"));

    const { pendingCreations, createKernel } = useKernelCreationTracker();
    await expect(createKernel(config(), { autoStart: true })).resolves.toMatchObject({
      id: "k1",
      state: "stopped",
    });

    expect(pendingCreations.value).toHaveLength(0);
    expect(mocks.notify).toHaveBeenCalledWith(expect.objectContaining({ type: "warning" }));
    expect(mocks.notify).not.toHaveBeenCalledWith(expect.objectContaining({ type: "success" }));
  });

  it("rejects a duplicate create for a pending id without a second POST", async () => {
    const gate = deferred<KernelInfo>();
    mocks.create.mockReturnValue(gate.promise);

    const { createKernel } = useKernelCreationTracker();
    const first = createKernel(config());

    await expect(createKernel(config())).rejects.toThrow("already being created");
    expect(mocks.create).toHaveBeenCalledTimes(1);
    expect(mocks.notify).toHaveBeenCalledWith(expect.objectContaining({ type: "warning" }));

    gate.resolve(info());
    await first;
  });
});
