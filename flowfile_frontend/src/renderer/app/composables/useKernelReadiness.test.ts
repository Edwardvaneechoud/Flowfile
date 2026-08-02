import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { KernelMatchBatchResponse, KernelMatchBatchSummary } from "@/types";

const mocks = vi.hoisted(() => ({
  matchKernelsBatch: vi.fn(),
}));

vi.mock("@/api/kernel.api", () => ({
  KernelApi: { matchKernelsBatch: mocks.matchKernelsBatch },
}));

import { invalidateReadiness, readinessKey, useKernelReadiness } from "./useKernelReadiness";

function summary(level: KernelMatchBatchSummary["level"]): KernelMatchBatchSummary {
  return {
    level,
    best_kernel_id: level === "none" ? null : "k1",
    best_kernel_name: level === "none" ? null : "Kernel One",
  };
}

function response(results: Record<string, KernelMatchBatchSummary>): KernelMatchBatchResponse {
  return { results, docker_available: true };
}

beforeEach(() => {
  invalidateReadiness();
  mocks.matchKernelsBatch.mockReset();
});

afterEach(() => {
  vi.useRealTimers();
});

describe("readinessKey", () => {
  it("is stable across dependency order", () => {
    expect(readinessKey(["pandas", "numpy"])).toBe(readinessKey(["numpy", "pandas"]));
  });

  it("does not mutate its input", () => {
    const deps = ["pandas", "numpy"];
    readinessKey(deps);
    expect(deps).toEqual(["pandas", "numpy"]);
  });

  it("differs for different dependency sets", () => {
    expect(readinessKey(["numpy"])).not.toBe(readinessKey(["numpy", "pandas"]));
  });
});

describe("ensureReadiness", () => {
  it("fetches all requested keys in one batch call and merges the results", async () => {
    const keyA = readinessKey(["numpy"]);
    const keyB = readinessKey(["xgboost"]);
    mocks.matchKernelsBatch.mockResolvedValue(
      response({ [keyA]: summary("full"), [keyB]: summary("none") }),
    );

    const { readiness, unavailable, ensureReadiness } = useKernelReadiness();
    await ensureReadiness({ [keyA]: ["numpy"], [keyB]: ["xgboost"] });

    expect(mocks.matchKernelsBatch).toHaveBeenCalledTimes(1);
    expect(mocks.matchKernelsBatch).toHaveBeenCalledWith({
      [keyA]: ["numpy"],
      [keyB]: ["xgboost"],
    });
    expect(readiness.value[keyA].level).toBe("full");
    expect(readiness.value[keyB].level).toBe("none");
    expect(unavailable.value).toBe(false);
  });

  it("skips keys already cached and requests only the new ones", async () => {
    const keyA = readinessKey(["numpy"]);
    const keyB = readinessKey(["xgboost"]);
    mocks.matchKernelsBatch.mockResolvedValue(response({ [keyA]: summary("full") }));

    const { readiness, ensureReadiness } = useKernelReadiness();
    await ensureReadiness({ [keyA]: ["numpy"] });

    mocks.matchKernelsBatch.mockResolvedValue(response({ [keyB]: summary("partial") }));
    await ensureReadiness({ [keyA]: ["numpy"], [keyB]: ["xgboost"] });

    expect(mocks.matchKernelsBatch).toHaveBeenCalledTimes(2);
    expect(mocks.matchKernelsBatch).toHaveBeenLastCalledWith({ [keyB]: ["xgboost"] });
    expect(readiness.value[keyA].level).toBe("full");
    expect(readiness.value[keyB].level).toBe("partial");
  });

  it("does not call the API when every key is cached", async () => {
    const key = readinessKey(["numpy"]);
    mocks.matchKernelsBatch.mockResolvedValue(response({ [key]: summary("full") }));

    const { ensureReadiness } = useKernelReadiness();
    await ensureReadiness({ [key]: ["numpy"] });
    await ensureReadiness({ [key]: ["numpy"] });

    expect(mocks.matchKernelsBatch).toHaveBeenCalledTimes(1);
  });

  it("marks readiness unavailable on failure without throwing", async () => {
    mocks.matchKernelsBatch.mockRejectedValue(new Error("boom"));

    const { readiness, unavailable, ensureReadiness } = useKernelReadiness();
    await expect(ensureReadiness({ k: ["numpy"] })).resolves.toBeUndefined();

    expect(unavailable.value).toBe(true);
    expect(readiness.value).toEqual({});
  });

  it("stays silent after a failure until the TTL expires", async () => {
    vi.useFakeTimers();
    mocks.matchKernelsBatch.mockRejectedValue(new Error("boom"));

    const { unavailable, ensureReadiness } = useKernelReadiness();
    await ensureReadiness({ k: ["numpy"] });
    await ensureReadiness({ k: ["numpy"] });
    expect(mocks.matchKernelsBatch).toHaveBeenCalledTimes(1);

    vi.advanceTimersByTime(31_000);
    mocks.matchKernelsBatch.mockResolvedValue(response({ k: summary("full") }));
    await ensureReadiness({ k: ["numpy"] });

    expect(mocks.matchKernelsBatch).toHaveBeenCalledTimes(2);
    expect(unavailable.value).toBe(false);
  });

  it("refetches cached keys after the TTL expires", async () => {
    vi.useFakeTimers();
    const key = readinessKey(["numpy"]);
    mocks.matchKernelsBatch.mockResolvedValue(response({ [key]: summary("partial") }));

    const { readiness, ensureReadiness } = useKernelReadiness();
    await ensureReadiness({ [key]: ["numpy"] });

    vi.advanceTimersByTime(31_000);
    mocks.matchKernelsBatch.mockResolvedValue(response({ [key]: summary("full") }));
    await ensureReadiness({ [key]: ["numpy"] });

    expect(mocks.matchKernelsBatch).toHaveBeenCalledTimes(2);
    expect(readiness.value[key].level).toBe("full");
  });

  it("invalidateReadiness clears the cache and the unavailable flag", async () => {
    const key = readinessKey(["numpy"]);
    mocks.matchKernelsBatch.mockResolvedValue(response({ [key]: summary("full") }));

    const { readiness, unavailable, ensureReadiness } = useKernelReadiness();
    await ensureReadiness({ [key]: ["numpy"] });
    expect(readiness.value[key]).toBeDefined();

    invalidateReadiness();
    expect(readiness.value).toEqual({});
    expect(unavailable.value).toBe(false);

    await ensureReadiness({ [key]: ["numpy"] });
    expect(mocks.matchKernelsBatch).toHaveBeenCalledTimes(2);
  });
});
