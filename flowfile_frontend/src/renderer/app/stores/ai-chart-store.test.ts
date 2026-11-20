// `useAiChartStore`: one request per owner, cancel, supersede, its target and its failures.

import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => {
  class AiDisabledError extends Error {
    constructor() {
      super("AI features are disabled.");
      this.name = "AiDisabledError";
    }
  }
  const aiStore = {
    aiDisabled: false,
    hasConfiguredProvider: true,
    configuredProviders: [{ provider: "openai" }, { provider: "anthropic" }] as {
      provider: string;
    }[],
    resolveSurface: vi.fn(() => ({ provider: "openai", model: "gpt-test" }) as Record<string, any>),
  };
  return { AiDisabledError, generateChart: vi.fn(), aiStore };
});

vi.mock("../api/ai.api", () => ({
  AiDisabledError: mocks.AiDisabledError,
  generateChart: mocks.generateChart,
}));

vi.mock("./ai-store", () => ({ useAiStore: () => mocks.aiStore }));

import { useAiChartStore } from "./ai-chart-store";

const okResponse = {
  chart: { mark: "bar" },
  title: "Revenue",
  explanation: "Revenue per region.",
  warnings: [],
  degraded: false,
  reason: null,
  message: null,
};

/** A generateChart that resolves when told to, and rejects like axios when its signal aborts. */
function pending() {
  let resolve: (value: unknown) => void = () => undefined;
  let signal: AbortSignal | undefined;
  mocks.generateChart.mockImplementationOnce((_body, s?: AbortSignal) => {
    signal = s;
    return new Promise((res, rej) => {
      resolve = res;
      s?.addEventListener("abort", () =>
        rej(Object.assign(new Error("canceled"), { name: "CanceledError" })),
      );
    });
  });
  return { resolve: (value: unknown) => resolve(value), signal: () => signal };
}

beforeEach(() => {
  setActivePinia(createPinia());
  mocks.generateChart.mockReset();
  mocks.aiStore.aiDisabled = false;
  mocks.aiStore.hasConfiguredProvider = true;
  mocks.aiStore.configuredProviders = [{ provider: "openai" }, { provider: "anthropic" }];
  mocks.aiStore.resolveSurface.mockReturnValue({ provider: "openai", model: "gpt-test" });
});

describe("generate", () => {
  it("sends the table, the prompt, the current chart and the chart surface's provider", async () => {
    mocks.generateChart.mockResolvedValueOnce(okResponse);
    const store = useAiChartStore();
    const current = { mark: "bar" as const, x: { field: "region" } };

    const outcome = await store.generate({
      tableId: 7,
      prompt: "make it a line",
      currentChart: current,
    });

    expect(outcome).toEqual({ status: "ok", response: okResponse });
    expect(mocks.aiStore.resolveSurface).toHaveBeenCalledWith("chart");
    expect(mocks.generateChart.mock.calls[0][0]).toEqual({
      tableId: 7,
      prompt: "make it a line",
      currentChart: current,
      provider: "openai",
      model: "gpt-test",
    });
    expect(store.loading).toBe(false);
  });

  it("targets the first configured provider, without the model, when the picked one has no key", async () => {
    mocks.aiStore.resolveSurface.mockReturnValue({ provider: "groq", model: "llama" });
    mocks.generateChart.mockResolvedValueOnce(okResponse);
    const store = useAiChartStore();

    expect(store.target).toEqual({ provider: "openai", model: null });
    await store.generate({ tableId: 1, prompt: "x" });
    expect(mocks.generateChart.mock.calls[0][0]).toMatchObject({ provider: "openai", model: null });
  });

  it("is busy while the request is out", async () => {
    const call = pending();
    const store = useAiChartStore();
    const result = store.generate({ tableId: 1, prompt: "x" });
    expect(store.loading).toBe(true);
    call.resolve(okResponse);
    await result;
    expect(store.loading).toBe(false);
  });

  it("lets a second Generate replace the first, whose answer is never handed back", async () => {
    const first = pending();
    const second = pending();
    const store = useAiChartStore();

    const firstResult = store.generate({ tableId: 1, prompt: "first" });
    const secondResult = store.generate({ tableId: 1, prompt: "second" });

    expect(first.signal()?.aborted).toBe(true);
    expect(await firstResult).toEqual({ status: "superseded" });
    expect(store.loading).toBe(true);
    second.resolve(okResponse);
    expect(await secondResult).toEqual({ status: "ok", response: okResponse });
    expect(store.loading).toBe(false);
  });

  it("keeps each owner's request apart: one bar never stops another's", async () => {
    const first = pending();
    pending();
    const store = useAiChartStore();
    const viewer = Symbol("viewer");
    const dialog = Symbol("dialog");

    const viewerResult = store.generate({ tableId: 1, prompt: "first" }, viewer);
    const dialogResult = store.generate({ tableId: 2, prompt: "second" }, dialog);
    expect(first.signal()?.aborted).toBe(false);

    store.cancel(dialog);
    expect(await dialogResult).toEqual({ status: "cancelled" });
    expect(store.isLoading(dialog)).toBe(false);
    expect(store.isLoading(viewer)).toBe(true);
    expect(first.signal()?.aborted).toBe(false);

    first.resolve(okResponse);
    expect(await viewerResult).toEqual({ status: "ok", response: okResponse });
    expect(store.loading).toBe(false);
  });

  it("drops an answer that lands after the request was replaced", async () => {
    let resolveFirst: (value: unknown) => void = () => undefined;
    mocks.generateChart.mockImplementationOnce(() => new Promise((res) => (resolveFirst = res)));
    mocks.generateChart.mockResolvedValueOnce(okResponse);
    const store = useAiChartStore();

    const firstResult = store.generate({ tableId: 1, prompt: "first" });
    const secondResult = store.generate({ tableId: 1, prompt: "second" });
    resolveFirst(okResponse);

    expect(await firstResult).toEqual({ status: "superseded" });
    expect(await secondResult).toEqual({ status: "ok", response: okResponse });
  });

  it("resolves cancelled when the user cancels", async () => {
    const call = pending();
    const store = useAiChartStore();
    const result = store.generate({ tableId: 1, prompt: "x" });

    store.cancel();

    expect(call.signal()?.aborted).toBe(true);
    expect(store.loading).toBe(false);
    expect(await result).toEqual({ status: "cancelled" });
  });

  it("reports AI switched off", async () => {
    mocks.generateChart.mockRejectedValueOnce(new mocks.AiDisabledError());
    const store = useAiChartStore();
    expect(await store.generate({ tableId: 1, prompt: "x" })).toEqual({ status: "disabled" });
  });

  it("turns a request that got no answer into a message", async () => {
    mocks.generateChart.mockRejectedValueOnce(
      Object.assign(new Error("Network Error"), { code: "ERR_NETWORK" }),
    );
    mocks.generateChart.mockRejectedValueOnce({
      response: { status: 409, data: { detail: "No AI provider configured." } },
    });
    const store = useAiChartStore();

    const network = await store.generate({ tableId: 1, prompt: "x" });
    expect(network).toMatchObject({ status: "failed", message: { tone: "error" } });
    expect(network.status === "failed" && network.message.text).toContain(
      "Could not reach the Flowfile server",
    );

    const noProvider = await store.generate({ tableId: 1, prompt: "x" });
    expect(noProvider.status === "failed" && noProvider.message.text).toBe(
      "No AI provider is set up. Add one under Settings → AI.",
    );
  });
});

describe("available", () => {
  it("is true only with AI on and a provider set up", () => {
    expect(useAiChartStore().available).toBe(true);

    setActivePinia(createPinia());
    mocks.aiStore.hasConfiguredProvider = false;
    expect(useAiChartStore().available).toBe(false);

    setActivePinia(createPinia());
    mocks.aiStore.hasConfiguredProvider = true;
    mocks.aiStore.aiDisabled = true;
    expect(useAiChartStore().available).toBe(false);
  });
});
