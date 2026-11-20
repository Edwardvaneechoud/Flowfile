// Unit tests for `useAiDashboardStore`: the three stages, the models each runs on, what is
// sent with data sharing off, partial failures, the story fallback and cancel.

import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => {
  class AiDisabledError extends Error {
    constructor() {
      super("AI features are disabled.");
      this.name = "AiDisabledError";
    }
  }
  const picks: Record<string, { provider: string; model: string | null }> = {
    dashboard: { provider: "anthropic", model: "planner" },
    chart: { provider: "openai", model: "charter" },
  };
  const aiStore = {
    aiDisabled: false,
    hasConfiguredProvider: true,
    sendsData: true,
    configuredProviders: [{ provider: "openai" }, { provider: "anthropic" }] as {
      provider: string;
    }[],
    resolveSurface: vi.fn((surface: string) => picks[surface]),
  };
  return {
    AiDisabledError,
    aiStore,
    planDashboard: vi.fn(),
    generateChart: vi.fn(),
    writeDashboardStory: vi.fn(),
  };
});

vi.mock("../api/ai.api", () => ({
  AiDisabledError: mocks.AiDisabledError,
  planDashboard: mocks.planDashboard,
  generateChart: mocks.generateChart,
  writeDashboardStory: mocks.writeDashboardStory,
}));

vi.mock("./ai-store", () => ({ useAiStore: () => mocks.aiStore }));
vi.mock("../api/catalog.api", () => ({ CatalogApi: {} }));
vi.mock("../composables/useGraphicWalkerCompute", () => ({ useGraphicWalkerCompute: vi.fn() }));

import type { ChartConverter } from "../views/DashboardsView/aiDashboardCharts";
import { CHART_CONCURRENCY, useAiDashboardStore } from "./ai-dashboard-store";

const check = (n: number, tableId = 1) => ({
  id: `c${n}`,
  table_id: tableId,
  title: `Chart ${n}`,
  brief: `Brief ${n}`,
  purpose: `Purpose ${n}`,
  relation: n === 2 ? { x: "discount", y: "quantity" } : null,
});
const planResponse = (count = 3, warnings: string[] = []) => ({
  plan: {
    title: "Sales",
    kpis: [{ id: "k1", table_id: 1, label: "Revenue", field: "revenue", agg: "sum" }],
    checks: Array.from({ length: count }, (_, i) => check(i + 1)),
    filters: [
      { id: "f1", table_id: 1, field: "order_date", kind: "date_range", label: "Order date" },
    ],
  },
  explanation: null,
  warnings,
  degraded: false,
  reason: null,
  message: null,
  attempts: 1,
});
const chartResponse = (mark = "bar") => ({
  chart: { mark },
  title: "T",
  explanation: null,
  warnings: [],
  degraded: false,
  reason: null,
  message: null,
});
const storyResponse = (extra = {}) => ({
  story: {
    title: "Sales, told",
    summary_md: "Sales are **48,210**.",
    charts: [
      { id: "c3", title: "Third", finding: "It rose.", width: "full" },
      { id: "c1", title: "First", finding: null, width: "half" },
    ],
    left_out: [{ id: "c2", reason: "No relation worth showing." }],
  },
  grounded: true,
  evidence: [
    {
      id: "c2",
      kind: "relation",
      status: "measured",
      detail: "Pearson r = 0.04: no linear relation",
    },
    { id: "c1", kind: "chart", status: "measured", detail: "4 rows read" },
  ],
  warnings: ["story warning"],
  degraded: false,
  reason: null,
  message: null,
  attempts: 1,
  ...extra,
});

const convert: ChartConverter = vi.fn(async (_tableId, request) => ({
  ok: true as const,
  chart: { name: request.mark } as any,
  payload: { workflow: [request.mark] },
}));

const args = () => ({ tableIds: [1, 2], prompt: "How are sales?", convert });

beforeEach(() => {
  setActivePinia(createPinia());
  vi.clearAllMocks();
  mocks.aiStore.sendsData = true;
  mocks.aiStore.aiDisabled = false;
  mocks.aiStore.hasConfiguredProvider = true;
  mocks.aiStore.configuredProviders = [{ provider: "openai" }, { provider: "anthropic" }];
  mocks.planDashboard.mockResolvedValue(planResponse());
  mocks.generateChart.mockResolvedValue(chartResponse());
  mocks.writeDashboardStory.mockResolvedValue(storyResponse());
});

describe("a build", () => {
  it("plans on the dashboard model, builds each chart on the chart model, and writes the story", async () => {
    const store = useAiDashboardStore();
    const outcome = await store.build(args());

    expect(mocks.planDashboard.mock.calls[0][0]).toEqual({
      tableIds: [1, 2],
      prompt: "How are sales?",
      sendValues: true,
      provider: "anthropic",
      model: "planner",
    });
    expect(mocks.generateChart.mock.calls.map(([body]) => body)).toEqual(
      [1, 2, 3].map((n) => ({
        tableId: 1,
        prompt: `Brief ${n}`,
        sendValues: true,
        provider: "openai",
        model: "charter",
      })),
    );
    const story = mocks.writeDashboardStory.mock.calls[0][0];
    expect(story).toMatchObject({
      tableIds: [1, 2],
      prompt: "How are sales?",
      title: "Sales",
      failed: [],
      sendValues: true,
      provider: "anthropic",
      model: "planner",
    });
    expect(story.kpis).toEqual(planResponse().plan.kpis);
    expect(story.charts[1]).toEqual({
      id: "c2",
      table_id: 1,
      title: "Chart 2",
      purpose: "Purpose 2",
      chart: { mark: "bar" },
      payload: { workflow: ["bar"] },
      relation: { x: "discount", y: "quantity" },
    });

    expect(outcome.status).toBe("ok");
    if (outcome.status !== "ok") return;
    expect(outcome.page).toMatchObject({
      title: "Sales, told",
      summaryMd: "Sales are **48,210**.",
      grounded: true,
      filters: planResponse().plan.filters,
      leftOut: [{ title: "Chart 2", reason: "No relation worth showing." }],
      failed: [],
      relations: [{ title: "Chart 2", reason: "Pearson r = 0.04: no linear relation" }],
      warnings: ["story warning"],
    });
    expect(outcome.page.charts.map((c) => [c.id, c.title, c.finding, c.width, c.tableId])).toEqual([
      ["c3", "Third", "It rose.", "full", 1],
      ["c1", "First", null, "half", 1],
    ]);
    expect(store.stage).toBe("idle");
    expect(store.checks.map((c) => c.status)).toEqual(["built", "built", "built"]);
  });

  it("carries no filters from a core that plans none", async () => {
    const older = planResponse();
    delete (older.plan as { filters?: unknown }).filters;
    mocks.planDashboard.mockResolvedValue(older);
    const outcome = await useAiDashboardStore().build(args());
    expect(outcome.status === "ok" && outcome.page.filters).toEqual([]);
  });

  it("sends no chart queries when data values are not shared", async () => {
    mocks.aiStore.sendsData = false;
    await useAiDashboardStore().build(args());
    expect(mocks.planDashboard.mock.calls[0][0].sendValues).toBe(false);
    expect(mocks.generateChart.mock.calls[0][0].sendValues).toBe(false);
    const story = mocks.writeDashboardStory.mock.calls[0][0];
    expect(story.sendValues).toBe(false);
    expect(story.charts.map((c: { payload: unknown }) => c.payload)).toEqual([null, null, null]);
  });

  it("falls back to the first configured provider without a model when a pick is not configured", async () => {
    mocks.aiStore.configuredProviders = [{ provider: "openai" }];
    const store = useAiDashboardStore();
    expect(store.dashboardTarget).toEqual({ provider: "openai", model: null });
    expect(store.chartTarget).toEqual({ provider: "openai", model: "charter" });
  });

  it("builds at most a few charts at a time", async () => {
    mocks.planDashboard.mockResolvedValue(planResponse(7));
    let running = 0;
    let peak = 0;
    mocks.generateChart.mockImplementation(async () => {
      running += 1;
      peak = Math.max(peak, running);
      await new Promise((resolve) => setTimeout(resolve, 5));
      running -= 1;
      return chartResponse();
    });
    mocks.writeDashboardStory.mockResolvedValue(storyResponse({ story: null, degraded: true }));
    const outcome = await useAiDashboardStore().build(args());
    expect(peak).toBe(CHART_CONCURRENCY);
    expect(outcome.status === "ok" && outcome.page.charts).toHaveLength(7);
  });

  it("reports the stage while it runs", async () => {
    const store = useAiDashboardStore();
    const seen: string[] = [];
    mocks.planDashboard.mockImplementation(async () => {
      seen.push(store.stage);
      return planResponse(1);
    });
    mocks.generateChart.mockImplementation(async () => {
      seen.push(`${store.stage}:${store.checks[0].status}`);
      return chartResponse();
    });
    mocks.writeDashboardStory.mockImplementation(async () => {
      seen.push(store.stage);
      return storyResponse();
    });
    expect(store.busy).toBe(false);
    await store.build(args());
    expect(seen).toEqual(["planning", "building:building", "writing"]);
  });
});

describe("partial failures", () => {
  it("keeps the charts that were built and names the ones that were not", async () => {
    mocks.generateChart
      .mockResolvedValueOnce(chartResponse())
      .mockResolvedValueOnce({ ...chartResponse(), chart: null, degraded: true, reason: "timeout" })
      .mockRejectedValueOnce({ response: { status: 500 } });
    mocks.writeDashboardStory.mockResolvedValue(
      storyResponse({
        story: {
          title: "Sales",
          summary_md: "S",
          charts: [{ id: "c1", title: "First", finding: "F", width: "half" }],
          left_out: [],
        },
      }),
    );
    const store = useAiDashboardStore();
    const outcome = await store.build(args());
    expect(store.checks.map((c) => c.status)).toEqual(["built", "failed", "failed"]);
    const story = mocks.writeDashboardStory.mock.calls[0][0];
    expect(story.charts.map((c: { id: string }) => c.id)).toEqual(["c1"]);
    expect(story.failed.map((f: { id: string }) => f.id)).toEqual(["c2", "c3"]);
    expect(outcome.status === "ok" && outcome.page.failed.map((f) => f.title)).toEqual([
      "Chart 2",
      "Chart 3",
    ]);
    expect(outcome.status === "ok" && outcome.page.failed[0].reason).toContain("took too long");
  });

  it("fails a chart the browser cannot draw", async () => {
    const refusing: ChartConverter = vi.fn(async () => ({ ok: false as const, error: "no" }));
    const outcome = await useAiDashboardStore().build({ ...args(), convert: refusing });
    expect(outcome).toMatchObject({
      status: "failed",
      message: { detail: "The AI's chart could not be drawn here." },
    });
    expect(mocks.writeDashboardStory).not.toHaveBeenCalled();
  });

  it("places the page in the planned order when the story is not written", async () => {
    mocks.writeDashboardStory.mockResolvedValue(
      storyResponse({ story: null, degraded: true, reason: "timeout", warnings: [] }),
    );
    const outcome = await useAiDashboardStore().build(args());
    expect(outcome.status).toBe("ok");
    if (outcome.status !== "ok") return;
    expect(outcome.page.charts.map((c) => [c.id, c.title, c.finding, c.width])).toEqual([
      ["c1", "Chart 1", null, "half"],
      ["c2", "Chart 2", null, "half"],
      ["c3", "Chart 3", null, "half"],
    ]);
    expect(outcome.page).toMatchObject({
      title: "Sales",
      summaryMd: "Charts for the question: How are sales?",
      grounded: false,
    });
    expect(outcome.page.warnings[0]).toContain("the AI took too long");
  });

  it("still places the page when the story request itself fails", async () => {
    mocks.planDashboard.mockResolvedValue(planResponse(1, ["plan warning"]));
    mocks.writeDashboardStory.mockRejectedValue(new Error("network"));
    const outcome = await useAiDashboardStore().build(args());
    expect(outcome.status === "ok" && outcome.page.warnings).toEqual([
      "plan warning",
      expect.stringContaining("the AI's answer could not be used"),
    ]);
  });
});

describe("a build that does not finish", () => {
  it("fails with the plan's own message and builds nothing", async () => {
    mocks.planDashboard.mockResolvedValue({
      ...planResponse(),
      plan: null,
      degraded: true,
      reason: "no_plan",
      explanation: "The tables hold sales.",
    });
    const outcome = await useAiDashboardStore().build(args());
    expect(outcome).toEqual({
      status: "failed",
      message: { tone: "warning", text: "The AI did not plan a page: The tables hold sales." },
    });
    expect(mocks.generateChart).not.toHaveBeenCalled();
  });

  it("words a refused plan request", async () => {
    mocks.planDashboard.mockRejectedValue({ response: { status: 403 } });
    const outcome = await useAiDashboardStore().build(args());
    expect(outcome.status === "failed" && outcome.message.text).toContain("may not read");
  });

  it("says disabled when AI is turned off at any stage", async () => {
    mocks.planDashboard.mockRejectedValueOnce(new mocks.AiDisabledError());
    expect((await useAiDashboardStore().build(args())).status).toBe("disabled");
    mocks.generateChart.mockRejectedValue(new mocks.AiDisabledError());
    expect((await useAiDashboardStore().build(args())).status).toBe("disabled");
  });

  it("cancel stops the charts in flight and resolves cancelled", async () => {
    const signals: AbortSignal[] = [];
    mocks.generateChart.mockImplementation(
      (_body, signal: AbortSignal) =>
        new Promise((_resolve, reject) => {
          signals.push(signal);
          signal.addEventListener("abort", () =>
            reject(Object.assign(new Error("canceled"), { name: "CanceledError" })),
          );
        }),
    );
    const store = useAiDashboardStore();
    const running = store.build(args());
    await vi.waitFor(() => expect(signals).toHaveLength(CHART_CONCURRENCY));
    store.cancel();
    expect(await running).toEqual({ status: "cancelled" });
    expect(signals.every((s) => s.aborted)).toBe(true);
    expect(store.stage).toBe("idle");
    expect(mocks.writeDashboardStory).not.toHaveBeenCalled();
  });

  it("a second build cancels the first", async () => {
    let release: (value: unknown) => void = () => undefined;
    mocks.planDashboard.mockImplementationOnce(
      (_body, signal: AbortSignal) =>
        new Promise((resolve, reject) => {
          release = resolve;
          signal.addEventListener("abort", () => reject(new Error("canceled")));
        }),
    );
    const store = useAiDashboardStore();
    const first = store.build(args());
    const second = store.build(args());
    release(planResponse());
    expect(await first).toEqual({ status: "cancelled" });
    expect((await second).status).toBe("ok");
    expect(store.stage).toBe("idle");
  });

  it("is available only with AI on and a provider set up", () => {
    expect(useAiDashboardStore().available).toBe(true);
    mocks.aiStore.hasConfiguredProvider = false;
    setActivePinia(createPinia());
    expect(useAiDashboardStore().available).toBe(false);
    mocks.aiStore.hasConfiguredProvider = true;
    mocks.aiStore.aiDisabled = true;
    setActivePinia(createPinia());
    expect(useAiDashboardStore().available).toBe(false);
  });
});
