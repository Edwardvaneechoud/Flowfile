// @vitest-environment happy-dom
// The AI dashboard builder's dialog: when Build is offered, what a run shows, and what it
// says once the page is placed or when it could not be.

import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { element, mountIn, settle, text } from "../../__fixtures__/mount";
import { deferred } from "../../__fixtures__/promises";

const mocks = vi.hoisted(() => ({
  planDashboard: vi.fn(),
  generateChart: vi.fn(),
  writeDashboardStory: vi.fn(),
  loadProviders: vi.fn(async () => undefined),
  loadAllTables: vi.fn(async () => undefined),
  ai: { aiDisabled: false, hasConfiguredProvider: true, sendsData: true },
  convert: vi.fn(),
  tables: [] as Record<string, unknown>[],
}));

vi.mock("../../api/ai.api", () => ({
  AiDisabledError: class AiDisabledError extends Error {},
  planDashboard: mocks.planDashboard,
  generateChart: mocks.generateChart,
  writeDashboardStory: mocks.writeDashboardStory,
}));
vi.mock("../../api/catalog.api", () => ({ CatalogApi: {} }));
vi.mock("../../composables/useGraphicWalkerCompute", () => ({ useGraphicWalkerCompute: vi.fn() }));
vi.mock("./aiDashboardCharts", () => ({ createChartConverter: () => mocks.convert }));
vi.mock("../../stores/ai-store", () => ({
  useAiStore: () => ({
    get aiDisabled() {
      return mocks.ai.aiDisabled;
    },
    get hasConfiguredProvider() {
      return mocks.ai.hasConfiguredProvider;
    },
    get sendsData() {
      return mocks.ai.sendsData;
    },
    loadProviders: mocks.loadProviders,
    configuredProviders: [{ provider: "openai" }],
    resolveSurface: (surface: string) => ({
      provider: "openai",
      model: surface === "dashboard" ? "planner" : null,
    }),
  }),
}));
vi.mock("../../stores/catalog-store", () => ({
  useCatalogStore: () => ({
    get allTables() {
      return mocks.tables;
    },
    loadAllTables: mocks.loadAllTables,
  }),
}));

import DashboardAiBuilder, { type PlacedPage } from "./DashboardAiBuilder.vue";
import type { BuiltPage } from "./aiDashboard";
import { useAiDashboardStore } from "../../stores/ai-dashboard-store";

const STUBS = {
  "el-dialog": element("div"),
  "el-select": element("div"),
  "el-option-group": element("div"),
  "el-option": element("div"),
  "el-button": element("button"),
};

const plan = () => ({
  plan: {
    title: "Sales",
    kpis: [{ id: "k1", table_id: 1, label: "Revenue", field: "revenue", agg: "sum" }],
    checks: [
      { id: "c1", table_id: 1, title: "By region", brief: "b1", purpose: "p1", relation: null },
      { id: "c2", table_id: 1, title: "By month", brief: "b2", purpose: "p2", relation: null },
    ],
  },
  explanation: null,
  warnings: [],
  degraded: false,
  reason: null,
  message: null,
  attempts: 1,
});
const chart = () => ({
  chart: { mark: "bar" },
  title: null,
  explanation: null,
  warnings: [],
  degraded: false,
  reason: null,
  message: null,
});
const story = () => ({
  story: {
    title: "Sales, told",
    summary_md: "Sales rose.",
    charts: [{ id: "c1", title: "By region", finding: "West leads.", width: "half" }],
    left_out: [{ id: "c2", reason: "It repeats the first chart." }],
  },
  grounded: true,
  evidence: [{ id: "c1", kind: "relation", status: "measured", detail: "Pearson r = 0.04" }],
  warnings: [],
  degraded: false,
  reason: null,
  message: null,
  attempts: 1,
});

const mountBuilder = async (props: Record<string, unknown> = {}) => {
  const pinia = createPinia();
  setActivePinia(pinia);
  const place = vi.fn<(page: BuiltPage) => Promise<PlacedPage>>(async () => ({
    charts: 1,
    kpis: 1,
    filters: ["Order date"],
    failed: [],
  }));
  const host = await mountIn(DashboardAiBuilder, {
    pinia,
    stubs: STUBS,
    props: { modelValue: true, place, initialTableIds: [1], ...props },
  });
  const find = (id: string) => host.querySelector<HTMLElement>(`[data-testid="${id}"]`);
  const ask = async (question: string) => {
    const box = find("dashboard-ai-question") as HTMLTextAreaElement;
    box.value = question;
    box.dispatchEvent(new Event("input"));
    await settle();
  };
  const click = async (id: string) => {
    find(id)?.click();
    await settle();
  };
  return { host, find, ask, click, place: (props.place as typeof place) ?? place };
};

beforeEach(() => {
  vi.clearAllMocks();
  mocks.ai = { aiDisabled: false, hasConfiguredProvider: true, sendsData: true };
  mocks.tables = [
    { id: 1, name: "orders", full_table_name: "sales.orders", table_type: "physical" },
    { id: 2, name: "top_customers", table_type: "virtual", sql_query: "SELECT 1" },
    { id: 3, name: "remote", table_type: "physical", source: "hub" },
  ];
  mocks.planDashboard.mockResolvedValue(plan());
  mocks.generateChart.mockResolvedValue(chart());
  mocks.writeDashboardStory.mockResolvedValue(story());
  mocks.convert.mockResolvedValue({ ok: true, chart: {}, payload: { workflow: [] } });
});

describe("the form", () => {
  it("lists tables and saved queries apart, without hub tables", async () => {
    const { find } = await mountBuilder();
    const options = text(find("dashboard-ai-sources"));
    expect(options).toContain("orders");
    expect(options).toContain("sales.orders");
    expect(options).toContain("top_customers");
    expect(options).not.toContain("remote");
    expect(mocks.loadProviders).toHaveBeenCalled();
  });

  it("offers Build only with a table and a question", async () => {
    const { find, ask } = await mountBuilder();
    const build = () => find("dashboard-ai-build") as HTMLButtonElement;
    expect(build().disabled).toBe(true);
    await ask("How are sales?");
    expect(build().disabled).toBe(false);
    await ask("   ");
    expect(build().disabled).toBe(true);
    const without = await mountBuilder({ initialTableIds: [] });
    await without.ask("How are sales?");
    expect((without.find("dashboard-ai-build") as HTMLButtonElement).disabled).toBe(true);
  });

  it("says what the AI sees and which models run it", async () => {
    const { find } = await mountBuilder();
    expect(text(find("dashboard-ai-data-line"))).toContain("some of the values");
    expect(text(find("dashboard-ai-data-line"))).toContain("reads what each chart returns");
    expect(text(find("dashboard-ai-models"))).toBe(
      "Page planned and written by openai (planner); charts built by openai. Change them under Settings → AI.",
    );
    mocks.ai.sendsData = false;
    const names = await mountBuilder();
    expect(text(names.find("dashboard-ai-data-line"))).toContain("column names and types only");
    expect(text(names.find("dashboard-ai-data-line"))).toContain("without stating what they show");
  });

  it("loads the tables when it has none", async () => {
    mocks.tables = [];
    await mountBuilder();
    expect(mocks.loadAllTables).toHaveBeenCalled();
  });
});

describe("a build", () => {
  it("shows each stage and each chart while it runs, then what was placed", async () => {
    const first = deferred<ReturnType<typeof chart>>();
    mocks.generateChart.mockReturnValueOnce(first.promise);
    const { find, ask, click, place } = await mountBuilder();
    await ask("How are sales?");
    await click("dashboard-ai-build");

    const progress = find("dashboard-ai-progress") as HTMLElement;
    expect(text(progress)).toContain("Building the charts (1 of 2)");
    expect(
      Array.from(progress.querySelectorAll("[data-status]")).map((el) =>
        el.getAttribute("data-status"),
      ),
    ).toEqual(["building", "built"]);
    expect(
      Array.from(progress.querySelectorAll("[data-state]")).map((el) =>
        el.getAttribute("data-state"),
      ),
    ).toEqual(["done", "active", "todo", "todo"]);
    expect(find("dashboard-ai-cancel")).not.toBeNull();

    first.resolve(chart());
    await settle();
    expect(place).toHaveBeenCalledTimes(1);
    expect(place.mock.calls[0][0]).toMatchObject({ title: "Sales, told", grounded: true });
    expect(mocks.planDashboard.mock.calls[0][0]).toMatchObject({
      tableIds: [1],
      prompt: "How are sales?",
    });

    const result = find("dashboard-ai-result");
    expect(text(result)).toContain("“Sales, told” was added with 1 chart, 1 KPI and 1 filter.");
    expect(text(find("dashboard-ai-filters"))).toBe(
      "Filter in the strip above the page: Order date.",
    );
    expect(text(find("dashboard-ai-relations"))).toContain("By region: Pearson r = 0.04");
    expect(text(find("dashboard-ai-left-out"))).toContain("By month: It repeats the first chart.");
    expect(find("dashboard-ai-failed")).toBeNull();
    expect(find("dashboard-ai-question")).toBeNull();
  });

  it("lists the charts that were not built or not saved", async () => {
    mocks.generateChart
      .mockResolvedValueOnce(chart())
      .mockResolvedValueOnce({ ...chart(), chart: null, degraded: true, reason: "timeout" });
    const place = vi.fn(async () => ({
      charts: 0,
      kpis: 0,
      filters: [],
      failed: [{ title: "By region", reason: "This schema is read-only." }],
    }));
    const { find, ask, click } = await mountBuilder({ place });
    await ask("How are sales?");
    await click("dashboard-ai-build");
    const failed = text(find("dashboard-ai-failed"));
    expect(failed).toContain("By month: The AI took too long");
    expect(failed).toContain("By region: This schema is read-only.");
    expect(text(find("dashboard-ai-result"))).toContain("was added with 0 charts.");
    expect(find("dashboard-ai-filters")).toBeNull();
  });

  it("shows the build log with every step, a failed chart's reason and the placing", async () => {
    mocks.generateChart.mockResolvedValueOnce(chart()).mockResolvedValueOnce({
      ...chart(),
      chart: null,
      degraded: true,
      reason: "no_chart",
      explanation: "The chart options do not support geographic maps.",
    });
    const { host, find, ask, click } = await mountBuilder();
    await ask("How are sales?");
    await click("dashboard-ai-build");
    const log = find("dashboard-ai-log");
    expect(log).not.toBeNull();
    const entries = Array.from(
      host.querySelectorAll<HTMLElement>('[data-testid="dashboard-ai-log-entry"]'),
    );
    expect(entries.map((e) => e.dataset.status)).toEqual(["ok", "ok", "failed", "ok", "ok"]);
    expect(text(entries[2])).toContain("By month");
    expect(text(entries[2])).toContain("Askedb2");
    expect(text(entries[2])).toContain("The chart options do not support geographic maps.");
    expect(text(entries[2])).toContain("no_chart");
    expect(text(entries[4])).toContain("Add to dashboard");
    expect(text(log)).toContain("FLOWFILE_AI_LOG_PROMPTS=true");
  });

  it("opens without the log of a build made before it opened", async () => {
    const pinia = createPinia();
    setActivePinia(pinia);
    useAiDashboardStore().beginPlacing("Another dashboard's page");
    expect(useAiDashboardStore().log).toHaveLength(1);
    const host = await mountIn(DashboardAiBuilder, {
      pinia,
      stubs: STUBS,
      props: { modelValue: true, place: vi.fn(), initialTableIds: [1] },
    });
    expect(host.querySelector('[data-testid="dashboard-ai-log"]')).toBeNull();
    expect(useAiDashboardStore().log).toEqual([]);
  });

  it("keeps the build log in view when the plan fails", async () => {
    mocks.planDashboard.mockResolvedValue({
      ...plan(),
      plan: null,
      degraded: true,
      reason: "timeout",
    });
    const { host, find, ask, click } = await mountBuilder();
    await ask("How are sales?");
    await click("dashboard-ai-build");
    expect(find("dashboard-ai-result")).toBeNull();
    const entries = Array.from(
      host.querySelectorAll<HTMLElement>('[data-testid="dashboard-ai-log-entry"]'),
    );
    expect(entries.map((e) => e.dataset.status)).toEqual(["failed"]);
    expect(text(entries[0])).toContain("timeout");
  });

  it("says when the text was written without the data", async () => {
    mocks.writeDashboardStory.mockResolvedValue({ ...story(), grounded: false, evidence: [] });
    const { find, ask, click } = await mountBuilder();
    await ask("How are sales?");
    await click("dashboard-ai-build");
    expect(text(find("dashboard-ai-result"))).toContain("without numbers read from the data");
  });

  it("keeps the form and shows why when the plan fails", async () => {
    mocks.planDashboard.mockResolvedValue({
      ...plan(),
      plan: null,
      degraded: true,
      reason: "no_plan",
      explanation: "The tables hold sales.",
    });
    const { find, ask, click, place } = await mountBuilder();
    await ask("What is the weather?");
    await click("dashboard-ai-build");
    expect(text(find("dashboard-ai-message"))).toBe(
      "The AI did not plan a page: The tables hold sales.",
    );
    expect(find("dashboard-ai-message")?.className).toContain("is-warning");
    expect((find("dashboard-ai-question") as HTMLTextAreaElement).value).toBe(
      "What is the weather?",
    );
    expect(place).not.toHaveBeenCalled();
  });

  it("says so when the built page cannot be added", async () => {
    const place = vi.fn(async () => {
      throw new Error("This schema is read-only.");
    });
    const { find, ask, click } = await mountBuilder({ place });
    await ask("How are sales?");
    await click("dashboard-ai-build");
    const shown = text(find("dashboard-ai-message"));
    expect(shown).toContain("The page was built, but it could not be added to the dashboard.");
    expect(shown).toContain("This schema is read-only.");
    expect(find("dashboard-ai-result")).toBeNull();
  });

  it("Cancel stops the run and places nothing", async () => {
    mocks.planDashboard.mockImplementation(
      (_body, signal: AbortSignal) =>
        new Promise((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(new Error("canceled")));
        }),
    );
    const { find, ask, click, place } = await mountBuilder();
    await ask("How are sales?");
    await click("dashboard-ai-build");
    expect(text(find("dashboard-ai-progress"))).toContain("Planning the page");
    await click("dashboard-ai-cancel");
    expect(text(find("dashboard-ai-message"))).toBe("Stopped. Nothing was added to the dashboard.");
    expect(find("dashboard-ai-progress")).toBeNull();
    expect(place).not.toHaveBeenCalled();
  });
});
