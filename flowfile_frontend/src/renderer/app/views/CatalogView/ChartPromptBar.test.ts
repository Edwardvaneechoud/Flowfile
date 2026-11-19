// @vitest-environment happy-dom
// The prompt bar against a stand-in builder: what goes with the prompt, what gets applied,
// and the rule for an answer that comes back late (cancelled, replaced, builder reloaded,
// chart changed by hand meanwhile).

import { createApp, h, nextTick, type FunctionalComponent } from "vue";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { IChart, IMutField } from "@kanaries/graphic-walker/interfaces";
import { normalize } from "@kanaries/graphic-walker/models/normalize";
import {
  expandTerse,
  parseShorthand,
  projectTerse,
  terseComputedFid,
} from "@kanaries/graphic-walker/models/terse";
import { createPinia } from "pinia";
import { deferred } from "../../__fixtures__/promises";
import { element, mountIn, settle, text } from "../../__fixtures__/mount";
import type { ChartBuilderHandle } from "./chartPrompt";
import { chartFromRequest, type GraphicWalkerFunctions } from "./chartRequest";

const mocks = vi.hoisted(() => ({
  generateChart: vi.fn(),
  loadProviders: vi.fn(),
  ai: { aiDisabled: false, hasConfiguredProvider: true, sendsData: true },
}));

vi.mock("../../api/ai.api", () => ({
  AiDisabledError: class AiDisabledError extends Error {},
  generateChart: mocks.generateChart,
}));

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
    providersLoading: false,
    loadProviders: mocks.loadProviders,
    configuredProviders: [{ provider: "openai" }],
    resolveSurface: () => ({ provider: "openai", model: null }),
  }),
}));

import ChartPromptBar from "./ChartPromptBar.vue";

const gw: GraphicWalkerFunctions = {
  normalize,
  expandTerse,
  projectTerse,
  parseShorthand,
  terseComputedFid,
};

const FIELDS: IMutField[] = [
  { fid: "region", name: "region", semanticType: "nominal", analyticType: "dimension" },
  { fid: "amount", name: "amount", semanticType: "quantitative", analyticType: "measure" },
];

const BAR = {
  mark: "bar" as const,
  x: { field: "region" },
  y: { field: "amount", aggregate: "sum" as const },
};
const LINE = { ...BAR, mark: "line" as const };

const build = (request: typeof BAR | typeof LINE, current: IChart | null = null): IChart => {
  const built = chartFromRequest(request, { fields: FIELDS, current, gw });
  if (!built.ok) throw new Error(built.error);
  return built.chart;
};

const ok = (chart: object, title = "Revenue", explanation = "Done it.") => ({
  chart,
  title,
  explanation,
  warnings: [],
  degraded: false,
  reason: null,
  message: null,
});

/** A builder that holds one chart and records what was applied. */
function fakeBuilder(chart: IChart | null = null) {
  const state = { chart, ready: true };
  const applied: IChart[] = [];
  const handle: ChartBuilderHandle = {
    applyChart: (next) => {
      applied.push(next);
      state.chart = next;
      return true;
    },
    currentChart: () => (state.chart ? JSON.parse(JSON.stringify(state.chart)) : null),
    datasetFields: () => (state.ready ? FIELDS : null),
    graphicWalker: () => (state.ready ? gw : null),
  };
  return { state, applied, handle };
}

const TextareaStub: FunctionalComponent<{ modelValue?: string }, ["update:modelValue"]> = (
  props,
  { emit, attrs },
) =>
  h("textarea", {
    ...attrs,
    value: props.modelValue ?? "",
    onInput: (e: Event) => emit("update:modelValue", (e.target as HTMLTextAreaElement).value),
  });
TextareaStub.props = ["modelValue"];
TextareaStub.emits = ["update:modelValue"];
TextareaStub.inheritAttrs = false;

async function mountBar(builder: () => ChartBuilderHandle | null, tableId: number | null = 3) {
  const onApplied = vi.fn();
  const el = await mountIn(ChartPromptBar, {
    props: { tableId, builder, onApplied },
    pinia: createPinia(),
    stubs: { "el-input": TextareaStub as FunctionalComponent, "el-icon": element("i") },
  });
  const q = (id: string) => el.querySelector(`[data-testid="${id}"]`) as HTMLElement | null;
  const type = async (value: string) => {
    const area = q("chart-prompt-input") as HTMLTextAreaElement;
    area.value = value;
    area.dispatchEvent(new Event("input"));
    area.dispatchEvent(new Event("focus"));
    await settle();
  };
  const generate = async (value: string) => {
    await type(value);
    (q("chart-prompt-generate") as HTMLButtonElement).click();
    await settle();
  };
  return { el, q, type, generate, onApplied };
}

beforeEach(() => {
  mocks.generateChart.mockReset();
  mocks.loadProviders.mockReset();
  mocks.ai.aiDisabled = false;
  mocks.ai.hasConfiguredProvider = true;
  mocks.ai.sendsData = true;
});

describe("ChartPromptBar", () => {
  it("is hidden on a chart that reads no catalog table, and without a provider", async () => {
    const { handle } = fakeBuilder();
    const bar = await mountBar(() => handle, null);
    expect(bar.q("chart-prompt-bar")).toBeNull();

    mocks.ai.hasConfiguredProvider = false;
    const noProvider = await mountBar(() => handle);
    expect(noProvider.q("chart-prompt-bar")).toBeNull();
    expect(mocks.loadProviders).toHaveBeenCalled();
  });

  it("waits for the builder, then asks for a chart or for a change", async () => {
    const builder = fakeBuilder();
    builder.state.ready = false;
    const bar = await mountBar(() => builder.handle);
    await bar.type("revenue by region");
    expect((bar.q("chart-prompt-generate") as HTMLButtonElement).disabled).toBe(true);
    expect(bar.q("chart-prompt-input")?.getAttribute("placeholder")).toBe("Describe a chart");

    builder.state.ready = true;
    builder.state.chart = build(BAR);
    await bar.type("revenue by region");
    expect((bar.q("chart-prompt-generate") as HTMLButtonElement).disabled).toBe(false);
    expect(bar.q("chart-prompt-input")?.getAttribute("placeholder")).toBe("Describe a change");
  });

  it("sends the chart on screen, applies the answer and shows the explanation", async () => {
    const builder = fakeBuilder(build(BAR));
    mocks.generateChart.mockResolvedValueOnce(ok(LINE, "Revenue trend", "Now a line."));
    const bar = await mountBar(() => builder.handle);

    await bar.generate("make it a line");

    expect(mocks.generateChart.mock.calls[0][0]).toMatchObject({
      tableId: 3,
      prompt: "make it a line",
      currentChart: BAR,
      provider: "openai",
    });
    expect(builder.applied).toHaveLength(1);
    expect(builder.applied[0].config.geoms).toEqual(["line"]);
    expect(text(bar.q("chart-prompt-explanation"))).toContain("Now a line.");
    expect(bar.onApplied).toHaveBeenCalledWith(expect.objectContaining({ title: "Revenue trend" }));
  });

  it("sends no data values when they are not shared, and says so", async () => {
    mocks.ai.sendsData = false;
    const builder = fakeBuilder(null);
    mocks.generateChart.mockResolvedValueOnce(ok(BAR));
    const bar = await mountBar(() => builder.handle);

    expect(text(bar.q("chart-prompt-sends"))).toContain("names and types only");
    await bar.generate("revenue by region");

    expect(mocks.generateChart.mock.calls[0][0]).toMatchObject({ sendValues: false });
  });

  it("says which part cannot go along and sends the prompt alone", async () => {
    const chart = build(BAR);
    chart.encodings.details = [chart.encodings.dimensions[0]];
    const builder = fakeBuilder(chart);
    mocks.generateChart.mockResolvedValueOnce(ok(BAR));
    const bar = await mountBar(() => builder.handle);
    await bar.type("x");
    expect(text(bar.q("chart-prompt-carry-notice"))).toContain("This chart has a detail field");

    await bar.generate("by region");
    expect(mocks.generateChart.mock.calls[0][0].currentChart).toBeNull();
    expect(builder.applied).toHaveLength(1);
  });

  it("does not apply an answer over a chart the user changed meanwhile, unless asked", async () => {
    const builder = fakeBuilder(build(BAR));
    const answer = deferred<unknown>();
    mocks.generateChart.mockReturnValueOnce(answer.promise);
    const bar = await mountBar(() => builder.handle);

    await bar.generate("make it a line");
    builder.state.chart = { ...builder.state.chart!, name: "Edited by hand" };
    answer.resolve(ok(LINE));
    await settle();

    expect(builder.applied).toHaveLength(0);
    expect(text(bar.q("chart-prompt-message"))).toContain(
      "You changed the chart while the AI was working",
    );
    bar.q("chart-prompt-apply-anyway")!.click();
    await settle();
    expect(builder.applied).toHaveLength(1);
    expect(builder.applied[0].name).toBe("Edited by hand");
  });

  it("does not apply an answer to a builder that was remounted meanwhile", async () => {
    const first = fakeBuilder(build(BAR));
    const second = fakeBuilder(build(BAR));
    let current = first.handle;
    const answer = deferred<unknown>();
    mocks.generateChart.mockReturnValueOnce(answer.promise);
    const bar = await mountBar(() => current);

    await bar.generate("make it a line");
    current = second.handle;
    answer.resolve(ok(LINE));
    await settle();

    expect(first.applied).toHaveLength(0);
    expect(second.applied).toHaveLength(0);
    expect(text(bar.q("chart-prompt-message"))).toContain("The chart builder reloaded");
  });

  it("applies only the latest of two requests", async () => {
    const builder = fakeBuilder(null);
    const firstAnswer = deferred<unknown>();
    mocks.generateChart.mockImplementationOnce(
      (_body, signal: AbortSignal) =>
        new Promise((resolve, reject) => {
          firstAnswer.promise.then(resolve);
          signal.addEventListener("abort", () =>
            reject(Object.assign(new Error("x"), { name: "CanceledError" })),
          );
        }),
    );
    mocks.generateChart.mockResolvedValueOnce(ok(LINE, "Second"));
    const bar = await mountBar(() => builder.handle);

    await bar.generate("first");
    await bar.generate("second");
    firstAnswer.resolve(ok(BAR, "First"));
    await settle();

    expect(builder.applied).toHaveLength(1);
    expect(builder.applied[0].config.geoms).toEqual(["line"]);
    expect(bar.q("chart-prompt-message")).toBeNull();
  });

  it("shows Cancel while busy, and a cancelled request changes nothing", async () => {
    const builder = fakeBuilder(build(BAR));
    mocks.generateChart.mockImplementationOnce(
      (_body, signal: AbortSignal) =>
        new Promise((_resolve, reject) =>
          signal.addEventListener("abort", () =>
            reject(Object.assign(new Error("x"), { name: "CanceledError" })),
          ),
        ),
    );
    const bar = await mountBar(() => builder.handle);

    await bar.generate("anything");
    expect(text(bar.q("chart-prompt-busy"))).toContain("Building the chart…");
    bar.q("chart-prompt-cancel")!.click();
    await settle();

    expect(bar.q("chart-prompt-busy")).toBeNull();
    expect(text(bar.q("chart-prompt-message"))).toContain("Cancelled. The chart is unchanged.");
    expect(builder.applied).toHaveLength(0);
  });

  it("shows the server's warnings with a degraded answer", async () => {
    const builder = fakeBuilder(null);
    mocks.generateChart.mockResolvedValueOnce({
      ...ok(null as never),
      chart: null,
      degraded: true,
      reason: "timeout",
      warnings: ["Column statistics were not available."],
    });
    const bar = await mountBar(() => builder.handle);

    await bar.generate("anything");

    expect(text(bar.q("chart-prompt-message"))).toContain("The AI took too long to answer.");
    expect(text(bar.q("chart-prompt-warning"))).toBe("Column statistics were not available.");
  });

  it("shows why a chart the converter refuses was not drawn", async () => {
    const builder = fakeBuilder(null);
    mocks.generateChart.mockResolvedValueOnce(ok({ mark: "bar", x: { field: "nope" } }));
    const bar = await mountBar(() => builder.handle);

    await bar.generate("by nope");

    expect(builder.applied).toHaveLength(0);
    expect(text(bar.q("chart-prompt-message"))).toContain('"nope" is not a column of this table.');
  });

  it("generates on Ctrl+Enter and stops the builder's undo key in the field", async () => {
    const builder = fakeBuilder(null);
    mocks.generateChart.mockResolvedValueOnce(ok(BAR));
    const bar = await mountBar(() => builder.handle);
    await bar.type("revenue by region");
    const area = bar.q("chart-prompt-input")!;

    const undo = new KeyboardEvent("keydown", { key: "z", ctrlKey: true, bubbles: true });
    const bodyHeard = vi.fn();
    document.body.addEventListener("keydown", bodyHeard);
    area.dispatchEvent(undo);
    document.body.removeEventListener("keydown", bodyHeard);
    expect(bodyHeard).not.toHaveBeenCalled();

    area.dispatchEvent(
      new KeyboardEvent("keydown", { key: "Enter", ctrlKey: true, bubbles: true }),
    );
    await settle();
    await nextTick();
    expect(mocks.generateChart).toHaveBeenCalledTimes(1);
    expect(builder.applied).toHaveLength(1);
  });

  it("drops the request when the builder closes", async () => {
    const builder = fakeBuilder(null);
    let aborted = false;
    mocks.generateChart.mockImplementationOnce(
      (_body, signal: AbortSignal) =>
        new Promise(() => signal.addEventListener("abort", () => (aborted = true))),
    );
    const host = document.createElement("div");
    document.body.appendChild(host);
    const app = createApp(ChartPromptBar, { tableId: 3, builder: () => builder.handle });
    app.use(createPinia());
    app.component("ElInput", TextareaStub);
    app.component("ElIcon", element("i"));
    app.mount(host);
    await settle();
    const area = host.querySelector("textarea")!;
    area.value = "anything";
    area.dispatchEvent(new Event("input"));
    await settle();
    (host.querySelector('[data-testid="chart-prompt-generate"]') as HTMLButtonElement).click();
    await settle();

    app.unmount();
    host.remove();
    expect(aborted).toBe(true);
    expect(builder.applied).toHaveLength(0);
  });
});
