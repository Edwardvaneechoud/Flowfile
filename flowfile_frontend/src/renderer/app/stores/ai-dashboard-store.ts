// Pinia store for the AI dashboard builder: one build at a time, in three stages.
//
// Plan (POST /ai/plan_dashboard) → one chart per planned check (POST /ai/generate_chart, a few
// at a time) → story (POST /ai/dashboard_story), written from what the built charts return.
// The plan and the story run on the `dashboard` task's model, each chart on the `chart` task's.
// A build ends in a page that is not saved or placed yet; the dashboard editor does that.

import { defineStore } from "pinia";
import { computed, ref } from "vue";

import {
  AiDisabledError,
  generateChart,
  planDashboard,
  writeDashboardStory,
  type DashboardPlan,
  type DashboardStoryResponse,
  type PlannedCheck,
  type StoryChartBody,
} from "../api/ai.api";
import {
  degradedPlanMessage,
  planRequestFailure,
  storyFallbackWarning,
  type BuiltChart,
  type BuiltPage,
} from "../views/DashboardsView/aiDashboard";
import type { ChartConverter } from "../views/DashboardsView/aiDashboardCharts";
import type { ChartRequest } from "../views/CatalogView/chartRequest";
import {
  degradedMessage,
  requestFailureMessage,
  type ChartPromptMessage,
} from "../views/CatalogView/chartPrompt";
import { useAiStore } from "./ai-store";

export const DASHBOARD_SURFACE = "dashboard";
export const CHART_SURFACE = "chart";
export const CHART_CONCURRENCY = 3;

export type BuildStage = "idle" | "planning" | "building" | "writing";
export type CheckStatus = "waiting" | "building" | "built" | "failed";

export interface CheckProgress {
  id: string;
  title: string;
  status: CheckStatus;
  /** Why it failed. */
  message?: string;
}

export interface BuildDashboardArgs {
  tableIds: number[];
  prompt: string;
  convert: ChartConverter;
}

export type BuildDashboardOutcome =
  | { status: "ok"; page: BuiltPage }
  | { status: "cancelled" }
  | { status: "disabled" }
  | { status: "failed"; message: ChartPromptMessage };

interface Built {
  check: PlannedCheck;
  request: ChartRequest;
  chart: BuiltChart["chart"];
  payload: Record<string, unknown> | null;
}

export const useAiDashboardStore = defineStore("aiDashboard", () => {
  const aiStore = useAiStore();
  const stage = ref<BuildStage>("idle");
  const checks = ref<CheckProgress[]>([]);
  let inflight: AbortController | null = null;

  /** The provider and model of a task: its pick when that provider is configured, else the first configured one. */
  const targetOf = (surface: string): { provider: string | null; model: string | null } => {
    const picked = aiStore.resolveSurface(surface);
    const configured = aiStore.configuredProviders;
    if (picked.provider && configured.some((p) => p.provider === picked.provider)) {
      return { provider: picked.provider, model: picked.model };
    }
    return { provider: configured[0]?.provider ?? picked.provider ?? null, model: null };
  };

  const dashboardTarget = computed(() => targetOf(DASHBOARD_SURFACE));
  const chartTarget = computed(() => targetOf(CHART_SURFACE));

  /** True when a build may start: AI is on and some provider is set up. */
  const available = computed(() => !aiStore.aiDisabled && aiStore.hasConfiguredProvider);
  const busy = computed(() => stage.value !== "idle");

  /** Stop the build in flight; it resolves "cancelled". */
  const cancel = (): void => {
    inflight?.abort();
  };

  const setCheck = (id: string, change: Partial<CheckProgress>): void => {
    checks.value = checks.value.map((c) => (c.id === id ? { ...c, ...change } : c));
  };

  const buildCheck = async (
    check: PlannedCheck,
    convert: ChartConverter,
    signal: AbortSignal,
  ): Promise<Built | string> => {
    setCheck(check.id, { status: "building" });
    let response;
    try {
      response = await generateChart(
        {
          tableId: check.table_id,
          prompt: check.brief,
          sendValues: aiStore.sendsData,
          ...chartTarget.value,
        },
        signal,
      );
    } catch (error) {
      if (signal.aborted || error instanceof AiDisabledError) throw error;
      return requestFailureMessage(error).text;
    }
    if (response.degraded || !response.chart) return degradedMessage(response).text;
    const converted = await convert(check.table_id, response.chart);
    if (!converted.ok) return "The AI's chart could not be drawn here.";
    return { check, request: response.chart, chart: converted.chart, payload: converted.payload };
  };

  const buildCharts = async (
    plan: DashboardPlan,
    convert: ChartConverter,
    signal: AbortSignal,
  ): Promise<Map<string, Built>> => {
    const built = new Map<string, Built>();
    let next = 0;
    const worker = async (): Promise<void> => {
      while (next < plan.checks.length && !signal.aborted) {
        const check = plan.checks[next];
        next += 1;
        const result = await buildCheck(check, convert, signal);
        if (signal.aborted) return;
        if (typeof result === "string") {
          setCheck(check.id, { status: "failed", message: result });
        } else {
          built.set(check.id, result);
          setCheck(check.id, { status: "built" });
        }
      }
    };
    await Promise.all(Array.from({ length: CHART_CONCURRENCY }, worker));
    return built;
  };

  const pageOf = (
    plan: DashboardPlan,
    prompt: string,
    built: Map<string, Built>,
    story: DashboardStoryResponse | null,
    warnings: string[],
  ): BuiltPage => {
    const titleOf = (id: string): string => built.get(id)?.check.title ?? id;
    const written = story?.story ?? null;
    const placed = written
      ? written.charts
      : plan.checks
          .filter((c) => built.has(c.id))
          .map((c) => ({ id: c.id, title: c.title, finding: null, width: "half" as const }));
    const charts: BuiltChart[] = [];
    for (const item of placed) {
      const source = built.get(item.id);
      if (!source) continue;
      charts.push({
        id: item.id,
        tableId: source.check.table_id,
        title: item.title,
        finding: item.finding,
        width: item.width,
        chart: source.chart,
      });
    }
    return {
      title: written?.title ?? plan.title,
      summaryMd: written?.summary_md ?? `Charts for the question: ${prompt}`,
      grounded: Boolean(written && story?.grounded),
      kpis: plan.kpis,
      charts,
      filters: plan.filters ?? [],
      leftOut: (written?.left_out ?? []).map((o) => ({ title: titleOf(o.id), reason: o.reason })),
      failed: checks.value
        .filter((c) => c.status === "failed")
        .map((c) => ({ title: c.title, reason: c.message ?? "It could not be built." })),
      relations: (story?.evidence ?? [])
        .filter((e) => e.kind === "relation" && e.status === "measured" && e.detail)
        .map((e) => ({ title: titleOf(e.id), reason: e.detail as string })),
      warnings: [
        ...warnings,
        ...(story?.warnings ?? []),
        ...(written ? [] : [storyFallbackWarning(story)]),
      ],
    };
  };

  const build = async (args: BuildDashboardArgs): Promise<BuildDashboardOutcome> => {
    cancel();
    const controller = new AbortController();
    inflight = controller;
    const { signal } = controller;
    const sendValues = aiStore.sendsData;
    checks.value = [];
    stage.value = "planning";
    try {
      let planned;
      try {
        planned = await planDashboard(
          { tableIds: args.tableIds, prompt: args.prompt, sendValues, ...dashboardTarget.value },
          signal,
        );
      } catch (error) {
        if (signal.aborted) return { status: "cancelled" };
        if (error instanceof AiDisabledError) return { status: "disabled" };
        return { status: "failed", message: planRequestFailure(error) };
      }
      if (signal.aborted) return { status: "cancelled" };
      const plan = planned.plan;
      if (planned.degraded || !plan) {
        return { status: "failed", message: degradedPlanMessage(planned) };
      }

      checks.value = plan.checks.map((c) => ({ id: c.id, title: c.title, status: "waiting" }));
      stage.value = "building";
      const built = await buildCharts(plan, args.convert, signal);
      if (signal.aborted) return { status: "cancelled" };
      if (built.size === 0) {
        const first = checks.value.find((c) => c.message)?.message;
        return {
          status: "failed",
          message: {
            tone: "error",
            text: "None of the planned charts could be built. Try again, or put the question another way.",
            ...(first ? { detail: first } : {}),
          },
        };
      }

      stage.value = "writing";
      const charts: StoryChartBody[] = plan.checks
        .filter((c) => built.has(c.id))
        .map((c) => {
          const item = built.get(c.id) as Built;
          return {
            id: c.id,
            table_id: c.table_id,
            title: c.title,
            purpose: c.purpose,
            chart: item.request,
            payload: sendValues ? item.payload : null,
            relation: c.relation,
          };
        });
      let story: DashboardStoryResponse | null = null;
      try {
        story = await writeDashboardStory(
          {
            tableIds: args.tableIds,
            prompt: args.prompt,
            title: plan.title,
            kpis: plan.kpis,
            charts,
            failed: checks.value
              .filter((c) => c.status === "failed")
              .map((c) => ({ id: c.id, title: c.title, reason: (c.message ?? "").slice(0, 300) })),
            sendValues,
            ...dashboardTarget.value,
          },
          signal,
        );
      } catch (error) {
        if (signal.aborted) return { status: "cancelled" };
        if (error instanceof AiDisabledError) return { status: "disabled" };
        story = null;
      }
      if (signal.aborted) return { status: "cancelled" };
      return {
        status: "ok",
        page: pageOf(plan, args.prompt, built, story, planned.warnings),
      };
    } catch (error) {
      if (signal.aborted) return { status: "cancelled" };
      if (error instanceof AiDisabledError) return { status: "disabled" };
      return { status: "failed", message: requestFailureMessage(error) };
    } finally {
      if (inflight === controller) {
        inflight = null;
        stage.value = "idle";
      }
    }
  };

  return { stage, checks, busy, available, dashboardTarget, chartTarget, cancel, build };
});
