// Pinia store for the chart builder's prompt bar (POST /ai/generate_chart).
//
// One request in flight: a new Generate aborts the one before it, and only the latest
// request's answer comes back as "ok"; an older one resolves "superseded". Provider and
// model come from the AI settings through `resolveSurface("chart")`.

import { defineStore } from "pinia";
import { computed, ref } from "vue";

import {
  AiDisabledError,
  generateChart,
  type ChartGenerationResponse,
  type GenerateChartRequest,
} from "../api/ai.api";
import type { ChartRequest } from "../views/CatalogView/chartRequest";
import { requestFailureMessage, type ChartPromptMessage } from "../views/CatalogView/chartPrompt";
import { useAiStore } from "./ai-store";

export const CHART_SURFACE = "chart";

export interface GenerateChartArgs {
  tableId: number;
  prompt: string;
  currentChart?: ChartRequest | null;
}

export type GenerateChartOutcome =
  | { status: "ok"; response: ChartGenerationResponse }
  | { status: "cancelled" }
  | { status: "superseded" }
  | { status: "disabled" }
  | { status: "failed"; message: ChartPromptMessage };

type AbortReason = "cancelled" | "superseded";

const isAbortError = (err: unknown): boolean => {
  if (err instanceof DOMException && err.name === "AbortError") return true;
  return Boolean(
    err && typeof err === "object" && (err as { name?: string }).name === "CanceledError",
  );
};

export const useAiChartStore = defineStore("aiChart", () => {
  const aiStore = useAiStore();
  const loading = ref(false);
  let inflight: { controller: AbortController; reason: AbortReason | null } | null = null;

  /**
   * The provider a request goes to, and its model: the chart surface's pick when it is
   * configured, else the first configured provider (the route falls back the same way, and
   * a model only belongs to the provider it was picked for).
   */
  const target = computed<{ provider: string | null; model: string | null }>(() => {
    const picked = aiStore.resolveSurface(CHART_SURFACE);
    const configured = aiStore.configuredProviders;
    if (picked.provider && configured.some((p) => p.provider === picked.provider)) {
      return { provider: picked.provider, model: picked.model };
    }
    return { provider: configured[0]?.provider ?? picked.provider ?? null, model: null };
  });

  /** True when the bar may show: AI is on and some provider is set up. */
  const available = computed(() => !aiStore.aiDisabled && aiStore.hasConfiguredProvider);

  const abort = (reason: AbortReason): void => {
    if (!inflight) return;
    inflight.reason = reason;
    inflight.controller.abort();
    inflight = null;
    loading.value = false;
  };

  /** Stop the request in flight; it resolves "cancelled". */
  const cancel = (): void => abort("cancelled");

  const generate = async (args: GenerateChartArgs): Promise<GenerateChartOutcome> => {
    abort("superseded");
    const mine = { controller: new AbortController(), reason: null as AbortReason | null };
    inflight = mine;
    loading.value = true;
    const { provider, model } = target.value;
    const body: GenerateChartRequest = {
      tableId: args.tableId,
      prompt: args.prompt,
      currentChart: args.currentChart ?? null,
      sendValues: aiStore.sendsData,
      provider,
      model,
    };
    try {
      const response = await generateChart(body, mine.controller.signal);
      if (mine.reason) return { status: mine.reason };
      return { status: "ok", response };
    } catch (error) {
      if (mine.reason) return { status: mine.reason };
      if (isAbortError(error)) return { status: "cancelled" };
      if (error instanceof AiDisabledError) return { status: "disabled" };
      return { status: "failed", message: requestFailureMessage(error) };
    } finally {
      if (inflight === mine) {
        inflight = null;
        loading.value = false;
      }
    }
  };

  return { loading, target, available, cancel, generate };
});
