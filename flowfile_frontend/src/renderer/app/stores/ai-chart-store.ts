import { defineStore } from "pinia";
import { computed, reactive } from "vue";

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

interface Flight {
  controller: AbortController;
  reason: AbortReason | null;
}

/** Whoever asks: a prompt bar passes its own, so two bars never stop each other's request. */
export type ChartPromptOwner = symbol;

const SHARED_OWNER: ChartPromptOwner = Symbol("chart-prompt");

const isAbortError = (err: unknown): boolean => {
  if (err instanceof DOMException && err.name === "AbortError") return true;
  return Boolean(
    err && typeof err === "object" && (err as { name?: string }).name === "CanceledError",
  );
};

/**
 * The chart builder's prompt bar (`POST /ai/generate_chart`). Each owner has one request in
 * flight: its next Generate aborts the one before it, which resolves "superseded", and only its
 * latest request's answer comes back as "ok". Provider and model come from the AI settings
 * through `resolveSurface("chart")`.
 */
export const useAiChartStore = defineStore("aiChart", () => {
  const aiStore = useAiStore();
  const flights = new Map<ChartPromptOwner, Flight>();
  const busy = reactive(new Set<ChartPromptOwner>());

  /** True while any owner's request is out. */
  const loading = computed(() => busy.size > 0);

  /** True while this owner's request is out. */
  const isLoading = (owner: ChartPromptOwner = SHARED_OWNER): boolean => busy.has(owner);

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

  const abort = (owner: ChartPromptOwner, reason: AbortReason): void => {
    const flight = flights.get(owner);
    if (!flight) return;
    flight.reason = reason;
    flight.controller.abort();
    flights.delete(owner);
    busy.delete(owner);
  };

  /** Stop this owner's request in flight; it resolves "cancelled". */
  const cancel = (owner: ChartPromptOwner = SHARED_OWNER): void => abort(owner, "cancelled");

  const generate = async (
    args: GenerateChartArgs,
    owner: ChartPromptOwner = SHARED_OWNER,
  ): Promise<GenerateChartOutcome> => {
    abort(owner, "superseded");
    const mine: Flight = { controller: new AbortController(), reason: null };
    flights.set(owner, mine);
    busy.add(owner);
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
      if (flights.get(owner) === mine) {
        flights.delete(owner);
        busy.delete(owner);
      }
    }
  };

  return { loading, isLoading, target, available, cancel, generate };
});
