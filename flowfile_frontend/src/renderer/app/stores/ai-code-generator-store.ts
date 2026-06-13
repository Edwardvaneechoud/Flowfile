// Pinia store for the one-shot node code-generation surface (the "Generate
// with AI" button on the polars_code / sql_query / python_script editors).
//
// One in-flight request at a time (a fresh Generate cancels the prior call).
// Fire-and-forget: the calling component owns whatever it does with the
// returned code. Provider / model default to the user's chat selection when the
// caller doesn't pin one, matching ai-autocomplete-store's behaviour.

import { defineStore } from "pinia";
import { ref } from "vue";

import { AiDisabledError, generateNodeCode, NodeCodeGenerationResponse } from "../api/ai.api";
import { useAiStore } from "./ai-store";

// Master switch for the in-editor "Generate with AI" button. Off until the
// feature is ready to ship; flip to true to surface it in all three editors.
export const AI_GENERATE_CODE_ENABLED = false;

const isAbortError = (err: unknown): boolean => {
  if (err instanceof DOMException && err.name === "AbortError") return true;
  if (err && typeof err === "object" && (err as { name?: string }).name === "CanceledError")
    return true;
  return false;
};

export interface GenerateNodeCodeArgs {
  flowId: number;
  nodeId: number | string;
  prompt: string;
  provider?: string;
  model?: string | null;
}

export const useAiCodeGeneratorStore = defineStore("aiCodeGenerator", () => {
  const loading = ref(false);
  // True iff the backend last returned 503 (AI feature flag off) — lets the
  // caller render a one-time "AI disabled" hint.
  const aiDisabled = ref(false);
  const inflight = ref<AbortController | null>(null);

  const cancel = (): void => {
    if (inflight.value !== null) {
      inflight.value.abort();
      inflight.value = null;
    }
  };

  // Returns the generated response, or null on cancellation / AI-disabled /
  // transport error. Soft failures (timeout, parse error, model declined) come
  // back as a normal response with `degraded=true` and a `reason`.
  const generateCode = async (
    args: GenerateNodeCodeArgs,
  ): Promise<NodeCodeGenerationResponse | null> => {
    cancel();
    const controller = new AbortController();
    inflight.value = controller;
    loading.value = true;
    aiDisabled.value = false;

    const aiStore = useAiStore();
    try {
      return await generateNodeCode(
        {
          flowId: args.flowId,
          nodeId: args.nodeId,
          prompt: args.prompt,
          provider: args.provider ?? aiStore.selectedProvider ?? undefined,
          model: args.model ?? aiStore.selectedModel ?? undefined,
        },
        controller.signal,
      );
    } catch (error) {
      if (isAbortError(error)) return null;
      if (error instanceof AiDisabledError) {
        aiDisabled.value = true;
        return null;
      }
      throw error;
    } finally {
      if (inflight.value === controller) inflight.value = null;
      loading.value = false;
    }
  };

  return { loading, aiDisabled, cancel, generateCode };
});
