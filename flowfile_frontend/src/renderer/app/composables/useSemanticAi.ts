import { computed } from "vue";
import { SEMANTIC_ASK_SURFACE, SEMANTIC_SUGGEST_SURFACE, useTaskModels } from "./useTaskModels";

/** ``ask`` answers questions; ``suggest`` writes descriptions and proposes metrics, joins and roles. */
export type SemanticTask = "ask" | "suggest";

export const SEMANTIC_SURFACES: Record<SemanticTask, string> = {
  ask: SEMANTIC_ASK_SURFACE,
  suggest: SEMANTIC_SUGGEST_SURFACE,
};

/** Whether AI can answer for the semantic model, and the provider and model each of its two tasks runs on. */
export function useSemanticAi() {
  const { aiStore, plan } = useTaskModels();

  const usable = computed(() => !aiStore.aiDisabled && aiStore.hasConfiguredProvider);
  const canOfferLocal = computed(
    () => !aiStore.aiDisabled && !aiStore.hasConfiguredProvider && aiStore.canSetupLocal,
  );

  /** What the request carries: the task's own pick, nothing when the local model serves it, else the main pick. */
  function pick(task: SemanticTask): { provider?: string; model?: string } {
    return plan(SEMANTIC_SURFACES[task]).send;
  }

  return { aiStore, usable, canOfferLocal, pick };
}
