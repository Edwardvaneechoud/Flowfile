import { computed } from "vue";
import { useAiStore } from "../stores/ai-store";

export const SEMANTIC_SURFACE = "semantic";

/** Whether AI can answer for the semantic model, and the provider and model the user chose for it. */
export function useSemanticAi() {
  const aiStore = useAiStore();

  const usable = computed(() => !aiStore.aiDisabled && aiStore.hasConfiguredProvider);
  const canOfferLocal = computed(
    () => !aiStore.aiDisabled && !aiStore.hasConfiguredProvider && aiStore.canSetupLocal,
  );

  function pick(): { provider?: string; model?: string } {
    if (!aiStore.surfaceModels[SEMANTIC_SURFACE]) return {};
    const { provider, model } = aiStore.resolveSurface(SEMANTIC_SURFACE);
    return { ...(provider ? { provider } : {}), ...(model ? { model } : {}) };
  }

  return { aiStore, usable, canOfferLocal, pick };
}
