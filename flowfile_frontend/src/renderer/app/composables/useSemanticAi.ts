import { computed } from "vue";
import { useTaskModels } from "./useTaskModels";

/** Whether AI can answer for the semantic model, and what a request of one of its two tasks carries. */
export function useSemanticAi() {
  const { aiStore, plan } = useTaskModels();

  const usable = computed(() => !aiStore.aiDisabled && aiStore.hasConfiguredProvider);
  const canOfferLocal = computed(
    () => !aiStore.aiDisabled && !aiStore.hasConfiguredProvider && aiStore.canSetupLocal,
  );

  /** The task's own pick, nothing when the local model serves it, else the main pick. */
  const pick = (surface: string) => plan(surface).send;

  return { aiStore, usable, canOfferLocal, pick };
}
