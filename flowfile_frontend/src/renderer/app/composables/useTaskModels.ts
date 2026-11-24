import { computed } from "vue";
import { useAiStore } from "../stores/ai-store";
import type { SurfaceModel } from "../stores/ai-store-persistence";
import { LOCAL_PROVIDER_ID, LOCAL_PROVIDER_LABEL } from "../views/AiSettingsView/localModelApi";

export const SEMANTIC_ASK_SURFACE = "semantic_ask";
export const SEMANTIC_SUGGEST_SURFACE = "semantic_suggest";

// The semantic routes run the local model when no provider is named, so these tasks fall back to it first.
const LOCAL_FIRST = new Set([SEMANTIC_ASK_SURFACE, SEMANTIC_SUGGEST_SURFACE]);
// The backend's per-provider model preset is keyed by its own surface name.
const PRESET_KEY: Record<string, string> = {
  [SEMANTIC_ASK_SURFACE]: "semantic",
  [SEMANTIC_SUGGEST_SURFACE]: "semantic",
};

/** Which provider and model a task runs on, why, and what a semantic request sends for it. */
export interface TaskModelPlan {
  provider: string | null;
  model: string | null;
  source: "task" | "local" | "main" | "none";
  send: { provider?: string; model?: string };
}

/** Keep the selected model as a valid option even when the provider's list does not offer it. */
export const withSelected = (list: string[], selected: string | null): string[] =>
  selected && !list.includes(selected) ? [...list, selected] : list;

/** The per-task model picks shared by Settings → AI → Assistant and the in-place pickers. */
export function useTaskModels() {
  const aiStore = useAiStore();

  const providerLabel = (name: string): string =>
    name === LOCAL_PROVIDER_ID ? LOCAL_PROVIDER_LABEL : name;

  // Every provider is listed; unconfigured ones are greyed out, since they would fail at call time.
  const providerOptions = computed(() =>
    aiStore.providers.map((p) => ({
      value: p.provider,
      label:
        p.status === "unconfigured"
          ? `${providerLabel(p.provider)} (not configured)`
          : p.status === "env_fallback"
            ? `${providerLabel(p.provider)} (env key)`
            : providerLabel(p.provider),
      disabled: p.status === "unconfigured",
    })),
  );

  const isUnconfigured = (name: string | null): boolean =>
    name !== null &&
    aiStore.providers.some((p) => p.provider === name && p.status === "unconfigured");

  /** A page loaded before the split may still hold the one ``semantic`` pick; it serves both tasks. */
  function ownPick(surface: string): SurfaceModel | null {
    const own = aiStore.surfaceModels[surface];
    if (own) return own;
    return LOCAL_FIRST.has(surface) ? (aiStore.surfaceModels.semantic ?? null) : null;
  }

  function splitLegacy(): void {
    const legacy = aiStore.surfaceModels.semantic;
    if (!legacy) return;
    for (const surface of LOCAL_FIRST) {
      if (!aiStore.surfaceModels[surface]) aiStore.setSurfaceModel(surface, legacy);
    }
    aiStore.setSurfaceModel("semantic", null);
  }

  const taskModels = (surface: string): string[] => {
    const pick = ownPick(surface);
    return withSelected(aiStore.modelsForProvider(pick?.provider ?? null), pick?.model ?? null);
  };

  function setTaskProvider(surface: string, value: string): void {
    splitLegacy();
    aiStore.setSurfaceModel(surface, value ? { provider: value, model: null } : null);
  }

  function setTaskModel(surface: string, value: string): void {
    splitLegacy();
    const pick = aiStore.surfaceModels[surface];
    if (pick) aiStore.setSurfaceModel(surface, { provider: pick.provider, model: value || null });
  }

  function modelName(provider: string, model: string | null, surface: string): string | null {
    if (provider === LOCAL_PROVIDER_ID) return aiStore.localModelStatus?.selectedModelId ?? model;
    if (model) return model;
    const meta = aiStore.providers.find((p) => p.provider === provider);
    const preset = meta?.surfaces?.[PRESET_KEY[surface] ?? surface];
    return preset ?? meta?.credential?.defaultModel ?? meta?.defaultModel ?? null;
  }

  /** The task's own pick, else (semantic tasks) the installed local model, else the main provider and model. */
  function plan(surface: string): TaskModelPlan {
    const own = ownPick(surface);
    if (own) {
      return {
        provider: own.provider,
        model: modelName(own.provider, own.model, surface),
        source: "task",
        send: { provider: own.provider, ...(own.model ? { model: own.model } : {}) },
      };
    }
    if (LOCAL_FIRST.has(surface) && aiStore.localModelStatus?.installed) {
      return {
        provider: LOCAL_PROVIDER_ID,
        model: modelName(LOCAL_PROVIDER_ID, null, surface),
        source: "local",
        send: {},
      };
    }
    const main = aiStore.selectedProvider;
    if (!main) return { provider: null, model: null, source: "none", send: {} };
    const model = aiStore.selectedModel;
    return {
      provider: main,
      model: model ? modelName(main, model, surface) : null,
      source: "main",
      send: { provider: main, ...(model ? { model } : {}) },
    };
  }

  /** "Uses qwen3.5-2b on this computer", or null when no AI is set up for the task. */
  function describe(surface: string): string | null {
    const p = plan(surface);
    if (!p.provider) return null;
    if (p.provider === LOCAL_PROVIDER_ID) {
      return p.model ? `Uses ${p.model} on this computer` : "Uses the model on this computer";
    }
    if (!p.model) {
      return p.source === "main"
        ? "Uses your main AI model"
        : `Uses the default model of ${providerLabel(p.provider)}`;
    }
    return `Uses ${p.model} (${providerLabel(p.provider)})`;
  }

  return {
    aiStore,
    providerLabel,
    providerOptions,
    isUnconfigured,
    ownPick,
    taskModels,
    setTaskProvider,
    setTaskModel,
    plan,
    describe,
  };
}
