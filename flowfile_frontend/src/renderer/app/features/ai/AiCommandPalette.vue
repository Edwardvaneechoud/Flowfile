<script setup lang="ts">
// W33 — Cmd+K command palette overlay.
//
// A fixed-position single-line input centered above the canvas. Esc closes;
// Enter submits; while loading the input is disabled and a spinner shows.
// On success the W35 diff panel takes over (the store calls
// `useAiDiffStore.setCurrentDiff(...)` and the AI drawer opens) — this
// component fades out.
//
// Mounted once from Canvas.vue. The Cmd+K binding lives there too.
//
// Scope: pure presentation + a small amount of focus management. All
// async lifecycle is in `useAiCommandPaletteStore` so this component
// stays test-light and re-render-cheap.

import { computed, nextTick, onMounted, ref, watch } from "vue";
import { useAiCommandPaletteStore } from "../../stores/ai-command-palette-store";
import { useAiStore } from "../../stores/ai-store";
import { useFlowStore } from "../../stores/flow-store";
import { AiDisabledError } from "../../views/AiProvidersView/api";

const palette = useAiCommandPaletteStore();
const aiStore = useAiStore();
const flowStore = useFlowStore();

const inputEl = ref<HTMLInputElement | null>(null);
const providersLoadFailedDisabled = ref(false);

const placeholder = computed(() => {
  if (palette.aiDisabled) return "AI features are disabled.";
  if (!aiStore.hasConfiguredProvider) return "Configure a provider in Settings → AI Providers.";
  return "Type an action — e.g. 'filter to last 30 days'";
});

const canSubmit = computed(
  () =>
    !palette.loading &&
    aiStore.hasConfiguredProvider &&
    aiStore.selectedProvider !== null &&
    palette.prompt.trim().length > 0,
);

// W36 — model picker. Mirrors the chat drawer's pattern (AiAssistant.vue):
// prefer the W29-curated `credential.models` list, fall back to the singleton
// `defaultModel`, and only render the <select> when there are 2+ options.
// Existing behaviour is preserved verbatim for users with a single default
// model: the picker just doesn't render. The picker writes to the same
// `aiStore.selectedModel` that the palette already forwards on each request,
// so changing it routes the next call to the picked model and survives across
// surfaces (W27 persistence covers it via the existing watcher).
const selectedProviderMeta = computed(() => {
  const name = aiStore.selectedProvider;
  if (!name) return null;
  return aiStore.providers.find((p) => p.provider === name) ?? null;
});

const availableModels = computed<string[]>(() => {
  const meta = selectedProviderMeta.value;
  if (!meta) return [];
  const curated = meta.credential?.models;
  if (curated && curated.length > 0) return curated;
  const singleton = meta.credential?.defaultModel ?? meta.defaultModel ?? null;
  return singleton ? [singleton] : [];
});

const showModelPicker = computed(() => availableModels.value.length > 1);

const handleModelChange = (event: Event): void => {
  const target = event.target as HTMLSelectElement;
  aiStore.setSelectedModel(target.value || null);
};

const focusInput = (): void => {
  void nextTick(() => {
    requestAnimationFrame(() => inputEl.value?.focus());
  });
};

watch(
  () => palette.isOpen,
  (open) => {
    if (open) focusInput();
  },
);

onMounted(async () => {
  // Pre-load the provider list once so the first cmd+k press doesn't
  // race the BYOK fetch. The W20 drawer already does this on its own
  // mount, but the palette may be the first AI surface a user opens.
  if (aiStore.providers.length === 0) {
    try {
      await aiStore.loadProviders();
    } catch (err) {
      if (err instanceof AiDisabledError) providersLoadFailedDisabled.value = true;
    }
  }
});

const handleKeyDown = (event: KeyboardEvent): void => {
  if (event.key === "Escape") {
    event.preventDefault();
    palette.close();
    return;
  }
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    void doSubmit();
  }
};

const doSubmit = async (): Promise<void> => {
  if (!canSubmit.value) return;
  if (!aiStore.selectedProvider) return;
  // Pull selection from the live VueFlow instance the same way the
  // chat drawer does (D005 — palette mounted from Canvas.vue).
  const instance = flowStore.vueFlowInstance;
  let selectedNodeIds: number[] | undefined;
  let upstreamNodeIds: number[] = [];
  if (instance) {
    const selectedRefs = instance.getSelectedNodes?.value ?? [];
    type SelNode = { id: string; data?: { id?: number | string } };
    const ids = (selectedRefs as SelNode[])
      .map((n) => Number(n.data?.id ?? n.id))
      .filter((id) => Number.isFinite(id));
    if (ids.length > 0) {
      selectedNodeIds = ids;
      // Selection becomes the insertion-context upstream so the LLM
      // chains new ops after the selected node(s).
      upstreamNodeIds = ids;
    }
  }
  await palette.submit({
    flowId: flowStore.flowId,
    prompt: palette.prompt,
    provider: aiStore.selectedProvider,
    model: aiStore.selectedModel,
    selectedNodeIds,
    insertionContext: upstreamNodeIds.length > 0 ? { upstreamNodeIds, posX: 0.0, posY: 0.0 } : null,
  });
};

const handleBackdropClick = (): void => {
  if (!palette.loading) palette.close();
};

const _disabled = computed(() => palette.aiDisabled || providersLoadFailedDisabled.value);
</script>

<template>
  <Teleport to="body">
    <div
      v-if="palette.isOpen"
      class="ai-cmdk__backdrop"
      role="dialog"
      aria-modal="true"
      aria-label="Cmd+K command palette"
      @click="handleBackdropClick"
    >
      <div class="ai-cmdk__panel" @click.stop>
        <div v-if="showModelPicker && !_disabled" class="ai-cmdk__header">
          <label class="ai-cmdk__model-label" for="ai-cmdk-model">Model</label>
          <select
            id="ai-cmdk-model"
            class="ai-cmdk__model-select"
            :value="aiStore.selectedModel ?? ''"
            :disabled="palette.loading"
            :title="aiStore.selectedModel ?? 'Pick a model'"
            @change="handleModelChange"
          >
            <option v-for="model in availableModels" :key="model" :value="model">
              {{ model }}
            </option>
          </select>
        </div>
        <div class="ai-cmdk__row">
          <span class="ai-cmdk__icon" aria-hidden="true">✨</span>
          <input
            ref="inputEl"
            v-model="palette.prompt"
            class="ai-cmdk__input"
            type="text"
            :placeholder="placeholder"
            :disabled="palette.loading || _disabled"
            spellcheck="false"
            autocomplete="off"
            @keydown="handleKeyDown"
          />
          <button
            v-if="palette.loading"
            type="button"
            class="ai-cmdk__cancel"
            aria-label="Cancel"
            @click="palette.cancel"
          >
            Cancel
          </button>
          <button
            v-else
            type="button"
            class="ai-cmdk__submit"
            :disabled="!canSubmit"
            @click="doSubmit"
          >
            Run
          </button>
        </div>
        <div v-if="palette.loading" class="ai-cmdk__hint">Working…</div>
        <div v-else-if="palette.error" class="ai-cmdk__error">
          {{ palette.error }}
        </div>
        <div v-else-if="!aiStore.hasConfiguredProvider && !_disabled" class="ai-cmdk__hint">
          No AI provider configured. Open Settings → AI Providers to add a key.
        </div>
        <div v-else class="ai-cmdk__hint">
          Press <kbd>Enter</kbd> to run · <kbd>Esc</kbd> to close
        </div>
        <div v-if="palette.refused.length > 0" class="ai-cmdk__refused">
          <div class="ai-cmdk__refused-title">Some proposals were refused:</div>
          <ul>
            <li v-for="(refusal, idx) in palette.refused" :key="idx">
              <code>{{ refusal.toolName }}</code>
              <span v-if="refusal.refusalDetail"> — {{ refusal.refusalDetail }}</span>
            </li>
          </ul>
        </div>
      </div>
    </div>
  </Teleport>
</template>

<style scoped>
.ai-cmdk__backdrop {
  position: fixed;
  inset: 0;
  background: rgba(20, 24, 32, 0.32);
  display: flex;
  align-items: flex-start;
  justify-content: center;
  padding-top: 18vh;
  z-index: 4000;
}

.ai-cmdk__panel {
  width: min(640px, 92vw);
  background: #ffffff;
  border-radius: 12px;
  box-shadow: 0 24px 64px rgba(15, 20, 30, 0.28);
  padding: 14px 14px 10px 14px;
  display: flex;
  flex-direction: column;
  gap: 8px;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
}

.ai-cmdk__header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding-bottom: 6px;
  border-bottom: 1px solid #f3f4f6;
}

.ai-cmdk__model-label {
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: #6b7280;
  font-weight: 600;
}

.ai-cmdk__model-select {
  font-size: 12px;
  border: 1px solid #d1d5db;
  border-radius: 4px;
  padding: 2px 6px;
  background: #ffffff;
  color: #1f2937;
  cursor: pointer;
  max-width: 320px;
  text-overflow: ellipsis;
}

.ai-cmdk__model-select:disabled {
  cursor: not-allowed;
  opacity: 0.55;
}

.ai-cmdk__row {
  display: flex;
  align-items: center;
  gap: 10px;
}

.ai-cmdk__icon {
  font-size: 18px;
  line-height: 1;
}

.ai-cmdk__input {
  flex: 1;
  border: none;
  outline: none;
  font-size: 16px;
  background: transparent;
  padding: 6px 4px;
  color: #1f2937;
}

.ai-cmdk__input::placeholder {
  color: #9ca3af;
}

.ai-cmdk__input:disabled {
  color: #9ca3af;
  cursor: not-allowed;
}

.ai-cmdk__submit,
.ai-cmdk__cancel {
  border: 1px solid #d1d5db;
  background: #f9fafb;
  border-radius: 6px;
  padding: 4px 12px;
  font-size: 13px;
  cursor: pointer;
  color: #1f2937;
}

.ai-cmdk__submit:hover:not(:disabled),
.ai-cmdk__cancel:hover {
  background: #f3f4f6;
}

.ai-cmdk__submit:disabled {
  cursor: not-allowed;
  opacity: 0.55;
}

.ai-cmdk__hint {
  font-size: 12px;
  color: #6b7280;
}

.ai-cmdk__hint kbd {
  background: #f3f4f6;
  border: 1px solid #d1d5db;
  border-radius: 3px;
  padding: 0 4px;
  font-family: ui-monospace, SFMono-Regular, "SF Mono", monospace;
  font-size: 11px;
}

.ai-cmdk__error {
  font-size: 12px;
  color: #b91c1c;
}

.ai-cmdk__refused {
  font-size: 12px;
  color: #92400e;
  background: #fffbeb;
  border: 1px solid #fde68a;
  border-radius: 6px;
  padding: 6px 8px;
}

.ai-cmdk__refused-title {
  font-weight: 600;
  margin-bottom: 4px;
}

.ai-cmdk__refused ul {
  margin: 0;
  padding-left: 18px;
}

.ai-cmdk__refused code {
  font-family: ui-monospace, SFMono-Regular, "SF Mono", monospace;
  font-size: 11px;
}
</style>
