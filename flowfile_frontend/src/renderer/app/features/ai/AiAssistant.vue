<script setup lang="ts">
// AI Assistant chat drawer (W20). Read-only — no tool calls, no graph
// mutation. Mounted as an independent <draggable-item> from Canvas.vue
// (D005), so it coexists with the node-settings drawer.
//
// Scope of this component: provider/model picker, message list, composer,
// abort. Anything more (context pinning, @-mentions, diff preview) lands
// in W22 / W24 / W31+ and extends the store rather than this view.

import { computed, nextTick, onMounted, ref, watch } from "vue";
import { useAiStore } from "../../stores/ai-store";
import { AiDisabledError } from "../../views/AiProvidersView/api";
import AiMessage from "./AiMessage.vue";

const aiStore = useAiStore();

const composerText = ref("");
const messageContainerRef = ref<HTMLElement | null>(null);
const isDisabledByFlag = ref(false);

const placeholder = computed(() =>
  aiStore.hasConfiguredProvider
    ? "Ask anything about Flowfile or your data..."
    : "Configure a provider in Settings → AI Providers to start chatting.",
);

const canSend = computed(
  () =>
    !aiStore.isStreaming &&
    aiStore.hasConfiguredProvider &&
    composerText.value.trim().length > 0 &&
    aiStore.selectedProvider !== null,
);

const scrollToBottom = (): void => {
  void nextTick(() => {
    requestAnimationFrame(() => {
      const el = messageContainerRef.value;
      if (el) el.scrollTop = el.scrollHeight;
    });
  });
};

watch(
  () => aiStore.messages.length,
  () => scrollToBottom(),
);

watch(
  () => aiStore.messages.map((m) => m.content).join("\n"),
  () => scrollToBottom(),
);

onMounted(async () => {
  try {
    await aiStore.loadProviders();
  } catch (err) {
    if (err instanceof AiDisabledError) {
      isDisabledByFlag.value = true;
    }
  }
});

const handleProviderChange = (event: Event): void => {
  const target = event.target as HTMLSelectElement;
  aiStore.setSelectedProvider(target.value);
};

const handleSend = async (): Promise<void> => {
  if (!canSend.value) return;
  const text = composerText.value;
  composerText.value = "";
  await aiStore.sendMessage(text);
};

const handleAbort = (): void => {
  aiStore.abortStream();
};

const handleClear = (): void => {
  aiStore.clearMessages();
};

const handleComposerKeydown = (event: KeyboardEvent): void => {
  // Enter sends, Shift+Enter inserts a newline. Mirrors most chat UIs.
  if (event.key === "Enter" && !event.shiftKey && !event.isComposing) {
    event.preventDefault();
    void handleSend();
  }
};
</script>

<template>
  <div class="ai-assistant">
    <header class="ai-assistant__header">
      <select
        v-if="aiStore.providers.length > 0 && !isDisabledByFlag"
        class="ai-assistant__select"
        :value="aiStore.selectedProvider ?? ''"
        :disabled="aiStore.isStreaming"
        @change="handleProviderChange"
      >
        <option value="" disabled>Pick a provider</option>
        <option
          v-for="provider in aiStore.providers"
          :key="provider.provider"
          :value="provider.provider"
          :disabled="provider.status === 'unconfigured'"
        >
          {{ provider.provider }} —
          {{ provider.credential?.defaultModel ?? provider.defaultModel }}
          <template v-if="provider.status === 'env_fallback'"> (env)</template>
          <template v-else-if="provider.status === 'unconfigured'"> (not configured)</template>
        </option>
      </select>
      <button
        v-if="aiStore.messages.length > 0"
        type="button"
        class="ai-assistant__clear"
        :disabled="aiStore.isStreaming"
        @click="handleClear"
      >
        Clear
      </button>
    </header>

    <div v-if="isDisabledByFlag" class="ai-assistant__notice">
      <p>AI features are disabled.</p>
      <p class="ai-assistant__notice-hint">
        Set <code>FEATURE_FLAG_AI=true</code> on the backend and restart <code>flowfile_core</code>.
      </p>
    </div>

    <div v-else-if="aiStore.providersLoading" class="ai-assistant__notice">Loading providers…</div>

    <div v-else-if="!aiStore.hasConfiguredProvider" class="ai-assistant__notice">
      <p>No providers configured yet.</p>
      <p class="ai-assistant__notice-hint">
        Open <strong>Connections → AI Providers</strong> and add an API key (or run a local Ollama
        server) to start chatting.
      </p>
    </div>

    <div v-else ref="messageContainerRef" class="ai-assistant__messages">
      <div v-if="aiStore.messages.length === 0" class="ai-assistant__empty">
        <p>This chat is read-only — it can't change your flow yet.</p>
        <p class="ai-assistant__notice-hint">
          Ask anything; tool-driven edits land in a later release.
        </p>
      </div>
      <AiMessage v-for="message in aiStore.messages" :key="message.id" :message="message" />
    </div>

    <div v-if="aiStore.streamError" class="ai-assistant__error">
      {{ aiStore.streamError }}
    </div>

    <footer v-if="!isDisabledByFlag" class="ai-assistant__composer">
      <textarea
        v-model="composerText"
        class="ai-assistant__textarea"
        rows="3"
        :placeholder="placeholder"
        :disabled="!aiStore.hasConfiguredProvider"
        @keydown="handleComposerKeydown"
      ></textarea>
      <div class="ai-assistant__actions">
        <button
          v-if="aiStore.isStreaming"
          type="button"
          class="ai-assistant__btn ai-assistant__btn--danger"
          @click="handleAbort"
        >
          Stop
        </button>
        <button
          v-else
          type="button"
          class="ai-assistant__btn ai-assistant__btn--primary"
          :disabled="!canSend"
          @click="handleSend"
        >
          Send
        </button>
      </div>
    </footer>
  </div>
</template>

<style scoped>
.ai-assistant {
  display: flex;
  flex-direction: column;
  height: 100%;
  gap: 8px;
  padding: 10px;
  font-family: var(--font-family-base, system-ui);
  background-color: var(--color-background-primary, #ffffff);
}

.ai-assistant__header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding-bottom: 8px;
  border-bottom: 1px solid var(--color-border-primary, #e1e4e8);
}

.ai-assistant__select {
  flex: 1;
  padding: 6px 8px;
  border-radius: 6px;
  border: 1px solid var(--color-border-primary, #e1e4e8);
  font-size: 12px;
  background-color: var(--color-background-primary, #ffffff);
  color: var(--color-text-primary, #24292e);
}

.ai-assistant__select:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.ai-assistant__clear {
  padding: 4px 10px;
  border-radius: 6px;
  border: 1px solid var(--color-border-primary, #e1e4e8);
  background-color: var(--color-background-secondary, #f6f8fa);
  font-size: 12px;
  cursor: pointer;
}

.ai-assistant__clear:hover:enabled {
  background-color: var(--color-background-hover, #ececec);
}

.ai-assistant__messages {
  flex: 1;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding: 4px 2px;
}

.ai-assistant__empty {
  padding: 20px 8px;
  text-align: center;
  color: var(--color-text-muted, #6a737d);
  font-size: 13px;
}

.ai-assistant__notice {
  padding: 16px;
  border-radius: 8px;
  background-color: var(--color-background-secondary, #f6f8fa);
  border: 1px solid var(--color-border-primary, #e1e4e8);
  font-size: 13px;
  color: var(--color-text-primary, #24292e);
}

.ai-assistant__notice-hint {
  margin-top: 6px;
  color: var(--color-text-muted, #6a737d);
  font-size: 12px;
}

.ai-assistant__error {
  padding: 8px 10px;
  border-radius: 6px;
  background-color: var(--color-danger-light, #ffe5e5);
  color: var(--color-danger, #c53030);
  font-size: 12px;
}

.ai-assistant__composer {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding-top: 8px;
  border-top: 1px solid var(--color-border-primary, #e1e4e8);
}

.ai-assistant__textarea {
  resize: vertical;
  min-height: 60px;
  padding: 8px;
  border-radius: 6px;
  border: 1px solid var(--color-border-primary, #e1e4e8);
  font-family: inherit;
  font-size: 13px;
  background-color: var(--color-background-primary, #ffffff);
  color: var(--color-text-primary, #24292e);
}

.ai-assistant__textarea:disabled {
  background-color: var(--color-background-secondary, #f6f8fa);
  cursor: not-allowed;
}

.ai-assistant__actions {
  display: flex;
  justify-content: flex-end;
}

.ai-assistant__btn {
  padding: 6px 14px;
  border-radius: 6px;
  font-size: 12px;
  font-weight: 500;
  cursor: pointer;
  border: 1px solid transparent;
}

.ai-assistant__btn--primary {
  background: linear-gradient(
    135deg,
    var(--color-gradient-purple-start, #6f42c1) 0%,
    var(--color-gradient-purple-end, #5933a8) 100%
  );
  color: var(--color-text-inverse, #ffffff);
}

.ai-assistant__btn--primary:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.ai-assistant__btn--danger {
  background-color: var(--color-danger, #c53030);
  color: var(--color-text-inverse, #ffffff);
}
</style>
