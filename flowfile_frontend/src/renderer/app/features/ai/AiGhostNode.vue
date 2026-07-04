<template>
  <div
    v-if="isVisible"
    class="ai-ghost-popover"
    :style="{ left: `${anchorX}px`, top: `${anchorY}px` }"
    @mousedown.stop
    @click.stop
  >
    <div v-if="awaitingIntent" class="ai-ghost-input">
      <input
        ref="intentInputRef"
        v-model="intentText"
        class="ai-ghost-intent-field"
        type="text"
        placeholder="Describe the next step…"
        @keydown.enter.stop.prevent="onSubmitIntent"
        @keydown.esc.stop.prevent="onDismiss"
      />
    </div>
    <div v-else-if="isLoading" class="ai-ghost-loading">
      <span class="ai-ghost-spinner" aria-hidden="true"></span>
      <span>Finding next node…</span>
    </div>
    <div v-else-if="aiDisabled" class="ai-ghost-empty">
      AI features are disabled. Ask an admin to enable them in settings.
    </div>
    <div v-else-if="degradedReason !== null" class="ai-ghost-empty">
      {{ degradedMessage }}
    </div>
    <ul v-else-if="suggestions.length > 0" class="ai-ghost-list">
      <li
        v-for="(suggestion, index) in suggestions"
        :key="`${suggestion.nodeType}:${index}`"
        class="ai-ghost-item"
        :class="{ 'ai-ghost-item--busy': pendingIndex === index }"
        @click.stop="onAccept(index)"
      >
        <div class="ai-ghost-item-header">
          <span class="ai-ghost-label">{{ suggestion.label || suggestion.nodeType }}</span>
          <span v-if="suggestion.predictedOutputSchema" class="ai-ghost-cols">
            {{ suggestion.predictedOutputSchema.length }} cols
          </span>
        </div>
        <p v-if="suggestion.description" class="ai-ghost-desc">
          {{ suggestion.description }}
        </p>
      </li>
    </ul>
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, ref, watch } from "vue";

import { useGhostNodeSuggestions } from "./useGhostNodeSuggestions";

interface Props {
  composable: ReturnType<typeof useGhostNodeSuggestions>;
}

const props = defineProps<Props>();
const emit = defineEmits<{ (e: "accepted"): void }>();
const pendingIndex = ref<number | null>(null);
const intentText = ref("");
const intentInputRef = ref<HTMLInputElement | null>(null);

const isVisible = computed(() => props.composable.isVisible.value);
const awaitingIntent = computed(() => props.composable.awaitingIntent.value);
const isLoading = computed(() => props.composable.isLoading.value);
const aiDisabled = computed(() => props.composable.aiDisabled.value);
const degradedReason = computed(() => props.composable.degradedReason.value);
const suggestions = computed(() => props.composable.suggestions.value);

const anchorX = computed(() => props.composable.anchor.value?.screenX ?? 0);
const anchorY = computed(() => props.composable.anchor.value?.screenY ?? 0);

// Focus (and reset) the inline box each time it opens.
watch(awaitingIntent, (open) => {
  if (open) {
    intentText.value = "";
    void nextTick(() => intentInputRef.value?.focus());
  }
});

const onSubmitIntent = (): void => {
  props.composable.submitIntent(intentText.value);
};

const onDismiss = (): void => {
  props.composable.clear();
};

const degradedMessage = computed(() => {
  switch (degradedReason.value) {
    case "upstream_schema_unknown":
      return "Run the upstream node first so AI can ground suggestions in its schema.";
    case "missing_upstream":
      return "Upstream node not found.";
    case "timeout":
      return "AI took too long — try again.";
    case "provider_error":
      return "AI provider error — check your provider configuration.";
    case "parse_error":
      return "AI returned an invalid response.";
    case "no_valid_suggestions":
      return "No suggestion fits this node's schema — try rephrasing.";
    default:
      return "AI couldn't suggest a next node.";
  }
});

const onAccept = async (index: number): Promise<void> => {
  if (pendingIndex.value !== null) return;
  pendingIndex.value = index;
  try {
    const newNodeId = await props.composable.acceptSuggestion(index);
    if (newNodeId !== null) emit("accepted");
  } finally {
    pendingIndex.value = null;
  }
};
</script>

<style scoped>
.ai-ghost-popover {
  position: fixed;
  z-index: var(--z-index-canvas-context-menu, 100002);
  min-width: 260px;
  max-width: 340px;
  padding: 4px;
  border: 1px solid var(--color-border-primary);
  border-radius: 6px;
  background-color: var(--color-background-primary);
  box-shadow: var(--shadow-lg);
  pointer-events: auto;
  font-family: var(--font-family-base);
  font-size: var(--font-size-md);
  color: var(--color-text-primary);
}

.ai-ghost-input {
  padding: 4px;
}

.ai-ghost-intent-field {
  width: 100%;
  box-sizing: border-box;
  padding: 7px 9px;
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  font-family: var(--font-family-base);
  font-size: var(--font-size-md);
  color: var(--color-text-primary);
  background-color: var(--color-background-primary);
  outline: none;
  transition: border-color 0.15s ease;
}

.ai-ghost-intent-field::placeholder {
  color: var(--color-text-tertiary);
}

.ai-ghost-intent-field:focus {
  border-color: var(--color-accent);
}

.ai-ghost-loading,
.ai-ghost-empty {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 12px;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
}

.ai-ghost-spinner {
  flex: 0 0 auto;
  width: 13px;
  height: 13px;
  border: 2px solid var(--color-border-primary);
  border-top-color: var(--color-accent);
  border-radius: 50%;
  animation: ai-ghost-spin 0.7s linear infinite;
}

@keyframes ai-ghost-spin {
  to {
    transform: rotate(360deg);
  }
}

.ai-ghost-list {
  margin: 0;
  padding: 0;
  list-style: none;
  display: flex;
  flex-direction: column;
}

.ai-ghost-item {
  padding: 8px 10px;
  border-radius: 4px;
  cursor: pointer;
  transition: background-color 0.15s ease;
}

.ai-ghost-item:hover {
  background-color: var(--color-background-hover);
}

.ai-ghost-item--busy {
  pointer-events: none;
  opacity: 0.55;
}

.ai-ghost-item-header {
  display: flex;
  align-items: baseline;
  gap: 8px;
}

.ai-ghost-label {
  flex: 1 1 auto;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-weight: 500;
  color: var(--color-text-primary);
}

.ai-ghost-cols {
  flex: 0 0 auto;
  color: var(--color-text-tertiary);
  font-size: var(--font-size-xs);
}

.ai-ghost-desc {
  margin: 3px 0 0;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
  line-height: 1.4;
}
</style>
