<template>
  <div
    v-if="isVisible"
    class="ai-ghost-popover"
    :style="{ left: `${anchorX}px`, top: `${anchorY}px` }"
    @mousedown.stop
    @click.stop
  >
    <div v-if="isLoading" class="ai-ghost-loading">
      <span class="ai-ghost-spark">✨</span>
      <span>Suggesting next node…</span>
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
          <span class="ai-ghost-spark">✨</span>
          <span class="ai-ghost-label">{{ suggestion.label || suggestion.nodeType }}</span>
        </div>
        <div class="ai-ghost-item-meta">
          <span class="ai-ghost-type">{{ suggestion.nodeType }}</span>
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
import { computed, ref } from "vue";

import { useGhostNodeSuggestions } from "./useGhostNodeSuggestions";

interface Props {
  composable: ReturnType<typeof useGhostNodeSuggestions>;
}

const props = defineProps<Props>();
const pendingIndex = ref<number | null>(null);

const isVisible = computed(() => props.composable.isVisible.value);
const isLoading = computed(() => props.composable.isLoading.value);
const aiDisabled = computed(() => props.composable.aiDisabled.value);
const degradedReason = computed(() => props.composable.degradedReason.value);
const suggestions = computed(() => props.composable.suggestions.value);

const anchorX = computed(() => props.composable.anchor.value?.edgeMidX ?? 0);
const anchorY = computed(() => props.composable.anchor.value?.edgeMidY ?? 0);

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
      return "No schema-grounded suggestion fits this edge.";
    default:
      return "AI couldn't suggest a next node.";
  }
});

const onAccept = async (index: number): Promise<void> => {
  if (pendingIndex.value !== null) return;
  pendingIndex.value = index;
  try {
    await props.composable.acceptSuggestion(index);
  } finally {
    pendingIndex.value = null;
  }
};
</script>

<style scoped>
.ai-ghost-popover {
  position: absolute;
  z-index: 999;
  min-width: 280px;
  max-width: 360px;
  padding: 8px;
  border: 1.5px dashed #6366f1;
  border-radius: 10px;
  background: rgba(255, 255, 255, 0.97);
  box-shadow: 0 6px 24px rgba(0, 0, 0, 0.12);
  backdrop-filter: blur(2px);
  pointer-events: auto;
  font-size: 13px;
  color: #1f2937;
}

.ai-ghost-loading,
.ai-ghost-empty {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 6px 8px;
  color: #4b5563;
  font-style: italic;
}

.ai-ghost-list {
  margin: 0;
  padding: 0;
  list-style: none;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.ai-ghost-item {
  padding: 6px 8px;
  border-radius: 6px;
  cursor: pointer;
  transition: background 0.12s ease;
  opacity: 0.85;
}

.ai-ghost-item:hover {
  background: rgba(99, 102, 241, 0.08);
  opacity: 1;
}

.ai-ghost-item--busy {
  pointer-events: none;
  opacity: 0.5;
}

.ai-ghost-item-header {
  display: flex;
  align-items: center;
  gap: 6px;
  font-weight: 600;
}

.ai-ghost-item-meta {
  display: flex;
  gap: 8px;
  margin-top: 2px;
  color: #6b7280;
  font-size: 11px;
}

.ai-ghost-type {
  font-family: ui-monospace, SFMono-Regular, monospace;
}

.ai-ghost-desc {
  margin: 4px 0 0;
  color: #4b5563;
  font-size: 12px;
  line-height: 1.4;
}

.ai-ghost-spark {
  color: #6366f1;
  font-size: 14px;
}

.ai-ghost-label {
  flex: 1 1 auto;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
</style>
