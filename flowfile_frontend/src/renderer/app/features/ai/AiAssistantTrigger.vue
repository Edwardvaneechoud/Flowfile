<script setup lang="ts">
// Sparkle button that toggles the AI assistant drawer.
//
// Mounted inside the header's status-wrapper alongside the flow-status
// indicator and the results-panel toggle — same control-button visual
// language (32px circle, neutral default, primary-on-active) so the AI
// trigger reads as a peer panel-toggle, not a competing floating widget.

import { computed } from "vue";
import { useEditorStore } from "../../stores/editor-store";

const editorStore = useEditorStore();

const isOpen = computed(() => editorStore.isAiOpen);

const handleClick = (event: MouseEvent): void => {
  event.preventDefault();
  event.stopPropagation();
  editorStore.toggleAiDrawer();
};
</script>

<template>
  <button
    class="ai-trigger-btn"
    :class="{ 'is-active': isOpen }"
    title="AI Assistant"
    aria-label="Toggle AI assistant"
    @click="handleClick"
  >
    <span class="ai-trigger-btn__icon" aria-hidden="true">✨</span>
  </button>
</template>

<style scoped>
.ai-trigger-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  border-radius: 50%;
  border: none;
  background: var(--color-gray-400);
  color: var(--color-button-primary);
  cursor: pointer;
  transition: all 0.3s ease;
  box-shadow: var(--shadow-sm);
}

.ai-trigger-btn:hover {
  background: var(--color-button-primary);
  color: var(--color-text-inverse);
  transform: translateY(-1px);
  box-shadow: var(--shadow-md);
}

.ai-trigger-btn.is-active {
  background: var(--color-button-primary);
  color: var(--color-text-inverse);
}

.ai-trigger-btn__icon {
  font-size: 16px;
  line-height: 1;
  pointer-events: none;
}
</style>
