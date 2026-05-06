<script setup lang="ts">
// "Ask AI" header pill — toggles the AI assistant sidebar.
//
// Click toggles the AI sidebar (editorStore.isAiOpen). The kbd hint
// advertises the Cmd+K command palette, which is bound directly in
// Canvas.vue and is the only path to the palette now that the dropdown
// caret has been removed.

import { computed } from "vue";
import { useEditorStore } from "../../stores/editor-store";

const editorStore = useEditorStore();

const isOpen = computed(() => editorStore.isAiOpen);
const isMac = computed(
  () => typeof navigator !== "undefined" && /Mac|iPhone|iPad/.test(navigator.platform),
);
const modifierLabel = computed(() => (isMac.value ? "⌘" : "Ctrl"));

const handleClick = (event: MouseEvent): void => {
  event.preventDefault();
  event.stopPropagation();
  editorStore.toggleAiDrawer();
};
</script>

<template>
  <button
    class="action-btn ai-trigger__pill"
    :class="{ active: isOpen }"
    type="button"
    title="AI Assistant"
    aria-label="Toggle AI assistant"
    @click="handleClick"
  >
    <span class="material-icons btn-icon" aria-hidden="true">auto_awesome</span>
    <span class="btn-text">Ask AI</span>
    <span class="ai-trigger__shortcut" aria-hidden="true">
      <kbd>{{ modifierLabel }}</kbd>
      <kbd>K</kbd>
    </span>
  </button>
</template>

<style scoped>
.action-btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  height: 28px;
  padding: 0 10px;
  border-radius: 6px;
  border: 1px solid var(--color-border-primary);
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  font-size: 12px;
  font-weight: 500;
  letter-spacing: 0.01em;
  cursor: pointer;
  transition:
    background var(--transition-fast),
    border-color var(--transition-fast),
    color var(--transition-fast),
    box-shadow var(--transition-fast),
    transform var(--transition-fast);
  box-shadow: var(--shadow-xs);
  white-space: nowrap;
}

.action-btn:hover {
  border-color: var(--color-border-secondary, var(--color-border-primary));
  color: var(--color-text-primary);
  box-shadow: var(--shadow-sm);
}

.action-btn:active {
  transform: translateY(1px);
  box-shadow: none;
}

.action-btn.active {
  background: linear-gradient(
    135deg,
    var(--color-accent-purple) 0%,
    var(--color-accent-purple-hover) 100%
  );
  border-color: var(--color-accent-purple-hover);
  color: #ffffff;
}

.action-btn.active:hover {
  background: var(--color-accent-purple-hover);
  border-color: var(--color-accent-purple-hover);
}

.btn-icon {
  font-size: 14px;
  line-height: 1;
  color: var(--color-text-secondary, #6b7280);
}

.action-btn:hover .btn-icon,
.action-btn.active .btn-icon {
  color: inherit;
}

.btn-text {
  line-height: 1;
}

.ai-trigger__shortcut {
  display: inline-flex;
  gap: 2px;
  margin-left: 4px;
}

.ai-trigger__shortcut kbd {
  font-family: ui-monospace, SFMono-Regular, "SF Mono", monospace;
  font-size: 9px;
  background: var(--color-gray-200, #e5e7eb);
  border: 1px solid var(--color-border-primary, #d1d5db);
  border-radius: 3px;
  padding: 0 3px;
  color: var(--color-text-secondary, #6b7280);
  line-height: 1.4;
}

.action-btn.active .ai-trigger__shortcut kbd {
  background: rgba(255, 255, 255, 0.18);
  border-color: rgba(255, 255, 255, 0.32);
  color: #ffffff;
}
</style>
