<script setup lang="ts">
// Floating ✨ button that toggles the AI assistant drawer.
//
// Anchored to the top-right of the canvas <main> element. Top-right keeps
// the trigger out of the bottom-right contention zone (MiniMap, layout
// controls, and bottom/right-anchored draggable panels) and frames AI as a
// primary app action rather than a corner utility.

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
    :class="{ 'is-open': isOpen }"
    title="AI Assistant"
    aria-label="Toggle AI assistant"
    @click="handleClick"
  >
    <span class="ai-trigger-btn__icon" aria-hidden="true">✨</span>
  </button>
</template>

<style scoped>
.ai-trigger-btn {
  position: absolute;
  top: 10px;
  right: 10px;
  width: 45px;
  height: 45px;
  border-radius: 50%;
  border: none;
  background: linear-gradient(
    135deg,
    var(--color-gradient-purple-start, #6f42c1) 0%,
    var(--color-gradient-purple-end, #5933a8) 100%
  );
  box-shadow: var(--shadow-md, 0 2px 8px rgba(0, 0, 0, 0.18));
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  /* Same z-index tier as layoutControls (FLOATING_WIDGET = 200). */
  z-index: 200;
  transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
}

.ai-trigger-btn:hover {
  width: 55px;
  height: 55px;
  box-shadow: var(--shadow-lg, 0 4px 12px rgba(0, 0, 0, 0.22));
}

.ai-trigger-btn.is-open {
  /* Slight visual cue when the drawer is showing — outline, not glow. */
  outline: 2px solid var(--color-focus-ring-purple-light, #d4c1ff);
  outline-offset: 2px;
}

.ai-trigger-btn__icon {
  font-size: 22px;
  line-height: 1;
  color: var(--color-text-inverse, #ffffff);
  pointer-events: none;
}
</style>
