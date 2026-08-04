<template>
  <div v-if="isActive" class="file-drop-overlay" aria-live="polite">
    <div class="file-drop-card">
      <span class="material-icons file-drop-icon">upload_file</span>
      <p class="file-drop-title">{{ title }}</p>
      <div class="file-drop-chips">
        <span v-for="chip in chips" :key="chip" class="file-drop-chip">{{ chip }}</span>
      </div>
      <p class="file-drop-hint">{{ hint }}</p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";

import type { FileDropImport } from "../../composables/useFileDropImport";
import { canLinkDroppedFiles, isDesktop } from "../../../lib/desktop";

const props = defineProps<{ controller: FileDropImport }>();

const isActive = computed(() => props.controller.isDragActive.value);
const summary = computed(() => props.controller.dragSummary.value);
const count = computed(() => Math.max(1, summary.value.fileCount));

const title = computed(() => `Drop to add ${count.value} Read node${count.value === 1 ? "" : "s"}`);
const chips = computed(() =>
  summary.value.allUnknown
    ? [`${count.value} file${count.value === 1 ? "" : "s"}`]
    : summary.value.typeLabels,
);
const hint = computed(() => {
  if (canLinkDroppedFiles) return "Files are linked when possible, otherwise copied locally";
  if (isDesktop) return "You will be asked before files are copied locally";
  return "You will be asked before anything is uploaded";
});
</script>

<style scoped>
/* Above every DraggableItem panel and the context menu, below Element Plus toasts. */
.file-drop-overlay {
  position: absolute;
  inset: 0;
  z-index: 1500;
  display: flex;
  align-items: center;
  justify-content: center;
  /* Load-bearing: the overlay must never become the drag target. */
  pointer-events: none;
  background-color: color-mix(in srgb, var(--color-background-primary) 72%, transparent);
}

.file-drop-card {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 8px;
  padding: 28px 40px;
  border: 2px dashed var(--color-primary);
  border-radius: 12px;
  background-color: var(--color-background-primary);
  box-shadow: var(--shadow-lg);
  animation: file-drop-in 120ms ease-out;
}

.file-drop-icon {
  font-size: 32px;
  color: var(--color-primary);
}

.file-drop-title {
  margin: 0;
  font-size: 16px;
  font-weight: 600;
  color: var(--color-text-primary);
}

.file-drop-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  justify-content: center;
}

.file-drop-chip {
  padding: 2px 10px;
  border-radius: 10px;
  background-color: var(--color-background-secondary);
  color: var(--color-text-secondary);
  font-size: 12px;
}

.file-drop-hint {
  margin: 0;
  font-size: 12px;
  color: var(--color-text-secondary);
}

@keyframes file-drop-in {
  from {
    transform: scale(0.97);
    opacity: 0;
  }
}

@media (prefers-reduced-motion: reduce) {
  .file-drop-card {
    animation: none;
  }
}
</style>
