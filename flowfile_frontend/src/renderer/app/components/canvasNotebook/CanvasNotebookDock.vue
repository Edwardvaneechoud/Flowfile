<template>
  <aside
    class="canvas-notebook-dock"
    data-canvas-notebook
    :data-render-count="store.renderCount"
    :style="{ width: `${store.width}px` }"
    aria-label="Notebook"
  >
    <div
      class="cn-resizer"
      role="separator"
      aria-orientation="vertical"
      aria-label="Resize notebook"
      :class="{ dragging: resizing }"
      @pointerdown="startResize"
      @pointermove="onResize"
      @pointerup="endResize"
      @pointercancel="endResize"
    />
    <header class="cn-header">
      <span class="material-icons cn-header-icon" aria-hidden="true">menu_book</span>
      <span class="cn-title">Notebook</span>
      <span class="cn-subtitle">read-only</span>
      <button
        class="cn-close"
        type="button"
        aria-label="Close notebook"
        @click="store.setOpen(false)"
      >
        <span class="material-icons" aria-hidden="true">close</span>
      </button>
    </header>
    <p v-if="!store.sessionsEnabled" class="cn-note" data-testid="cn-sessions-disabled">
      Sessions are disabled on this server.
    </p>
    <p v-if="store.error" class="cn-error">{{ store.error }}</p>
    <ul v-if="store.warnings.length" class="cn-warnings">
      <li v-for="(warning, index) in store.warnings" :key="index">{{ warning }}</li>
    </ul>
    <div class="cn-cells">
      <canvas-notebook-cell v-for="cell in store.cells" :key="cell.cellId" :cell="cell" />
      <p v-if="!store.cells.length && !store.error" class="cn-empty">Rendering…</p>
    </div>
  </aside>
</template>

<script setup lang="ts">
import { onBeforeUnmount, ref, watch } from "vue";
import { CanvasNotebookApi } from "../../api/canvasNotebook.api";
import { mutationGeneration, whenMutationsIdle } from "../../services/axios.config";
import { clampDockWidth, useCanvasNotebookStore } from "../../stores/canvasNotebook-store";
import { useNodeStore } from "../../stores/column-store";
import { useEditorStore } from "../../stores/editor-store";
import CanvasNotebookCell from "./CanvasNotebookCell.vue";
import { createRenderSync } from "./renderSync";

const store = useCanvasNotebookStore();
const nodeStore = useNodeStore();
const editorStore = useEditorStore();

const errorMessage = (error: unknown): string => {
  const detail = (error as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
  if (typeof detail === "string") return detail;
  return error instanceof Error ? error.message : "Could not render the notebook.";
};

const sync = createRenderSync({
  getFlowId: () => (nodeStore.flow_id > 0 ? nodeStore.flow_id : null),
  fetchRendering: (flowId) => CanvasNotebookApi.render(flowId),
  whenMutationsIdle,
  mutationGeneration,
  apply: (flowId, rendering) => store.applyRendering(flowId, rendering),
  onError: (flowId, error) => store.setError(flowId, errorMessage(error)),
});

watch(
  () => nodeStore.flow_id,
  (flowId) => {
    store.resetForFlow(flowId > 0 ? flowId : null);
    void sync.refreshNow();
  },
  { immediate: true },
);

watch(
  () => editorStore.graphVersion,
  () => sync.schedule(),
);

onBeforeUnmount(() => sync.dispose());

const resizing = ref(false);
let startX = 0;
let startWidth = 0;

const startResize = (event: PointerEvent) => {
  resizing.value = true;
  startX = event.clientX;
  startWidth = store.width;
  (event.target as HTMLElement).setPointerCapture?.(event.pointerId);
  event.preventDefault();
};

const onResize = (event: PointerEvent) => {
  if (!resizing.value) return;
  store.setWidth(clampDockWidth(startWidth + startX - event.clientX, window.innerWidth));
};

const endResize = (event: PointerEvent) => {
  if (!resizing.value) return;
  resizing.value = false;
  (event.target as HTMLElement).releasePointerCapture?.(event.pointerId);
  store.setWidth(store.width, true);
};
</script>

<style scoped>
.canvas-notebook-dock {
  position: relative;
  flex: 0 0 auto;
  display: flex;
  flex-direction: column;
  min-width: 0;
  background: var(--color-background-secondary);
  border-left: 1px solid var(--color-border-primary);
  font-family: var(--font-family-base);
}
.cn-resizer {
  position: absolute;
  top: 0;
  bottom: 0;
  left: -3px;
  width: 6px;
  cursor: col-resize;
  z-index: 2;
  touch-action: none;
}
.cn-resizer:hover,
.cn-resizer.dragging {
  background: var(--el-color-primary-light-5, #a0cfff);
}
.cn-header {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 8px 12px;
  border-bottom: 1px solid var(--color-border-primary);
}
.cn-header-icon {
  font-size: 18px;
  color: var(--color-text-secondary);
}
.cn-title {
  font-weight: 600;
  font-size: 14px;
}
.cn-subtitle {
  flex: 1;
  font-size: 12px;
  color: var(--color-text-secondary);
}
.cn-close {
  display: inline-flex;
  border: none;
  background: transparent;
  cursor: pointer;
  color: var(--color-text-secondary);
  padding: 2px;
}
.cn-note,
.cn-error,
.cn-warnings {
  margin: 8px 12px 0;
  font-size: 12px;
}
.cn-note {
  color: var(--color-text-secondary);
}
.cn-error {
  color: var(--el-color-danger, #f56c6c);
}
.cn-warnings {
  padding-left: 16px;
  color: var(--el-color-warning, #e6a23c);
}
.cn-cells {
  flex: 1;
  overflow: auto;
  padding: 12px;
}
.cn-empty {
  font-size: 12px;
  color: var(--color-text-secondary);
}
</style>
