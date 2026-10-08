<template>
  <div class="notebook-window" data-testid="notebook-window">
    <header class="notebook-window__bar">
      <span class="notebook-window__title" :title="title">{{ title }}</span>
      <button
        type="button"
        class="notebook-window__return"
        data-testid="notebook-window-return"
        @click="returnToDesigner"
      >
        <i class="fa-solid fa-arrow-right-to-bracket" aria-hidden="true"></i>
        Return to designer
      </button>
    </header>
    <div v-if="state === 'ready'" class="notebook-window__body">
      <NotebookPanel :key="flowId" :flow-id="flowId" host="window" />
    </div>
    <div v-else-if="state === 'missing'" class="notebook-window__empty">
      <p>This flow is not open in Flowfile.</p>
      <button type="button" class="notebook-window__return" @click="closeThisWindow">
        Close window
      </button>
    </div>
    <div v-else class="notebook-window__empty"><p>Loading the flow…</p></div>
  </div>
</template>

<script lang="ts" setup>
// One flow's canvas notebook in its own window. Core's change feed keeps it in step with the
// designer: a foreign canvas edit re-renders the cells, a run disables Push while it lasts, a
// closed flow closes the window, a Save As follows the flow to its new id.
import { computed, onMounted, ref, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import { ElMessage, ElMessageBox } from "element-plus";
import NotebookPanel from "../CatalogView/NotebookPanel.vue";
import { FlowApi } from "../../api";
import { desktop } from "../../../lib/desktop";
import { parseFlowQuery, windowTitle } from "../../services/popoutWindow";
import { useEditorStore } from "../../stores/editor-store";
import { useFlowStore } from "../../stores/flow-store";
import { useFlowSyncStore } from "../../stores/flow-sync-store";
import { flowNeedsSync, useNotebookStore } from "../../stores/notebook-store";

const route = useRoute();
const router = useRouter();
const flowStore = useFlowStore();
const editorStore = useEditorStore();
const notebookStore = useNotebookStore();
const flowSync = useFlowSyncStore();

const flowId = computed(() => parseFlowQuery(route.query.flow));
const flowName = ref<string | null>(null);
const state = ref<"loading" | "ready" | "missing">("loading");
const title = computed(() => windowTitle(flowName.value));

async function loadFlow(id: number): Promise<void> {
  state.value = "loading";
  const settings = await FlowApi.getFlowSettings(id);
  if (flowId.value !== id) return;
  if (!settings) {
    state.value = "missing";
    document.title = windowTitle(null);
    return;
  }
  flowName.value = settings.name;
  editorStore.isRunning = !!settings.is_running;
  document.title = title.value;
  state.value = "ready";
}

// What the designer's header does on a run-state event: re-read whether the flow runs.
async function refreshRunState(): Promise<void> {
  const settings = await FlowApi.getFlowSettings(flowId.value);
  if (settings) editorStore.isRunning = !!settings.is_running;
}

onMounted(() => {
  // This window's notebook store must not restore, nor write over, the designer's catalog tabs.
  notebookStore.setPersistence(false);
});

watch(
  flowId,
  (id) => {
    if (id > 0) {
      flowStore.setFlowId(id);
      void loadFlow(id);
    } else {
      state.value = "missing";
    }
  },
  { immediate: true },
);

// Without a canvas, a reload request becomes the graph-version bump the panel re-renders on.
watch(
  () => flowStore.pendingReloadCounter,
  () => editorStore.bumpGraphVersion(),
);

watch(
  () => flowStore.pendingRunStateCounter,
  () => void refreshRunState(),
);

watch(
  () => flowSync.closeCount,
  () => {
    if (flowSync.closedFlowId !== flowId.value) return;
    ElMessage.info("The flow was closed in the designer.");
    void closeThisWindow();
  },
);

watch(
  () => flowSync.rekeyedTo,
  (moved) => {
    if (!moved || moved.from !== flowId.value) return;
    const old = notebookStore.openNotebooks.find((n) => n.flowId === moved.from);
    if (old) notebookStore.closeTab(old.tabId);
    void router.replace({ query: { ...route.query, flow: String(moved.to) } });
  },
);

const hasUnpushedEdits = (): boolean => {
  const nb = notebookStore.openNotebooks.find((n) => n.flowId === flowId.value);
  return !!nb && flowNeedsSync(nb);
};

async function returnToDesigner(): Promise<void> {
  if (hasUnpushedEdits()) {
    try {
      await ElMessageBox.confirm(
        "Cells edited here were not pushed to the canvas yet; closing the window discards them.",
        "Close the notebook?",
        { confirmButtonText: "Close", cancelButtonText: "Keep open", type: "warning" },
      );
    } catch {
      return;
    }
  }
  await closeThisWindow();
}

async function closeThisWindow(): Promise<void> {
  await desktop.closeCurrentWindow();
}
</script>

<style scoped>
.notebook-window {
  display: flex;
  flex-direction: column;
  height: 100vh;
  background: var(--color-background-primary);
  color: var(--color-text-primary);
}

.notebook-window__bar {
  display: flex;
  flex: none;
  align-items: center;
  gap: var(--spacing-sm);
  height: 40px;
  padding: 0 var(--spacing-md);
  border-bottom: 1px solid var(--color-border-light);
  background: var(--color-background-secondary);
}

.notebook-window__title {
  flex: 1;
  min-width: 0;
  overflow: hidden;
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.notebook-window__return {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 4px 10px;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  font-size: var(--font-size-sm);
  cursor: pointer;
}

.notebook-window__return:hover {
  background: var(--color-background-tertiary);
}

.notebook-window__body {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
}

.notebook-window__body > :deep(.notebook-panel) {
  flex: 1;
  min-height: 0;
}

.notebook-window__empty {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-sm);
  color: var(--color-text-secondary);
}
</style>
