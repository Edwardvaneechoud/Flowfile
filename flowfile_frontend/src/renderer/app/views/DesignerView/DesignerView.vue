<template>
  <div class="designer-view">
    <div v-if="initialLoadComplete" class="header">
      <div class="header-top">
        <div class="left-section">
          <header-buttons
            ref="headerButtons"
            @open-flow="openFlow"
            @refresh-flow="refreshFlow"
            @flow-saved="handleFlowSaved"
          />
          <undo-redo-controls v-if="hasOpenFlow" @refresh-flow="refreshFlow" />
        </div>
      </div>
      <div class="header-bottom">
        <div class="middle-section">
          <flow-selector
            ref="flowSelector"
            @flow-changed="handleFlowChange"
            @close-tab="handleCloseFlow"
            @close-tabs="handleCloseFlows"
            @flow-renamed="handleFlowRenamed"
            @create-flow="headerButtons?.handleQuickCreate()"
          />
        </div>
        <div v-if="hasOpenFlow" class="right-section">
          <right-action-cluster ref="rightCluster" @open-settings="headerButtons?.openSettings()" />
        </div>
      </div>
    </div>
    <!-- Initial-boot loading state: shown only until the first setup completes.
         After that the canvas stays mounted across all flow switches. -->
    <div v-if="!initialLoadComplete" class="loading-state">
      <div class="loading-state-content">
        <p>Loading flows...</p>
      </div>
    </div>
    <div v-else class="canvas-wrap">
      <canvas-flow
        ref="canvasFlow"
        class="canvas"
        @save="headerButtons?.openSaveModal()"
        @run="rightCluster?.runFlow()"
        @new="headerButtons?.handleQuickCreate()"
        @open="headerButtons?.openOpenDialog()"
        @open-settings="headerButtons?.openSettings()"
      />
      <designer-empty-state
        v-if="!hasOpenFlow"
        @new-flow="headerButtons?.handleQuickCreate()"
        @open-flow="headerButtons?.openOpenDialog()"
        @open-recent="handleOpenRecent"
      />
      <div v-if="showSwitchIndicator" class="switch-indicator" aria-live="polite">
        <span class="switch-spinner" />
        <span>Loading flow…</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, nextTick, watch } from "vue";
import HeaderButtons from "../../components/layout/Header/HeaderButtons.vue";
import RightActionCluster from "../../components/layout/Header/RightActionCluster.vue";
import CanvasFlow from "./Canvas.vue";
import FlowSelector from "../FlowSelectorView/FlowSelectorView.vue";
import UndoRedoControls from "./UndoRedoControls.vue";
import DesignerEmptyState from "./DesignerEmptyState.vue";
import { FlowApi } from "../../api";
import { fetchNodes } from "../../features/designer/utils";
import type { NodeTemplate, FlowSettings } from "../../types";
import { useNodeStore } from "../../stores/column-store";
import { useEditorStore } from "../../stores/editor-store";
import { useFlowOpener } from "../../composables/useFlowOpener";
import type { RecentFlow } from "../../composables/useRecentFlows";
import { resolveBootFlowId, resolveNextFlowAfterClose } from "./flowSessionState";

const getAllFlows = FlowApi.getAllFlows;
const closeFlow = FlowApi.closeFlow;

const flowsActive = ref<FlowSettings[]>([]);
// isLoading gates only the initial app boot — once the first load completes,
// the canvas stays mounted across flow changes. Use isSwitching for inline
// indicators during subsequent flow operations.
const isLoading = ref(true);
const isSwitching = ref(false);
const canvasFlow = ref<InstanceType<typeof CanvasFlow>>();
const headerButtons = ref<InstanceType<typeof HeaderButtons>>();
const rightCluster = ref<InstanceType<typeof RightActionCluster>>();
const flowSelector = ref<InstanceType<typeof FlowSelector>>();
const nodeOptions = ref<NodeTemplate[]>([]);
const initialLoadComplete = ref(false);

const nodeStore = useNodeStore();
const editorStore = useEditorStore();
const { openFlow: openFlowFromPath } = useFlowOpener();

// Hide undo/redo when no flow is loaded — same gating as the Save button.
const hasOpenFlow = computed(() => !!nodeStore.flow_id && nodeStore.flow_id > 0);

// Spinner stays visible across the whole switch sequence: from "user clicked"
// (isSwitching) through the Canvas watcher's async loadFlow (isLoadingFlow).
const showSwitchIndicator = computed(
  () => isSwitching.value || canvasFlow.value?.isLoadingFlow === true,
);

const fetchActiveFlows = async () => {
  try {
    const flows = await getAllFlows();
    flowsActive.value = flows;

    if (flowSelector.value) {
      await flowSelector.value.loadFlows();
    }
    return flows;
  } catch (error) {
    console.error("Failed to load active flows:", error);
    return [];
  }
};

const openFlow = (eventData: {
  message: string;
  flowPath: string;
  flowName?: string;
  catalogRef?: string;
}) => {
  reloadCanvas(eventData.flowPath, { name: eventData.flowName, catalogRef: eventData.catalogRef });
};

const reloadCanvas = async (flowPath: string, meta?: { name?: string; catalogRef?: string }) => {
  isSwitching.value = true;
  try {
    // openFlow owns importFlow + setFlowId + the recents record/prune contract.
    const flowId = await openFlowFromPath(flowPath, meta);
    if (flowId === null) return;
    if (headerButtons.value) {
      await headerButtons.value.loadFlowSettings();
    }
    await fetchActiveFlows();
  } finally {
    isSwitching.value = false;
  }
};

const handleOpenRecent = (flow: RecentFlow) => {
  reloadCanvas(flow.path, { name: flow.name, catalogRef: flow.catalogRef });
};

// A run_flow node's "Go to Flow" menu dispatches this; route it through the
// same reloadCanvas the catalog/header use so the tab strip + header refresh.
watch(
  () => editorStore.openFlowRequest.token,
  () => {
    const { flowPath, name } = editorStore.openFlowRequest;
    if (flowPath) reloadCanvas(flowPath, { name });
  },
);

const handleCloseFlow = async (flowId: number) => {
  await handleCloseFlows([flowId]);
};

// Bulk closes route through here with one list refresh + one active-flow switch —
// per-flow close-tab emits would cascade intermediate flow loads through the canvas.
const handleCloseFlows = async (flowIds: number[]) => {
  if (flowIds.length === 0) return;
  try {
    console.log("Closing flows:", flowIds);

    const currentFlowId = nodeStore.flow_id;
    isSwitching.value = true;

    for (const flowId of flowIds) {
      await closeFlow(flowId);
      nodeStore.clearFlowResults(flowId);
      nodeStore.clearFlowDescriptionCache(flowId);
    }

    await fetchActiveFlows();

    const next = resolveNextFlowAfterClose(
      flowsActive.value.map((f) => f.flow_id),
      flowIds,
      currentFlowId,
    );
    if (next !== null) {
      if (next > 0) {
        console.log("Switching to flow:", next);
        await handleFlowChange(next);
      } else {
        // Last flow closed — the empty state takes over (no auto-created flow).
        nodeStore.setFlowId(-1);
      }
    }
  } catch (error) {
    console.error("Error closing flows:", error);
  } finally {
    isSwitching.value = false;
  }
};

// HeaderButtons caches a full FlowSettings and posts it back wholesale from the
// settings modal — refresh it so a stale cached name can't revert the rename.
const handleFlowRenamed = async () => {
  await headerButtons.value?.loadFlowSettings();
};

const handleFlowChange = async (flowId: number) => {
  if (isSwitching.value && flowId === nodeStore.flow_id) {
    console.log("Already loading flow ID:", flowId);
    return;
  }

  isSwitching.value = true;
  try {
    console.log("Handling flow change to:", flowId);
    // setFlowId triggers the Canvas watcher which loads the flow. The watcher
    // is the single source of truth for kicking off loadFlow — no explicit
    // canvasFlow.value.loadFlow() call here.
    nodeStore.setFlowId(flowId);
    if (headerButtons.value) {
      await headerButtons.value.loadFlowSettings();
    }
  } finally {
    isSwitching.value = false;
  }
};

const handleFlowSaved = (flowId: number) => {
  flowSelector.value?.refreshDirtyState(flowId);
};

const refreshFlow = async () => {
  isSwitching.value = true;
  try {
    console.log("refreshFlow");
    await fetchActiveFlows();
    // Same flowId — watcher won't fire, so trigger reload explicitly.
    if (canvasFlow.value && flowsActive.value.length > 0 && nodeStore.flow_id > 0) {
      await canvasFlow.value.reloadCurrentFlow();
    }
    console.log("refreshFlow end");
    if (headerButtons.value) {
      await headerButtons.value.loadFlowSettings();
    }
  } finally {
    isSwitching.value = false;
  }
};

const initialSetup = async () => {
  if (initialLoadComplete.value) {
    console.log("Initial setup already completed");
    return;
  }

  isLoading.value = true;
  console.log("Starting initial setup");

  try {
    const [nodes, flows] = await Promise.all([fetchNodes(), fetchActiveFlows()]);

    nodeOptions.value = nodes;
    // Zero flows parks at -1 (empty state) — also clears a stale persisted id
    // so the Canvas watcher never 404s against a session gone from the backend.
    const bootFlowId = resolveBootFlowId(
      flows.map((f) => f.flow_id),
      nodeStore.flow_id,
    );
    console.log("Setting initial flow ID to:", bootFlowId);
    nodeStore.setFlowId(bootFlowId);

    console.log("Initial setup completed");
  } catch (error) {
    console.error("Error during initial setup:", error);
  } finally {
    // Mark initial load complete even on error so the header still appears
    // and the user can retry via the refresh button.
    initialLoadComplete.value = true;
    isLoading.value = false;
    if (nodeStore.flow_id && nodeStore.flow_id > 0) {
      await nextTick();
      await headerButtons.value?.loadFlowSettings();
    }
  }
};

onMounted(async () => {
  console.log("Component mounted, starting initialization");
  await initialSetup();
});
</script>

<style scoped>
.designer-view {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.canvas-wrap {
  position: relative;
  height: calc(100vh - 100px);
}

.canvas {
  height: 100%;
}

.switch-indicator {
  position: absolute;
  top: 12px;
  right: 12px;
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 12px;
  border-radius: 6px;
  background-color: var(--color-background-secondary, rgba(255, 255, 255, 0.95));
  border: 1px solid var(--color-border-primary, #d4d7de);
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
  font-size: 13px;
  pointer-events: none;
  z-index: 10;
}

.switch-spinner {
  width: 12px;
  height: 12px;
  border: 2px solid var(--color-border-primary, #d4d7de);
  border-top-color: var(--color-primary, #409eff);
  border-radius: 50%;
  animation: switch-spin 0.8s linear infinite;
}

@keyframes switch-spin {
  to {
    transform: rotate(360deg);
  }
}

.header {
  background-color: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-primary);
}

/* Desktop layout - single row */
@media (min-width: 1025px) {
  .header {
    display: flex;
    justify-content: space-between;
    align-items: stretch;
    height: 50px;
  }

  .header-top {
    display: contents;
  }

  .header-bottom {
    display: contents;
  }

  .left-section {
    min-width: 250px;
    padding: 0 var(--spacing-4);
    display: flex;
    align-items: center;
  }

  .middle-section {
    flex: 1;
    display: flex;
    align-items: center;
    overflow: hidden;
  }

  .right-section {
    padding: 0 var(--spacing-4);
    display: flex;
    align-items: center;
    justify-content: flex-end;
  }

  .canvas {
    height: calc(100vh - 50px);
  }
}

/* Mobile/tablet layout - stacked */
@media (max-width: 1024px) {
  .header {
    height: auto;
    min-height: 80px;
  }

  .header-top {
    display: flex;
    justify-content: space-between;
    align-items: center;
    height: 50px;
    border-bottom: 1px solid var(--color-border-primary);
  }

  .header-bottom {
    display: flex;
    height: 40px;
    padding: 0 var(--spacing-2);
  }

  .left-section {
    padding: 0 var(--spacing-3);
    display: flex;
    align-items: center;
  }

  .middle-section {
    flex: 1;
    display: flex;
    align-items: center;
    overflow: hidden;
    padding: 0 var(--spacing-2);
  }

  .right-section {
    padding: 0 var(--spacing-3);
    display: flex;
    align-items: center;
    justify-content: flex-end;
  }

  .canvas {
    height: calc(100vh - 90px);
  }
}

/* Very narrow screens */
@media (max-width: 480px) {
  .left-section {
    padding: 0 var(--spacing-2);
    min-width: auto;
  }

  .right-section {
    padding: 0 var(--spacing-2);
    min-width: auto;
  }

  .middle-section {
    padding: 0 var(--spacing-1);
  }
}

/* Loading state styles */
.loading-state {
  height: calc(100vh - 50px);
  display: flex;
  justify-content: center;
  align-items: center;
  background-color: var(--color-background-secondary);
}

.loading-state-content {
  text-align: center;
  padding: var(--spacing-8);
}

.loading-state-content p {
  color: var(--color-text-secondary);
  margin-top: var(--spacing-4);
}
</style>
