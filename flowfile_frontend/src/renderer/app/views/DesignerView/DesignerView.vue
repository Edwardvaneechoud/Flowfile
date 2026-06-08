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
    <!-- Empty state when initial load completed but no flows are active. -->
    <div v-else-if="flowsActive.length === 0" class="empty-state">
      <div class="empty-state-content">
        <span class="empty-state-badge">
          <span class="material-icons">account_tree</span>
        </span>
        <h2 class="empty-state-heading">No Active Flows</h2>
        <p class="empty-state-text">
          Create a new flow, open an existing one, or head back to Home.
        </p>
        <div class="empty-state-actions">
          <button class="es-btn es-btn--primary" @click="createFlowDialog">
            <span class="material-icons">add</span>
            Create new flow
          </button>
          <button class="es-btn" @click="openFlowDialog">
            <span class="material-icons">folder_open</span>
            Open existing flow
          </button>
          <button class="es-btn" @click="openQuickCreateDialog">
            <span class="material-icons">bolt</span>
            Quick create
          </button>
          <button class="es-btn" @click="browseTemplates">
            <span class="material-icons">layers</span>
            Browse Templates
          </button>
          <button class="es-btn es-btn--ghost" @click="goHome">
            <span class="material-icons">home</span>
            Go to Home
          </button>
        </div>
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
      <div v-if="showSwitchIndicator" class="switch-indicator" aria-live="polite">
        <span class="switch-spinner" />
        <span>Loading flow…</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, nextTick } from "vue";
import { useRouter } from "vue-router";
import HeaderButtons from "../../components/layout/Header/HeaderButtons.vue";
import RightActionCluster from "../../components/layout/Header/RightActionCluster.vue";
import CanvasFlow from "./Canvas.vue";
import FlowSelector from "../FlowSelectorView/FlowSelectorView.vue";
import UndoRedoControls from "./UndoRedoControls.vue";
import { FlowApi } from "../../api";
import { fetchNodes } from "../../features/designer/utils";
import type { NodeTemplate, FlowSettings } from "../../types";
import { useNodeStore } from "../../stores/column-store";
import { useFlowOpener } from "../../composables/useFlowOpener";

const router = useRouter();

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

const handleCloseFlow = async (flowId: number) => {
  try {
    console.log("Closing flow:", flowId);

    const isCurrentFlow = nodeStore.flow_id === flowId;

    await closeFlow(flowId);

    nodeStore.clearFlowResults(flowId);
    nodeStore.clearFlowDescriptionCache(flowId);
    isSwitching.value = true;

    await fetchActiveFlows();

    if (isCurrentFlow) {
      if (flowsActive.value.length > 0) {
        const newFlowId = flowsActive.value[0].flow_id;
        console.log("Switching to flow:", newFlowId);
        await handleFlowChange(newFlowId);
      } else {
        // No flows left — Canvas's watcher clears the canvas on flowId<=0.
        nodeStore.setFlowId(-1);
      }
    }
  } catch (error) {
    console.error("Error closing flow:", error);
  } finally {
    isSwitching.value = false;
  }
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
    if (canvasFlow.value && flowsActive.value.length > 0) {
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

const createFlowDialog = () => {
  headerButtons.value?.openCreateDialog();
};

const openFlowDialog = () => {
  headerButtons.value?.openOpenDialog();
};

const openQuickCreateDialog = () => {
  headerButtons.value?.handleQuickCreate();
};

const browseTemplates = () => {
  router.push({ name: "templates" });
};

const goHome = () => {
  router.push({ name: "home" });
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
    const persistedFlowId = nodeStore.flow_id;
    const persistedIsActive =
      persistedFlowId > 0 && flows.some((f) => f.flow_id === persistedFlowId);

    if (flows.length === 0) {
      // Drop any stale persisted ID so `hasOpenFlow` guards don't render
      // canvas-only controls (right-cluster, undo/redo, Save) over the
      // "No Active Flows" empty state.
      if (persistedFlowId > 0) nodeStore.setFlowId(-1);
    } else if (!persistedIsActive) {
      console.log("Setting initial flow ID to:", flows[0].flow_id);
      nodeStore.setFlowId(flows[0].flow_id);
    } else {
      console.log("Using existing flow ID:", persistedFlowId);
    }

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

/* Empty state styles */
.empty-state {
  height: calc(100vh - 50px);
  display: flex;
  justify-content: center;
  align-items: center;
  background-color: var(--color-background-secondary);
}

.empty-state-content {
  text-align: center;
  padding: var(--spacing-8);
  max-width: 720px;
}

.empty-state-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 56px;
  height: 56px;
  border-radius: var(--border-radius-xl);
  background-color: var(--color-accent-subtle);
  color: var(--color-accent);
  margin-bottom: var(--spacing-4);
}

.empty-state-badge .material-icons {
  font-size: 28px;
}

.empty-state-heading {
  margin: 0 0 var(--spacing-2);
  font-size: var(--font-size-2xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.empty-state-text {
  margin: 0 0 var(--spacing-6);
  font-size: var(--font-size-base);
  color: var(--color-text-tertiary);
}

.empty-state-actions {
  display: flex;
  flex-wrap: wrap;
  justify-content: center;
  gap: var(--spacing-2);
}

.es-btn {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1-5);
  height: 38px;
  padding: 0 var(--spacing-4);
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-lg);
  cursor: pointer;
  font-family: inherit;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
  transition: all var(--transition-fast);
}

.es-btn:hover {
  border-color: var(--color-accent);
  color: var(--color-accent);
  box-shadow: var(--shadow-xs);
}

.es-btn .material-icons {
  font-size: 18px;
}

.es-btn--primary {
  background-color: var(--color-accent);
  border-color: var(--color-accent);
  color: #ffffff;
}

.es-btn--primary:hover {
  background-color: var(--color-accent-hover);
  border-color: var(--color-accent-hover);
  color: #ffffff;
}

.es-btn--ghost {
  background: transparent;
  border-color: transparent;
  color: var(--color-text-tertiary);
}

.es-btn--ghost:hover {
  border-color: transparent;
  background-color: var(--color-background-tertiary);
  color: var(--color-accent);
}
</style>
