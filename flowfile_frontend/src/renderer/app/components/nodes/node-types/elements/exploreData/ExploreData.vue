<script lang="ts" setup>
import { ref, computed, watch, onBeforeUnmount } from "vue";
import { CodeLoader } from "vue-content-loader";
import { ElMessage } from "element-plus";
import type { IMutField, IChart } from "@kanaries/graphic-walker/interfaces";
import VueGraphicWalker from "./vueGraphicWalker/VueGraphicWalker.vue";
import type { NodeGraphicWalker } from "./vueGraphicWalker/interfaces";
import {
  computeNodeVisualization,
  fetchGraphicWalkerData,
  fetchNodeVisualizationFields,
} from "./vueGraphicWalker/utils";
import EmptyState from "../../../../common/EmptyState/EmptyState.vue";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import { useNodeStore } from "../../../../../stores/column-store";
import { useEditorStore } from "../../../../../stores/editor-store";
import { useItemStore } from "../../../../common/DraggableItem/stateStore";
import { useFlowExecution } from "../../../../../composables/useFlowExecution";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { useGraphicWalkerCompute } from "../../../../../composables/useGraphicWalkerCompute";
import { useGraphicWalkerAppearance } from "../../../../../composables/useGraphicWalkerAppearance";

type Status = "loading" | "not-run" | "fetching" | "failed" | "ready";

const status = ref<Status>("loading");
const failureDetail = ref("");
const fetchAttempted = ref(false);
const nodeSettings = ref<NodeGraphicWalker | null>(null);
const chartList = ref<IChart[]>([]);
const fields = ref<IMutField[]>([]);
const globalNodeId = ref(-1);

const nodeStore = useNodeStore();
const editorStore = useEditorStore();
const windowStore = useItemStore();
const vueGraphicWalkerRef = ref<InstanceType<typeof VueGraphicWalker> | null>(null);

const graphicWalkerAppearance = useGraphicWalkerAppearance();

const {
  saveSettings,
  pushNodeData: pushSettings,
  handleGenericSettingsUpdate,
} = useNodeSettings({
  nodeRef: nodeSettings,
  // The chart builder reads fields and rows from the worker; the node stores only its specs.
  onBeforeSave: () => {
    if (nodeSettings.value?.graphic_walker_input) {
      nodeSettings.value.graphic_walker_input.dataModel = { data: [], fields: [] };
    }
  },
});

// Default polling key (`flow_<id>`) on purpose: the header's Cancel sweeps
// `flow_<id>_node_<n>`, so cancelling also stops our completion poll.
const { triggerNodeFetch, isPollingActive } = useFlowExecution(
  () => nodeStore.flow_id,
  { interval: 2000, enabled: true },
  { persistPolling: true },
);

// Every aggregation round-trips to the worker (polars-gw), so charts cover the
// node's full result rather than a browser-side sample.
const { computation, lastError: computeError } = useGraphicWalkerCompute(async (payload) => {
  if (globalNodeId.value === -1) return { rows: [], error: null };
  const resp = await computeNodeVisualization(nodeStore.flow_id, globalNodeId.value, payload);
  return { rows: resp.rows, error: resp.error ?? null };
}, "explore-data");

// Readiness is the field schema, not rows — no route here returns rows.
const hasFields = computed(() => fields.value.length > 0);
const showWalker = computed(() => status.value === "ready" && hasFields.value);
const canFetch = computed(() => !editorStore.isRunning && status.value !== "fetching");

const notRunDescription = computed(() => {
  if (editorStore.isRunning) {
    return "A run is already in progress — this screen refreshes when it finishes.";
  }
  if (fetchAttempted.value) {
    return "The run finished but this step didn't produce any data. Check the logs for errors.";
  }
  return "Run this step to open its data in the chart builder.";
});

const loadSettings = async (nodeId: number) => {
  const data = await nodeStore.getNodeData(nodeId, false);
  nodeSettings.value = (data?.setting_input as NodeGraphicWalker | undefined) ?? null;
  if (!nodeSettings.value) throw new Error("Couldn't read this step's settings.");
};

// Core reconciles the saved specs on this read, so the settings (and their fingerprint) load after it.
const readSpecs = async (nodeId: number) => {
  try {
    return await fetchGraphicWalkerData(nodeStore.flow_id, nodeId);
  } finally {
    await loadSettings(nodeId);
  }
};

const loadNodeData = async (nodeId: number) => {
  if (nodeId !== globalNodeId.value) {
    fetchAttempted.value = false;
  }
  status.value = "loading";
  failureDetail.value = "";
  globalNodeId.value = nodeId;
  fields.value = [];
  chartList.value = [];

  // Specs come from the node's settings; the field schema from polars-gw.
  const [specs, fieldsResult] = await Promise.allSettled([
    readSpecs(nodeId),
    fetchNodeVisualizationFields(nodeStore.flow_id, nodeId),
  ]);
  try {
    if (specs.status === "rejected") throw specs.reason;
    if (fieldsResult.status === "rejected") throw fieldsResult.reason;
    if (fieldsResult.value.error) throw new Error(fieldsResult.value.error);

    fields.value = (fieldsResult.value.fields as IMutField[]) ?? [];
    chartList.value = specs.value.graphic_walker_input.specList || [];
    status.value = "ready";
  } catch (error: any) {
    // 422 means the step hasn't produced data yet: an empty state, not a failure.
    if (error?.response?.status === 422) {
      status.value = "not-run";
      return;
    }
    console.error("Error loading GraphicWalker data:", error);
    failureDetail.value =
      error?.response?.data?.detail ?? error?.message ?? "An unknown error occurred.";
    status.value = "failed";
  }
};

// The drawer can be torn down mid-fetch, and polling persists across unmount —
// so a completion arriving afterwards must not reload into a dead component.
let disposed = false;

// Runs this step only, and keeps the user here: `focusResultPanels: false` stops
// the Results/Logs tabs stealing the drawer. `performanceMode` skips storing the
// node's result — the chart builder reads none of it, and on a large source that
// store is the entire cost of the fetch.
const handleFetchData = async () => {
  const nodeId = globalNodeId.value;
  if (nodeId === -1 || status.value === "fetching" || isPollingActive(`node_${nodeId}`)) return;
  // The fetch places the node by its saved Compute backend, so a pick made here goes first.
  if (!(await saveSettings())) return;

  fetchAttempted.value = true;
  status.value = "fetching";
  try {
    await triggerNodeFetch(nodeId, {
      focusResultPanels: false,
      performanceMode: true,
      // Fires on every terminal state, so a failed run lands on the empty state
      // rather than a stuck spinner.
      onComplete: () => {
        if (!disposed && globalNodeId.value === nodeId) loadNodeData(nodeId);
      },
    });
  } catch {
    // triggerNodeFetch already surfaced the backend detail as a notification.
    status.value = "not-run";
  }
};

// A run started elsewhere refreshes this screen, but only while nothing is
// charted — reloading resets the walker and would discard in-progress edits.
watch(
  () => editorStore.isRunning,
  (running, wasRunning) => {
    if (running || !wasRunning) return;
    if (status.value !== "not-run" || globalNodeId.value === -1) return;
    loadNodeData(globalNodeId.value);
  },
);

// Fullscreen is worth it for the chart builder, not for an empty state.
watch(showWalker, (full) => windowStore.setFullScreen("rightDrawer", full), { immediate: true });

const getCurrentSpec = async (): Promise<IChart[] | null> => {
  if (!vueGraphicWalkerRef.value) return null;
  try {
    const exportedCharts: IChart[] | null = await vueGraphicWalkerRef.value.exportCode();
    if (exportedCharts === null) {
      ElMessage.error({
        message: "Failed to read the current chart configuration.",
        duration: 5000,
      });
      return null;
    }
    return exportedCharts;
  } catch (error: any) {
    console.error("Error exporting the Graphic Walker spec:", error);
    ElMessage.error({
      message: `Failed to read the chart configuration: ${error?.message ?? "unknown error"}`,
      duration: 5000,
    });
    return null;
  }
};

// The charts on screen join the General Settings in one save; a spec that cannot be read
// toasts and leaves the saved specs as they are, since losing the chart builder is worse.
const pushNodeData = async (): Promise<boolean> => {
  const walkerInput = nodeSettings.value?.graphic_walker_input;
  if (showWalker.value && vueGraphicWalkerRef.value && walkerInput) {
    const currentSpec = await getCurrentSpec();
    if (currentSpec !== null && currentSpec.length > 0) walkerInput.specList = currentSpec;
  }
  return pushSettings();
};

const canApply = computed(() => nodeSettings.value !== null);

// Close/minimize paths hide the drawer (unmounting it) before the async
// drawCloseFunction cleanup can run, so pushNodeData never fires — restore the
// drawer out of fullscreen from our own teardown so it can't stay stuck.
onBeforeUnmount(() => {
  disposed = true;
  windowStore.setFullScreen("rightDrawer", false);
});

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
  canApply,
});
</script>

<template>
  <generic-node-settings
    v-if="nodeSettings"
    v-model="nodeSettings"
    @update:model-value="handleGenericSettingsUpdate"
    @request-save="saveSettings"
  >
    <div class="explore-data-container">
      <CodeLoader v-if="status === 'loading'" />

      <EmptyState
        v-else-if="status === 'fetching'"
        icon="fa-solid fa-spinner fa-spin"
        title="Fetching data…"
        description="Running this step and everything it depends on. This screen refreshes when it's done."
      />

      <EmptyState
        v-else-if="status === 'not-run'"
        icon="fa-solid fa-chart-column"
        title="No data to explore yet"
        :description="notRunDescription"
      >
        <template #actions>
          <el-button type="primary" size="small" :disabled="!canFetch" @click="handleFetchData">
            {{ fetchAttempted ? "Try again" : "Fetch data" }}
          </el-button>
        </template>
      </EmptyState>

      <EmptyState
        v-else-if="status === 'failed'"
        icon="fa-solid fa-triangle-exclamation"
        title="Couldn't load the data"
        :description="failureDetail"
      >
        <template #actions>
          <el-button size="small" @click="loadNodeData(globalNodeId)">Try again</el-button>
        </template>
      </EmptyState>

      <div v-else-if="showWalker" class="graphic-walker-wrapper">
        <el-alert
          v-if="computeError"
          :title="computeError"
          type="error"
          :closable="false"
          show-icon
        />
        <VueGraphicWalker
          ref="vueGraphicWalkerRef"
          :appearance="graphicWalkerAppearance"
          :computation="computation"
          :fields="fields"
          :spec-list="chartList"
        />
      </div>

      <EmptyState
        v-else
        icon="fa-solid fa-table"
        title="This step has no columns to chart"
        description="The run produced an empty schema. Check the steps upstream of this one."
      >
        <template #actions>
          <el-button type="primary" size="small" :disabled="!canFetch" @click="handleFetchData">
            Fetch again
          </el-button>
        </template>
      </EmptyState>
    </div>
  </generic-node-settings>
  <div v-else-if="status === 'failed'" class="explore-data-container">
    <EmptyState
      icon="fa-solid fa-triangle-exclamation"
      title="Couldn't load the data"
      :description="failureDetail"
    >
      <template #actions>
        <el-button size="small" @click="loadNodeData(globalNodeId)">Try again</el-button>
      </template>
    </EmptyState>
  </div>
  <CodeLoader v-else />
</template>

<style scoped>
.explore-data-container {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
  background-color: var(--color-background-primary);
}
.graphic-walker-wrapper {
  flex-grow: 1; /* Allow wrapper to fill space */
  min-height: 300px; /* Ensure minimum size */
  overflow: hidden; /* Prevent content spillover if needed */
}
/* Ensure the child fills the wrapper if necessary */
:deep(.graphic-walker-wrapper > div) {
  height: 100%;
}
/* Centre the shared empty state in whatever height the drawer has. */
.explore-data-container :deep(.empty-state) {
  flex: 1;
  min-height: 0;
  justify-content: center;
}
</style>
