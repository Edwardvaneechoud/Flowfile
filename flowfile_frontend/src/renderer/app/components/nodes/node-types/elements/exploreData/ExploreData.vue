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
import { useNodeStore } from "../../../../../stores/column-store";
import { useEditorStore } from "../../../../../stores/editor-store";
import { useItemStore } from "../../../../common/DraggableItem/stateStore";
import { useFlowExecution } from "../../../../../composables/useFlowExecution";
import { useGraphicWalkerCompute } from "../../../../../composables/useGraphicWalkerCompute";
import { useGraphicWalkerAppearance } from "../../../../../composables/useGraphicWalkerAppearance";

type Status = "loading" | "not-run" | "fetching" | "failed" | "ready";

const status = ref<Status>("loading");
const failureDetail = ref("");
const fetchAttempted = ref(false);
const nodeData = ref<NodeGraphicWalker | null>(null);
const chartList = ref<IChart[]>([]);
const fields = ref<IMutField[]>([]);
const globalNodeId = ref(-1);

const nodeStore = useNodeStore();
const editorStore = useEditorStore();
const windowStore = useItemStore();
const vueGraphicWalkerRef = ref<InstanceType<typeof VueGraphicWalker> | null>(null);

const graphicWalkerAppearance = useGraphicWalkerAppearance();

// Default polling key (`flow_<id>`) on purpose: the header's Cancel sweeps
// `flow_<id>_node_<n>`, so cancelling also stops our completion poll.
const { triggerNodeFetch, isPollingActive } = useFlowExecution(
  () => nodeStore.flow_id,
  { interval: 2000, enabled: true },
  { persistPolling: true },
);

// Every aggregation round-trips to the worker (polars-gw) instead of running in
// the browser, so charts cover the node's full result rather than a sample —
// and agree with the same chart built on a catalog table.
const { computation, lastError: computeError } = useGraphicWalkerCompute(async (payload) => {
  if (globalNodeId.value === -1) return { rows: [], error: null };
  const resp = await computeNodeVisualization(nodeStore.flow_id, globalNodeId.value, payload);
  return { rows: resp.rows, error: resp.error ?? null };
}, "explore-data");

// Readiness is the field schema, not rows: `graphic_walker_input` carries a
// rows-free payload (the walker aggregates through /analysis_data/compute),
// and fields are present whenever the step has run.
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

const loadNodeData = async (nodeId: number) => {
  if (nodeId !== globalNodeId.value) {
    fetchAttempted.value = false;
  }
  status.value = "loading";
  failureDetail.value = "";
  globalNodeId.value = nodeId;
  nodeData.value = null;
  fields.value = [];
  chartList.value = [];

  try {
    // The specs come from the node's settings; the field schema comes from
    // polars-gw on the worker, so the node and the catalog classify columns
    // (dimension vs measure) the same way.
    const [fetchedNodeData, fieldsResponse] = await Promise.all([
      fetchGraphicWalkerData(nodeStore.flow_id, nodeId),
      fetchNodeVisualizationFields(nodeStore.flow_id, nodeId),
    ]);
    if (!fetchedNodeData?.graphic_walker_input)
      throw new Error("Received invalid data structure from backend.");
    if (fieldsResponse.error) throw new Error(fieldsResponse.error);

    nodeData.value = fetchedNodeData;
    fields.value = (fieldsResponse.fields as IMutField[]) ?? [];
    chartList.value = fetchedNodeData.graphic_walker_input.specList || [];
    status.value = "ready";
  } catch (error: any) {
    // 422 is the backend saying this step hasn't produced data yet — an empty
    // state with a way out, not a failure.
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

let pollTimer: ReturnType<typeof setInterval> | null = null;
let safetyTimer: ReturnType<typeof setTimeout> | null = null;

const clearFetchTimers = () => {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
  if (safetyTimer) {
    clearTimeout(safetyTimer);
    safetyTimer = null;
  }
};

// Runs this step (and lazily everything upstream of it) rather than the whole
// flow, keeping the user on this screen: `focusResultPanels: false` stops the
// Results/Logs tabs stealing the drawer. Polling is persistent, so completion
// is read off `isPollingActive`; the safety timeout re-loads rather than giving
// up, so a stalled poll degrades to the empty state, not a permanent spinner.
const handleFetchData = async () => {
  const nodeId = globalNodeId.value;
  const pollingKeySuffix = `node_${nodeId}`;
  if (nodeId === -1 || status.value === "fetching" || isPollingActive(pollingKeySuffix)) return;

  fetchAttempted.value = true;
  status.value = "fetching";
  try {
    await triggerNodeFetch(nodeId, { focusResultPanels: false });
  } catch {
    // triggerNodeFetch already surfaced the backend detail as a notification.
    status.value = "not-run";
    return;
  }

  clearFetchTimers();
  pollTimer = setInterval(() => {
    if (isPollingActive(pollingKeySuffix)) return;
    clearFetchTimers();
    loadNodeData(nodeId);
  }, 250);
  safetyTimer = setTimeout(() => {
    clearFetchTimers();
    loadNodeData(nodeId);
  }, 60000);
};

// A run started elsewhere (the header Run button) refreshes this screen too,
// but only while nothing is charted — reloading resets the walker to the saved
// spec, which would discard in-progress chart edits.
watch(
  () => editorStore.isRunning,
  (running, wasRunning) => {
    if (running || !wasRunning) return;
    if (status.value !== "not-run" || globalNodeId.value === -1) return;
    loadNodeData(globalNodeId.value);
  },
);

// Fullscreen is worth it for the chart builder, not for an empty state, so the
// drawer expands only once there is something to show.
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

const saveSpecToNodeStore = async (specsToSave: IChart[]) => {
  if (!nodeData.value) {
    ElMessage.error({
      message: "Cannot save: the node data is no longer available.",
      duration: 5000,
    });
    return false;
  }
  try {
    const saveData: NodeGraphicWalker = {
      ...nodeData.value,
      graphic_walker_input: {
        ...nodeData.value.graphic_walker_input,
        specList: specsToSave,
        dataModel: { data: [], fields: [] },
      },
    };

    await nodeStore.updateSettingsDirectly(saveData);
    return true;
  } catch (error: any) {
    console.error("Error saving spec to node store:", error);
    ElMessage.error({
      message: `Failed to save the chart configuration: ${error?.message ?? "unknown error"}`,
      duration: 5000,
    });
    return false;
  }
};

// Save failures toast instead of taking over the panel — losing the chart
// builder is a worse outcome than a failed save.
const pushNodeData = async () => {
  if (!vueGraphicWalkerRef.value) return;
  const currentSpec = await getCurrentSpec();
  if (currentSpec === null || currentSpec.length === 0) return;
  await saveSpecToNodeStore(currentSpec);
};

// Close/minimize paths hide the drawer (unmounting it) before the async
// drawCloseFunction cleanup can run, so pushNodeData never fires — restore the
// drawer out of fullscreen from our own teardown so it can't stay stuck.
onBeforeUnmount(() => {
  clearFetchTimers();
  windowStore.setFullScreen("rightDrawer", false);
});

defineExpose({
  loadNodeData,
  pushNodeData,
  canApply: showWalker,
});
</script>

<template>
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
</template>

<style scoped>
.explore-data-container {
  height: 100%;
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
