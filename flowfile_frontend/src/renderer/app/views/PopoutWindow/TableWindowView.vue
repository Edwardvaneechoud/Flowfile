<template>
  <PopoutWindowFrame
    :title="title"
    :state="state"
    test-id="table-window"
    @return="returnToDesigner"
    @close="closeThisWindow"
  >
    <div class="table-window">
      <p v-if="nodeGone" class="table-window__note" data-testid="table-window-gone">
        This step is no longer on the canvas.
      </p>
      <DataPreview
        :flow-id="flowId"
        :node-id="outputs === null ? null : nodeId"
        :refresh-token="refreshToken"
        :active="true"
        :outputs="outputs ?? []"
      />
    </div>
  </PopoutWindowFrame>
</template>

<script lang="ts" setup>
// One flow's data preview in its own window, on the node the designer sends it.
import { computed, ref, shallowRef, watch } from "vue";
import { useRoute } from "vue-router";
import DataPreview from "../../features/designer/dataPreview.vue";
import PopoutWindowFrame from "./PopoutWindowFrame.vue";
import { usePopoutWindowHost } from "./usePopoutWindowHost";
import { usePopoutChannel } from "./usePopoutChannel";
import { applySelection, outputsForNode, type ShownPreview } from "./tableWindow";
import { FlowApi } from "../../api";
import { parseFlowQuery } from "../../../lib/popoutWindow";
import { useFlowStore } from "../../stores/flow-store";
import type { VueFlowInput } from "../../types/flow.types";

const route = useRoute();
const flowStore = useFlowStore();
const initialNode = parseFlowQuery(route.query.node);
const shown = ref<ShownPreview>({ nodeId: initialNode > 0 ? initialNode : null, token: null });
const nodeId = computed(() => shown.value.nodeId);
const refreshToken = ref(0);
const flowData = shallowRef<VueFlowInput | null>(null);
let wasRunning = false;

const { flowId, state, title, returnToDesigner, closeThisWindow } = usePopoutWindowHost({
  kind: "table",
  // The graph changed elsewhere: the node's outputs, or the node itself, may have changed.
  onReload: () => void loadFlowData(),
  onRunStateChanged: (isRunning) => {
    // A finished run has new rows and artifacts for the node.
    if (wasRunning && !isRunning) {
      refreshToken.value += 1;
      void flowStore.fetchArtifacts(flowId.value);
    }
    wasRunning = isRunning;
  },
});

async function loadFlowData(): Promise<void> {
  const id = flowId.value;
  if (id <= 0) return;
  try {
    const data = await FlowApi.getFlowData(id);
    if (flowId.value === id) flowData.value = data;
  } catch (error) {
    console.error("[popout] could not read the flow's nodes:", error);
  }
}

watch(state, (now) => {
  if (now !== "ready") return;
  void loadFlowData();
  void flowStore.fetchArtifacts(flowId.value);
});

usePopoutChannel({
  kind: "table",
  flowId,
  onMessage: (message) => {
    const { preview, refetch } = applySelection(shown.value, message);
    shown.value = preview;
    if (refetch) refreshToken.value += 1;
  },
});

// `/node/data` fails for a node that is gone, so a node not on the canvas is never asked for.
const outputs = computed(() => outputsForNode(flowData.value, nodeId.value));
const nodeGone = computed(
  () => nodeId.value !== null && !!flowData.value && outputs.value === null,
);
</script>

<style scoped>
.table-window {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
  position: relative;
}

.table-window > :deep(.table-container),
.table-window > :deep(.spinner-overlay) {
  flex: 1;
  min-height: 0;
}

.table-window__note {
  flex: none;
  margin: 0;
  padding: var(--spacing-xs) var(--spacing-md);
  border-bottom: 1px solid var(--color-border-light);
  background: var(--color-background-secondary);
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
}
</style>
