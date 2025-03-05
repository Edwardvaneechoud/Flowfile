<template>
  <div class="flow-tabs-container">
    <div class="flow-tabs">
      <div
        v-for="flow in flows"
        :key="flow.flow_id"
        class="flow-tab"
        :class="{ active: selectedFlowId === flow.flow_id }"
        @click="selectFlow(flow.flow_id)"
      >
        <div class="tab-content">
          <span class="material-icons tab-icon">account_tree</span>
          <span class="tab-name">{{ flow.name }}</span>
        </div>
        <span class="material-icons close-icon" @click.stop="onCloseTab(flow.flow_id)">
          close
        </span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, watch, onMounted } from "vue";
import { useNodeStore } from "../../stores/column-store";
import { getAllFlows } from "../../features/designer/components/Canvas/backendInterface";
import { FlowSettings } from "../../features/designer/nodes/nodeLogic";

const props = defineProps({
  onFlowChange: {
    type: Function,
    required: false,
  },
  onCloseTab: {
    type: Function,
    required: false,
  },
});

const emit = defineEmits(["flow-changed", "close-tab"]);

const flows = ref<FlowSettings[]>([]);
const selectedFlowId = ref<number | null>(null);
const nodeStore = useNodeStore();

// Load all flows from backend
const loadFlows = async () => {
  try {
    const flowsData = await getAllFlows();
    flows.value = flowsData;

    // Check for stored flow ID in session storage
    if (nodeStore.flow_id && nodeStore.flow_id !== -1) {
      selectedFlowId.value = nodeStore.flow_id;
    } else if (flows.value.length > 0) {
      // If no selected flow, default to the first one
      selectedFlowId.value = flows.value[0].flow_id;
      nodeStore.setFlowId(flows.value[0].flow_id);
    }
  } catch (error) {
    console.error("Failed to load flows for selector:", error);
  }
};

// Select a flow and make it the active tab
const selectFlow = (flowId: number) => {
  selectedFlowId.value = flowId;
  nodeStore.setFlowId(flowId);
  emit("flow-changed", flowId);
  if (props.onFlowChange) props.onFlowChange(flowId);
};

// Handle close tab button click
const onCloseTab = (flowId: number) => {
  emit("close-tab", flowId);
};

// Watch for changes to the flow ID in the store
watch(
  () => nodeStore.flow_id,
  (newFlowId) => {
    if (newFlowId && newFlowId !== -1 && newFlowId !== selectedFlowId.value) {
      selectedFlowId.value = newFlowId;
    }
  },
);

onMounted(() => {
  loadFlows();
});

defineExpose({
  loadFlows,
  selectedFlowId,
});
</script>

<style scoped>
.flow-tabs-container {
  width: 100%;
  font-family:
    "Inter",
    "Roboto",
    -apple-system,
    BlinkMacSystemFont,
    sans-serif;
}

.flow-tabs {
  display: flex;
  align-items: center;
  overflow-x: auto;
  background-color: rgba(16, 24, 40, 0.02);
  border-bottom: 1px solid rgba(16, 24, 40, 0.08);
  height: 42px;
  padding-left: 4px;
  scrollbar-width: thin;
}

.flow-tabs::-webkit-scrollbar {
  height: 4px;
}

.flow-tabs::-webkit-scrollbar-track {
  background: transparent;
}

.flow-tabs::-webkit-scrollbar-thumb {
  background-color: rgba(16, 24, 40, 0.2);
  border-radius: 2px;
}

.flow-tab {
  display: flex;
  align-items: center;
  padding: 0 16px;
  height: 38px;
  background-color: transparent;
  border-right: 1px solid rgba(16, 24, 40, 0.06);
  cursor: pointer;
  min-width: 120px;
  max-width: 180px;
  position: relative;
  user-select: none;
  border-radius: 6px 6px 0 0;
  margin-right: 1px;
  transition: all 0.2s ease;
}

.flow-tab.active {
  background-color: #fff;
  border-top: 2px solid rgba(80, 70, 230, 0.9);
  box-shadow: 0 2px 5px rgba(16, 24, 40, 0.04);
  z-index: 1;
}

.flow-tab:not(.active):hover {
  background-color: rgba(16, 24, 40, 0.03);
}

.tab-content {
  display: flex;
  align-items: center;
  gap: 8px;
  width: calc(100% - 20px);
  overflow: hidden;
}

.tab-icon {
  font-size: 16px;
  color: rgba(80, 70, 230, 0.8);
  flex-shrink: 0;
}

.tab-name {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  font-size: 13px;
  font-weight: 500;
  letter-spacing: 0.01em;
  color: rgba(16, 24, 40, 0.8);
}

.active .tab-name {
  color: rgba(16, 24, 40, 0.95);
}

.close-icon {
  font-size: 15px;
  color: rgba(16, 24, 40, 0.4);
  opacity: 0;
  position: absolute;
  right: 8px;
  border-radius: 50%;
  padding: 2px;
  transition: all 0.15s ease;
  transform: scale(0.9);
}

.flow-tab:hover .close-icon {
  opacity: 1;
}

.close-icon:hover {
  background-color: rgba(16, 24, 40, 0.06);
  color: rgba(16, 24, 40, 0.7);
  transform: scale(1);
}

.active .close-icon:hover {
  background-color: rgba(80, 70, 230, 0.1);
  color: rgba(80, 70, 230, 0.9);
}
</style>
