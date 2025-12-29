<template>
  <div class="flow-tabs-container">
    <div class="flow-tabs">
      <div
        v-for="flow in flows"
        :key="flow.flow_id"
        class="flow-tab"
        :class="{ active: selectedFlowId === flow.flow_id }"
        :title="flow.name"
        @click="selectFlow(flow.flow_id)"
      >
        <div class="tab-content">
          <span class="material-icons tab-icon">account_tree</span>
          <span class="tab-name">{{ flow.name }}</span>
        </div>
        <span
          class="material-icons close-icon"
          :title="'Close ' + flow.name"
          @click.stop="confirmCloseTab(flow.flow_id)"
        >
          close
        </span>
      </div>
    </div>
  </div>

  <!-- Save Confirmation Modal -->
  <save-confirmation-modal
    ref="saveConfirmationModal"
    @save="handleSaveAndClose"
    @dont-save="handleCloseWithoutSaving"
  />

  <!-- Save Dialog -->
  <save-dialog
    ref="saveDialog"
    v-model:visible="saveDialogVisible"
    :flow-id="pendingCloseFlowId || 0"
    @save-complete="handleSaveComplete"
    @save-cancelled="handleSaveCancelled"
  />
</template>

<script setup lang="ts">
import { ref, watch, onMounted, computed } from "vue";
import { useNodeStore } from "../../stores/column-store";
import { FlowApi } from "../../api";
import type { FlowSettings } from "../../types";
import SaveDialog from "../../features/designer/components/SaveDialog.vue";
import SaveConfirmationModal from "../DesignerView/SaveConfirmationModal.vue";

const getAllFlows = FlowApi.getAllFlows;

const props = defineProps({
  onFlowChange: {
    type: Function,
    required: false,
  },
});

const emit = defineEmits(["flow-changed", "close-tab", "create-flow"]);

// State
const flows = ref<FlowSettings[]>([]);
const selectedFlowId = ref<number | null>(null);
const nodeStore = useNodeStore();
const saveDialogVisible = ref(false);
const pendingCloseFlowId = ref<number | null>(null);
const isLoading = ref(false);

// Component references
const saveConfirmationModal = ref<InstanceType<typeof SaveConfirmationModal> | null>(null);
const saveDialog = ref<InstanceType<typeof SaveDialog> | null>(null);

// Computed properties
const hasFlows = computed(() => flows.value.length > 0);
const selectedFlow = computed(
  () => flows.value.find((flow) => flow.flow_id === selectedFlowId.value) || null,
);

// Load all flows from backend
const loadFlows = async () => {
  if (isLoading.value) return;

  try {
    isLoading.value = true;
    const flowsData = await getAllFlows();
    flows.value = flowsData;

    // Check for stored flow ID in store
    if (nodeStore.flow_id && nodeStore.flow_id !== -1) {
      // Verify the flow still exists
      const flowExists = flowsData.some((flow) => flow.flow_id === nodeStore.flow_id);
      if (flowExists) {
        selectedFlowId.value = nodeStore.flow_id;
      } else if (flowsData.length > 0) {
        // Fall back to first flow if stored ID doesn't exist
        selectedFlowId.value = flowsData[0].flow_id;
        nodeStore.setFlowId(flowsData[0].flow_id);
      }
    } else if (flowsData.length > 0) {
      // If no selected flow, default to the first one
      selectedFlowId.value = flowsData[0].flow_id;
      nodeStore.setFlowId(flowsData[0].flow_id);
    }
  } catch (error) {
    console.error("Failed to load flows for selector:", error);
  } finally {
    isLoading.value = false;
  }
};

// Select a flow and make it the active tab
const selectFlow = (flowId: number) => {
  if (flowId === selectedFlowId.value) return;

  selectedFlowId.value = flowId;
  nodeStore.setFlowId(flowId);
  emit("flow-changed", flowId);

  if (props.onFlowChange) {
    props.onFlowChange(flowId);
  }
};

// Create a new flow
const createNewFlow = () => {
  emit("create-flow");
};

// Show confirmation dialog before closing tab
const confirmCloseTab = (flowId: number) => {
  pendingCloseFlowId.value = flowId;

  // Use the exposed open method
  if (saveConfirmationModal.value) {
    saveConfirmationModal.value.open(flowId);
  } else {
    console.error("Save confirmation modal reference not available");
  }
};

// Handle saving flow before closing
const handleSaveAndClose = async (flowId: number) => {
  // Show save dialog
  if (saveDialog.value) {
    saveDialog.value.open();
  } else {
    saveDialogVisible.value = true;
  }
};

// After successful save, close the tab
const handleSaveComplete = (flowId: number) => {
  saveDialogVisible.value = false;
  emit("close-tab", flowId);
  pendingCloseFlowId.value = null;
};

// If save is cancelled, keep the tab open
const handleSaveCancelled = () => {
  pendingCloseFlowId.value = null;
};

// Close without saving
const handleCloseWithoutSaving = (flowId: number) => {
  emit("close-tab", flowId);
  pendingCloseFlowId.value = null;
};

// Watch for changes to the flow ID in the store
watch(
  () => nodeStore.flow_id,
  (newFlowId) => {
    if (newFlowId && newFlowId !== -1 && newFlowId !== selectedFlowId.value) {
      // Verify the flow exists in our list
      const flowExists = flows.value.some((flow) => flow.flow_id === newFlowId);
      if (flowExists) {
        selectedFlowId.value = newFlowId;
      }
    }
  },
);

// Watch for changes to the flows array
watch(
  () => flows.value,
  (newFlows) => {
    // Handle case where selected flow was deleted
    if (
      selectedFlowId.value !== null &&
      !newFlows.some((flow) => flow.flow_id === selectedFlowId.value)
    ) {
      if (newFlows.length > 0) {
        // Select the first available flow
        selectFlow(newFlows[0].flow_id);
      } else {
        selectedFlowId.value = null;
        nodeStore.setFlowId(-1);
      }
    }
  },
);

onMounted(() => {
  loadFlows();
});

// Expose methods and state for parent component
defineExpose({
  loadFlows,
  selectedFlowId,
  selectedFlow,
  selectFlow,
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
  border-top: 2px solid rgb(0, 34, 60);
  box-shadow: 0 2px 5px rgba(16, 24, 40, 0.04);
  z-index: 1;
}

.flow-tab:not(.active):hover {
  background-color: rgba(19, 37, 73, 0.164);
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
  color: rgb(2, 27, 45);
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

/* New flow tab styling */
.new-flow-tab {
  min-width: 40px;
  max-width: 40px;
  display: flex;
  justify-content: center;
  align-items: center;
  color: rgba(80, 70, 230, 0.8);
}

.new-flow-tab:hover {
  background-color: rgba(80, 70, 230, 0.1);
}
</style>
