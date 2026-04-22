<template>
  <!-- TODO(ux): switch the dirty-dot color based on the active theme's
       accent, and consider moving it so it sits adjacent to the close icon
       on the right instead of inline with the tab name. -->
  <div class="flow-tabs-container">
    <div class="flow-tabs">
      <el-tooltip
        v-for="flow in flows"
        :key="flow.flow_id"
        :content="flow.display_name || flow.name"
        placement="bottom"
        :show-after="400"
        :hide-after="0"
      >
        <div
          class="flow-tab"
          :class="{ active: selectedFlowId === flow.flow_id, dirty: isDirty(flow.flow_id) }"
          @click="selectFlow(flow.flow_id)"
        >
          <div class="tab-content">
            <span class="material-icons tab-icon">account_tree</span>
            <span class="tab-name">{{ flow.display_name || flow.name }}</span>
            <span
              v-if="isDirty(flow.flow_id)"
              class="dirty-dot"
              aria-label="Unsaved changes"
              title="Unsaved changes"
            ></span>
          </div>
          <span class="material-icons close-icon" @click.stop="confirmCloseTab(flow.flow_id)">
            close
          </span>
        </div>
      </el-tooltip>
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
import { useEditorStore } from "../../stores/editor-store";
import { FlowApi } from "../../api";
import type { FlowSettings } from "../../types";
import SaveDialog from "../../features/designer/components/SaveDialog.vue";
import SaveConfirmationModal from "../DesignerView/SaveConfirmationModal.vue";

const getAllFlows = FlowApi.getAllFlows;

const props = defineProps({
  onFlowChange: {
    type: Function,
    required: false,
    default: null,
  },
});

const emit = defineEmits(["flow-changed", "close-tab", "create-flow"]);

// State
const flows = ref<FlowSettings[]>([]);
const selectedFlowId = ref<number | null>(null);
const nodeStore = useNodeStore();
const editorStore = useEditorStore();
const saveDialogVisible = ref(false);
const pendingCloseFlowId = ref<number | null>(null);
const isLoading = ref(false);

const isDirty = (flowId: number): boolean => {
  const flow = flows.value.find((f) => f.flow_id === flowId);
  return flow?.has_unsaved_changes === true;
};

// Refresh dirty state for a single flow from the backend, mutating the local
// flows[] entry in place so the tab bar re-renders.
const refreshDirtyState = async (flowId: number | null) => {
  if (flowId === null || flowId < 0) return;
  try {
    const settings = await FlowApi.getFlowSettings(flowId);
    if (!settings) return;
    const idx = flows.value.findIndex((f) => f.flow_id === flowId);
    if (idx !== -1) {
      flows.value[idx] = { ...flows.value[idx], has_unsaved_changes: settings.has_unsaved_changes };
    }
  } catch (err) {
    // Non-critical: tab dot may be stale until the next refresh
    console.debug("refreshDirtyState failed for flow", flowId, err);
  }
};

// Debounce helper — coalesces rapid mutation events into a single refresh.
// Captures the flow id at schedule time so a tab-switch mid-debounce doesn't
// refresh the wrong flow.
let dirtyRefreshTimer: ReturnType<typeof setTimeout> | null = null;
const scheduleDirtyRefresh = () => {
  const flowId = selectedFlowId.value;
  if (dirtyRefreshTimer) clearTimeout(dirtyRefreshTimer);
  dirtyRefreshTimer = setTimeout(() => {
    refreshDirtyState(flowId);
  }, 400);
};

// Component references
const saveConfirmationModal = ref<InstanceType<typeof SaveConfirmationModal> | null>(null);
const saveDialog = ref<InstanceType<typeof SaveDialog> | null>(null);

// Computed properties
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
  // Refresh the newly-selected tab's dirty state immediately so the dot
  // reflects reality as soon as the user switches.
  refreshDirtyState(flowId);
};

// Show confirmation dialog before closing tab — only if the flow has unsaved changes
const confirmCloseTab = async (flowId: number) => {
  // Check backend for dirty state; fall back to prompting if the check fails
  try {
    const settings = await FlowApi.getFlowSettings(flowId);
    if (settings && settings.has_unsaved_changes === false) {
      emit("close-tab", flowId);
      return;
    }
  } catch (error) {
    console.warn("Could not check flow dirty state; showing save prompt", error);
  }

  pendingCloseFlowId.value = flowId;

  if (saveConfirmationModal.value) {
    saveConfirmationModal.value.open(flowId);
  } else {
    console.error("Save confirmation modal reference not available");
  }
};

// Handle saving flow before closing
const handleSaveAndClose = async () => {
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
  // The saved flow is now clean; refresh its dirty state so the dot clears.
  refreshDirtyState(flowId);
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

// Any graph mutation bumps editorStore.graphVersion; refresh the active tab's
// dirty state (debounced) so the dot lights up live as the user edits.
watch(
  () => editorStore.graphVersion,
  () => {
    scheduleDirtyRefresh();
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
  refreshDirtyState,
});
</script>

<style scoped>
.flow-tabs-container {
  width: 100%;
  font-family: var(--font-family-base);
}

.flow-tabs {
  display: flex;
  align-items: center;
  overflow-x: auto;
  background-color: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-primary);
  height: 42px;
  padding-left: var(--spacing-1);
  scrollbar-width: thin;
}

.flow-tabs::-webkit-scrollbar {
  height: 4px;
}

.flow-tabs::-webkit-scrollbar-track {
  background: transparent;
}

.flow-tabs::-webkit-scrollbar-thumb {
  background-color: var(--color-gray-300);
  border-radius: var(--border-radius-full);
}

.flow-tab {
  display: flex;
  align-items: center;
  padding: 0 var(--spacing-4);
  height: 38px;
  background-color: transparent;
  border-right: 1px solid var(--color-border-light);
  cursor: pointer;
  min-width: 120px;
  max-width: 180px;
  position: relative;
  user-select: none;
  border-radius: var(--border-radius-md) var(--border-radius-md) 0 0;
  margin-right: 1px;
  transition: all var(--transition-fast);
}

.flow-tab.active {
  background-color: var(--color-background-primary);
  border-top: 2px solid var(--primary-blue);
  box-shadow: var(--shadow-xs);
  z-index: 1;
}

.flow-tab:not(.active):hover {
  background-color: var(--color-background-hover);
}

.tab-content {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  width: calc(100% - 20px);
  overflow: hidden;
}

.tab-icon {
  font-size: var(--font-size-xs);
  color: var(--color-accent);
  flex-shrink: 0;
}

.tab-name {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  letter-spacing: 0.01em;
  color: var(--color-text-secondary);
}

/* TODO(ux): announce dirty-state changes via aria-live so screen readers
   are notified when a flow becomes dirty, instead of relying on the static
   aria-label alone. */
.dirty-dot {
  flex-shrink: 0;
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background-color: var(--color-accent, #1976d2);
  margin-left: var(--spacing-1, 4px);
}

.flow-tab.dirty .tab-name {
  color: var(--color-text-primary);
}

.active .tab-name {
  color: var(--color-text-primary);
}

.close-icon {
  font-size: var(--font-size-lg);
  color: var(--color-text-muted);
  opacity: 0;
  position: absolute;
  right: var(--spacing-2);
  border-radius: var(--border-radius-full);
  padding: 2px;
  transition: all var(--transition-fast);
  transform: scale(0.9);
}

.flow-tab:hover .close-icon {
  opacity: 1;
}

.close-icon:hover {
  background-color: var(--color-background-tertiary);
  color: var(--color-text-secondary);
  transform: scale(1);
}

.active .close-icon:hover {
  background-color: var(--color-background-hover);
  color: var(--color-primary);
}

/* New flow tab styling */
.new-flow-tab {
  min-width: 40px;
  max-width: 40px;
  display: flex;
  justify-content: center;
  align-items: center;
  color: var(--color-primary);
}

.new-flow-tab:hover {
  background-color: var(--color-background-hover);
}
</style>
