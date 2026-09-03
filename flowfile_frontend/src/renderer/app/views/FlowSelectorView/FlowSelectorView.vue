<template>
  <div class="flow-tabs-container">
    <div class="flow-tabs">
      <!-- trigger-keys=[] : the trigger's default keys include Space, and its keydown
           handler preventDefaults it — swallowing spaces typed in the rename input. -->
      <el-tooltip
        v-for="flow in flows"
        :key="flow.flow_id"
        placement="bottom"
        :show-after="400"
        :hide-after="0"
        :trigger-keys="[]"
      >
        <template #content>
          <div>{{ flow.display_name || flow.name }}</div>
          <div v-if="isDirty(flow.flow_id)" class="tooltip-dirty-line">● Unsaved changes</div>
        </template>
        <div
          class="flow-tab"
          :class="{ active: selectedFlowId === flow.flow_id, dirty: isDirty(flow.flow_id) }"
          @click="selectFlow(flow.flow_id)"
          @dblclick="startRename(flow.flow_id)"
          @contextmenu.prevent="openTabMenu($event, flow.flow_id)"
        >
          <div class="tab-content">
            <span class="material-icons tab-icon">account_tree</span>
            <!-- keydown.stop: typing must not reach the tooltip trigger or the
                 canvas/undo global shortcut handlers. -->
            <input
              v-if="renamingFlowId === flow.flow_id"
              :ref="setRenameInput"
              v-model="renameValue"
              class="tab-rename-input"
              @keydown.stop
              @keydown.enter.prevent="commitRename"
              @keydown.escape="cancelRename"
              @blur="commitRename"
              @click.stop
              @dblclick.stop
            />
            <span v-else class="tab-name">{{ flow.display_name || flow.name }}</span>
          </div>
          <span class="tab-indicator" @dblclick.stop>
            <span
              v-if="isDirty(flow.flow_id)"
              class="dirty-dot"
              aria-label="Unsaved changes"
            ></span>
            <span class="material-icons close-icon" @click.stop="confirmCloseTab(flow.flow_id)">
              close
            </span>
          </span>
        </div>
      </el-tooltip>
      <button
        type="button"
        class="new-flow-tab"
        aria-label="New flow"
        title="New flow"
        @click="emit('create-flow')"
      >
        <span class="material-icons">add</span>
      </button>
    </div>
  </div>

  <!-- Save Confirmation Modal -->
  <save-confirmation-modal
    ref="saveConfirmationModal"
    @save="handleSaveAndClose"
    @dont-save="handleCloseWithoutSaving"
    @cancelled="handleCloseCancelled"
  />

  <!-- Save Dialog -->
  <save-dialog
    ref="saveDialog"
    v-model:visible="saveDialogVisible"
    :flow-id="pendingCloseFlowId || 0"
    @save-complete="handleSaveComplete"
    @save-cancelled="handleSaveCancelled"
  />

  <!-- Right-click context menu for flow tabs -->
  <Teleport to="body">
    <ContextMenu
      v-if="tabMenu"
      :position="{ x: tabMenu.x, y: tabMenu.y }"
      :options="tabMenuOptionsComputed"
      @select="onTabMenuSelect"
      @close="tabMenu = null"
    />
  </Teleport>
</template>

<script setup lang="ts">
import { ref, watch, onMounted, computed, nextTick } from "vue";
import { ElMessage } from "element-plus";
import { useNodeStore } from "../../stores/column-store";
import { useEditorStore } from "../../stores/editor-store";
import { useCatalogStore } from "../../stores/catalog-store";
import { useRecentFlows } from "../../composables/useRecentFlows";
import { FlowApi } from "../../api";
import type { FlowSettings } from "../../types";
import SaveDialog from "../../features/designer/components/SaveDialog.vue";
import SaveConfirmationModal from "../DesignerView/SaveConfirmationModal.vue";
import ContextMenu from "../../components/common/ContextMenu/ContextMenu.vue";
import { computeCloseTargets, tabMenuOptions, type TabCloseAction } from "./flowTabActions";

const getAllFlows = FlowApi.getAllFlows;

const props = defineProps({
  onFlowChange: {
    type: Function,
    required: false,
    default: null,
  },
});

const emit = defineEmits([
  "flow-changed",
  "close-tab",
  "close-tabs",
  "flow-renamed",
  "create-flow",
]);

// State
const flows = ref<FlowSettings[]>([]);
const selectedFlowId = ref<number | null>(null);
const nodeStore = useNodeStore();
const editorStore = useEditorStore();
const catalogStore = useCatalogStore();
const { renameFlow: renameRecentFlow, reconcileWithCatalog } = useRecentFlows();
const saveDialogVisible = ref(false);
const pendingCloseFlowId = ref<number | null>(null);
const isLoading = ref(false);

// Inline rename
const renamingFlowId = ref<number | null>(null);
const renameValue = ref("");
const renameSeed = ref("");
const renameInput = ref<HTMLInputElement | null>(null);
// Function ref: template refs inside v-for collect into arrays; only one rename
// input exists at a time, so capture it directly.
const setRenameInput = (el: unknown) => {
  renameInput.value = (el as HTMLInputElement) ?? null;
};

// Tab context menu
const tabMenu = ref<{ flowId: number; x: number; y: number } | null>(null);

// Bulk close (Close Others / All / Left / Right)
const bulkQueue = ref<number[]>([]);
const decidedCloseIds = ref<number[]>([]);
const bulkActive = ref(false);

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

const saveConfirmationModal = ref<InstanceType<typeof SaveConfirmationModal> | null>(null);
const saveDialog = ref<InstanceType<typeof SaveDialog> | null>(null);

const selectedFlow = computed(
  () => flows.value.find((flow) => flow.flow_id === selectedFlowId.value) || null,
);

const loadFlows = async () => {
  if (isLoading.value) return;

  try {
    isLoading.value = true;
    const flowsData = await getAllFlows();
    flows.value = flowsData;

    if (nodeStore.flow_id && nodeStore.flow_id !== -1) {
      const flowExists = flowsData.some((flow) => flow.flow_id === nodeStore.flow_id);
      if (flowExists) {
        selectedFlowId.value = nodeStore.flow_id;
      } else if (flowsData.length > 0) {
        selectedFlowId.value = flowsData[0].flow_id;
        nodeStore.setFlowId(flowsData[0].flow_id);
      }
    } else if (flowsData.length > 0) {
      selectedFlowId.value = flowsData[0].flow_id;
      nodeStore.setFlowId(flowsData[0].flow_id);
    }
  } catch (error) {
    console.error("Failed to load flows for selector:", error);
  } finally {
    isLoading.value = false;
  }
};

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

const tabLabelFor = (flowId: number): string | null => {
  const flow = flows.value.find((f) => f.flow_id === flowId);
  return flow ? flow.display_name || flow.name : null;
};

// ---- Inline rename ----

const startRename = (flowId: number) => {
  const label = tabLabelFor(flowId);
  if (label === null) return;
  renameSeed.value = label;
  renameValue.value = renameSeed.value;
  renamingFlowId.value = flowId;
  nextTick(() => {
    renameInput.value?.focus();
    renameInput.value?.select();
  });
};

const commitRename = async () => {
  // Cleared first: Enter unmounts the input, whose blur re-enters this handler.
  if (renamingFlowId.value === null) return;
  const flowId = renamingFlowId.value;
  renamingFlowId.value = null;
  const trimmed = renameValue.value.trim();
  if (!trimmed || trimmed === renameSeed.value) return;
  try {
    const settings = await FlowApi.renameFlow(flowId, trimmed);
    await loadFlows();
    emit("flow-renamed", flowId);
    // The rename never moves the file, so the path-keyed recents entry only needs
    // its label updated.
    if (settings?.path) renameRecentFlow(settings.path, trimmed);
    void reconcileWithCatalog();
    // Keep the catalog views in sync, but don't force-load the catalog from here.
    if (catalogStore.tree.length) catalogStore.loadTree();
    if (catalogStore.allFlows.length) catalogStore.loadAllFlows();
  } catch (error: any) {
    ElMessage.error(error?.response?.data?.detail ?? "Failed to rename flow");
  }
};

const cancelRename = () => {
  renamingFlowId.value = null;
};

// ---- Tab context menu ----

const openTabMenu = (event: MouseEvent, flowId: number) => {
  tabMenu.value = {
    flowId,
    // The menu doesn't self-clamp; keep it on-screen for right-edge tabs.
    x: Math.min(event.clientX, window.innerWidth - 200),
    y: event.clientY,
  };
};

const tabMenuOptionsComputed = computed(() =>
  tabMenu.value
    ? tabMenuOptions(
        flows.value.map((f) => f.flow_id),
        tabMenu.value.flowId,
      )
    : [],
);

const onTabMenuSelect = (action: string) => {
  const target = tabMenu.value?.flowId;
  if (target === undefined) return;
  if (action === "rename") {
    startRename(target);
  } else if (action === "close") {
    confirmCloseTab(target);
  } else {
    startBulkClose(
      computeCloseTargets(
        flows.value.map((f) => f.flow_id),
        target,
        action as TabCloseAction,
      ),
    );
  }
};

// ---- Bulk close queue ----
// Walks the targets sequentially: clean flows are marked for closing, dirty ones
// get the save-confirmation modal. Cancel (dialog X or backing out of the save
// dialog) aborts the rest; decisions already made still close (VS Code semantics).

const startBulkClose = (targets: number[]) => {
  if (bulkActive.value || targets.length === 0) return;
  bulkActive.value = true;
  bulkQueue.value = [...targets];
  decidedCloseIds.value = [];
  processNextBulkClose();
};

const processNextBulkClose = async () => {
  const next = bulkQueue.value.shift();
  if (next === undefined) {
    flushBulkClose();
    return;
  }
  const settings = await FlowApi.getFlowSettings(next);
  // Gone sessions (null) close as a no-op — close_flow is idempotent.
  if (settings === null || settings.has_unsaved_changes === false) {
    decidedCloseIds.value.push(next);
    processNextBulkClose();
    return;
  }
  pendingCloseFlowId.value = next;
  if (saveConfirmationModal.value) {
    saveConfirmationModal.value.open(next, tabLabelFor(next));
  } else {
    // Modal unavailable — leave the flow open rather than dropping unsaved work.
    pendingCloseFlowId.value = null;
    processNextBulkClose();
  }
};

const flushBulkClose = () => {
  const ids = decidedCloseIds.value;
  bulkActive.value = false;
  bulkQueue.value = [];
  decidedCloseIds.value = [];
  pendingCloseFlowId.value = null;
  if (ids.length === 1) {
    emit("close-tab", ids[0]);
  } else if (ids.length > 1) {
    emit("close-tabs", ids);
  }
};

// Show confirmation dialog before closing tab — only if the flow has unsaved changes
const confirmCloseTab = async (flowId: number) => {
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
    saveConfirmationModal.value.open(flowId, tabLabelFor(flowId));
  } else {
    console.error("Save confirmation modal reference not available");
  }
};

const handleSaveAndClose = async () => {
  if (saveDialog.value) {
    saveDialog.value.open();
  } else {
    saveDialogVisible.value = true;
  }
};

const handleSaveComplete = (flowId: number) => {
  saveDialogVisible.value = false;
  if (bulkActive.value) {
    // Save-As during a close can rekey the flow — flowId is the current id.
    decidedCloseIds.value.push(flowId);
    pendingCloseFlowId.value = null;
    processNextBulkClose();
    return;
  }
  emit("close-tab", flowId);
  pendingCloseFlowId.value = null;
  // The saved flow is now clean; refresh its dirty state so the dot clears.
  refreshDirtyState(flowId);
};

const handleSaveCancelled = () => {
  pendingCloseFlowId.value = null;
  // Backing out of the save dialog aborts the rest; the undecided flow stays open.
  if (bulkActive.value) flushBulkClose();
};

const handleCloseWithoutSaving = (flowId: number) => {
  if (bulkActive.value) {
    decidedCloseIds.value.push(flowId);
    pendingCloseFlowId.value = null;
    processNextBulkClose();
    return;
  }
  emit("close-tab", flowId);
  pendingCloseFlowId.value = null;
};

const handleCloseCancelled = () => {
  pendingCloseFlowId.value = null;
  if (bulkActive.value) flushBulkClose();
};

watch(
  () => nodeStore.flow_id,
  (newFlowId) => {
    if (newFlowId && newFlowId !== -1 && newFlowId !== selectedFlowId.value) {
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

watch(
  () => flows.value,
  (newFlows) => {
    if (
      selectedFlowId.value !== null &&
      !newFlows.some((flow) => flow.flow_id === selectedFlowId.value)
    ) {
      if (newFlows.length > 0) {
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
  height: 34px;
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
  background-color: var(--color-border-secondary);
  border-radius: var(--border-radius-full);
}

.flow-tab {
  display: flex;
  align-items: center;
  padding: 0 var(--spacing-4);
  height: 30px;
  background-color: transparent;
  border-right: 1px solid var(--color-border-light);
  cursor: pointer;
  min-width: 120px;
  max-width: 180px;
  position: relative;
  user-select: none;
  border-radius: var(--border-radius-lg) var(--border-radius-lg) 0 0;
  margin-right: 1px;
  transition: all var(--transition-fast);
}

.flow-tab.active {
  background-color: var(--color-background-primary);
  border-top: 2px solid var(--color-primary);
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
  width: calc(100% - 24px);
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

.tab-rename-input {
  width: 100%;
  min-width: 0;
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-primary);
  border-radius: var(--border-radius-sm);
  padding: 1px 4px;
  outline: none;
}

/* TODO(ux): announce dirty-state changes via aria-live so screen readers
   are notified when a flow becomes dirty, instead of relying on the static
   aria-label alone. */
.tab-indicator {
  position: absolute;
  right: var(--spacing-2);
  top: 50%;
  transform: translateY(-50%);
  display: grid;
  place-items: center;
  width: 18px;
  height: 18px;
  flex-shrink: 0;
}

.tab-indicator > .dirty-dot,
.tab-indicator > .close-icon {
  grid-area: 1 / 1;
}

.dirty-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background-color: var(--color-primary, #1976d2);
  transition: opacity var(--transition-fast);
  pointer-events: none;
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
  border-radius: var(--border-radius-full);
  padding: 2px;
  cursor: pointer;
  transform: scale(0.9);
  transition:
    opacity var(--transition-fast),
    background-color var(--transition-fast),
    color var(--transition-fast),
    transform var(--transition-fast);
}

.flow-tab:hover .close-icon {
  opacity: 1;
  transform: scale(1);
}

.flow-tab.dirty:hover .dirty-dot {
  opacity: 0;
}

.close-icon:hover {
  background-color: var(--color-background-tertiary);
  color: var(--color-text-secondary);
}

.active .close-icon:hover {
  background-color: var(--color-background-hover);
  color: var(--color-primary);
}

.tooltip-dirty-line {
  margin-top: 4px;
  font-size: 11px;
  opacity: 0.9;
}

.new-flow-tab {
  flex-shrink: 0;
  width: 34px;
  height: 30px;
  display: flex;
  justify-content: center;
  align-items: center;
  border: none;
  background: transparent;
  cursor: pointer;
  color: var(--color-primary);
  border-radius: var(--border-radius-lg) var(--border-radius-lg) 0 0;
}

.new-flow-tab .material-icons {
  font-size: 18px;
}

.new-flow-tab:hover {
  background-color: var(--color-background-hover);
}
</style>
