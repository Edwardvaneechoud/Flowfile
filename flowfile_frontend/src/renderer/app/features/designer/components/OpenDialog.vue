<template>
  <el-dialog
    v-model="isVisible"
    title="Open Flow"
    width="70%"
    :close-on-click-modal="false"
    custom-class="high-z-index-dialog"
  >
    <div class="open-dialog-body">
      <aside class="open-dialog-sidebar">
        <button
          class="sidebar-btn"
          :class="{ active: openMode === 'file' }"
          @click="openMode = 'file'"
        >
          <i class="fa-solid fa-folder"></i>
          <span>File System</span>
        </button>
        <button
          class="sidebar-btn"
          :class="{ active: openMode === 'catalog' }"
          @click="openMode = 'catalog'"
        >
          <i class="fa-solid fa-book"></i>
          <span>Catalog</span>
        </button>
      </aside>

      <section class="open-dialog-main">
        <!-- File System tab -->
        <div v-show="openMode === 'file'" class="open-panel">
          <file-browser
            :allowed-file-types="FLOWFILE_EXTENSIONS"
            mode="open"
            context="flows"
            :is-visible="isVisible && openMode === 'file'"
            @file-selected="handleFileSelected"
          />
        </div>

        <!-- Catalog tab -->
        <div v-show="openMode === 'catalog'" class="open-panel catalog-panel">
          <div class="catalog-tree-pane">
            <label class="pane-label">Namespace</label>
            <catalog-namespace-picker v-model="selectedNamespaceId" />
          </div>
          <div class="catalog-flows-pane">
            <label class="pane-label">Flows</label>
            <div v-if="loadingFlows" class="flows-status">Loading flows...</div>
            <div v-else-if="selectedNamespaceId === null" class="flows-status">
              Select a namespace to browse flows.
            </div>
            <div v-else-if="visibleFlows.length === 0" class="flows-status">
              No flows registered in this namespace.
            </div>
            <ul v-else class="flows-list">
              <li
                v-for="flow in visibleFlows"
                :key="flow.id"
                class="flow-row"
                :class="{ selected: selectedFlowId === flow.id }"
                @click="handleRowClick(flow)"
                @dblclick="handleOpenCatalogFlow(flow)"
              >
                <div class="flow-name">
                  <span class="material-icons flow-icon">description</span>
                  <span>{{ flow.name }}</span>
                </div>
                <div class="flow-meta">
                  <span class="flow-path" :title="flow.flow_path">{{ flow.flow_path }}</span>
                  <span class="flow-last-run">{{ formatLastRun(flow) }}</span>
                </div>
              </li>
            </ul>
            <div class="catalog-actions">
              <el-button @click="isVisible = false">Cancel</el-button>
              <el-button
                type="primary"
                :disabled="!selectedFlow"
                @click="selectedFlow && handleOpenCatalogFlow(selectedFlow)"
              >
                Open
              </el-button>
            </div>
          </div>
        </div>
      </section>
    </div>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, watch, computed } from "vue";
import { ElMessage } from "element-plus";
import FileBrowser from "../../../components/common/FileBrowser/fileBrowser.vue";
import { FLOWFILE_EXTENSIONS } from "../../../components/common/FileBrowser/constants";
import type { FileInfo } from "../../../types";
import CatalogNamespacePicker from "./CatalogNamespacePicker.vue";
import { CatalogApi } from "../../../api";
import type { FlowRegistration } from "../../../types";

const props = defineProps({
  visible: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits<{
  (e: "update:visible", value: boolean): void;
  (e: "open-flow", payload: { message: string; flowPath: string }): void;
  (e: "open-cancelled"): void;
}>();

const isVisible = ref(props.visible);
const openMode = ref<"file" | "catalog">("file");

// Catalog tab state
const selectedNamespaceId = ref<number | null>(null);
const flows = ref<FlowRegistration[]>([]);
const loadingFlows = ref(false);
const selectedFlowId = ref<number | null>(null);

// Hide flows whose files no longer exist on disk — catalog registrations can
// outlive their files (e.g. the user deleted or moved the file outside of
// Flowfile). Showing them would only result in failed-open errors.
const visibleFlows = computed(() => flows.value.filter((f) => f.file_exists));

const selectedFlow = computed(
  () => visibleFlows.value.find((f) => f.id === selectedFlowId.value) ?? null,
);

watch(
  () => props.visible,
  (v) => {
    isVisible.value = v;
  },
);

watch(isVisible, (v) => {
  if (v !== props.visible) emit("update:visible", v);
  if (!v) emit("open-cancelled");
});

// Reload flow list when the selected namespace changes
watch(selectedNamespaceId, async (nsId) => {
  selectedFlowId.value = null;
  if (nsId === null) {
    flows.value = [];
    return;
  }
  loadingFlows.value = true;
  try {
    flows.value = await CatalogApi.getFlows(nsId);
  } catch (err: any) {
    console.error("Failed to load flows for namespace", nsId, err);
    ElMessage.error({
      message: err?.message || "Failed to load flows",
      duration: 5000,
    });
    flows.value = [];
  } finally {
    loadingFlows.value = false;
  }
});

// When the dialog is re-opened, refresh the flow list so the user sees the
// latest registrations (e.g. flows saved during this session).
watch(isVisible, async (v) => {
  if (v && openMode.value === "catalog" && selectedNamespaceId.value !== null) {
    const nsId = selectedNamespaceId.value;
    loadingFlows.value = true;
    try {
      flows.value = await CatalogApi.getFlows(nsId);
    } catch (err) {
      console.error("Failed to refresh flows", err);
    } finally {
      loadingFlows.value = false;
    }
  }
});

function handleFileSelected(info: FileInfo | null) {
  if (!info) return;
  emit("open-flow", { message: "Flow opened", flowPath: info.path });
  isVisible.value = false;
}

function handleRowClick(flow: FlowRegistration) {
  selectedFlowId.value = flow.id;
}

function handleOpenCatalogFlow(flow: FlowRegistration) {
  emit("open-flow", {
    message: "Flow opened from catalog",
    flowPath: flow.flow_path,
  });
  isVisible.value = false;
}

function formatLastRun(flow: FlowRegistration): string {
  if (!flow.last_run_at) return "never run";
  const status = flow.last_run_success === true ? "✔" : flow.last_run_success === false ? "✖" : "…";
  const when = new Date(flow.last_run_at).toLocaleString();
  return `${when} · ${status}`;
}

defineExpose({
  open: () => {
    isVisible.value = true;
  },
  close: () => {
    isVisible.value = false;
  },
});
</script>

<style scoped>
.open-dialog-body {
  display: flex;
  min-height: 420px;
  gap: var(--spacing-4);
}

.open-dialog-sidebar {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
  width: 160px;
  padding-right: var(--spacing-3);
  border-right: 1px solid var(--color-border-light);
  flex-shrink: 0;
}

.sidebar-btn {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  background: transparent;
  border: 1px solid transparent;
  border-radius: var(--border-radius-md);
  text-align: left;
  cursor: pointer;
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  transition: all var(--transition-fast);
}

.sidebar-btn:hover {
  background-color: var(--color-background-tertiary);
}

.sidebar-btn.active {
  background-color: var(--color-accent-subtle, #e8f0fe);
  border-color: var(--color-accent, #1976d2);
  color: var(--color-accent, #1976d2);
  font-weight: var(--font-weight-medium);
}

.sidebar-btn i {
  width: 16px;
  text-align: center;
}

.open-dialog-main {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
}

.open-panel {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-4);
  flex: 1;
  min-height: 0;
}

.catalog-panel {
  flex-direction: row;
  gap: var(--spacing-4);
}

.catalog-tree-pane {
  width: 240px;
  flex-shrink: 0;
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.catalog-flows-pane {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
}

.pane-label {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.flows-status {
  padding: var(--spacing-4);
  text-align: center;
  font-size: var(--font-size-sm);
  color: var(--color-text-muted, #999);
  border: 1px dashed var(--color-border-light);
  border-radius: var(--border-radius-md);
}

.flows-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
  max-height: 380px;
  overflow-y: auto;
  flex: 1;
}

.flow-row {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
  padding: var(--spacing-2) var(--spacing-3);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background-color: var(--color-background-primary);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.flow-row:hover {
  background-color: var(--color-background-tertiary);
  border-color: var(--color-border-secondary);
}

.flow-row.selected {
  background-color: var(--color-accent-subtle, #e8f0fe);
  border-color: var(--color-accent, #1976d2);
}

.flow-name {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.flow-icon {
  font-size: 16px;
  color: var(--color-text-secondary);
}

.flow-meta {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-3);
  font-size: var(--font-size-xs, 11px);
  color: var(--color-text-muted, #999);
}

.flow-path {
  font-family: var(--font-family-mono);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  min-width: 0;
  flex: 1;
}

.flow-last-run {
  flex-shrink: 0;
}

.catalog-actions {
  display: flex;
  justify-content: flex-end;
  gap: var(--spacing-2);
  margin-top: auto;
  padding-top: var(--spacing-3);
  border-top: 1px solid var(--color-border-light);
}
</style>
