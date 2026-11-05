<template>
  <!-- TODO(ux): add up/down/Enter keyboard navigation to the catalog flow
       list; it currently only supports click and dblclick.
       TODO(ux): replace the Unicode ✔/✖/… last-run icons below with Material
       Icons for visual consistency with the rest of the app.
       TODO(ux): drop the flow_path column — it's redundant with the selected
       namespace and eats horizontal space; keep filename + last-run only.
       TODO(ux): add role='tablist' / role='tab' / aria-selected to the
       sidebar tab buttons. Same applies to SaveDialog. -->
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
          :class="{ active: shownMode === 'file' }"
          @click="openMode = 'file'"
        >
          <i class="fa-solid fa-folder"></i>
          <span>File System</span>
        </button>
        <button
          class="sidebar-btn"
          :class="{ active: shownMode === 'catalog' }"
          @click="openMode = 'catalog'"
        >
          <i class="fa-solid fa-book"></i>
          <span>Catalog</span>
        </button>
        <button
          v-if="hub.connected"
          class="sidebar-btn"
          :class="{ active: shownMode === 'hub' }"
          data-testid="open-hub-tab"
          @click="openMode = 'hub'"
        >
          <i class="fa-solid fa-building"></i>
          <span>Hub</span>
        </button>
      </aside>

      <section class="open-dialog-main">
        <!-- File System tab -->
        <div v-show="shownMode === 'file'" class="open-panel">
          <file-browser
            :allowed-file-types="FLOWFILE_EXTENSIONS"
            mode="open"
            context="flows"
            :is-visible="isVisible && shownMode === 'file'"
            @file-selected="handleFileSelected"
          />
        </div>

        <!-- Catalog tab -->
        <div v-show="shownMode === 'catalog'" class="open-panel catalog-panel">
          <div class="catalog-tree-pane">
            <label class="pane-label">Namespace</label>
            <catalog-namespace-picker ref="namespacePicker" v-model="selectedNamespaceId" />
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

        <!-- Hub tab: the hub's namespaces from the one catalog tree, a flow's versions beside its flows -->
        <div
          v-if="hub.connected"
          v-show="shownMode === 'hub'"
          class="open-panel catalog-panel"
          data-testid="open-hub-panel"
        >
          <div class="catalog-tree-pane">
            <label class="pane-label">Namespace</label>
            <!-- Mounted per open, so the hub's namespaces are read afresh each time. -->
            <catalog-namespace-picker
              v-if="isVisible"
              ref="hubPicker"
              hub
              :model-value="hubNamespaceId"
              @update:model-value="selectHubNamespace"
            />
          </div>
          <div class="catalog-flows-pane">
            <label class="pane-label">Flows</label>
            <div v-if="loadingHubFlows" class="flows-status">Loading flows...</div>
            <div v-else-if="hubNamespaceId === null" class="flows-status">
              Select a namespace to browse flows.
            </div>
            <div v-else-if="hubFlows.length === 0" class="flows-status">
              No flows in this namespace.
            </div>
            <ul v-else class="flows-list" data-testid="hub-open-flows">
              <li
                v-for="flow in hubFlows"
                :key="flow.flowUuid"
                class="flow-row"
                :class="{ selected: selectedHubFlow?.flowUuid === flow.flowUuid }"
                @click="selectHubFlow(flow)"
                @dblclick="openHubFlow()"
              >
                <div class="flow-name">
                  <span class="material-icons flow-icon">description</span>
                  <span>{{ flow.name }}</span>
                </div>
                <div class="flow-meta">
                  <span class="flow-last-run">{{ latestVersionLabel(flow.latestVersion) }}</span>
                </div>
              </li>
            </ul>
          </div>
          <div class="catalog-flows-pane">
            <label class="pane-label">Version</label>
            <div v-if="loadingHubVersions" class="flows-status">Loading versions...</div>
            <div v-else-if="!selectedHubFlow" class="flows-status">Select a flow.</div>
            <div v-else-if="hubVersions.length === 0" class="flows-status">
              This flow has no published version yet.
            </div>
            <ul v-else class="flows-list" data-testid="hub-open-versions">
              <li
                v-for="version in hubVersions"
                :key="version.version"
                class="flow-row"
                :class="{ selected: selectedHubVersion === version.version }"
                @click="selectedHubVersion = version.version"
                @dblclick="openHubFlow()"
              >
                <div class="flow-name">
                  <span>v{{ version.version }}</span>
                </div>
                <div class="flow-meta">
                  <span>{{ authorLabel(version) }}</span>
                  <span class="flow-last-run">{{ publishedAt(version) }}</span>
                </div>
                <div v-if="version.message" class="flow-message">{{ version.message }}</div>
              </li>
            </ul>
            <el-alert
              v-if="hubError"
              type="error"
              :closable="false"
              show-icon
              :title="hubError"
              data-testid="hub-open-error"
            />
            <div class="catalog-actions">
              <el-button :disabled="hubBusy" @click="isVisible = false">Cancel</el-button>
              <el-button
                type="primary"
                :loading="hubBusy"
                :disabled="!canOpenHubFlow"
                data-testid="hub-open"
                @click="openHubFlow()"
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
import { ref, watch, computed, onMounted } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import FileBrowser from "../../../components/common/FileBrowser/fileBrowser.vue";
import { FLOWFILE_EXTENSIONS } from "../../../components/common/FileBrowser/constants";
import type { FileInfo } from "../../../types";
import CatalogNamespacePicker from "./CatalogNamespacePicker.vue";
import { CatalogApi } from "../../../api";
import type { FlowRegistration } from "../../../types";
import {
  listHubFlowVersions,
  listHubFlows,
  openFromHub,
  refusalOf,
  type HubFlow,
  type HubFlowVersion,
} from "../../../api/hubFlows.api";
import { useHubConnectionStore } from "../../../stores/hub-connection-store";
import { detailOf } from "../../../utils/apiDetail";
import {
  OPENED_FROM_HUB,
  authorLabel,
  latestVersionLabel,
  publishedTime,
  refusalMessage,
} from "../hubFlows";

const props = defineProps({
  visible: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits<{
  (e: "update:visible", value: boolean): void;
  (
    e: "open-flow",
    payload: { message: string; flowPath: string; flowName?: string; catalogRef?: string },
  ): void;
  (e: "open-cancelled"): void;
}>();

type OpenMode = "file" | "catalog" | "hub";

// Persist the last-used source (file vs catalog) and namespace so the dialog
// reopens where the user left off, instead of always defaulting to the local
// file system.
const OPEN_MODE_STORAGE_KEY = "openDialog_mode";
const NAMESPACE_STORAGE_KEY = "openDialog_namespaceId";

function loadOpenMode(): OpenMode {
  const stored = localStorage.getItem(OPEN_MODE_STORAGE_KEY);
  return stored === "catalog" || stored === "hub" ? stored : "file";
}

function loadNamespaceId(): number | null {
  const stored = localStorage.getItem(NAMESPACE_STORAGE_KEY);
  if (!stored) return null;
  const n = Number(stored);
  return Number.isNaN(n) ? null : n;
}

const hub = useHubConnectionStore();
const isVisible = ref(props.visible);
const openMode = ref<OpenMode>(loadOpenMode());
// The Hub tab is only there while connected; a remembered Hub tab shows the file system until then.
const shownMode = computed<OpenMode>(() =>
  openMode.value === "hub" && !hub.connected ? "file" : openMode.value,
);
const namespacePicker = ref<InstanceType<typeof CatalogNamespacePicker> | null>(null);

// Catalog tab state
const selectedNamespaceId = ref<number | null>(loadNamespaceId());
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
    if (v) resetHub();
  },
);

watch(isVisible, (v) => {
  if (v !== props.visible) emit("update:visible", v);
  if (!v) emit("open-cancelled");
});

// Remember the last-used source and namespace across sessions.
watch(openMode, (m) => localStorage.setItem(OPEN_MODE_STORAGE_KEY, m));
watch(selectedNamespaceId, (nsId) => {
  if (nsId === null) localStorage.removeItem(NAMESPACE_STORAGE_KEY);
  else localStorage.setItem(NAMESPACE_STORAGE_KEY, String(nsId));
});

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
  if (v && shownMode.value === "catalog" && selectedNamespaceId.value !== null) {
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
  const nsPath = namespacePicker.value?.getNamespacePath(
    flow.namespace_id ?? selectedNamespaceId.value,
  );
  emit("open-flow", {
    message: "Flow opened from catalog",
    flowPath: flow.flow_path,
    flowName: flow.name,
    catalogRef: nsPath ? `${nsPath}.${flow.name}` : undefined,
  });
  isVisible.value = false;
}

function formatLastRun(flow: FlowRegistration): string {
  if (!flow.last_run_at) return "never run";
  const status = flow.last_run_success === true ? "✔" : flow.last_run_success === false ? "✖" : "…";
  const when = new Date(flow.last_run_at).toLocaleString();
  return `${when} · ${status}`;
}

// Hub tab state: a namespace of the hub, its flows, the picked flow's versions, newest first and preselected.
const hubPicker = ref<InstanceType<typeof CatalogNamespacePicker> | null>(null);
const hubNamespaceId = ref<number | null>(null);
const hubFlows = ref<HubFlow[]>([]);
const hubVersions = ref<HubFlowVersion[]>([]);
const selectedHubFlow = ref<HubFlow | null>(null);
const selectedHubVersion = ref<number | null>(null);
const loadingHubFlows = ref(false);
const loadingHubVersions = ref(false);
const hubBusy = ref(false);
const hubError = ref("");

onMounted(() => void hub.load());

const canOpenHubFlow = computed(
  () => !!selectedHubFlow.value && hubVersions.value.length > 0 && !hubBusy.value,
);

const publishedAt = (version: HubFlowVersion) => publishedTime(version.publishedAt);

function resetHub() {
  hubError.value = "";
  hubFlows.value = [];
  hubVersions.value = [];
  hubNamespaceId.value = null;
  selectedHubFlow.value = null;
  selectedHubVersion.value = null;
}

async function selectHubNamespace(id: number | null) {
  hubNamespaceId.value = id;
  hubError.value = "";
  selectedHubFlow.value = null;
  hubVersions.value = [];
  const hubId = hubPicker.value?.hubIdOf(id) ?? null;
  if (hubId === null) {
    hubFlows.value = [];
    return;
  }
  loadingHubFlows.value = true;
  try {
    hubFlows.value = await listHubFlows(hubId);
  } catch (err) {
    hubFlows.value = [];
    hubError.value = detailOf(err, "Couldn't list the hub's flows.");
  } finally {
    loadingHubFlows.value = false;
  }
}

async function selectHubFlow(flow: HubFlow) {
  selectedHubFlow.value = flow;
  selectedHubVersion.value = null;
  hubVersions.value = [];
  hubError.value = "";
  loadingHubVersions.value = true;
  try {
    hubVersions.value = await listHubFlowVersions(flow.flowUuid);
    selectedHubVersion.value = hubVersions.value[0]?.version ?? null;
  } catch (err) {
    hubError.value = detailOf(err, "Couldn't list the flow's versions.");
  } finally {
    loadingHubVersions.value = false;
  }
}

async function openHubFlow(replace = false) {
  if (!selectedHubFlow.value || selectedHubVersion.value === null) return;
  hubBusy.value = true;
  hubError.value = "";
  const flowUuid = selectedHubFlow.value.flowUuid;
  const version = selectedHubVersion.value;
  try {
    const opened = await openFromHub({ flowUuid, version, replace });
    emit("open-flow", { message: OPENED_FROM_HUB, flowPath: opened.flowPath });
    ElMessage.success(OPENED_FROM_HUB);
    isVisible.value = false;
  } catch (err) {
    const refusal = refusalOf(err);
    if (refusal?.kind === "local_copy_exists" && !replace) {
      hubBusy.value = false;
      await confirmReplace(refusal.flowPath, version);
      return;
    }
    hubError.value = refusal ? refusalMessage(refusal) : detailOf(err, "Couldn't open the flow.");
  } finally {
    hubBusy.value = false;
  }
}

async function confirmReplace(flowPath: string, version: number) {
  try {
    await ElMessageBox.confirm(
      `A local copy of this flow exists at ${flowPath}. Replace it with v${version} from the hub? Edits you have not published are lost.`,
      "Replace the local copy?",
      { type: "warning", confirmButtonText: "Replace", cancelButtonText: "Cancel" },
    );
  } catch {
    return;
  }
  await openHubFlow(true);
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

.flow-message {
  font-size: var(--font-size-xs, 11px);
  color: var(--color-text-secondary);
  overflow-wrap: anywhere;
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
