<template>
  <div v-if="dataLoaded && nodePythonScript" class="listbox-wrapper">
    <generic-node-settings
      :model-value="nodePythonScript"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="python-script-settings">
        <!-- Kernel Selection -->
        <div class="setting-block">
          <label class="setting-label">Kernel</label>
          <div class="kernel-row">
            <el-select
              v-model="selectedKernelId"
              placeholder="Select a kernel..."
              class="kernel-select"
              size="small"
              :loading="kernelsLoading"
              @change="handleKernelChange"
            >
              <el-option
                v-for="kernel in kernels"
                :key="kernel.id"
                :value="kernel.id"
                :label="`${kernel.name} (${kernel.state})`"
              >
                <span class="kernel-option">
                  <span
                    class="kernel-state-dot"
                    :class="`kernel-state-dot--${kernel.state}`"
                  ></span>
                  <span>{{ kernel.name }}</span>
                  <span class="kernel-state-label">({{ kernel.state }})</span>
                </span>
              </el-option>
            </el-select>
            <router-link :to="{ name: 'kernelManager' }" class="manage-kernels-link">
              Manage Kernels
            </router-link>
          </div>

          <!-- Kernel warnings -->
          <div v-if="!selectedKernelId" class="kernel-warning">
            <i class="fa-solid fa-triangle-exclamation"></i>
            No kernel selected. A kernel is required to run Python code.
          </div>
          <div v-else-if="selectedKernelState && selectedKernelState !== 'idle'" class="kernel-warning">
            <i class="fa-solid fa-triangle-exclamation"></i>
            Kernel is {{ selectedKernelState }}.
            <template v-if="selectedKernelState === 'stopped'">Start it from the Kernel Manager to execute code.</template>
            <template v-else-if="selectedKernelState === 'error'">Check the Kernel Manager for details.</template>
            <template v-else-if="selectedKernelState === 'starting'">Please wait for it to become idle.</template>
            <template v-else-if="selectedKernelState === 'executing'">Please wait for the current execution to finish.</template>
          </div>
        </div>

        <!-- Artifacts Panel — moved ABOVE code, it's reference info -->
        <div class="setting-block">
          <label class="setting-label">Artifacts</label>
          <div class="artifacts-panel">
            <div v-if="artifactsLoading" class="artifacts-loading">
              <i class="fas fa-spinner fa-spin"></i> Loading artifacts...
            </div>
            <template v-else>
              <div class="artifact-group">
                <span class="artifact-group-label">Available:</span>
                <template v-if="availableArtifacts.length > 0">
                  <span
                    v-for="artifact in availableArtifacts"
                    :key="artifact.name"
                    class="artifact-tag"
                  >
                    {{ artifact.name }}
                    <span v-if="artifact.type_name" class="artifact-type">({{ artifact.type_name }})</span>
                  </span>
                </template>
                <span v-else class="artifacts-empty">None</span>
              </div>
              <div class="artifact-group">
                <span class="artifact-group-label">Published:</span>
                <template v-if="publishedArtifacts.length > 0">
                  <span
                    v-for="artifact in publishedArtifacts"
                    :key="artifact.name"
                    class="artifact-tag artifact-tag--published"
                  >
                    {{ artifact.name }}
                    <span v-if="artifact.type_name" class="artifact-type">({{ artifact.type_name }})</span>
                  </span>
                </template>
                <span v-else class="artifacts-empty">Run the flow to see published artifacts</span>
              </div>
            </template>
          </div>
        </div>

        <!-- Code Editor — replaced with notebook -->
        <div class="setting-block">
          <div class="code-header">
            <label class="setting-label">Code</label>
            <div class="code-header-actions">
              <button class="icon-button" title="Expand Editor" @click="showExpandedEditor = true">
                <i class="fa-solid fa-expand"></i>
              </button>
              <button class="icon-button" title="API Reference" @click="showHelp = true">
                <i class="fa-solid fa-circle-question"></i>
              </button>
            </div>
          </div>
          <NotebookEditor
            v-if="showEditor && cells.length > 0"
            :cells="cells"
            :kernel-id="selectedKernelId"
            :flow-id="nodePythonScript!.flow_id as number"
            :node-id="nodePythonScript!.node_id"
            :depending-on-ids="nodePythonScript!.depending_on_ids ?? []"
            @update:cells="handleCellsUpdate"
          />
        </div>
      </div>
    </generic-node-settings>

    <!-- Help Modal -->
    <FlowfileApiHelp v-if="showHelp" @close="showHelp = false" />

    <!-- Expanded Editor Dialog -->
    <el-dialog
      v-model="showExpandedEditor"
      title="Python Script"
      fullscreen
      :close-on-click-modal="false"
      class="expanded-editor-dialog"
    >
      <template #header>
        <div class="expanded-dialog-header">
          <span class="expanded-dialog-title">Python Script</span>
          <div class="expanded-dialog-actions">
            <span v-if="selectedKernelId" class="kernel-indicator">
              <span
                class="kernel-state-dot"
                :class="`kernel-state-dot--${selectedKernelState}`"
              ></span>
              {{ kernels.find(k => k.id === selectedKernelId)?.name }}
            </span>
            <button class="icon-button" title="API Reference" @click="showHelp = true">
              <i class="fa-solid fa-circle-question"></i>
            </button>
          </div>
        </div>
      </template>
      <div class="expanded-editor-content">
        <NotebookEditor
          v-if="showExpandedEditor && cells.length > 0"
          :cells="cells"
          :kernel-id="selectedKernelId"
          :flow-id="nodePythonScript!.flow_id as number"
          :node-id="nodePythonScript!.node_id"
          :depending-on-ids="nodePythonScript!.depending_on_ids ?? []"
          @update:cells="handleCellsUpdate"
        />
      </div>
    </el-dialog>
  </div>

  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref, computed, onUnmounted } from "vue";
import { CodeLoader } from "vue-content-loader";

import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import type { NodePythonScript, NotebookCell } from "../../../../../types/node.types";
import type { NodeData } from "../../../baseNode/nodeInterfaces";
import type { KernelInfo } from "../../../../../types/kernel.types";
import { KernelApi } from "../../../../../api/kernel.api";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import FlowfileApiHelp from "./FlowfileApiHelp.vue";
import NotebookEditor from "./NotebookEditor.vue";
import { createPythonScriptNode, DEFAULT_PYTHON_SCRIPT_CODE } from "./utils";

// ─── State ──────────────────────────────────────────────────────────────────

const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const showEditor = ref(false);
const showHelp = ref(false);
const showExpandedEditor = ref(false);

const nodePythonScript = ref<NodePythonScript | null>(null);
const nodeData = ref<NodeData | null>(null);
const cells = ref<NotebookCell[]>([]);

// Kernel state
const kernels = ref<KernelInfo[]>([]);
const kernelsLoading = ref(false);
const selectedKernelId = ref<string | null>(null);
let kernelPollTimer: ReturnType<typeof setInterval> | null = null;

// Artifact state
interface ArtifactInfo {
  name: string;
  type_name: string;
  node_id?: number;
}

const availableArtifacts = ref<ArtifactInfo[]>([]);
const publishedArtifacts = ref<ArtifactInfo[]>([]);
const artifactsLoading = ref(false);

// ─── Kernel helpers ─────────────────────────────────────────────────────────

const selectedKernelState = computed(() => {
  if (!selectedKernelId.value) return null;
  const kernel = kernels.value.find((k) => k.id === selectedKernelId.value);
  return kernel?.state ?? null;
});

const loadKernels = async () => {
  kernelsLoading.value = true;
  try {
    kernels.value = await KernelApi.getAll();
  } catch (error) {
    console.error("Failed to load kernels:", error);
  } finally {
    kernelsLoading.value = false;
  }
};

const startKernelPolling = () => {
  stopKernelPolling();
  kernelPollTimer = setInterval(async () => {
    try {
      kernels.value = await KernelApi.getAll();
    } catch {
      // Silently ignore poll errors
    }
  }, 5000);
};

const stopKernelPolling = () => {
  if (kernelPollTimer !== null) {
    clearInterval(kernelPollTimer);
    kernelPollTimer = null;
  }
};

const handleKernelChange = (kernelId: string | null) => {
  if (nodePythonScript.value) {
    nodePythonScript.value.python_script_input.kernel_id = kernelId ?? null;
  }
  loadArtifacts();
};

// ─── Artifact helpers ───────────────────────────────────────────────────────

const loadArtifacts = async () => {
  const kernelId = selectedKernelId.value;
  if (!kernelId) {
    availableArtifacts.value = [];
    publishedArtifacts.value = [];
    return;
  }

  const kernel = kernels.value.find((k) => k.id === kernelId);
  if (!kernel || (kernel.state !== "idle" && kernel.state !== "executing")) {
    availableArtifacts.value = [];
    publishedArtifacts.value = [];
    return;
  }

  artifactsLoading.value = true;
  try {
    const response = await KernelApi.getArtifacts(kernelId);
    // The kernel /artifacts endpoint returns a dict of artifact name -> metadata
    // Each entry has: type_name, module, node_id, created_at, size_bytes
    const allArtifacts: ArtifactInfo[] = Object.entries(response).map(
      ([name, meta]: [string, any]) => ({
        name,
        type_name: meta?.type_name ?? "",
        node_id: meta?.node_id,
      }),
    );

    // Split artifacts: "available" = published by other nodes, "published" = by this node
    const currentNodeId = nodePythonScript.value?.node_id;
    if (currentNodeId != null) {
      availableArtifacts.value = allArtifacts.filter(
        (a) => a.node_id !== currentNodeId,
      );
      publishedArtifacts.value = allArtifacts.filter(
        (a) => a.node_id === currentNodeId,
      );
    } else {
      availableArtifacts.value = allArtifacts;
      publishedArtifacts.value = [];
    }
  } catch {
    availableArtifacts.value = [];
    publishedArtifacts.value = [];
  } finally {
    artifactsLoading.value = false;
  }
};

// ─── Cell sync ──────────────────────────────────────────────────────────────

const handleCellsUpdate = (updatedCells: NotebookCell[]) => {
  cells.value = updatedCells;
  syncCellsToNode();
};

const syncCellsToNode = () => {
  if (!nodePythonScript.value) return;

  // Persist cells WITHOUT output (outputs are runtime-only, contain base64 images)
  nodePythonScript.value.python_script_input.cells = cells.value.map(c => ({
    id: c.id,
    code: c.code,
  }));

  // Derive combined code for flow execution
  // flow_graph.py reads python_script_input.code — this must always be populated
  nodePythonScript.value.python_script_input.code =
    cells.value.map(c => c.code).filter(Boolean).join("\n\n");
};

// ─── Node settings composable ───────────────────────────────────────────────

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodePythonScript,
  onBeforeSave: () => {
    syncCellsToNode();
    // Validate that there's actual code
    const combinedCode = nodePythonScript.value?.python_script_input.code;
    if (!combinedCode?.trim()) {
      return false;
    }
    return true;
  },
});

// ─── Lifecycle ──────────────────────────────────────────────────────────────

const loadNodeData = async (nodeId: number) => {
  try {
    nodeData.value = await nodeStore.getNodeData(nodeId, false);
    if (nodeData.value) {
      const hasValidSetup = Boolean(
        nodeData.value?.setting_input?.is_setup &&
          nodeData.value?.setting_input?.python_script_input,
      );

      nodePythonScript.value = hasValidSetup
        ? nodeData.value.setting_input
        : createPythonScriptNode(nodeStore.flow_id, nodeStore.node_id);

      // Initialize cells from saved data or create from existing code
      const input = nodePythonScript.value!.python_script_input;
      if (input.cells && input.cells.length > 0) {
        // Load from saved cells (output is runtime-only, not persisted)
        cells.value = input.cells.map(c => ({
          id: c.id,
          code: c.code,
          output: null,
        }));
      } else {
        // Backward compat: create single cell from existing code
        cells.value = [{
          id: crypto.randomUUID(),
          code: input.code || DEFAULT_PYTHON_SCRIPT_CODE,
          output: null,
        }];
      }

      selectedKernelId.value = nodePythonScript.value!.python_script_input.kernel_id;

      showEditor.value = true;
      dataLoaded.value = true;

      // Load kernels and artifacts
      await loadKernels();
      startKernelPolling();
      loadArtifacts();
    }
  } catch (error) {
    console.error("Failed to load node data:", error);
    showEditor.value = false;
    dataLoaded.value = false;
  }
};

onUnmounted(() => {
  stopKernelPolling();
});

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>

<style scoped>
.python-script-settings {
  display: flex;
  flex-direction: column;
  gap: 0.65rem;
}

.code-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.code-header-actions {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.icon-button {
  background: none;
  border: none;
  cursor: pointer;
  font-size: 0.9rem;
  color: var(--el-text-color-secondary);
  padding: 0.2rem;
  line-height: 1;
  border-radius: 3px;
  transition: all 0.15s;
}

.icon-button:hover {
  color: var(--el-color-primary);
  background: var(--el-fill-color-light);
}

/* ─── Setting blocks ─────────────────────────────────────────────────────── */

.setting-block {
  display: flex;
  flex-direction: column;
  gap: 0.3rem;
}

.setting-label {
  font-weight: 500;
  font-size: 0.8rem;
  color: var(--el-text-color-primary);
}

/* ─── Kernel selection ───────────────────────────────────────────────────── */

.kernel-row {
  display: flex;
  align-items: center;
  gap: 0.75rem;
}

.kernel-select {
  flex: 1;
}

.manage-kernels-link {
  font-size: 0.8rem;
  color: var(--el-color-primary);
  text-decoration: none;
  white-space: nowrap;
}

.manage-kernels-link:hover {
  text-decoration: underline;
}

.kernel-option {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.kernel-state-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
}

.kernel-state-dot--idle {
  background-color: #67c23a;
}

.kernel-state-dot--executing {
  background-color: #e6a23c;
}

.kernel-state-dot--starting {
  background-color: #409eff;
}

.kernel-state-dot--stopped {
  background-color: #909399;
}

.kernel-state-dot--error {
  background-color: #f56c6c;
}

.kernel-state-label {
  font-size: 0.8rem;
  color: var(--el-text-color-secondary);
}

.kernel-warning {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.35rem 0.6rem;
  font-size: 0.75rem;
  color: var(--el-color-warning-dark-2, #b88230);
  background-color: var(--el-color-warning-light-9, #fdf6ec);
  border: 1px solid var(--el-color-warning-light-5, #f3d19e);
  border-radius: 4px;
}

.kernel-warning i {
  flex-shrink: 0;
}

/* ─── Artifacts panel ────────────────────────────────────────────────────── */

.artifacts-panel {
  background: transparent;
  border-top: 1px solid var(--el-border-color-lighter);
  border-radius: 0;
  padding: 0.5rem 0.75rem;
  font-size: 0.85rem;
}

.artifacts-loading {
  color: var(--el-text-color-secondary);
}

.artifact-group {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.4rem;
  margin-bottom: 0.5rem;
}

.artifact-group:last-child {
  margin-bottom: 0;
}

.artifact-group-label {
  font-weight: 500;
  color: var(--el-text-color-regular);
  margin-right: 0.25rem;
}

.artifact-tag {
  display: inline-flex;
  align-items: center;
  gap: 0.25rem;
  padding: 0.15rem 0.5rem;
  background-color: var(--el-color-primary-light-9, #ecf5ff);
  color: var(--el-color-primary, #409eff);
  border-radius: 3px;
  font-size: 0.8rem;
}

.artifact-tag--published {
  background-color: var(--el-color-success-light-9, #f0f9eb);
  color: var(--el-color-success, #67c23a);
}

.artifact-type {
  font-size: 0.75rem;
  opacity: 0.7;
}

.artifacts-empty {
  color: var(--el-text-color-placeholder);
  font-style: italic;
}

/* ─── Expanded Editor Dialog ─────────────────────────────────────────────── */

.expanded-dialog-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  width: 100%;
  padding-right: 2rem;
}

.expanded-dialog-title {
  font-size: 1.1rem;
  font-weight: 600;
  color: var(--el-text-color-primary);
}

.expanded-dialog-actions {
  display: flex;
  align-items: center;
  gap: 1rem;
}

.kernel-indicator {
  display: flex;
  align-items: center;
  gap: 0.4rem;
  font-size: 0.85rem;
  color: var(--el-text-color-secondary);
  padding: 0.25rem 0.75rem;
  background: var(--el-fill-color-light);
  border-radius: 4px;
}

.expanded-editor-content {
  height: calc(100vh - 80px);
  overflow-y: auto;
  padding: 0 1rem;
}
</style>
