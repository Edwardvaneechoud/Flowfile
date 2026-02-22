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

          <!-- Memory usage -->
          <div v-if="memoryDisplay" class="kernel-memory" :class="`kernel-memory--${memoryLevel}`">
            <i class="fa-solid fa-memory"></i>
            <span class="kernel-memory__text">{{ memoryDisplay }}</span>
            <span class="kernel-memory__percent">({{ memoryInfo!.usage_percent }}%)</span>
          </div>

          <!-- Kernel warnings -->
          <div v-if="!selectedKernelId" class="kernel-warning">
            <i class="fa-solid fa-triangle-exclamation"></i>
            No kernel selected. A kernel is required to run Python code.
          </div>
          <div
            v-else-if="selectedKernelState && selectedKernelState !== 'idle'"
            class="kernel-warning"
          >
            <i class="fa-solid fa-triangle-exclamation"></i>
            Kernel is {{ selectedKernelState }}.
            <template v-if="selectedKernelState === 'stopped'"
              >Start it from the Kernel Manager to execute code.</template
            >
            <template v-else-if="selectedKernelState === 'error'"
              >Check the Kernel Manager for details.</template
            >
            <template v-else-if="selectedKernelState === 'starting'"
              >Please wait for it to become idle.</template
            >
            <template v-else-if="selectedKernelState === 'executing'"
              >Please wait for the current execution to finish.</template
            >
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
                    <span v-if="artifact.type_name" class="artifact-type"
                      >({{ artifact.type_name }})</span
                    >
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
                    <span v-if="artifact.type_name" class="artifact-type"
                      >({{ artifact.type_name }})</span
                    >
                  </span>
                </template>
                <span v-else class="artifacts-empty">Run the flow to see published artifacts</span>
              </div>
            </template>
          </div>
        </div>

        <!-- Output Names -->
        <div class="setting-block">
          <label class="setting-label">Outputs</label>
          <div class="output-names-list">
            <div
              v-for="(name, index) in outputNames"
              :key="index"
              class="output-name-row"
            >
              <input
                class="output-name-input"
                :value="name"
                placeholder="Output name"
                @input="updateOutputName(index, ($event.target as HTMLInputElement).value)"
              />
              <button
                v-if="outputNames.length > 1"
                class="icon-button output-remove-btn"
                title="Remove output"
                @click="removeOutputName(index)"
              >
                <i class="fa-solid fa-xmark"></i>
              </button>
            </div>
            <button class="add-output-btn" @click="addOutputName">
              <i class="fa-solid fa-plus"></i> Add output
            </button>
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
              {{ kernels.find((k) => k.id === selectedKernelId)?.name }}
              <span
                v-if="memoryDisplay"
                class="kernel-indicator__memory"
                :class="`kernel-memory--${memoryLevel}`"
              >
                {{ memoryDisplay }}
              </span>
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
import { ref, computed, watch, onUnmounted } from "vue";
import { CodeLoader } from "vue-content-loader";
import { useVueFlow, Position } from "@vue-flow/core";

import { useNodeStore } from "../../../../../stores/node-store";
import { useEditorStore } from "../../../../../stores/editor-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import type { NodePythonScript, NotebookCell } from "../../../../../types/node.types";
import type { NodeData } from "../../../baseNode/nodeInterfaces";
import type { KernelInfo, KernelMemoryInfo } from "../../../../../types/kernel.types";
import { KernelApi } from "../../../../../api/kernel.api";
import { FlowApi } from "../../../../../api/flow.api";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import FlowfileApiHelp from "./FlowfileApiHelp.vue";
import NotebookEditor from "./NotebookEditor.vue";
import { createPythonScriptNode, DEFAULT_PYTHON_SCRIPT_CODE } from "./utils";

// ─── State ──────────────────────────────────────────────────────────────────

const nodeStore = useNodeStore();
const editorStore = useEditorStore();
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

// Memory stats
const memoryInfo = ref<KernelMemoryInfo | null>(null);
let memoryPollTimer: ReturnType<typeof setInterval> | null = null;

// Artifact state
interface ArtifactInfo {
  name: string;
  type_name: string;
  node_id?: number;
}

const availableArtifacts = ref<ArtifactInfo[]>([]);
const publishedArtifacts = ref<ArtifactInfo[]>([]);
const artifactsLoading = ref(false);

// ─── VueFlow handle management ──────────────────────────────────────────────

const { updateNode } = useVueFlow();

const outputNames = computed<string[]>({
  get: () => nodePythonScript.value?.python_script_input.output_names ?? ["main"],
  set: (val: string[]) => {
    if (nodePythonScript.value) {
      nodePythonScript.value.python_script_input.output_names = val;
    }
  },
});

const syncOutputHandles = () => {
  const nodeId = nodePythonScript.value?.node_id;
  if (nodeId == null) return;
  const names = outputNames.value;
  updateNode(String(nodeId), (node) => ({
    ...node,
    data: {
      ...node.data,
      outputs: names.map((name, i) => ({
        id: `output-${i}`,
        position: Position.Right,
        label: names.length > 1 ? name : undefined,
      })),
    },
  }));
};

const addOutputName = () => {
  const names = [...outputNames.value];
  names.push(`output_${names.length}`);
  outputNames.value = names;
  syncOutputHandles();
};

const removeOutputName = (index: number) => {
  if (outputNames.value.length <= 1) return;
  const names = [...outputNames.value];
  names.splice(index, 1);
  outputNames.value = names;
  syncOutputHandles();
};

const updateOutputName = (index: number, value: string) => {
  const names = [...outputNames.value];
  names[index] = value;
  outputNames.value = names;
  syncOutputHandles();
};

// ─── Kernel helpers ─────────────────────────────────────────────────────────

const selectedKernelState = computed(() => {
  if (!selectedKernelId.value) return null;
  const kernel = kernels.value.find((k) => k.id === selectedKernelId.value);
  return kernel?.state ?? null;
});

const formatBytes = (bytes: number): string => {
  const gb = bytes / (1024 * 1024 * 1024);
  return gb >= 1 ? `${gb.toFixed(1)} GB` : `${(bytes / (1024 * 1024)).toFixed(0)} MB`;
};

const memoryDisplay = computed(() => {
  if (!memoryInfo.value || memoryInfo.value.limit_bytes === 0) return null;
  const used = formatBytes(memoryInfo.value.used_bytes);
  const limit = formatBytes(memoryInfo.value.limit_bytes);
  return `${used} / ${limit}`;
});

const memoryLevel = computed((): "normal" | "warning" | "critical" => {
  if (!memoryInfo.value) return "normal";
  if (memoryInfo.value.usage_percent >= 95) return "critical";
  if (memoryInfo.value.usage_percent >= 80) return "warning";
  return "normal";
});

const loadMemoryStats = async () => {
  const kernelId = selectedKernelId.value;
  if (!kernelId) {
    memoryInfo.value = null;
    return;
  }
  const kernel = kernels.value.find((k) => k.id === kernelId);
  if (!kernel || (kernel.state !== "idle" && kernel.state !== "executing")) {
    memoryInfo.value = null;
    return;
  }
  memoryInfo.value = await KernelApi.getMemoryStats(kernelId);
};

const startMemoryPolling = () => {
  stopMemoryPolling();
  loadMemoryStats();
  memoryPollTimer = setInterval(loadMemoryStats, 3000);
};

const stopMemoryPolling = () => {
  if (memoryPollTimer !== null) {
    clearInterval(memoryPollTimer);
    memoryPollTimer = null;
  }
};

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
  if (kernelId) {
    startMemoryPolling();
  } else {
    stopMemoryPolling();
    memoryInfo.value = null;
  }
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
    const flowId = nodePythonScript.value?.flow_id;
    const currentNodeId = nodePythonScript.value?.node_id;

    // Fetch kernel artifacts and upstream node IDs in parallel
    const [response, upstreamIds] = await Promise.all([
      KernelApi.getArtifacts(kernelId),
      flowId != null && currentNodeId != null
        ? FlowApi.getNodeUpstreamIds(Number(flowId), currentNodeId)
        : Promise.resolve([] as number[]),
    ]);

    // The kernel /artifacts endpoint returns a dict of artifact name -> metadata
    // Each entry has: type_name, module, node_id, created_at, size_bytes
    const allArtifacts: ArtifactInfo[] = Object.entries(response).map(
      ([name, meta]: [string, any]) => ({
        name,
        type_name: meta?.type_name ?? "",
        node_id: meta?.node_id,
      }),
    );

    // Use the DAG-aware upstream set to filter available artifacts.
    // Only artifacts published by actual upstream nodes are reachable.
    const upstreamSet = new Set(upstreamIds);
    if (currentNodeId != null) {
      availableArtifacts.value = allArtifacts.filter(
        (a) => a.node_id != null && upstreamSet.has(a.node_id),
      );
      publishedArtifacts.value = allArtifacts.filter((a) => a.node_id === currentNodeId);
    } else {
      availableArtifacts.value = [];
      publishedArtifacts.value = [];
    }
  } catch {
    availableArtifacts.value = [];
    publishedArtifacts.value = [];
  } finally {
    artifactsLoading.value = false;
  }
};

// ─── Flow run display outputs ────────────────────────────────────────────────

const loadFlowRunDisplayOutputs = async () => {
  const kernelId = selectedKernelId.value;
  const flowId = nodePythonScript.value?.flow_id;
  const nodeId = nodePythonScript.value?.node_id;
  if (!kernelId || flowId == null || nodeId == null) return;

  try {
    const outputs = await KernelApi.getDisplayOutputs(kernelId, Number(flowId), nodeId);
    if (outputs.length > 0 && cells.value.length > 0) {
      // Attach display outputs to the last cell
      const lastCell = cells.value[cells.value.length - 1];
      cells.value = cells.value.map((c) =>
        c.id === lastCell.id
          ? {
              ...c,
              output: {
                stdout: "",
                stderr: "",
                display_outputs: outputs,
                error: null,
                execution_time_ms: 0,
                execution_count: 0,
              },
            }
          : c,
      );
    }
  } catch {
    // Silently ignore — display outputs are best-effort
  }
};

// Watch for flow run completion to refresh display outputs
watch(
  () => editorStore.isRunning,
  (running, wasRunning) => {
    if (wasRunning && !running && dataLoaded.value) {
      loadFlowRunDisplayOutputs();
      loadArtifacts();
    }
  },
);

// ─── Cell sync ──────────────────────────────────────────────────────────────

const handleCellsUpdate = (updatedCells: NotebookCell[]) => {
  cells.value = updatedCells;
  syncCellsToNode();
};

const syncCellsToNode = () => {
  if (!nodePythonScript.value) return;

  // Persist cells WITHOUT output (outputs are runtime-only, contain base64 images)
  nodePythonScript.value.python_script_input.cells = cells.value.map((c) => ({
    id: c.id,
    code: c.code,
  }));

  // Derive combined code for flow execution
  // flow_graph.py reads python_script_input.code — this must always be populated
  nodePythonScript.value.python_script_input.code = cells.value
    .map((c) => c.code)
    .filter(Boolean)
    .join("\n\n");
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
        cells.value = input.cells.map((c) => ({
          id: c.id,
          code: c.code,
          output: null,
        }));
      } else {
        // Backward compat: create single cell from existing code
        cells.value = [
          {
            id: crypto.randomUUID(),
            code: input.code || DEFAULT_PYTHON_SCRIPT_CODE,
            output: null,
          },
        ];
      }

      selectedKernelId.value = nodePythonScript.value!.python_script_input.kernel_id;

      // Ensure output_names has a default value
      if (!nodePythonScript.value!.python_script_input.output_names) {
        nodePythonScript.value!.python_script_input.output_names = ["main"];
      }

      showEditor.value = true;
      dataLoaded.value = true;

      // Sync output handles with current output_names (labels + count)
      syncOutputHandles();

      // Load kernels, artifacts, display outputs, and start memory polling
      await loadKernels();
      startKernelPolling();
      loadArtifacts();
      loadFlowRunDisplayOutputs();
      if (selectedKernelId.value) {
        startMemoryPolling();
      }
    }
  } catch (error) {
    console.error("Failed to load node data:", error);
    showEditor.value = false;
    dataLoaded.value = false;
  }
};

onUnmounted(() => {
  stopKernelPolling();
  stopMemoryPolling();
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

/* ─── Memory usage ────────────────────────────────────────────────────── */

.kernel-memory {
  display: flex;
  align-items: center;
  gap: 0.4rem;
  padding: 0.25rem 0.5rem;
  font-size: 0.75rem;
  border-radius: 4px;
  font-family: var(--el-font-family, monospace);
}

.kernel-memory--normal {
  color: var(--el-color-success-dark-2, #529b2e);
  background-color: var(--el-color-success-light-9, #f0f9eb);
}

.kernel-memory--warning {
  color: var(--el-color-warning-dark-2, #b88230);
  background-color: var(--el-color-warning-light-9, #fdf6ec);
}

.kernel-memory--critical {
  color: var(--el-color-danger-dark-2, #c45656);
  background-color: var(--el-color-danger-light-9, #fef0f0);
}

.kernel-memory__text {
  font-weight: 500;
}

.kernel-memory__percent {
  opacity: 0.7;
}

.kernel-indicator__memory {
  margin-left: 0.5rem;
  padding: 0.1rem 0.4rem;
  border-radius: 3px;
  font-size: 0.75rem;
  font-family: var(--el-font-family, monospace);
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

/* ─── Output names editor ─────────────────────────────────────────────────── */

.output-names-list {
  display: flex;
  flex-direction: column;
  gap: 0.3rem;
}

.output-name-row {
  display: flex;
  align-items: center;
  gap: 0.3rem;
}

.output-name-input {
  flex: 1;
  padding: 0.25rem 0.5rem;
  border: 1px solid var(--el-border-color, #dcdfe6);
  border-radius: 4px;
  font-size: 0.8rem;
  background: var(--el-bg-color, #fff);
  color: var(--el-text-color-primary);
  font-family: var(--el-font-family, monospace);
}

.output-name-input:focus {
  outline: none;
  border-color: var(--el-color-primary, #409eff);
}

.output-remove-btn {
  color: var(--el-color-danger, #f56c6c) !important;
  font-size: 0.75rem;
}

.add-output-btn {
  display: flex;
  align-items: center;
  gap: 0.3rem;
  padding: 0.25rem 0.5rem;
  border: 1px dashed var(--el-border-color, #dcdfe6);
  border-radius: 4px;
  background: transparent;
  color: var(--el-text-color-secondary);
  font-size: 0.75rem;
  cursor: pointer;
  transition: all 0.15s;
}

.add-output-btn:hover {
  border-color: var(--el-color-primary, #409eff);
  color: var(--el-color-primary, #409eff);
}
</style>
