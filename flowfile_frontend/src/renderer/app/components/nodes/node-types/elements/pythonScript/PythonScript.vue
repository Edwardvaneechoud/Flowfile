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

        <!-- Code Editor -->
        <div class="setting-block">
          <div class="code-header">
            <label class="setting-label">Code</label>
            <button class="help-button" title="API Reference" @click="showHelp = true">
              <i class="fa-solid fa-circle-question"></i>
            </button>
          </div>
          <div class="editor-container">
            <codemirror
              v-if="showEditor"
              v-model="code"
              placeholder="Write your Python code here..."
              :style="{ height: '400px' }"
              :autofocus="true"
              :indent-with-tab="false"
              :tab-size="4"
              :extensions="editorExtensions"
              @ready="handleEditorReady"
              @blur="handleEditorBlur"
            />
          </div>
        </div>

        <!-- Artifacts Panel -->
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
      </div>
    </generic-node-settings>

    <!-- Help Modal -->
    <FlowfileApiHelp v-if="showHelp" @close="showHelp = false" />
  </div>

  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref, computed, watch, onMounted, onUnmounted } from "vue";
import type { Extension } from "@codemirror/state";
import { EditorView, keymap } from "@codemirror/view";
import { EditorState, Prec } from "@codemirror/state";
import { Codemirror } from "vue-codemirror";
import { python } from "@codemirror/lang-python";
import { oneDark } from "@codemirror/theme-one-dark";
import { autocompletion, acceptCompletion } from "@codemirror/autocomplete";
import type { CompletionSource } from "@codemirror/autocomplete";
import { indentMore, indentLess } from "@codemirror/commands";
import { CodeLoader } from "vue-content-loader";

import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import type { NodePythonScript } from "../../../../../types/node.types";
import type { NodeData } from "../../../baseNode/nodeInterfaces";
import type { KernelInfo } from "../../../../../types/kernel.types";
import { KernelApi } from "../../../../../api/kernel.api";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import FlowfileApiHelp from "./FlowfileApiHelp.vue";
import { flowfileCompletionVals } from "./flowfileCompletions";
import { createPythonScriptNode } from "./utils";

// ─── State ──────────────────────────────────────────────────────────────────

const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const showEditor = ref(false);
const showHelp = ref(false);

const nodePythonScript = ref<NodePythonScript | null>(null);
const nodeData = ref<NodeData | null>(null);
const code = ref("");

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

// ─── Code editor setup ─────────────────────────────────────────────────────

const flowfileCompletions: CompletionSource = (context) => {
  const word = context.matchBefore(/\w*/);
  if (word?.from === word?.to && !context.explicit) {
    return null;
  }
  return {
    from: word?.from ?? 0,
    options: flowfileCompletionVals,
  };
};

const tabKeymap = keymap.of([
  {
    key: "Tab",
    run: (view: EditorView): boolean => {
      if (acceptCompletion(view)) return true;
      return indentMore(view);
    },
  },
  {
    key: "Shift-Tab",
    run: (view: EditorView): boolean => {
      return indentLess(view);
    },
  },
]);

const editorExtensions: Extension[] = [
  python(),
  oneDark,
  EditorState.tabSize.of(4),
  autocompletion({
    override: [flowfileCompletions],
    defaultKeymap: true,
    closeOnBlur: false,
  }),
  Prec.highest(tabKeymap),
];

const handleEditorReady = (_payload: { view: EditorView }) => {
  // Editor is ready
};

const handleEditorBlur = () => {
  // Sync code back to node on blur
  syncCodeToNode();
};

const syncCodeToNode = () => {
  if (nodePythonScript.value) {
    nodePythonScript.value.python_script_input.code = code.value;
  }
};

// Keep code synced as user types
watch(code, () => {
  syncCodeToNode();
});

// ─── Node settings composable ───────────────────────────────────────────────

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodePythonScript,
  onBeforeSave: () => {
    syncCodeToNode();
    if (!nodePythonScript.value?.python_script_input.code) {
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

      // Sync local state from node data
      code.value = nodePythonScript.value!.python_script_input.code;
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

onMounted(() => {
  // Polling starts after loadNodeData
});

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

.help-button {
  background: none;
  border: none;
  cursor: pointer;
  font-size: 1rem;
  color: var(--el-text-color-secondary);
  padding: 0;
  line-height: 1;
}

.help-button:hover {
  color: var(--el-color-primary);
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

/* ─── Code editor ────────────────────────────────────────────────────────── */

.editor-container {
  border: 1px solid var(--el-border-color);
  border-radius: 3px;
  overflow: hidden;
  box-shadow: inset 0 1px 3px rgba(0, 0, 0, 0.08);
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
</style>
