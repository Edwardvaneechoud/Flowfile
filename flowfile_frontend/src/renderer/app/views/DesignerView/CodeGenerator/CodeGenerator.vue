<template>
  <div class="code-container">
    <div class="code-header">
      <h4>Generated code</h4>
      <div class="mode-toggle">
        <button
          :class="['toggle-button', { active: codeMode === 'flowframe' }]"
          @click="setMode('flowframe')"
        >
          FlowFrame
        </button>
        <button
          :class="['toggle-button', { active: codeMode === 'polars' }]"
          @click="setMode('polars')"
        >
          Polars
        </button>
        <button
          :class="['toggle-button', { active: codeMode === 'plain' }]"
          title="No dataframe library — just lists, dicts and loops"
          @click="setMode('plain')"
        >
          Plain Python
        </button>
        <button
          :class="['toggle-button', { active: codeMode === 'project' }]"
          @click="setMode('project')"
        >
          Project
        </button>
      </div>
    </div>
    <div v-if="codeMode !== 'project'" class="code-toolbar">
      <button class="action-btn" :disabled="loading" @click="refreshCode">
        <svg
          v-if="!loading"
          width="14"
          height="14"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          stroke-width="2"
        >
          <path d="M23 4v6h-6"></path>
          <path d="M1 20v-6h6"></path>
          <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"></path>
        </svg>
        <span v-if="loading" class="spinner"></span>
        {{ loading ? "Loading..." : "Refresh" }}
      </button>
      <button class="action-btn" @click="copyCode">
        <svg
          width="14"
          height="14"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          stroke-width="2"
        >
          <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
          <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
        </svg>
        {{ copied ? "Copied ✓" : "Copy" }}
      </button>
      <button class="action-btn primary" @click="exportCode">
        <svg
          width="14"
          height="14"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          stroke-width="2"
        >
          <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
          <polyline points="7 10 12 15 17 10"></polyline>
          <line x1="12" y1="15" x2="12" y2="3"></line>
        </svg>
        Export Code
      </button>
    </div>
    <template v-if="active">
      <ProjectExport v-if="codeMode === 'project'" />
      <codemirror v-else v-model="code" :extensions="extensions" :disabled="true" />
    </template>
  </div>
</template>

<script lang="ts" setup>
import { ref, watch } from "vue";
import axios from "axios";
import { Codemirror } from "vue-codemirror";
import { python } from "@codemirror/lang-python";
import { oneDark } from "@codemirror/theme-one-dark";
import { EditorView } from "@codemirror/view";
import ProjectExport from "./ProjectExport.vue";
import { useNodeStore } from "../../../stores/column-store";
import { useEditorStore } from "../../../stores/editor-store";
import { copyToClipboard } from "../../../utils/clipboardUtils";

// `active` = this is the visible tab. CodeMirror must not be created while its
// pane is display:none, so the editor renders (and code fetches) only when active.
const props = defineProps<{ active?: boolean }>();

type CodeMode = "flowframe" | "polars" | "plain" | "project";

const code = ref("");
const loading = ref(false);
const copied = ref(false);
const codeMode = ref<CodeMode>("flowframe");
const nodeStore = useNodeStore();
const editorStore = useEditorStore();
const lastLoadedFlowId = ref<number | null>(null);

const extensions = [
  python(),
  oneDark,
  EditorView.theme({
    "&": { fontSize: "11px" },
    ".cm-content": { padding: "20px" },
    ".cm-focused": { outline: "none" },
  }),
];

const endpointMap: Partial<Record<CodeMode, string>> = {
  flowframe: "/editor/code_to_flowframe",
  polars: "/editor/code_to_polars",
  plain: "/editor/code_to_plain_python",
};

const fetchCode = async () => {
  // Project mode fetches its own manifest in ProjectExport.vue.
  if (codeMode.value === "project") return;
  loading.value = true;
  try {
    const endpoint = endpointMap[codeMode.value];
    const response = await axios.get(`${endpoint}?flow_id=${nodeStore.flow_id}`);
    code.value = response.data;
    lastLoadedFlowId.value = nodeStore.flow_id;
  } catch (error: any) {
    console.error("Failed to fetch code:", error);
    const detail = error?.response?.data?.detail;
    if (detail) {
      code.value = `# ${detail}`;
    } else {
      code.value = "# Failed to generate code. Please check your flow configuration.";
    }
  } finally {
    loading.value = false;
  }
};

const setMode = (mode: CodeMode) => {
  if (codeMode.value !== mode) {
    codeMode.value = mode;
    if (nodeStore.flow_id > 0) {
      fetchCode();
    }
  }
};

// Fetch when the tab becomes visible (active) for a flow we haven't loaded yet.
watch(
  () => [props.active, nodeStore.flow_id] as const,
  ([active, flowId]) => {
    if (active && flowId > 0 && flowId !== lastLoadedFlowId.value) {
      fetchCode();
    }
  },
  { immediate: true },
);

// A graph edit invalidates the cached code; drop the cache so the next time the
// Code tab opens it re-fetches (rather than regenerating on every hidden edit).
watch(
  () => editorStore.graphVersion,
  () => {
    lastLoadedFlowId.value = null;
  },
);

const refreshCode = () => {
  if (nodeStore.flow_id > 0) {
    fetchCode();
  }
};

const copyCode = async () => {
  if (!code.value) return;
  const ok = await copyToClipboard(code.value);
  if (!ok) return;
  copied.value = true;
  setTimeout(() => {
    copied.value = false;
  }, 1500);
};

const exportCode = () => {
  const blob = new Blob([code.value], { type: "text/plain" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = codeMode.value === "plain" ? "pipeline_plain_python.py" : "pipeline_code.py";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
};
</script>

<style scoped>
.code-container {
  height: 100%;
  display: flex;
  flex-direction: column;
  padding: 20px;
  box-sizing: border-box;
}

.code-header {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 20px;
  flex-shrink: 0;
}

/* The disabled CodeMirror viewer fills the remaining tab height and scrolls. */
.code-container :deep(.cm-editor) {
  flex: 1;
  min-height: 0;
}

.code-header h4 {
  margin: 0;
}

.mode-toggle {
  display: flex;
  gap: 2px;
  padding: 2px;
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
}

.toggle-button {
  padding: 4px 12px;
  border: none;
  border-radius: var(--border-radius-sm);
  background: transparent;
  color: var(--color-text-secondary);
  cursor: pointer;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  transition: all var(--transition-fast);
}

.toggle-button.active {
  background: var(--color-accent);
  color: var(--color-text-inverse);
}

.toggle-button:not(.active):hover {
  color: var(--color-text-primary);
  background: var(--color-background-tertiary);
}

.code-toolbar {
  display: flex;
  justify-content: flex-end;
  align-items: center;
  gap: 8px;
  margin-bottom: 12px;
  flex-shrink: 0;
}

.action-btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  height: 30px;
  padding: 0 12px;
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  box-shadow: var(--shadow-xs);
  transition: all var(--transition-fast);
}

.action-btn svg {
  width: 14px;
  height: 14px;
}

.action-btn:hover:not(:disabled) {
  background: var(--color-background-tertiary);
  border-color: var(--color-border-secondary);
}

.action-btn:active:not(:disabled) {
  transform: translateY(1px);
  box-shadow: none;
}

.action-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.action-btn.primary {
  background: var(--color-accent);
  border-color: var(--color-accent);
  color: var(--color-text-inverse);
}

.action-btn.primary:hover:not(:disabled) {
  background: var(--color-accent-hover);
  border-color: var(--color-accent-hover);
}

.spinner {
  display: inline-block;
  width: 12px;
  height: 12px;
  border: 2px solid currentColor;
  border-radius: 50%;
  border-top-color: transparent;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}
</style>
