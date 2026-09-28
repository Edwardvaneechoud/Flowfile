<template>
  <div :class="['code-container', { 'is-notebook': codeMode === 'notebook' }]">
    <div class="code-header">
      <h4>Generated code</h4>
      <div class="mode-toggle" role="group" aria-label="Code mode">
        <button
          :class="['mode-segment', { active: codeMode === 'flowframe' }]"
          :aria-pressed="codeMode === 'flowframe'"
          data-testid="code-mode-flowframe"
          @click="setMode('flowframe')"
        >
          FlowFrame
        </button>
        <button
          :class="['mode-segment', { active: codeMode === 'polars' }]"
          :aria-pressed="codeMode === 'polars'"
          data-testid="code-mode-polars"
          @click="setMode('polars')"
        >
          Polars
        </button>
        <button
          :class="['mode-segment', { active: codeMode === 'project' }]"
          :aria-pressed="codeMode === 'project'"
          data-testid="code-mode-project"
          @click="setMode('project')"
        >
          Project
        </button>
        <button
          :class="['mode-segment', { active: codeMode === 'notebook' }]"
          :aria-pressed="codeMode === 'notebook'"
          data-testid="code-mode-notebook"
          @click="setMode('notebook')"
        >
          Notebook
        </button>
      </div>
      <button
        class="close-btn"
        type="button"
        aria-label="Close code pane"
        title="Close (Ctrl/Cmd+G)"
        @click="editorStore.setCodeGeneratorVisibility(false)"
      >
        <span class="material-icons" aria-hidden="true">close</span>
      </button>
    </div>
    <div v-if="!ownsBody" class="code-toolbar">
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
      <div v-if="codeMode === 'project'" class="code-project">
        <ProjectExport />
      </div>
      <div v-else-if="codeMode === 'notebook'" class="code-notebook">
        <NotebookPanel :key="nodeStore.flow_id" :flow-id="nodeStore.flow_id" />
      </div>
      <codemirror v-else v-model="code" :extensions="extensions" :disabled="true" />
    </template>
  </div>
</template>

<script lang="ts" setup>
import { computed, defineAsyncComponent, onBeforeUnmount, ref, watch } from "vue";
import axios from "axios";
import { Codemirror } from "vue-codemirror";
import { python } from "@codemirror/lang-python";
import { EditorView } from "@codemirror/view";
import { flowfileEditorTheme } from "@/utils/codemirrorTheme";
import ProjectExport from "./ProjectExport.vue";
import { useNodeStore } from "../../../stores/column-store";
import { useEditorStore } from "../../../stores/editor-store";

// `active` = the pane is shown. CodeMirror must not be created while its pane is
// display:none, so the editor renders (and code fetches) only when active.
const props = defineProps<{ active?: boolean }>();

const NotebookPanel = defineAsyncComponent(() => import("../../CatalogView/NotebookPanel.vue"));

type CodeMode = "flowframe" | "polars" | "project" | "notebook";

const MODE_KEY = "flowfile.codeGenerator.mode.v1";
const MODES: readonly CodeMode[] = ["flowframe", "polars", "project", "notebook"];

const readMode = (): CodeMode => {
  try {
    const saved = localStorage.getItem(MODE_KEY) as CodeMode | null;
    return saved && MODES.includes(saved) ? saved : "flowframe";
  } catch {
    return "flowframe";
  }
};

const code = ref("");
const loading = ref(false);
const codeMode = ref<CodeMode>(readMode());
// Project and notebook render their own component and fetch for themselves.
const ownsBody = computed(() => codeMode.value === "project" || codeMode.value === "notebook");
const nodeStore = useNodeStore();
const editorStore = useEditorStore();
const lastLoadedFlowId = ref<number | null>(null);

const extensions = [
  python(),
  flowfileEditorTheme(),
  EditorView.theme({ ".cm-content": { padding: "8px 0" } }),
];

const endpointMap: Partial<Record<CodeMode, string>> = {
  flowframe: "/editor/code_to_flowframe",
  polars: "/editor/code_to_polars",
};

const exportConfirmMap: Partial<Record<CodeMode, string>> = {
  flowframe: "/editor/code_to_flowframe/exported",
  polars: "/editor/code_to_polars/exported",
};

// Only the latest request writes the viewer, so a mode switch mid-fetch can't be overwritten.
let fetchSeq = 0;

const fetchCode = async () => {
  if (ownsBody.value) return;
  const seq = ++fetchSeq;
  loading.value = true;
  try {
    const endpoint = endpointMap[codeMode.value];
    const response = await axios.get(`${endpoint}?flow_id=${nodeStore.flow_id}`);
    if (seq !== fetchSeq) return;
    code.value = response.data;
    lastLoadedFlowId.value = nodeStore.flow_id;
  } catch (error: any) {
    if (seq !== fetchSeq) return;
    console.error("Failed to fetch code:", error);
    const detail = error?.response?.data?.detail;
    if (detail) {
      code.value = `# ${detail}`;
    } else {
      code.value = "# Failed to generate code. Please check your flow configuration.";
    }
  } finally {
    if (seq === fetchSeq) loading.value = false;
  }
};

const setMode = (mode: CodeMode) => {
  if (codeMode.value !== mode) {
    codeMode.value = mode;
    try {
      localStorage.setItem(MODE_KEY, mode);
    } catch {
      // Storage unavailable: the mode just isn't remembered.
    }
    if (nodeStore.flow_id > 0) {
      fetchCode();
    }
  }
};

// Fetch when the pane becomes visible (active) for a flow we haven't loaded yet.
watch(
  () => [props.active, nodeStore.flow_id] as const,
  ([active, flowId]) => {
    if (active && flowId > 0 && flowId !== lastLoadedFlowId.value) {
      fetchCode();
    }
  },
  { immediate: true },
);

// A graph edit invalidates the cached code; the open pane regenerates it once edits settle.
let refetchTimer: ReturnType<typeof setTimeout> | null = null;
watch(
  () => editorStore.graphVersion,
  () => {
    lastLoadedFlowId.value = null;
    if (refetchTimer) clearTimeout(refetchTimer);
    refetchTimer = setTimeout(() => {
      refetchTimer = null;
      if (props.active && nodeStore.flow_id > 0) fetchCode();
    }, 400);
  },
);

onBeforeUnmount(() => {
  if (refetchTimer) clearTimeout(refetchTimer);
});

const refreshCode = () => {
  if (nodeStore.flow_id > 0) {
    fetchCode();
  }
};

const exportCode = () => {
  const blob = new Blob([code.value], { type: "text/plain" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "pipeline_code.py";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);

  const confirmEndpoint = exportConfirmMap[codeMode.value];
  if (confirmEndpoint) {
    // Fire-and-forget: confirming the export must never affect the download.
    void axios.post(confirmEndpoint).catch(() => undefined);
  }
};
</script>

<style scoped>
.code-container {
  height: 100%;
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
}

.code-header {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  min-height: 44px;
  padding: 0 var(--spacing-2) 0 var(--spacing-4);
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-primary);
  flex-shrink: 0;
}

/* The notebook toolbar continues the header, so the seam between them is lighter. */
.code-container.is-notebook .code-header {
  border-bottom-color: var(--color-border-light);
}

/* The disabled CodeMirror viewer fills the remaining pane height and scrolls. */
.code-container:not(.is-notebook) :deep(.cm-editor) {
  flex: 1;
  min-height: 0;
  margin: 0 16px 16px;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  overflow: hidden;
}

.code-project {
  flex: 1;
  min-height: 0;
  overflow: auto;
  padding: 12px 16px 16px;
}

.code-notebook {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
  cursor: auto;
}

.code-notebook > :deep(.notebook-panel) {
  flex: 1;
  min-height: 0;
}

.code-header h4 {
  flex: 0 1 auto;
  min-width: 0;
  margin: 0;
  overflow: hidden;
  color: var(--color-text-primary);
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.close-btn {
  display: inline-flex;
  flex: none;
  align-items: center;
  justify-content: center;
  width: 28px;
  height: 28px;
  margin-left: auto;
  padding: 0;
  border: none;
  border-radius: var(--border-radius-md);
  background: transparent;
  color: var(--color-text-tertiary);
  cursor: pointer;
  transition:
    background-color var(--transition-fast),
    color var(--transition-fast);
}

.close-btn .material-icons {
  font-size: 18px;
}

.close-btn:hover {
  color: var(--color-text-primary);
  background: var(--color-background-tertiary);
}

/* Raised-segment control: tertiary track, the active mode lifts onto the surface. */
.mode-toggle {
  display: flex;
  flex: none;
  gap: 2px;
  padding: 2px;
  background: var(--color-background-tertiary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
}

.mode-segment {
  height: 24px;
  padding: 0 10px;
  border: none;
  border-radius: var(--border-radius-sm);
  background: transparent;
  color: var(--color-text-secondary);
  cursor: pointer;
  font-family: inherit;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  white-space: nowrap;
  transition:
    background-color var(--transition-fast),
    color var(--transition-fast),
    box-shadow var(--transition-fast);
}

.mode-segment.active {
  background: var(--color-background-primary);
  color: var(--color-primary);
  box-shadow: var(--shadow-xs);
}

.mode-segment:not(.active):hover {
  color: var(--color-text-primary);
}

.mode-segment:focus-visible {
  outline: 2px solid var(--color-focus-ring-accent-strong);
  outline-offset: 1px;
}

.code-toolbar {
  display: flex;
  justify-content: flex-end;
  align-items: center;
  gap: 8px;
  padding: 12px 16px;
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
