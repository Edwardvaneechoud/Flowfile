<template>
  <div class="code-container">
    <div class="code-header">
      <h4>Generated code</h4>
      <div class="header-actions">
        <button class="refresh-button" :disabled="loading" @click="refreshCode">
          <svg
            v-if="!loading"
            width="16"
            height="16"
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
        <button class="export-button" @click="exportCode">
          <svg
            width="16"
            height="16"
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
    </div>
    <codemirror v-model="code" :extensions="extensions" :disabled="true" />
  </div>
</template>

<script lang="ts" setup>
import { ref, onMounted, watch } from "vue";
import axios from "axios";
import { Codemirror } from "vue-codemirror";
import { python } from "@codemirror/lang-python";
import { oneDark } from "@codemirror/theme-one-dark";
import { EditorView } from "@codemirror/view";
import { useNodeStore } from "../../../../../stores/column-store";

const code = ref("");
const loading = ref(false);
const nodeStore = useNodeStore();
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

const fetchCode = async () => {
  loading.value = true;
  try {
    const response = await axios.get(`/editor/code_to_polars?flow_id=${nodeStore.flow_id}`);
    code.value = response.data;
    lastLoadedFlowId.value = nodeStore.flow_id;
  } catch (error) {
    console.error("Failed to fetch code:", error);
    code.value = "# Failed to generate code. Please check your flow configuration.";
  } finally {
    loading.value = false;
  }
};

// Watch for when the component becomes visible
watch(
  () => nodeStore.showCodeGenerator,
  (isShowing) => {
    if (isShowing && nodeStore.flow_id > 0) {
      fetchCode();
    }
  },
);

// Also watch for flow changes while the component is visible
watch(
  () => nodeStore.flow_id,
  (newFlowId) => {
    if (nodeStore.showCodeGenerator && newFlowId !== lastLoadedFlowId.value && newFlowId > 0) {
      fetchCode();
    }
  },
);

// Initial load if component is mounted while visible
onMounted(() => {
  if (nodeStore.showCodeGenerator && nodeStore.flow_id > 0) {
    fetchCode();
  }
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
};
</script>

<style scoped>
.code-container {
  max-width: 1200px;
  margin: 0 auto;
  padding: 20px;
}

.code-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
}

.code-header h4 {
  margin: 0;
}

.header-actions {
  display: flex;
  gap: 12px;
}

.export-button,
.refresh-button {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 16px;
  background: #007acc;
  color: white;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-size: 14px;
  transition: background 0.2s;
}

.export-button:hover,
.refresh-button:hover:not(:disabled) {
  background: #005a9e;
}

.refresh-button:disabled {
  background: #6c757d;
  cursor: not-allowed;
}

.spinner {
  display: inline-block;
  width: 14px;
  height: 14px;
  border: 2px solid #ffffff;
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
