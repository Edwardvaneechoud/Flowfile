<template>
  <div v-if="nodeId !== -1" class="node-explainer">
    <el-collapse v-model="activeNames">
      <el-collapse-item name="explainer">
        <template #title>
          <span class="explainer-title">How would I write this myself?</span>
        </template>
        <div v-if="loading" class="explainer-hint">Working it out…</div>
        <div v-else-if="failed" class="explainer-hint">Couldn't work this one out right now.</div>
        <template v-else>
          <p v-if="explanation?.explanation" class="explainer-prose">
            {{ explanation.explanation }}
          </p>
          <p v-else class="explainer-hint">
            There's no plain-Python walkthrough for this node type yet — it leans on a library to do
            its work.
          </p>
          <div v-if="explanation?.code" class="explainer-code">
            <codemirror :model-value="explanation.code" :extensions="extensions" :disabled="true" />
          </div>
          <div v-if="explanation?.code" class="explainer-actions">
            <button class="explainer-copy" @click="copySnippet">
              {{ copied ? "Copied ✓" : "Copy" }}
            </button>
          </div>
        </template>
      </el-collapse-item>
    </el-collapse>
  </div>
</template>

<script setup lang="ts">
import { ref, watch } from "vue";
import { Codemirror } from "vue-codemirror";
import { python } from "@codemirror/lang-python";
import { oneDark } from "@codemirror/theme-one-dark";
import { EditorState } from "@codemirror/state";
import { EditorView } from "@codemirror/view";
import { NodeApi } from "../../../api/node.api";
import { useNodeStore } from "../../../stores/column-store";
import { copyToClipboard } from "../../../utils/clipboardUtils";
import type { NodeExplanation } from "../../../types";

const props = defineProps<{ nodeId: number }>();

// A learner who wants this open wants it open for every node, not just this one.
const STORAGE_KEY = "flowfile-node-explainer-open";

const nodeStore = useNodeStore();
const activeNames = ref<string[]>(localStorage.getItem(STORAGE_KEY) === "1" ? ["explainer"] : []);
const explanation = ref<NodeExplanation | null>(null);
const loading = ref(false);
const failed = ref(false);
const copied = ref(false);
const loadedNodeId = ref<number | null>(null);

const extensions = [
  python(),
  oneDark,
  EditorView.editable.of(false),
  EditorState.readOnly.of(true),
  EditorView.lineWrapping,
  EditorView.theme({
    "&": { fontSize: "11px" },
    ".cm-content": { padding: "10px" },
    ".cm-focused": { outline: "none" },
  }),
];

// Generating a snippet runs the whole converter, so only fetch when actually open.
const fetchExplanation = async () => {
  if (props.nodeId === -1 || loadedNodeId.value === props.nodeId) return;
  loading.value = true;
  failed.value = false;
  try {
    explanation.value = await NodeApi.explainNode(nodeStore.flow_id, props.nodeId);
    loadedNodeId.value = props.nodeId;
  } catch {
    explanation.value = null;
    failed.value = true;
  } finally {
    loading.value = false;
  }
};

const copySnippet = async () => {
  if (!explanation.value?.code) return;
  const ok = await copyToClipboard(explanation.value.code);
  if (!ok) return;
  copied.value = true;
  setTimeout(() => {
    copied.value = false;
  }, 1500);
};

watch(
  () => props.nodeId,
  () => {
    explanation.value = null;
    loadedNodeId.value = null;
    failed.value = false;
    if (activeNames.value.includes("explainer")) void fetchExplanation();
  },
);

watch(
  activeNames,
  (names) => {
    const open = names.includes("explainer");
    localStorage.setItem(STORAGE_KEY, open ? "1" : "0");
    if (open) void fetchExplanation();
  },
  { immediate: true },
);
</script>

<style scoped>
.node-explainer {
  flex-shrink: 0;
  padding: 0 12px;
  border-bottom: 1px solid var(--el-border-color-lighter);
}

.explainer-title {
  font-size: 12px;
  font-weight: 600;
  color: var(--el-text-color-regular);
}

.explainer-prose {
  margin: 0 0 8px;
  font-size: 12px;
  line-height: 1.55;
  color: var(--el-text-color-regular);
}

.explainer-hint {
  margin: 0;
  font-size: 12px;
  font-style: italic;
  color: var(--el-text-color-secondary);
}

.explainer-code {
  border-radius: 4px;
}

.explainer-actions {
  display: flex;
  justify-content: flex-end;
  margin-top: 6px;
}

.explainer-copy {
  border: 1px solid var(--el-border-color);
  background: transparent;
  color: var(--el-text-color-regular);
  border-radius: 4px;
  padding: 2px 10px;
  font-size: 11px;
  cursor: pointer;
}

.explainer-copy:hover {
  background: var(--el-fill-color-light);
}

.node-explainer :deep(.el-collapse) {
  border: none;
}

.node-explainer :deep(.el-collapse-item__header) {
  border: none;
  height: 34px;
  line-height: 34px;
  background-color: transparent;
}

.node-explainer :deep(.el-collapse-item__wrap) {
  border: none;
  background-color: transparent;
}

/* Capped so the panel never starves the settings form below it — a join snippet
   runs ~25 lines and would otherwise push the actual controls off-screen. */
.node-explainer :deep(.el-collapse-item__content) {
  max-height: 260px;
  overflow: auto;
  padding-bottom: 10px;
}
</style>
