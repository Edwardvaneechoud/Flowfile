<template>
  <el-dialog
    :model-value="modelValue"
    title="Share flow as a browser link"
    width="640px"
    draggable
    @update:model-value="(v: boolean) => emit('update:modelValue', v)"
  >
    <div v-loading="loading" class="share-link-dialog">
      <p v-if="loading" class="muted">Building the share link…</p>

      <el-alert v-else-if="error" type="error" :closable="false" show-icon :title="error" />

      <template v-else-if="report && summary">
        <el-alert
          :type="summary.severity"
          :closable="false"
          show-icon
          :title="summary.headline"
          class="headline"
        />

        <div v-if="placeholders.length" class="section">
          <p class="section-label">Travels as a placeholder</p>
          <ul class="node-list">
            <li v-for="node in placeholders" :key="node.node_id" class="node-row">
              <span class="node-type">{{ node.node_type }}</span>
              <span class="node-reason">{{ nodeReason(node) }}</span>
              <el-button text size="small" @click="showOnCanvas(node.node_id)">
                Show on canvas
              </el-button>
            </li>
          </ul>
        </div>

        <div v-if="report.url" class="section url-row">
          <el-input :model-value="report.url" readonly aria-label="Share link" />
          <el-button type="primary" @click="copyUrl">Copy</el-button>
          <el-button @click="openUrl">Open in browser</el-button>
        </div>
        <el-alert
          v-else
          type="error"
          :closable="false"
          show-icon
          :title="TOO_LARGE_MESSAGE"
          class="section"
        />

        <ul class="notes">
          <li v-for="(note, index) in notes" :key="index">{{ note }}</li>
        </ul>
      </template>
    </div>

    <template #footer>
      <el-button @click="emit('update:modelValue', false)">Close</el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";
import { ElMessage } from "element-plus";
import { ShareLinkApi } from "../../api/shareLink.api";
import type { ShareLinkNodeReport, ShareLinkResponse } from "../../api/shareLink.api";
import { useFlowStore } from "../../stores/flow-store";
import { useResultsStore } from "../../stores/results-store";
import { copyTextEverywhere } from "../../utils/clipboardUtils";
import { desktop } from "../../../lib/desktop";
import {
  TOO_LARGE_MESSAGE,
  nodeReason,
  placeholderRows,
  shareNotes,
  summarizeReport,
} from "./shareLinkReport";

const props = defineProps<{ modelValue: boolean }>();
const emit = defineEmits<{ (e: "update:modelValue", value: boolean): void }>();

const flowStore = useFlowStore();
const resultsStore = useResultsStore();

const loading = ref(false);
const error = ref("");
const report = ref<ShareLinkResponse | null>(null);

const summary = computed(() => (report.value ? summarizeReport(report.value) : null));
const placeholders = computed<ShareLinkNodeReport[]>(() =>
  report.value ? placeholderRows(report.value) : [],
);
const notes = computed(() => (report.value ? shareNotes(report.value) : []));

// Node ids we marked invalid so the canvas shows amber dots while the dialog is
// open, pinned to the flow they were written against.
let markedFlowId: number | null = null;
let markedNodeIds: number[] = [];

function clearMarks() {
  if (markedFlowId === null) return;
  for (const nodeId of markedNodeIds) {
    resultsStore.setNodeValidation(markedFlowId, nodeId, { isValid: true, error: "" });
  }
  markedFlowId = null;
  markedNodeIds = [];
}

function markPlaceholders(flowId: number, rows: ShareLinkNodeReport[]) {
  clearMarks();
  for (const row of rows) {
    resultsStore.setNodeValidation(flowId, row.node_id, {
      isValid: false,
      error: nodeReason(row),
    });
  }
  markedFlowId = flowId;
  markedNodeIds = rows.map((row) => row.node_id);
}

async function onOpen() {
  const flowId = flowStore.flowId;
  error.value = "";
  report.value = null;
  clearMarks();
  if (!flowId) {
    error.value = "Open a flow before creating a share link.";
    return;
  }
  loading.value = true;
  try {
    const response = await ShareLinkApi.getShareLink(flowId);
    // The active flow can change while the request is in flight.
    if (flowStore.flowId !== flowId) return;
    report.value = response;
    markPlaceholders(flowId, placeholderRows(response));
  } catch (e: any) {
    error.value = e?.response?.data?.detail || "Failed to create a share link for this flow.";
  } finally {
    loading.value = false;
  }
}

// Parents mount this dialog with v-if AND open it in the same tick, so el-dialog's
// @open never fires on that initial already-open mount. Load on modelValue instead
// (immediate covers the mount-while-open case; reloads on each subsequent open).
watch(
  () => props.modelValue,
  (open) => {
    if (open) void onOpen();
    else clearMarks();
  },
  { immediate: true },
);

// A report is only meaningful for the flow it was built from.
watch(
  () => flowStore.flowId,
  () => {
    clearMarks();
    if (props.modelValue) emit("update:modelValue", false);
  },
);

onBeforeUnmount(clearMarks);

function showOnCanvas(nodeId: number) {
  flowStore.vueFlowInstance?.fitView?.({ nodes: [String(nodeId)] });
}

async function copyUrl() {
  if (!report.value?.url) return;
  const copied = await copyTextEverywhere(report.value.url);
  if (copied) ElMessage.success("Share link copied to the clipboard.");
  else ElMessage.error("Could not copy the link — select it and copy manually.");
}

async function openUrl() {
  if (!report.value?.url) return;
  await desktop.openExternal(report.value.url);
}
</script>

<style scoped>
.share-link-dialog {
  min-height: 140px;
}
.muted {
  color: var(--el-text-color-secondary);
  font-size: 13px;
  margin: 0;
}
.headline {
  margin-bottom: 12px;
}
.section {
  margin-bottom: 12px;
}
.section-label {
  font-weight: 600;
  margin: 0 0 8px;
  font-size: 13px;
}
.node-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 6px;
  max-height: 220px;
  overflow-y: auto;
}
.node-row {
  display: flex;
  align-items: center;
  gap: 10px;
  font-size: 13px;
}
.node-type {
  font-family: var(--font-family-mono, monospace);
  flex-shrink: 0;
}
.node-reason {
  flex: 1;
  color: var(--el-text-color-secondary);
}
.url-row {
  display: flex;
  gap: 8px;
  align-items: center;
}
.notes {
  margin: 0;
  padding-left: 18px;
  color: var(--el-text-color-secondary);
  font-size: 12px;
  line-height: 1.6;
}
</style>
