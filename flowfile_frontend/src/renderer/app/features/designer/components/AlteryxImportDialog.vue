<template>
  <el-dialog
    :model-value="visible"
    :title="title"
    width="620px"
    align-center
    append-to-body
    class="high-z-index-dialog alteryx-dialog"
    :show-close="!isConverting"
    :close-on-click-modal="!isConverting"
    :close-on-press-escape="!isConverting"
    @update:model-value="onModelUpdate"
  >
    <div v-if="phase === 'idle'">
      <p class="ax-intro">
        Flowfile converts the Alteryx tools it supports and flags everything that needs manual work
        in a report. The original file is never changed.
      </p>
      <div
        class="ax-drop"
        :class="{ 'ax-drop--active': isDragging }"
        role="button"
        tabindex="0"
        @click="pickFile"
        @keydown.enter="pickFile"
        @keydown.space.prevent="pickFile"
        @dragover.prevent="isDragging = true"
        @dragleave.prevent="isDragging = false"
        @drop.prevent="handleDrop"
      >
        <span class="material-icons ax-drop-icon">upload_file</span>
        <span class="ax-drop-text">Drop a workflow here, or click to browse</span>
        <span class="ax-drop-hint">.yxmd or .xml · up to 20 MB</span>
      </div>
    </div>

    <div v-else-if="phase === 'confirm'">
      <p class="ax-file">
        <span class="material-icons ax-file-icon">description</span>
        <span class="ax-file-name" :title="fileName">{{ fileName }}</span>
      </p>
      <p class="ax-note">
        The workflow is uploaded to Flowfile, converted, and saved as a new flow.
      </p>
    </div>

    <div v-else-if="phase === 'converting'">
      <p class="ax-file">
        <span class="material-icons ax-file-icon">description</span>
        <span class="ax-file-name" :title="fileName">{{ fileName }}</span>
      </p>
      <el-progress :percentage="progress" />
      <p class="ax-note">Uploading and converting…</p>
    </div>

    <div v-else-if="phase === 'error'">
      <p class="ax-error">{{ errorMessage }}</p>
    </div>

    <div v-else-if="report">
      <p class="ax-headline" :class="{ 'ax-headline--warn': attention > 0 }">
        <span class="material-icons ax-headline-icon">
          {{ attention > 0 ? "warning" : "check_circle" }}
        </span>
        {{ headline }}
      </p>
      <p class="ax-summary">{{ summary }}</p>
      <ul class="ax-rows">
        <li v-for="(row, index) in rows" :key="`${row.alteryx_tool_id}-${index}`" class="ax-row">
          <div class="ax-row-head">
            <span class="ax-tool" :title="row.alteryx_tool">{{ row.alteryx_tool }}</span>
            <span class="ax-tool-id">#{{ row.alteryx_tool_id }}</span>
            <span class="status-badge ax-chip" :class="statusChip(row.status).className">
              {{ statusChip(row.status).label }}
            </span>
            <span class="ax-node">{{ row.flowfile_node_type || "—" }}</span>
          </div>
          <ul v-if="row.messages.length" class="ax-messages">
            <li v-for="(message, i) in row.messages" :key="i">{{ message }}</li>
          </ul>
        </li>
      </ul>
    </div>

    <template #footer>
      <div class="ax-footer">
        <template v-if="phase === 'converting'">
          <el-button @click="controller.cancel">Cancel</el-button>
        </template>
        <template v-else-if="phase === 'confirm'">
          <el-button @click="close">Cancel</el-button>
          <el-button type="primary" @click="controller.confirmImport">Convert workflow</el-button>
        </template>
        <template v-else-if="phase === 'error'">
          <el-button @click="close">Close</el-button>
          <el-button type="primary" @click="pickFile">Choose another file</el-button>
        </template>
        <template v-else-if="phase === 'report'">
          <el-button @click="close">Close</el-button>
          <el-button type="primary" @click="openImportedFlow">Open flow</el-button>
        </template>
        <template v-else>
          <el-button @click="close">Cancel</el-button>
        </template>
      </div>
    </template>
  </el-dialog>

  <input
    ref="fileInput"
    type="file"
    accept=".yxmd,.xml"
    class="ax-hidden-input"
    @change="handleInputChange"
  />
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { ElMessage } from "element-plus";
import { useRouter } from "vue-router";

import { useAlteryxImport } from "../../../composables/useAlteryxImport";
import { useFlowStore } from "../../../stores/flow-store";
import { needsAttentionCount, sortReportRows, statusChip, summaryLine } from "./alteryxReport";

const props = defineProps<{ visible: boolean }>();

const emit = defineEmits<{
  (e: "update:visible", value: boolean): void;
  (e: "imported", flowId: number): void;
}>();

const router = useRouter();
const flowStore = useFlowStore();
const controller = useAlteryxImport();

const fileInput = ref<HTMLInputElement | null>(null);
const isDragging = ref(false);

const phase = computed(() => controller.phase.value);
const fileName = computed(() => controller.fileName.value);
const progress = computed(() => controller.progress.value);
const errorMessage = computed(() => controller.errorMessage.value);
const report = computed(() => controller.result.value?.report ?? null);
const isConverting = computed(() => phase.value === "converting");

const rows = computed(() => (report.value ? sortReportRows(report.value.rows) : []));
const attention = computed(() => (report.value ? needsAttentionCount(report.value) : 0));
const summary = computed(() => (report.value ? summaryLine(report.value) : ""));

const title = computed(() =>
  report.value ? `Imported "${report.value.workflow_name}"` : "Import Alteryx workflow",
);

const headline = computed(() => {
  if (!report.value) return "";
  const count = attention.value;
  if (count === 0) return "Every tool converted — the flow is ready to open.";
  const subject = count === 1 ? "1 tool needs" : `${count} tools need`;
  return `${subject} manual work — the flow opens with notes on those nodes.`;
});

function pickFile() {
  fileInput.value?.click();
}

function handleInputChange(event: Event) {
  const input = event.target as HTMLInputElement;
  const file = input.files?.[0];
  // Clear the input so re-picking the same file fires change again.
  input.value = "";
  if (file) controller.start(file);
}

function handleDrop(event: DragEvent) {
  isDragging.value = false;
  const file = event.dataTransfer?.files?.[0];
  if (file) controller.start(file);
}

// State is reset on open, not on close, so the body doesn't flicker while the
// dialog animates away.
function close() {
  emit("update:visible", false);
}

function onModelUpdate(open: boolean) {
  if (!open) close();
}

function openImportedFlow() {
  const imported = controller.result.value;
  if (!imported) return;
  const workflowName = imported.report.workflow_name;
  const flowId = imported.flow_id;
  close();
  // Canvas renders off the flowId watcher — never call loadFlow directly.
  flowStore.setFlowId(flowId);
  emit("imported", flowId);
  ElMessage.success(`Imported "${workflowName}"`);
  void router.push({ name: "designer" });
}

// @open never fires for a dialog mounted already-open — watch the prop instead.
watch(
  () => props.visible,
  (open) => {
    if (open) controller.reset();
    isDragging.value = false;
  },
  { immediate: true },
);
</script>

<style scoped>
.ax-hidden-input {
  display: none;
}

.ax-intro {
  margin: 0 0 var(--spacing-4);
  font-size: 13px;
  color: var(--color-text-secondary);
}

.ax-drop {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: var(--spacing-1);
  padding: var(--spacing-8) var(--spacing-4);
  border: 1px dashed var(--color-border-secondary);
  border-radius: var(--border-radius-lg);
  background-color: var(--color-background-secondary);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.ax-drop:hover,
.ax-drop--active,
.ax-drop:focus-visible {
  border-color: var(--color-accent);
  background-color: var(--color-accent-subtle);
  outline: none;
}

.ax-drop-icon {
  font-size: 30px;
  color: var(--color-accent);
}

.ax-drop-text {
  font-size: 13px;
  color: var(--color-text-primary);
}

.ax-drop-hint {
  font-size: 11px;
  color: var(--color-text-muted);
}

.ax-file {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  margin: 0 0 var(--spacing-3);
  font-size: 13px;
  color: var(--color-text-primary);
}

.ax-file-icon {
  font-size: 18px;
  color: var(--color-text-secondary);
}

.ax-file-name {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.ax-note {
  margin: var(--spacing-2) 0 0;
  font-size: 12px;
  color: var(--color-text-secondary);
}

.ax-error {
  margin: 0;
  font-size: 13px;
  color: var(--color-danger);
}

.ax-headline {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  margin: 0 0 var(--spacing-1);
  font-size: 13px;
  font-weight: var(--font-weight-medium);
  color: var(--color-success);
}

.ax-headline--warn {
  color: var(--color-warning);
}

.ax-headline-icon {
  font-size: 18px;
}

.ax-summary {
  margin: 0 0 var(--spacing-3);
  font-size: 12px;
  color: var(--color-text-secondary);
}

.ax-rows {
  margin: 0;
  padding: 0;
  list-style: none;
  max-height: 320px;
  overflow-y: auto;
}

.ax-row {
  padding: var(--spacing-2) 0;
  border-bottom: 1px solid var(--color-border-primary);
}

.ax-row-head {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  font-size: 13px;
}

.ax-tool {
  flex: 1 1 auto;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: var(--color-text-primary);
}

.ax-tool-id {
  flex: none;
  font-size: 11px;
  color: var(--color-text-muted);
  font-variant-numeric: tabular-nums;
}

.ax-node {
  flex: none;
  min-width: 90px;
  text-align: right;
  font-family: var(--font-family-mono);
  font-size: 11px;
  color: var(--color-text-secondary);
}

.ax-chip {
  flex: none;
}

.ax-messages {
  margin: var(--spacing-1) 0 0;
  padding-left: var(--spacing-5);
  font-size: 12px;
  color: var(--color-text-secondary);
}

.ax-footer {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: var(--spacing-3);
}
</style>
