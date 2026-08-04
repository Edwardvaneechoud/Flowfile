<template>
  <el-dialog
    :model-value="isOpen"
    :title="title"
    width="560px"
    align-center
    append-to-body
    class="high-z-index-dialog file-drop-dialog"
    :show-close="!isUploading"
    :close-on-click-modal="!isUploading"
    :close-on-press-escape="!isUploading"
    @update:model-value="onModelUpdate"
  >
    <p class="fd-intro">{{ intro }}</p>

    <ul class="fd-rows">
      <li v-for="entry in entries" :key="entry.key" class="fd-row">
        <span class="fd-name" :title="entry.name">{{ entry.name }}</span>
        <span class="fd-chip">{{ entry.fileType }}</span>
        <span class="fd-size">{{ formatBytes(entry.size) }}</span>
        <span class="fd-status">
          <template v-if="entry.status === 'pending'">Ready</template>
          <template v-else-if="entry.status === 'uploading'">
            <el-progress :percentage="entry.progress" :show-text="false" class="fd-bar" />
            {{ entry.progress }}%
          </template>
          <template v-else-if="entry.status === 'done'">
            <span class="fd-done">✓ {{ uploadedLabel(entry) }}</span>
          </template>
          <template v-else-if="entry.status === 'error'">
            <span class="fd-error">{{ entry.error }}</span>
          </template>
          <template v-else>
            <span class="fd-skipped">Skipped</span>
          </template>
        </span>
      </li>
    </ul>

    <p class="fd-totals">{{ totalsLine }}</p>
    <p class="fd-footnote">{{ limitsLine }}</p>
    <p v-if="isMultiUser" class="fd-footnote">
      Uploads land in the shared uploads folder, managed from File Manager.
    </p>

    <template #footer>
      <div v-if="phase === 'uploading'" class="fd-footer">
        <el-progress :percentage="overallProgress" class="fd-overall" />
        <el-button @click="controller.cancelUpload">Cancel upload</el-button>
      </div>
      <div v-else-if="phase === 'error'" class="fd-footer">
        <el-button type="primary" @click="controller.cancelDialog">Close</el-button>
      </div>
      <div v-else class="fd-footer">
        <el-button @click="controller.cancelDialog">Cancel</el-button>
        <el-button type="primary" :disabled="!hasUploadable" @click="controller.confirmUpload">
          Upload and add {{ uploadableCount }} node{{ uploadableCount === 1 ? "" : "s" }}
        </el-button>
      </div>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, watch } from "vue";

import { useMultiUser } from "../../composables/useMultiUser";
import {
  formatBytes,
  type DroppedFileEntry,
  type FileDropImport,
} from "../../composables/useFileDropImport";

const props = defineProps<{ controller: FileDropImport }>();

const { isMultiUser, refresh } = useMultiUser();

const phase = computed(() => props.controller.phase.value);
const entries = computed(() => props.controller.entries.value);
const limits = computed(() => props.controller.limits.value);
const overallProgress = computed(() => props.controller.overallProgress.value);
const isOpen = computed(() => phase.value !== "idle");
const isUploading = computed(() => phase.value === "uploading");
const isLocalCopy = computed(() => props.controller.uploadTargetKind.value === "local");

const uploadableCount = computed(() => entries.value.filter((e) => e.status !== "error").length);
const hasUploadable = computed(() => uploadableCount.value > 0);

const title = computed(() => {
  const count = entries.value.length;
  const files = `${count} file${count === 1 ? "" : "s"}`;
  return isLocalCopy.value ? `Copy ${files} into Flowfile?` : `Upload ${files} to the server?`;
});

const intro = computed(() =>
  isLocalCopy.value
    ? "This file can't be linked directly, so Flowfile will copy it into its local uploads folder."
    : "Files must be uploaded before a Read node can open them.",
);

const totalsLine = computed(() => {
  const bytes = props.controller.totalBytes.value;
  const count = entries.value.length;
  return `${count} file${count === 1 ? "" : "s"} · ${formatBytes(bytes)} total`;
});

const limitsLine = computed(() => {
  const allowed = limits.value.allowed_extensions.map((e) => `.${e}`).join(" ");
  return `Max ${formatBytes(limits.value.max_file_size_bytes)} per file — Allowed: ${allowed}`;
});

function uploadedLabel(entry: DroppedFileEntry): string {
  return entry.serverName && entry.serverName !== entry.name
    ? `Uploaded as "${entry.serverName}"`
    : "Uploaded";
}

function onModelUpdate(open: boolean) {
  if (!open) props.controller.cancelDialog();
}

// @open never fires for a dialog mounted already-open — watch the open state instead.
watch(
  isOpen,
  (open) => {
    if (open) void refresh();
  },
  { immediate: true },
);
</script>

<style scoped>
.fd-intro {
  margin: 0 0 12px;
  font-size: 13px;
  color: var(--color-text-secondary);
}

.fd-rows {
  margin: 0;
  padding: 0;
  list-style: none;
  max-height: 260px;
  overflow-y: auto;
}

.fd-row {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 0;
  border-bottom: 1px solid var(--color-border-primary);
  font-size: 13px;
}

.fd-name {
  flex: 1 1 auto;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: var(--color-text-primary);
}

.fd-chip {
  flex: none;
  padding: 1px 8px;
  border-radius: 10px;
  background-color: var(--color-background-secondary);
  color: var(--color-text-secondary);
  font-size: 11px;
}

.fd-size {
  flex: none;
  color: var(--color-text-secondary);
  font-variant-numeric: tabular-nums;
}

.fd-status {
  display: flex;
  align-items: center;
  gap: 6px;
  flex: none;
  min-width: 140px;
  justify-content: flex-end;
  color: var(--color-text-secondary);
  font-size: 12px;
}

.fd-bar {
  width: 70px;
}

.fd-done {
  color: var(--color-success);
}

.fd-error {
  color: var(--color-danger);
}

.fd-skipped {
  color: var(--color-text-secondary);
}

.fd-totals {
  margin: 12px 0 4px;
  font-size: 12px;
  color: var(--color-text-primary);
}

.fd-footnote {
  margin: 0 0 4px;
  font-size: 11px;
  color: var(--color-text-secondary);
}

.fd-footer {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 12px;
}

.fd-overall {
  flex: 1 1 auto;
}
</style>
