<template>
  <el-dialog
    :model-value="visible"
    :title="UPDATE_COPY.headline"
    width="min(480px, calc(100vw - 2rem))"
    align-center
    append-to-body
    :close-on-click-modal="dismissible"
    :close-on-press-escape="dismissible"
    :show-close="dismissible"
    :before-close="onUserClose"
  >
    <div class="update-body">
      <template v-if="phase === 'idle'">
        <p class="update-text">{{ UPDATE_COPY.body(version, currentVersion) }}</p>
        <p v-if="note" class="update-text">{{ note }}</p>
        <button class="link-btn" type="button" @click="openReleaseNotes">
          <i class="fa-solid fa-arrow-up-right-from-square"></i>
          {{ UPDATE_COPY.releaseNotesLabel }}
        </button>
      </template>

      <template v-else-if="phase === 'downloading'">
        <p class="update-text">{{ UPDATE_COPY.downloadingLine }}</p>
        <el-progress
          :percentage="percentage"
          :indeterminate="updateStore.progress.total === null"
          :show-text="updateStore.progress.total !== null"
        />
      </template>

      <template v-else-if="phase === 'backing-up'">
        <p class="update-text">{{ UPDATE_COPY.backingUpLine }}</p>
      </template>

      <template v-else-if="phase === 'installing'">
        <p class="update-text">{{ UPDATE_COPY.installingLine }}</p>
        <p v-if="updateStore.backupPath" class="update-detail">
          {{ UPDATE_COPY.backupPathLine(updateStore.backupPath) }}
        </p>
      </template>

      <template v-else>
        <p class="update-error" role="alert">
          <i class="fa-solid fa-triangle-exclamation"></i>
          {{ failureLine }}
        </p>
        <p v-if="updateStore.error" class="update-detail">{{ updateStore.error }}</p>
      </template>
    </div>

    <template #footer>
      <template v-if="phase === 'idle'">
        <el-button @click="updateStore.skipVersion()">{{ UPDATE_COPY.skipLabel }}</el-button>
        <el-button @click="updateStore.dismiss()">{{ UPDATE_COPY.laterLabel }}</el-button>
        <el-button type="primary" @click="install">{{ UPDATE_COPY.installLabel }}</el-button>
      </template>
      <template v-else-if="phase === 'download-failed'">
        <el-button @click="updateStore.dismiss()">{{ UPDATE_COPY.laterLabel }}</el-button>
        <el-button type="primary" @click="install">{{ UPDATE_COPY.retryLabel }}</el-button>
      </template>
      <template v-else-if="phase === 'backup-failed'">
        <el-button @click="updateStore.resetPhase()">{{ UPDATE_COPY.cancelLabel }}</el-button>
        <el-button type="primary" @click="continueWithoutBackup">
          {{ UPDATE_COPY.continueWithoutBackupLabel }}
        </el-button>
      </template>
      <template v-else-if="phase === 'install-failed'">
        <el-button type="primary" @click="restart">{{ UPDATE_COPY.restartLabel }}</el-button>
      </template>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, watch } from "vue";
import { ElButton, ElDialog, ElProgress } from "element-plus";
import { desktop, desktopPlatform } from "../../../lib/desktop";
import { useUpdateStore } from "../../stores/update-store";
import { UPDATE_COPY, platformNote, releasePageUrl } from "./updatePrompt";

const updateStore = useUpdateStore();

const visible = computed(() => updateStore.promptVisible);
const phase = computed(() => updateStore.phase);
// Only the offer screen may be closed by accident: an install in flight must run to an end.
const dismissible = computed(() => phase.value === "idle");
const version = computed(() => updateStore.info?.version ?? "");
const currentVersion = computed(() => updateStore.info?.currentVersion ?? "");
const note = computed(() => platformNote(desktopPlatform));

const percentage = computed(() => {
  const { downloaded, total } = updateStore.progress;
  if (!total) return 100;
  return Math.min(100, Math.round((downloaded / total) * 100));
});

const failureLine = computed(() => {
  if (phase.value === "download-failed") return UPDATE_COPY.downloadFailedLine;
  if (phase.value === "backup-failed") return UPDATE_COPY.backupFailedLine;
  return UPDATE_COPY.installFailedLine;
});

// el-dialog's @open does not fire when it mounts already-open, so reset on the visibility flip.
watch(
  visible,
  (open) => {
    if (open) updateStore.resetPhase();
  },
  { immediate: true },
);

function install() {
  void updateStore.install();
}

function continueWithoutBackup() {
  void updateStore.continueWithoutBackup();
}

function restart() {
  void updateStore.restart();
}

function onUserClose(done: () => void) {
  updateStore.dismiss();
  done();
}

function openReleaseNotes() {
  void desktop.openExternal(releasePageUrl(version.value));
}
</script>

<style scoped>
.update-body {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
}

.update-text {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  line-height: 1.5;
}

.update-detail {
  margin: 0;
  font-family: var(--font-family-mono);
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  overflow-wrap: anywhere;
}

.update-error {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-2);
  margin: 0;
  padding: var(--spacing-2) var(--spacing-3);
  background: var(--color-warning-light);
  color: var(--color-warning-dark);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
}

.link-btn {
  background: none;
  border: none;
  padding: 0;
  cursor: pointer;
  color: var(--color-accent);
  font-size: var(--font-size-sm);
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  align-self: flex-start;
}

.link-btn:hover {
  text-decoration: underline;
}
</style>
