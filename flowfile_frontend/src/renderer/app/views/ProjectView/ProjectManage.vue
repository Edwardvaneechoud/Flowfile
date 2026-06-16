<template>
  <div class="project-manage">
    <div class="manage-header mb-3">
      <div>
        <h2 class="page-title">
          <i class="fa-solid fa-folder"></i>
          {{ store.activeProject?.name }}
        </h2>
        <p class="page-description folder-path">{{ store.activeProject?.folder_path }}</p>
      </div>
    </div>

    <!-- Files changed outside Flowfile -->
    <div v-if="store.hasExternalChanges" class="banner banner--warning mb-3">
      <i class="fa-solid fa-triangle-exclamation"></i>
      <div class="banner__body">
        <p class="banner__title">Files changed outside Flowfile</p>
        <p class="banner__text">
          Some of this project's files were changed by something other than Flowfile. Reload to
          bring those changes in.
        </p>
      </div>
      <el-button
        v-if="store.reloadAvailable"
        type="warning"
        size="small"
        :loading="reloading"
        @click="handleReload"
      >
        Reload
      </el-button>
    </div>

    <!-- Secrets that need values on this machine -->
    <div v-if="store.placeholderSecrets.length" class="banner banner--info mb-3">
      <i class="fa-solid fa-key"></i>
      <div class="banner__body">
        <p class="banner__title">
          {{ store.placeholderSecrets.length }} secret(s) need values on this computer
        </p>
        <p class="banner__text">
          Flows that use these won't run until you add their values:
          <span class="secret-names">{{ store.placeholderSecrets.join(", ") }}</span
          >. Add a standalone secret's value under Secrets; for a connection, open it and re-enter
          its credentials.
        </p>
      </div>
      <el-button type="primary" size="small" @click="goToSecrets">Add secret values</el-button>
    </div>

    <!-- Save a version -->
    <div class="card mb-3">
      <div class="card-header">
        <h3 class="card-title">Save a version</h3>
        <span class="status-chip" :class="`status-chip--${store.status}`">{{ statusLabel }}</span>
      </div>
      <div class="card-content">
        <p v-if="store.status === 'clean'" class="clean-line">
          <i class="fa-solid fa-circle-check"></i>
          All changes are saved as the latest version.
        </p>
        <template v-else>
          <SaveVersionForm :rows="3" @saved="loadUnsaved" />
          <div v-if="unsavedChanges.length" class="unsaved-changes">
            <p class="unsaved-changes__caption">This version will include:</p>
            <ChangeList :changes="unsavedChanges" />
          </div>
        </template>
      </div>
    </div>

    <!-- Version history -->
    <ProjectVersionHistory v-if="store.versionsAvailable" />

    <!-- Stop using this project -->
    <div class="card mb-3">
      <div class="card-content close-row">
        <div>
          <p class="close-title">Stop using this project</p>
          <p class="close-text">
            Your files stay on disk — Flowfile just stops tracking this folder.
          </p>
        </div>
        <el-button v-if="store.closeAvailable" :loading="closing" @click="handleClose">
          Stop tracking
        </el-button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { useRouter } from "vue-router";
import { ElMessage, ElMessageBox } from "element-plus";
import SaveVersionForm from "../../components/project/SaveVersionForm.vue";
import ChangeList from "./ChangeList.vue";
import ProjectVersionHistory from "./ProjectVersionHistory.vue";
import { useProjectStore } from "../../stores/project-store";
import type { ProjectVersionChange } from "../../types";

const store = useProjectStore();
const router = useRouter();
const reloading = ref(false);
const closing = ref(false);
const unsavedChanges = ref<ProjectVersionChange[]>([]);

const statusLabel = computed(
  () =>
    ({
      clean: "All saved",
      unsaved: "Unsaved changes",
      external: "Changed outside",
      none: "",
    })[store.status],
);

const loadUnsaved = async () => {
  if (!store.isActive || store.status === "clean") {
    unsavedChanges.value = [];
    return;
  }
  unsavedChanges.value = await store.loadUncommittedChanges();
};

onMounted(() => {
  store.loadVersions();
  loadUnsaved();
});

watch(() => store.status, loadUnsaved);

const goToSecrets = () => router.push({ name: "connections", query: { tab: "secrets" } });

const handleReload = async () => {
  if (store.status === "unsaved") {
    try {
      await ElMessageBox.confirm(
        "Reloading replaces your current unsaved changes with what's on disk. Continue?",
        "Reload project",
        { confirmButtonText: "Reload", cancelButtonText: "Cancel", type: "warning" },
      );
    } catch {
      return;
    }
  }
  reloading.value = true;
  try {
    await store.reloadFromDisk();
    ElMessage.success("Project reloaded from disk");
  } catch (e: any) {
    ElMessage.error(e?.message || "Could not reload project");
  } finally {
    reloading.value = false;
  }
};

const handleClose = async () => {
  try {
    await ElMessageBox.confirm(
      "Stop tracking this project? Your files stay on disk; Flowfile just stops mirroring and versioning this folder.",
      "Stop tracking",
      { confirmButtonText: "Stop tracking", cancelButtonText: "Cancel", type: "info" },
    );
  } catch {
    return;
  }
  closing.value = true;
  try {
    await store.closeProject();
    ElMessage.success("Stopped tracking the project");
  } catch (e: any) {
    ElMessage.error(e?.message || "Could not close project");
  } finally {
    closing.value = false;
  }
};
</script>

<style scoped>
.project-manage {
  max-width: 820px;
}

.manage-header .page-title {
  display: flex;
  align-items: center;
  gap: var(--spacing-2, 8px);
}

.folder-path {
  font-family: var(--font-family-mono, monospace);
  font-size: 12px;
  word-break: break-all;
}

.banner {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-3, 12px);
  padding: var(--spacing-3, 12px) var(--spacing-4, 16px);
  border-radius: var(--border-radius-md, 8px);
  border-left: 4px solid var(--color-accent, #2563eb);
  background: var(--color-background-muted, #f8fafc);
}

.banner--warning {
  border-left-color: var(--color-warning, #d97706);
  background: color-mix(in srgb, var(--color-warning, #d97706) 8%, transparent);
}

.banner--info {
  border-left-color: var(--color-accent, #2563eb);
}

.banner > i {
  margin-top: 3px;
  font-size: var(--font-size-lg, 18px);
}

.banner--warning > i {
  color: var(--color-warning, #d97706);
}

.banner--info > i {
  color: var(--color-accent, #2563eb);
}

.banner__body {
  flex: 1;
  min-width: 0;
}

.banner__title {
  margin: 0 0 2px;
  font-size: var(--font-size-sm, 14px);
  font-weight: var(--font-weight-medium, 500);
  color: var(--color-text-primary, #0f172a);
}

.banner__text {
  margin: 0;
  font-size: 13px;
  line-height: 1.5;
  color: var(--color-text-secondary, #475569);
}

.secret-names {
  font-family: var(--font-family-mono, monospace);
  color: var(--color-text-primary, #0f172a);
}

.status-chip {
  font-size: 12px;
  font-weight: var(--font-weight-medium, 500);
  padding: 2px 10px;
  border-radius: 999px;
}

.status-chip--clean {
  color: var(--color-success, #16a34a);
  background: color-mix(in srgb, var(--color-success, #16a34a) 12%, transparent);
}

.status-chip--unsaved {
  color: var(--color-warning, #d97706);
  background: color-mix(in srgb, var(--color-warning, #d97706) 14%, transparent);
}

.status-chip--external {
  color: var(--color-danger, #ef4444);
  background: color-mix(in srgb, var(--color-danger, #ef4444) 12%, transparent);
}

.clean-line {
  display: flex;
  align-items: center;
  gap: 8px;
  margin: 0;
  font-size: var(--font-size-sm, 14px);
  color: var(--color-text-secondary, #475569);
}

.clean-line i {
  color: var(--color-success, #16a34a);
}

.unsaved-changes {
  margin-top: 12px;
  padding-top: 12px;
  border-top: 1px solid var(--color-border-light, #eef2f7);
}

.unsaved-changes__caption {
  margin: 0 0 8px;
  font-size: 12px;
  font-weight: var(--font-weight-medium, 500);
  color: var(--color-text-secondary, #475569);
}

.close-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-4, 16px);
}

.close-title {
  margin: 0 0 2px;
  font-size: var(--font-size-sm, 14px);
  font-weight: var(--font-weight-medium, 500);
  color: var(--color-text-primary, #0f172a);
}

.close-text {
  margin: 0;
  font-size: 13px;
  color: var(--color-text-tertiary, #94a3b8);
}
</style>
