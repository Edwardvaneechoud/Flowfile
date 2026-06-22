<template>
  <div class="project-manage">
    <!-- Header -->
    <header class="manage-header mb-3">
      <div class="manage-header__main">
        <div class="manage-header__text">
          <div class="manage-header__title-row">
            <h2 class="page-title">Project Tracking</h2>
            <span class="status-pill" :class="`status-pill--${store.status}`">
              <span class="status-pill__dot"></span>{{ statusLabel }}
            </span>
          </div>
          <p class="project-meta">
            <span class="project-name">{{ store.activeProject?.name }}</span>
            <span class="folder-path">
              <i class="fa-solid fa-folder"></i>
              {{ store.activeProject?.folder_path }}
            </span>
          </p>
        </div>
      </div>
      <button type="button" class="ghost-btn" @click="settingsVisible = true">
        <i class="fa-solid fa-gear"></i>
        <span>Settings</span>
      </button>
    </header>

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
    <section class="card mb-3">
      <div class="card-header">
        <h3 class="card-title">
          <i class="fa-solid fa-camera card-title__icon"></i>
          Save a version
          <el-tooltip
            effect="dark"
            placement="top"
            content="A version is a named snapshot of your whole project. Save one whenever you reach a point you might want to return to."
          >
            <button type="button" class="help-hint" aria-label="What is a version?">
              <i class="fa-solid fa-circle-info"></i>
            </button>
          </el-tooltip>
        </h3>
      </div>
      <div class="card-content">
        <div v-if="store.status === 'clean'" class="saved-state">
          <span class="saved-state__check"><i class="fa-solid fa-check"></i></span>
          <div class="saved-state__text">
            <p class="saved-state__title">All changes saved</p>
            <p v-if="latestVersion" class="saved-state__sub">
              Latest version: <strong>“{{ latestVersion.message }}”</strong>
            </p>
            <p v-else class="saved-state__sub">Your work is mirrored to the project folder.</p>
          </div>
        </div>
        <template v-else>
          <p class="save-prompt">
            <i class="fa-solid fa-pen-to-square"></i>
            Describe what changed, then save a snapshot you can return to.
          </p>
          <SaveVersionForm :rows="3" @saved="loadUnsaved" />
          <div v-if="unsavedChanges.length" class="unsaved-changes">
            <p class="unsaved-changes__caption">This version will include:</p>
            <ChangeList :changes="unsavedChanges" />
          </div>
        </template>
      </div>
    </section>

    <!-- Version history -->
    <ProjectVersionHistory v-if="store.versionsAvailable" />

    <!-- Stop using this project -->
    <section class="card danger-card mb-3">
      <div class="card-content close-row">
        <div class="close-row__text">
          <p class="close-title"><i class="fa-solid fa-link-slash"></i> Stop using this project</p>
          <p class="close-text">
            Your files stay on disk — Flowfile just stops tracking this folder.
          </p>
        </div>
        <el-button
          v-if="store.closeAvailable"
          type="danger"
          plain
          :loading="closing"
          @click="handleClose"
        >
          Stop tracking
        </el-button>
      </div>
    </section>

    <ProjectSettingsDialog v-model="settingsVisible" />
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { useRouter } from "vue-router";
import { ElMessage, ElMessageBox } from "element-plus";
import SaveVersionForm from "../../components/project/SaveVersionForm.vue";
import ChangeList from "./ChangeList.vue";
import ProjectVersionHistory from "./ProjectVersionHistory.vue";
import ProjectSettingsDialog from "./ProjectSettingsDialog.vue";
import { useProjectStore } from "../../stores/project-store";
import type { ProjectVersionChange } from "../../types";

const store = useProjectStore();
const router = useRouter();
const reloading = ref(false);
const closing = ref(false);
const settingsVisible = ref(false);
const unsavedChanges = ref<ProjectVersionChange[]>([]);

const latestVersion = computed(() => store.versions[0] ?? null);

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
  margin: 0 auto;
}

/* ===== Header (matches the page-header pattern used across tabs) ===== */
.manage-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-4);
}

.manage-header__main {
  display: flex;
  align-items: center;
  gap: var(--spacing-4);
  min-width: 0;
}

.manage-header__text {
  min-width: 0;
}

.manage-header__title-row {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: var(--spacing-2);
}

.project-meta {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: var(--spacing-1) var(--spacing-2);
  margin: var(--spacing-1) 0 0;
}

.project-name {
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
}

.folder-path {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-family: var(--font-family-mono);
  font-size: var(--font-size-sm);
  color: var(--color-text-tertiary);
  word-break: break-all;
}

.folder-path i {
  font-size: var(--font-size-xs);
}

.status-pill {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 3px 10px;
  border-radius: var(--border-radius-full);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  white-space: nowrap;
}

.status-pill__dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: currentColor;
}

.status-pill--clean {
  color: var(--color-success-dark);
  background: var(--color-success-light);
}

.status-pill--unsaved {
  color: var(--color-warning-dark);
  background: var(--color-warning-light);
}

.status-pill--external {
  color: var(--color-danger-dark);
  background: var(--color-danger-light);
}

/* danger-dark reads as a dark red — fine on the light tint, too dark on the dark fill. */
[data-theme="dark"] .status-pill--external {
  color: #fca5a5;
}

.status-pill--none {
  display: none;
}

/* ===== Settings button ===== */
.ghost-btn {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-2);
  flex-shrink: 0;
  padding: var(--spacing-2) var(--spacing-4);
  border-radius: var(--border-radius-md);
  border: 1px solid var(--color-border-primary);
  background: var(--color-background-primary);
  color: var(--color-text-secondary);
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-medium);
  cursor: pointer;
  transition: all var(--transition-base) var(--transition-timing);
}

.ghost-btn:hover {
  border-color: var(--color-accent);
  color: var(--color-accent);
  background: var(--color-accent-subtle);
}

.ghost-btn:focus-visible {
  outline: none;
  border-color: var(--color-accent);
  color: var(--color-accent);
  box-shadow: 0 0 0 2px var(--color-accent-subtle);
}

.ghost-btn i {
  font-size: var(--font-size-md);
}

/* ===== Card title icon + inline help ===== */
.card-title {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-2);
}

.card-title__icon {
  color: var(--color-accent);
  font-size: var(--font-size-md);
}

.help-hint {
  display: inline-flex;
  align-items: center;
  padding: 0;
  border: none;
  background: transparent;
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
  line-height: 1;
  cursor: help;
  border-radius: var(--border-radius-full);
  transition: color var(--transition-base) var(--transition-timing);
}

.help-hint:hover {
  color: var(--color-accent);
}

.help-hint:focus-visible {
  outline: none;
  color: var(--color-accent);
  box-shadow: 0 0 0 2px var(--color-accent-subtle);
}

/* ===== Banners ===== */
.banner {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-3);
  padding: var(--spacing-3) var(--spacing-4);
  border-radius: var(--border-radius-lg);
  border-left: 4px solid var(--color-accent);
  background: var(--color-background-muted);
}

.banner--warning {
  border-left-color: var(--color-warning);
  background: var(--color-warning-light);
}

.banner--info {
  border-left-color: var(--color-accent);
}

.banner > i {
  margin-top: 3px;
  font-size: var(--font-size-2xl);
}

.banner--warning > i {
  color: var(--color-warning);
}

.banner--info > i {
  color: var(--color-accent);
}

.banner__body {
  flex: 1;
  min-width: 0;
}

.banner__title {
  margin: 0 0 2px;
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.banner__text {
  margin: 0;
  font-size: var(--font-size-md);
  line-height: 1.5;
  color: var(--color-text-secondary);
}

.secret-names {
  font-family: var(--font-family-mono);
  color: var(--color-text-primary);
}

/* ===== Saved / unsaved state ===== */
.saved-state {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
}

.saved-state__check {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 36px;
  height: 36px;
  flex-shrink: 0;
  border-radius: 50%;
  background: var(--color-success-light);
  color: var(--color-success-dark);
  font-size: var(--font-size-md);
}

.saved-state__title {
  margin: 0;
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.saved-state__sub {
  margin: 2px 0 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-tertiary);
}

.saved-state__sub strong {
  color: var(--color-text-secondary);
  font-weight: var(--font-weight-medium);
}

.save-prompt {
  display: flex;
  align-items: center;
  gap: 6px;
  margin: 0 0 var(--spacing-3);
  font-size: var(--font-size-md);
  color: var(--color-text-secondary);
}

.save-prompt i {
  color: var(--color-accent);
}

.unsaved-changes {
  margin-top: var(--spacing-3);
  padding-top: var(--spacing-3);
  border-top: 1px solid var(--color-border-light);
}

.unsaved-changes__caption {
  margin: 0 0 var(--spacing-2);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
}

/* ===== Stop tracking ===== */
.danger-card {
  background: var(--color-background-muted);
}

.close-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-4);
}

.close-row__text {
  min-width: 0;
}

.close-title {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  margin: 0 0 2px;
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.close-title i {
  color: var(--color-text-tertiary);
}

.close-text {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-tertiary);
}
</style>
