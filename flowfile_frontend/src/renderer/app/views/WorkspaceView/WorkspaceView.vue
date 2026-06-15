<template>
  <div class="workspace-view">
    <div v-if="loadingInitial" v-loading="true" style="flex: 1" />

    <WorkspaceEmptyState v-else-if="!store.hasProjects" @create="initDialogVisible = true" />

    <template v-else>
      <WorkspaceHeader
        :projects="store.projects"
        :selected-root="store.selectedRoot"
        :status="store.status"
        :pending-count="store.pendingChangeCount"
        :exporting="store.exporting"
        :applying="store.applying"
        :loading="store.loadingStatus"
        @select="onSelectProject"
        @export="onExport"
        @apply="onApply"
        @refresh="onRefresh"
        @new="initDialogVisible = true"
      />

      <div class="workspace-body">
        <DriftPanel v-if="store.status" :drift="store.status.drift" />
        <ExportResultPanel v-if="store.lastExport" :result="store.lastExport" />
        <ApplyResultPanel v-if="store.lastApply" :result="store.lastApply" />
        <RequiredSecretsPanel
          v-if="store.requiredSecrets.length"
          :secrets="store.requiredSecrets"
        />
        <GitGuideCard
          :project-name="store.selectedProject?.name ?? 'my-flowfile-project'"
          :root-path="store.selectedRoot ?? ''"
          :git-enabled="store.status?.git_enabled ?? false"
        />
      </div>
    </template>

    <InitProjectDialog v-model="initDialogVisible" @created="onProjectCreated" />
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import { useWorkspaceStore } from "../../stores/workspace-store";
import WorkspaceEmptyState from "./WorkspaceEmptyState.vue";
import WorkspaceHeader from "./WorkspaceHeader.vue";
import DriftPanel from "./DriftPanel.vue";
import ExportResultPanel from "./ExportResultPanel.vue";
import ApplyResultPanel from "./ApplyResultPanel.vue";
import RequiredSecretsPanel from "./RequiredSecretsPanel.vue";
import GitGuideCard from "./GitGuideCard.vue";
import InitProjectDialog from "./InitProjectDialog.vue";
import "./workspace.css";

const store = useWorkspaceStore();
const initDialogVisible = ref(false);
const ready = ref(false);

const loadingInitial = computed(() => !ready.value && store.loadingProjects);

onMounted(async () => {
  try {
    await store.loadProjects();
    if (store.hasProjects) {
      await store.loadStatus();
    }
  } catch {
    if (store.error) ElMessage.error(store.error);
  } finally {
    ready.value = true;
  }
});

async function onSelectProject(root: string) {
  try {
    await store.selectProject(root);
  } catch {
    ElMessage.error(store.error ?? "Failed to load project");
  }
}

async function onRefresh() {
  try {
    await store.loadStatus();
  } catch {
    ElMessage.error(store.error ?? "Failed to refresh status");
  }
}

async function onExport() {
  try {
    const result = await store.exportProject();
    const changed = result.written.length + result.removed.length;
    ElMessage.success(
      changed
        ? `Exported — ${result.written.length} written, ${result.removed.length} removed`
        : "Already up to date — nothing to export",
    );
    if (result.warnings.length) {
      ElMessage.warning(`${result.warnings.length} warning(s) — see "Last export" below`);
    }
  } catch {
    ElMessage.error(store.error ?? "Export failed");
  }
}

async function onApply() {
  try {
    await ElMessageBox.confirm(
      "Apply rewrites your runtime catalog from the project files: flows, connections and " +
        "cron/interval schedules are upserted (managed schedules are replaced). Secret values are " +
        "refilled from environment variables / .env — missing ones are reported, not fatal.",
      "Apply project to database?",
      { confirmButtonText: "Apply", cancelButtonText: "Cancel", type: "warning" },
    );
  } catch {
    return; // cancelled
  }
  try {
    const result = await store.applyProject();
    ElMessage.success("Applied project to the database");
    if (result.missing_secrets.length) {
      ElMessage.warning(
        `${result.missing_secrets.length} secret value(s) missing — set FLOWFILE_SECRET_* and apply again`,
      );
    }
  } catch {
    ElMessage.error(store.error ?? "Apply failed");
  }
}

function onProjectCreated() {
  ElMessage.success("Project created and exported");
}
</script>
