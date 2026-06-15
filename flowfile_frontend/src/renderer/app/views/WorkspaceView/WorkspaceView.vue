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
        <HistoryPanel
          :history="store.history"
          :committing="store.committing"
          @commit="onCommit"
          @view-diff="onViewDiff"
          @restore="onRestore"
        />
      </div>
    </template>

    <InitProjectDialog v-model="initDialogVisible" @created="onProjectCreated" />
    <DiffDialog
      v-model="diffDialog.visible"
      :title="diffDialog.title"
      :diff="diffDialog.diff"
      :loading="diffDialog.loading"
    />
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import { useWorkspaceStore } from "../../stores/workspace-store";
import type { GitCommit } from "../../types";
import WorkspaceEmptyState from "./WorkspaceEmptyState.vue";
import WorkspaceHeader from "./WorkspaceHeader.vue";
import DriftPanel from "./DriftPanel.vue";
import ExportResultPanel from "./ExportResultPanel.vue";
import ApplyResultPanel from "./ApplyResultPanel.vue";
import RequiredSecretsPanel from "./RequiredSecretsPanel.vue";
import HistoryPanel from "./HistoryPanel.vue";
import InitProjectDialog from "./InitProjectDialog.vue";
import DiffDialog from "./DiffDialog.vue";
import "./workspace.css";

const store = useWorkspaceStore();
const initDialogVisible = ref(false);
const ready = ref(false);

const diffDialog = reactive({ visible: false, title: "", diff: "", loading: false });

const loadingInitial = computed(() => !ready.value && store.loadingProjects);

onMounted(async () => {
  try {
    await store.loadProjects();
    if (store.hasProjects) {
      await store.refresh();
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
  await store.refresh();
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

async function onCommit() {
  let message: string;
  try {
    const result = await ElMessageBox.prompt("Describe this snapshot", "Commit", {
      confirmButtonText: "Commit",
      cancelButtonText: "Cancel",
      inputValue: "Update Flowfile project",
      inputValidator: (v) => (v && v.trim() ? true : "A message is required"),
    });
    message = result.value;
  } catch {
    return; // cancelled
  }
  try {
    const result = await store.commit(message.trim());
    if (result.committed) {
      ElMessage.success("Committed snapshot");
    } else {
      ElMessage.info("Nothing to commit — already up to date");
    }
  } catch {
    ElMessage.error(store.error ?? "Commit failed");
  }
}

async function onViewDiff(sha: string | null) {
  diffDialog.visible = true;
  diffDialog.loading = true;
  diffDialog.title = sha ? `Changes in ${sha.slice(0, 8)}` : "Uncommitted changes";
  diffDialog.diff = "";
  try {
    const result = await store.fetchDiff(sha ?? undefined);
    diffDialog.diff = result.diff;
  } catch {
    ElMessage.error("Failed to load diff");
    diffDialog.visible = false;
  } finally {
    diffDialog.loading = false;
  }
}

async function onRestore(commit: GitCommit) {
  try {
    await ElMessageBox.confirm(
      `Restore the project to “${commit.subject}” (${commit.short_sha})? This rewrites the project ` +
        "files and re-applies them to the database. A new snapshot records the rollback so you can undo it.",
      "Restore version",
      { confirmButtonText: "Restore", cancelButtonText: "Cancel", type: "warning" },
    );
  } catch {
    return; // cancelled
  }
  try {
    await store.restore(commit.sha);
    ElMessage.success(`Restored to ${commit.short_sha}`);
  } catch {
    ElMessage.error(store.error ?? "Restore failed");
  }
}
</script>
