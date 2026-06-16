<template>
  <div class="workspace-view">
    <div v-if="loadingInitial" v-loading="true" style="flex: 1" />

    <WorkspaceEmptyState v-else-if="!store.hasProjects" @create="initDialogVisible = true" />

    <template v-else>
      <WorkspaceHeader
        :projects="store.projects"
        :selected-root="store.selectedRoot"
        :status="store.status"
        :has-changes="store.hasChanges"
        :pending-count="store.pendingChangeCount"
        :busy="busy"
        :loading="store.loadingStatus"
        @select="onSelectProject"
        @checkpoint="onCheckpoint"
        @load-from-files="onLoadFromFiles"
        @refresh="onRefresh"
        @new="initDialogVisible = true"
      />

      <div class="workspace-body">
        <ChangesPanel v-if="store.hasChanges && store.status" :drift="store.status.drift" />
        <RequiredSecretsPanel
          v-if="store.requiredSecrets.length"
          :secrets="store.requiredSecrets"
        />
        <HistoryPanel :history="store.history" @view-diff="onViewDiff" @restore="onRestore" />
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
import { useCatalogStore } from "../../stores/catalog-store";
import type { GitCommit } from "../../types";
import WorkspaceEmptyState from "./WorkspaceEmptyState.vue";
import WorkspaceHeader from "./WorkspaceHeader.vue";
import ChangesPanel from "./ChangesPanel.vue";
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
const busy = computed(() => store.exporting || store.applying || store.committing);

// A human summary of what an apply/restore loaded into the runtime.
function summarizeApply(counts: Record<string, number>): string {
  const flows = counts.flow ?? 0;
  const schedules = counts.schedule ?? 0;
  const connections =
    (counts.database_connection ?? 0) +
    (counts.cloud_connection ?? 0) +
    (counts.ga_connection ?? 0) +
    (counts.kafka_connection ?? 0);
  const parts: string[] = [];
  if (flows) parts.push(`${flows} flow${flows > 1 ? "s" : ""}`);
  if (connections) parts.push(`${connections} connection${connections > 1 ? "s" : ""}`);
  if (schedules) parts.push(`${schedules} schedule${schedules > 1 ? "s" : ""}`);
  return parts.join(", ");
}

// Apply/restore rewrites the runtime catalog, so the Catalog view's cached flow
// list is now stale — refresh it (best-effort) so restored flows show up.
async function refreshCatalog() {
  try {
    const catalog = useCatalogStore();
    await Promise.allSettled([catalog.loadAllFlows(), catalog.loadTree()]);
  } catch {
    /* best-effort */
  }
}

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

async function onCheckpoint() {
  let message: string;
  try {
    const result = await ElMessageBox.prompt("Name this checkpoint", "Create checkpoint", {
      confirmButtonText: "Create",
      cancelButtonText: "Cancel",
      inputValue: `Checkpoint ${new Date().toLocaleString()}`,
      inputValidator: (v) => (v && v.trim() ? true : "A name is required"),
    });
    message = result.value.trim();
  } catch {
    return; // cancelled
  }
  try {
    const { exported } = await store.createCheckpoint(message);
    ElMessage.success("Checkpoint created");
    if (exported.warnings.length) {
      ElMessage.warning(`${exported.warnings.length} item(s) skipped — ${exported.warnings[0]}`);
    }
  } catch {
    ElMessage.error(store.error ?? "Failed to create checkpoint");
  }
}

async function onLoadFromFiles() {
  try {
    await ElMessageBox.confirm(
      "Load the project files into the app? This rebuilds the runtime catalog from the files on " +
        "disk (flows, connections, cron/interval schedules) — use it after cloning the project or " +
        "editing files outside the app. Missing secret values are reported, not fatal.",
      "Load from files",
      { confirmButtonText: "Load", cancelButtonText: "Cancel", type: "warning" },
    );
  } catch {
    return; // cancelled
  }
  try {
    const result = await store.applyProject();
    const summary = summarizeApply(result.counts);
    ElMessage.success(summary ? `Loaded — ${summary}` : "Loaded project from files");
    if (result.missing_secrets.length) {
      ElMessage.warning(
        `${result.missing_secrets.length} secret value(s) missing — set FLOWFILE_SECRET_* and load again`,
      );
    }
    await refreshCatalog();
  } catch {
    ElMessage.error(store.error ?? "Failed to load from files");
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
        "files and re-applies them to the app. A new checkpoint records the rollback so you can undo it.",
      "Restore checkpoint",
      { confirmButtonText: "Restore", cancelButtonText: "Cancel", type: "warning" },
    );
  } catch {
    return; // cancelled
  }
  try {
    const result = await store.restore(commit.sha);
    const summary = result.applied ? summarizeApply(result.applied.counts) : "";
    ElMessage.success(
      summary ? `Restored ${commit.short_sha} — ${summary}` : `Restored to ${commit.short_sha}`,
    );
    if (result.applied?.warnings.length) {
      ElMessage.warning(
        `${result.applied.warnings.length} item(s) skipped — ${result.applied.warnings[0]}`,
      );
    }
    await refreshCatalog();
  } catch {
    ElMessage.error(store.error ?? "Restore failed");
  }
}

function onProjectCreated() {
  ElMessage.success("Project created");
}
</script>
