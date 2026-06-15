<template>
  <header class="ws-header">
    <div class="ws-header-main">
      <div class="ws-project">
        <i class="fa-solid fa-code-branch ws-project-icon" />
        <el-select
          v-if="projects.length > 1"
          :model-value="selectedRoot"
          class="ws-project-select"
          size="default"
          @change="(v: string) => emit('select', v)"
        >
          <el-option
            v-for="p in projects"
            :key="p.root_path"
            :label="p.name"
            :value="p.root_path"
          />
        </el-select>
        <span v-else class="ws-project-name">{{ selectedProject?.name ?? "Project" }}</span>

        <span class="ws-sync" :class="`ws-sync-${sync.cls}`">
          <i :class="sync.icon" /> {{ sync.label }}
        </span>
      </div>

      <div class="ws-path">
        <code class="ws-path-text" :title="selectedRoot ?? ''">{{ selectedRoot }}</code>
        <button class="ws-icon-btn" title="Copy path" @click="copy(selectedRoot)">
          <i class="fa-regular fa-copy" />
        </button>
        <span v-if="status?.git_enabled" class="ws-sync ws-sync-neutral">
          <i class="fa-brands fa-git-alt" /> git
        </span>
      </div>
    </div>

    <div class="ws-actions">
      <button class="ws-btn" :disabled="loading" @click="emit('refresh')">
        <i class="fa-solid fa-rotate" :class="{ 'fa-spin': loading }" /> Refresh
      </button>
      <button
        class="ws-btn ws-btn-primary"
        :disabled="exporting || applying"
        @click="emit('export')"
      >
        <span v-if="exporting" class="ws-spin" />
        <i v-else class="fa-solid fa-arrow-up-from-bracket" /> Export
      </button>
      <button
        class="ws-btn ws-btn-warning"
        :disabled="applying || exporting"
        @click="emit('apply')"
      >
        <span v-if="applying" class="ws-spin" />
        <i v-else class="fa-solid fa-arrow-down-to-bracket" /> Apply
      </button>
      <button class="ws-btn" @click="emit('new')"><i class="fa-solid fa-plus" /> New</button>
    </div>
  </header>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { ElMessage } from "element-plus";
import type { WorkspaceProjectInfo, WorkspaceStatus } from "../../types";

const props = defineProps<{
  projects: WorkspaceProjectInfo[];
  selectedRoot: string | null;
  status: WorkspaceStatus | null;
  pendingCount: number;
  exporting: boolean;
  applying: boolean;
  loading: boolean;
}>();

const emit = defineEmits<{
  (e: "select", root: string): void;
  (e: "export"): void;
  (e: "apply"): void;
  (e: "refresh"): void;
  (e: "new"): void;
}>();

const selectedProject = computed(
  () => props.projects.find((p) => p.root_path === props.selectedRoot) ?? null,
);

const sync = computed(() => {
  if (props.loading) {
    return { label: "Checking…", cls: "neutral", icon: "fa-solid fa-rotate fa-spin" };
  }
  const drift = props.status?.drift;
  if (!drift) {
    return { label: "Unknown", cls: "neutral", icon: "fa-solid fa-circle-question" };
  }
  if (drift.in_sync) {
    return { label: "In sync", cls: "ok", icon: "fa-solid fa-circle-check" };
  }
  if (drift.conflict.length) {
    const n = drift.conflict.length;
    return {
      label: `${n} conflict${n > 1 ? "s" : ""}`,
      cls: "danger",
      icon: "fa-solid fa-triangle-exclamation",
    };
  }
  const n = props.pendingCount;
  return { label: `${n} change${n > 1 ? "s" : ""}`, cls: "warn", icon: "fa-solid fa-pen" };
});

async function copy(text: string | null) {
  if (!text) return;
  try {
    await navigator.clipboard.writeText(text);
    ElMessage.success("Copied path");
  } catch {
    /* clipboard unavailable — ignore */
  }
}
</script>
