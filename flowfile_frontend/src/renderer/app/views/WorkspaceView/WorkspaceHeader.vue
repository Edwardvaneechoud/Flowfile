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
      <button
        class="ws-btn ws-btn-primary"
        :disabled="!hasChanges || busy"
        :title="
          hasChanges
            ? 'Snapshot the current state'
            : 'Nothing has changed since your last checkpoint'
        "
        @click="emit('checkpoint')"
      >
        <span v-if="busy" class="ws-spin" />
        <i v-else class="fa-solid fa-bookmark" /> Create checkpoint
      </button>
      <button class="ws-btn" @click="emit('new')"><i class="fa-solid fa-plus" /> New</button>
      <el-dropdown trigger="click" @command="onCommand">
        <button class="ws-btn ws-btn-icon" title="More actions">
          <i class="fa-solid fa-ellipsis-vertical" />
        </button>
        <template #dropdown>
          <el-dropdown-menu>
            <el-dropdown-item command="refresh" :disabled="loading">
              <i class="fa-solid fa-rotate" /> Refresh
            </el-dropdown-item>
            <el-dropdown-item command="load" divided :disabled="busy">
              <i class="fa-solid fa-arrow-down-to-bracket" /> Load from files…
            </el-dropdown-item>
          </el-dropdown-menu>
        </template>
      </el-dropdown>
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
  hasChanges: boolean;
  pendingCount: number;
  busy: boolean;
  loading: boolean;
}>();

const emit = defineEmits<{
  (e: "select", root: string): void;
  (e: "checkpoint"): void;
  (e: "load-from-files"): void;
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
  if (drift && drift.conflict.length) {
    const n = drift.conflict.length;
    return {
      label: `${n} conflict${n > 1 ? "s" : ""}`,
      cls: "danger",
      icon: "fa-solid fa-triangle-exclamation",
    };
  }
  if (props.hasChanges) {
    const n = props.pendingCount;
    return {
      label: n > 0 ? `${n} unsaved change${n > 1 ? "s" : ""}` : "Unsaved changes",
      cls: "warn",
      icon: "fa-solid fa-pen",
    };
  }
  return { label: "Up to date", cls: "ok", icon: "fa-solid fa-circle-check" };
});

function onCommand(command: string) {
  if (command === "refresh") emit("refresh");
  if (command === "load") emit("load-from-files");
}

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
