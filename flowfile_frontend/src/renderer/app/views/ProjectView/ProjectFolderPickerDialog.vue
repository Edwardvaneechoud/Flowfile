<template>
  <el-dialog
    :model-value="modelValue"
    :title="title"
    width="70%"
    :close-on-click-modal="false"
    :close-on-press-escape="!store.loading"
    :show-close="!store.loading"
    custom-class="high-z-index-dialog"
    @update:model-value="onVisibility"
    @closed="onClosed"
  >
    <div v-if="store.loading" class="picker-loading">
      <div class="loading-spinner"></div>
      <p>Opening project… importing your flows, connections and schedules.</p>
    </div>

    <template v-else>
      <div v-if="mode === 'create' && pickedFolder" class="picker-name-step">
        <p class="picker-folder">
          <span class="picker-eyebrow">Location</span>
          <span><i class="fa-solid fa-folder"></i> {{ pickedFolder }}</span>
        </p>
        <label class="form-label" for="project-name">Project name</label>
        <el-input id="project-name" v-model="projectName" placeholder="my-project" />
        <p v-if="fullPath" class="picker-fullpath">
          <i class="fa-solid fa-folder-plus"></i>
          A new folder will be created at <code>{{ fullPath }}</code>
        </p>
        <p class="picker-hint">
          Nothing is overwritten — your flows and connections are mirrored as files you can save
          versions of.
        </p>
        <div class="picker-actions">
          <el-button @click="pickedFolder = null">Choose another folder</el-button>
          <el-button type="primary" :disabled="!fullPath" @click="handleCreate">
            Create project
          </el-button>
        </div>
      </div>

      <div v-else-if="!browserReady" class="picker-loading">
        <div class="loading-spinner"></div>
        <p>Preparing your project area…</p>
      </div>

      <file-browser
        v-else
        mode="open"
        context="flows"
        :is-visible="modelValue"
        :root-path="confinedRoot ?? undefined"
        allow-directory-selection
        @directory-selected="onFolderPicked"
      />
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, computed, watch } from "vue";
import { ElMessage, ElNotification } from "element-plus";
import FileBrowser from "../../components/common/FileBrowser/fileBrowser.vue";
import { joinPath } from "../../components/common/FileBrowser/fileSystemApi";
import { useProjectStore } from "../../stores/project-store";
import { useMultiUser } from "../../composables/useMultiUser";
import { ProjectApi } from "../../api/project.api";

const props = defineProps<{ modelValue: boolean; mode: "create" | "open" }>();
const emit = defineEmits<{
  (e: "update:modelValue", value: boolean): void;
  (e: "done"): void;
}>();

const store = useProjectStore();
const { projectsConfined, refresh: refreshCaps } = useMultiUser();
const pickedFolder = ref<string | null>(null);
const projectName = ref("");

// In confined modes (docker/package) the browser is locked to the user's per-owner project area.
const confinedRoot = ref<string | null>(null);
const browserReady = computed(() => !projectsConfined.value || !!confinedRoot.value);

const ensureRoot = async () => {
  await refreshCaps();
  if (!projectsConfined.value || confinedRoot.value) return;
  try {
    confinedRoot.value = await ProjectApi.getRoot();
  } catch {
    confinedRoot.value = null;
  }
};

watch(
  () => props.modelValue,
  (v) => v && ensureRoot(),
  { immediate: true },
);

const title = computed(() => (props.mode === "create" ? "Create a project" : "Open a project"));

// Turn the project name into a safe folder segment (the name itself stays the
// project's display name; the folder lives *inside* the picked location).
const toFolderSegment = (name: string): string =>
  name
    .trim()
    .replace(/[/\\]+/g, "-")
    .replace(/\s+/g, "-")
    .replace(/[^A-Za-z0-9._-]/g, "")
    .replace(/-+/g, "-")
    .replace(/^[-.]+|[-.]+$/g, "");

const folderName = computed(() => toFolderSegment(projectName.value));
const fullPath = computed(() =>
  pickedFolder.value && folderName.value ? joinPath(pickedFolder.value, folderName.value) : "",
);

const onFolderPicked = (path: string) => {
  pickedFolder.value = path;
  if (props.mode === "open") handleOpen(path);
};

const handleCreate = async () => {
  if (!fullPath.value) return;
  try {
    await store.initProject(fullPath.value, projectName.value.trim());
    ElMessage.success("Project created");
    close();
    emit("done");
  } catch (e: any) {
    ElMessage.error(e?.message || "Could not create project");
  }
};

const handleOpen = async (folder: string) => {
  try {
    const res = await store.openProject(folder);
    const { flows, connections, schedules } = res.imported;
    const lines = [
      `Imported ${flows} flow(s), ${connections} connection(s), ${schedules} schedule(s).`,
    ];
    if (res.placeholder_secrets.length) {
      lines.push(`${res.placeholder_secrets.length} secret(s) need values on this computer.`);
    }
    ElNotification({
      title: "Project opened",
      message: lines.join(" "),
      type: "success",
      position: "top-left",
      duration: 6000,
    });
    close();
    emit("done");
  } catch (e: any) {
    ElMessage.error(e?.message || "Could not open project");
    pickedFolder.value = null; // keep the picker open to try another folder
  }
};

const onVisibility = (v: boolean) => {
  if (!store.loading) emit("update:modelValue", v);
};
const close = () => emit("update:modelValue", false);
const onClosed = () => {
  pickedFolder.value = null;
  projectName.value = "";
};
</script>

<style scoped>
.picker-loading {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 16px;
  min-height: 240px;
  color: var(--color-text-secondary, #475569);
}

.picker-name-step {
  display: flex;
  flex-direction: column;
  gap: 10px;
  max-width: 520px;
}

.picker-folder {
  display: flex;
  flex-direction: column;
  gap: 4px;
  margin: 0;
  font-size: 13px;
  color: var(--color-text-secondary, #475569);
  word-break: break-all;
}

.picker-folder > span:last-child {
  font-family: var(--font-family-mono, monospace);
}

.picker-eyebrow {
  font-family: inherit;
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: var(--color-text-tertiary, #94a3b8);
}

.picker-fullpath {
  display: flex;
  align-items: baseline;
  gap: 8px;
  margin: 0;
  font-size: 13px;
  color: var(--color-text-secondary, #475569);
}

.picker-fullpath i {
  color: var(--color-accent, #2563eb);
}

.picker-fullpath code {
  font-family: var(--font-family-mono, monospace);
  background: var(--color-background-muted, #f8fafc);
  padding: 2px 6px;
  border-radius: 4px;
  word-break: break-all;
  color: var(--color-text-primary, #0f172a);
}

.picker-hint {
  margin: 0;
  font-size: 13px;
  line-height: 1.5;
  color: var(--color-text-tertiary, #94a3b8);
}

.picker-actions {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  margin-top: 8px;
}
</style>
