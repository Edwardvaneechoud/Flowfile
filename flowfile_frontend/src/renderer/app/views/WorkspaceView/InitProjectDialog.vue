<template>
  <el-dialog
    :model-value="modelValue"
    title="Create project"
    width="70%"
    :close-on-click-modal="false"
    @update:model-value="(v: boolean) => emit('update:modelValue', v)"
  >
    <p class="ws-muted">
      A git-ready project tree (flows, connections, schedules) is written into a new
      <code class="ws-code-inline">{{ folderName }}/</code> directory inside the folder you choose.
      Secret values are never written — only <code class="ws-code-inline">${secret:NAME}</code>
      placeholders.
    </p>

    <div class="ws-init-field">
      <label class="ws-init-label" for="ws-project-name">Project name</label>
      <el-input id="ws-project-name" v-model="name" placeholder="Sales Analytics" />
    </div>

    <div class="ws-init-field">
      <label class="ws-init-label">Choose a parent folder, then click Create project</label>
      <div class="ws-init-browser">
        <FileBrowser
          ref="fileBrowserRef"
          mode="open"
          :allow-directory-selection="true"
          @directory-selected="createInFolder"
        />
      </div>
    </div>

    <template #footer>
      <div class="ws-dialog-footer">
        <el-button @click="emit('update:modelValue', false)">Cancel</el-button>
        <el-button type="primary" :disabled="!name.trim()" :loading="creating" @click="createHere">
          Create project
        </el-button>
      </div>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { ElMessage } from "element-plus";
import FileBrowser from "../../components/common/FileBrowser/fileBrowser.vue";
import { useWorkspaceStore } from "../../stores/workspace-store";

defineProps<{ modelValue: boolean }>();
const emit = defineEmits<{
  (e: "update:modelValue", value: boolean): void;
  (e: "created"): void;
}>();

const store = useWorkspaceStore();
const name = ref("");
const creating = ref(false);
const fileBrowserRef = ref<InstanceType<typeof FileBrowser> | null>(null);

function slugify(value: string): string {
  return (
    value
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/^_+|_+$/g, "") || "my_flowfile_project"
  );
}

const folderName = computed(() => slugify(name.value || "my-flowfile-project"));

function joinPath(directory: string, child: string): string {
  const sep = directory.includes("\\") ? "\\" : "/";
  return directory.endsWith(sep) ? `${directory}${child}` : `${directory}${sep}${child}`;
}

function currentBrowserPath(): string | undefined {
  return (fileBrowserRef.value as { getCurrentPath?: () => string } | null)?.getCurrentPath?.();
}

async function createInFolder(directory: string | null | undefined) {
  const projectName = name.value.trim();
  if (!projectName) {
    ElMessage.warning("Enter a project name first");
    return;
  }
  if (!directory) {
    ElMessage.warning("Choose a parent folder in the browser first");
    return;
  }
  creating.value = true;
  try {
    await store.initProject(projectName, joinPath(directory, folderName.value));
    name.value = "";
    emit("update:modelValue", false);
    emit("created");
  } catch {
    ElMessage.error(store.error ?? "Failed to create project");
  } finally {
    creating.value = false;
  }
}

// The footer CTA: create in whatever folder the browser is currently showing.
function createHere() {
  createInFolder(currentBrowserPath());
}
</script>
