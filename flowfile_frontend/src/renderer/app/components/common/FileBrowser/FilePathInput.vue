<template>
  <div class="file-path-row">
    <el-input
      :model-value="modelValue"
      :placeholder="placeholder"
      :size="size"
      :disabled="disabled"
      clearable
      class="file-path-input"
      @update:model-value="emit('update:modelValue', $event)"
    >
      <template #prefix>
        <i class="fas fa-file" />
      </template>
    </el-input>
    <el-button :size="size" :disabled="disabled" :title="title" @click="open = true">
      <span class="material-icons browse-icon">folder_open</span>
    </el-button>

    <el-dialog
      v-model="open"
      :title="title"
      width="70%"
      append-to-body
      :close-on-click-modal="false"
    >
      <FileBrowser
        v-if="open"
        :mode="mode"
        :context="context ?? defaultContext"
        :allowed-file-types="allowedFileTypes"
        :allow-directory-selection="allowDirectorySelection"
        :initial-file-path="modelValue"
        :is-visible="open"
        @file-selected="onFileSelected"
        @directory-selected="pick"
        @create-file="pick"
        @overwrite-file="pick"
      />
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
// A path text field with a Browse button that opens the standard FileBrowser.
// Owns the dialog + selection plumbing once, so hosts only bind a path string.
import { computed, ref } from "vue";
import FileBrowser from "./fileBrowser.vue";
import type { FileInfo } from "../../../types";
import type { FileBrowserContext } from "../../../stores/fileBrowserStore";

const props = withDefaults(
  defineProps<{
    modelValue: string;
    placeholder?: string;
    /** "open" picks an existing path; "create" also allows naming a new file. */
    mode?: "open" | "create";
    allowedFileTypes?: string[];
    allowDirectorySelection?: boolean;
    /** Which last-path memory to reuse; defaults to the one matching `mode`. */
    context?: FileBrowserContext;
    dialogTitle?: string;
    size?: "small" | "default" | "large";
    disabled?: boolean;
  }>(),
  {
    placeholder: "Path to a file",
    mode: "open",
    allowedFileTypes: () => [],
    allowDirectorySelection: false,
    context: undefined,
    dialogTitle: undefined,
    size: "small",
    disabled: false,
  },
);

const emit = defineEmits<{ (e: "update:modelValue", value: string): void }>();

const open = ref(false);

const defaultContext = computed<FileBrowserContext>(() =>
  props.mode === "create" ? "output" : "dataFiles",
);

const title = computed(
  () =>
    props.dialogTitle ??
    (props.mode === "create" ? "Select a location to write to" : "Select a file"),
);

function pick(path: string) {
  emit("update:modelValue", path);
  open.value = false;
}

function onFileSelected(file: FileInfo) {
  pick(file.path);
}
</script>

<style scoped>
.file-path-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2, 8px);
  width: 100%;
}

.file-path-input {
  flex: 1;
  min-width: 0;
}

.browse-icon {
  font-size: 16px;
  line-height: 1;
}
</style>
