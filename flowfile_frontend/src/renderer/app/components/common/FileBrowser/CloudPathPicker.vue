<template>
  <el-dialog
    :model-value="modelValue"
    :title="title"
    width="70%"
    append-to-body
    :close-on-click-modal="false"
    @update:model-value="emit('update:modelValue', $event)"
  >
    <div v-if="unsupportedReason" class="picker-notice">
      <span class="material-icons">info</span>
      <span>{{ unsupportedReason }}</span>
    </div>
    <FileBrowser
      v-else
      :location="location"
      :mode="mode"
      :context="context"
      :allowed-file-types="allowedFileTypes"
      :allow-directory-selection="allowDirectorySelection"
      :initial-file-path="initialFilePath"
      :default-new-file-name="defaultNewFileName"
      :is-visible="modelValue"
      @file-selected="onFileSelected"
      @directory-selected="onDirectorySelected"
      @create-file="onCreateFile"
      @overwrite-file="onCreateFile"
    />
  </el-dialog>
</template>

<script setup lang="ts">
import { computed } from "vue";
import FileBrowser from "./fileBrowser.vue";
import { browseUnsupportedReason } from "./browseSupport";
import type { FileInfo, StorageLocation } from "../../../types";
import type { FileBrowserContext } from "../../../stores/fileBrowserStore";
import type { FullCloudStorageConnectionInterface } from "../../../views/CloudConnectionView/CloudConnectionTypes";
import { storageTypeForUri } from "../../../utils/storagePath";

interface Props {
  modelValue: boolean;
  connection?: FullCloudStorageConnectionInterface | null;
  mode?: "open" | "create";
  context?: FileBrowserContext;
  allowedFileTypes?: string[];
  allowDirectorySelection?: boolean;
  initialFilePath?: string;
  defaultNewFileName?: string;
  title?: string;
}

const props = withDefaults(defineProps<Props>(), {
  connection: null,
  mode: "open",
  context: "cloudRead",
  allowedFileTypes: () => [],
  allowDirectorySelection: false,
  initialFilePath: "",
  defaultNewFileName: "",
  title: "Select a cloud storage path",
});

const emit = defineEmits<{
  (e: "update:modelValue", value: boolean): void;
  (e: "select", path: string, isDirectory: boolean): void;
}>();

/** Fall back to the scheme already typed in the field when no connection is picked. */
const location = computed<StorageLocation>(() => ({
  kind: "cloud",
  storageType: props.connection?.storageType ?? storageTypeForUri(props.initialFilePath) ?? "s3",
  connectionName: props.connection?.connectionName ?? null,
}));

const unsupportedReason = computed(() =>
  browseUnsupportedReason(props.connection?.storageType, props.connection?.authMethod),
);

const close = () => emit("update:modelValue", false);

const onFileSelected = (file: FileInfo) => {
  emit("select", file.path, false);
  close();
};

const onDirectorySelected = (path: string) => {
  emit("select", path, true);
  close();
};

const onCreateFile = (filePath: string) => {
  emit("select", filePath, false);
  close();
};
</script>

<style scoped>
.picker-notice {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 24px;
  color: var(--color-text-secondary);
}

.picker-notice .material-icons {
  font-size: 20px;
}
</style>
