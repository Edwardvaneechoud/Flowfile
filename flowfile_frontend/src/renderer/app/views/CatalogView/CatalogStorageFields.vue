<template>
  <div class="storage-section">
    <label class="storage-label">Object storage (optional)</label>
    <div class="storage-uri-row">
      <input
        :value="uri"
        class="input-field"
        placeholder="s3://bucket/catalog (leave blank for local storage)"
        @input="emit('update:uri', ($event.target as HTMLInputElement).value)"
      />
      <el-button
        size="small"
        :disabled="!!browseDisabledReason"
        :title="browseDisabledReason ?? 'Browse cloud storage'"
        @click="showBrowser = true"
      >
        Browse
      </el-button>
    </div>
    <CloudConnectionPicker
      :model-value="connection"
      :connections="connections"
      :loading="connectionsLoading"
      label="Storage connection"
      no-connection-label="No connection (local storage)"
      helper-text="Leave unset to store this catalog's tables on local storage."
      @update:model-value="emit('update:connection', $event)"
    />
    <p v-if="error" class="field-error">{{ error }}</p>
    <CloudPathPicker
      v-model="showBrowser"
      :connection="connection"
      :initial-file-path="uri"
      context="cloudWrite"
      allow-directory-selection
      title="Select a folder for this catalog's tables"
      @select="emit('update:uri', $event)"
    />
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from "vue";
import { CloudConnectionPicker } from "../../components/common";
import type { FullCloudStorageConnectionInterface } from "../CloudConnectionView/CloudConnectionTypes";
import CloudPathPicker from "../../components/common/FileBrowser/CloudPathPicker.vue";
import { browseUnsupportedReason } from "../../components/common/FileBrowser/browseSupport";
import { catalogStorageError } from "./catalogStorageEdit";

const props = defineProps<{
  uri: string;
  connection: FullCloudStorageConnectionInterface | null;
  connections: FullCloudStorageConnectionInterface[];
  connectionsLoading: boolean;
}>();

const emit = defineEmits<{
  "update:uri": [value: string];
  "update:connection": [value: FullCloudStorageConnectionInterface | null];
}>();

const showBrowser = ref(false);

const error = computed(() =>
  catalogStorageError(props.uri, props.connection?.connectionName ?? null),
);

const browseDisabledReason = computed<string | null>(() => {
  if (props.connectionsLoading) return "Loading connections…";
  if (!props.connection) return "Select a storage connection to browse.";
  return browseUnsupportedReason(props.connection.storageType, props.connection.authMethod);
});
</script>

<style scoped>
.input-field {
  width: 100%;
  padding: var(--spacing-2) var(--spacing-3);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  font-size: var(--font-size-sm);
  margin-bottom: var(--spacing-3);
  box-sizing: border-box;
}

.input-field:focus {
  outline: none;
  border-color: var(--color-primary);
  box-shadow: 0 0 0 2px color-mix(in srgb, var(--color-info) 15%, transparent);
}

.field-error {
  margin: calc(-1 * var(--spacing-2)) 0 var(--spacing-3);
  font-size: var(--font-size-xs);
  color: var(--el-color-danger);
}

.storage-section {
  margin-top: var(--spacing-2);
  padding-top: var(--spacing-3);
  border-top: 1px solid var(--color-border-primary);
}

.storage-uri-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.storage-uri-row .input-field {
  flex: 1;
  min-width: 0;
}

.storage-label {
  display: block;
  margin-bottom: var(--spacing-2);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
}
</style>
