<template>
  <div v-if="visible" class="modal-overlay" @click.self="$emit('close')">
    <div class="modal-container">
      <div class="modal-header">
        <h3 class="modal-title">{{ parentId ? "Create Schema" : "Create Catalog" }}</h3>
      </div>
      <div class="modal-content">
        <input
          v-model="name"
          class="input-field"
          :placeholder="parentId ? 'Schema name' : 'Catalog name'"
          @keyup.enter="submit"
        />
        <p v-if="nameError" class="field-error">{{ nameError }}</p>
        <input v-model="description" class="input-field" placeholder="Description (optional)" />

        <div v-if="!parentId" class="storage-section">
          <label class="storage-label">Object storage (optional)</label>
          <input
            v-model="storageUri"
            class="input-field"
            placeholder="s3://bucket/catalog (leave blank for local storage)"
          />
          <CloudConnectionPicker
            v-model="selectedConnection"
            :connections="connections"
            :loading="connectionsLoading"
            label="Storage connection"
            no-connection-label="No connection (local storage)"
            helper-text="Leave unset to store this catalog's tables on local storage."
          />
          <p v-if="storageError" class="field-error">{{ storageError }}</p>
        </div>
      </div>
      <div class="modal-actions">
        <button class="btn btn-secondary" @click="$emit('close')">Cancel</button>
        <button
          class="btn btn-primary"
          :disabled="!name.trim() || !!nameError || !!storageError"
          @click="submit"
        >
          Create
        </button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, watch, computed } from "vue";
import { ElMessage } from "element-plus";
import { CatalogApi } from "../../api/catalog.api";
import type { NamespaceCreate } from "../../types/catalog.types";
import { useCatalogStore } from "../../stores/catalog-store";
import { validateCatalogName } from "../../composables/catalogNameValidation";
import { CloudConnectionPicker } from "../../components/common";
import { fetchCloudStorageConnectionsInterfaces } from "../CloudConnectionView/api";
import type { FullCloudStorageConnectionInterface } from "../CloudConnectionView/CloudConnectionTypes";

const CLOUD_URI_SCHEMES = [
  "s3://",
  "s3a://",
  "az://",
  "abfs://",
  "abfss://",
  "adl://",
  "gs://",
  "gcs://",
];

const props = defineProps<{
  visible: boolean;
  parentId: number | null;
}>();

const emit = defineEmits(["close"]);

const catalogStore = useCatalogStore();
const name = ref("");
const description = ref("");

const storageUri = ref("");
const selectedConnection = ref<FullCloudStorageConnectionInterface | null>(null);
const connections = ref<FullCloudStorageConnectionInterface[]>([]);
const connectionsLoading = ref(false);

const nameError = computed(() =>
  validateCatalogName(name.value, props.parentId ? "Schema" : "Catalog"),
);

const storageError = computed(() => {
  if (props.parentId) return null;
  const uri = storageUri.value.trim();
  const connectionName = selectedConnection.value?.connectionName ?? null;
  if (uri && !CLOUD_URI_SCHEMES.some((s) => uri.startsWith(s))) {
    return "Storage URI must start with s3://, gs://, abfss://, …";
  }
  if (uri && !connectionName) return "Select a storage connection for this URI";
  if (connectionName && !uri)
    return "Enter a storage URI (e.g. s3://bucket/path) for this connection";
  return null;
});

async function loadConnections() {
  connectionsLoading.value = true;
  try {
    connections.value = await fetchCloudStorageConnectionsInterfaces();
  } catch {
    connections.value = [];
  } finally {
    connectionsLoading.value = false;
  }
}

watch(
  () => props.visible,
  (val) => {
    if (val) {
      name.value = "";
      description.value = "";
      storageUri.value = "";
      selectedConnection.value = null;
      if (!props.parentId) loadConnections();
    }
  },
);

async function submit() {
  if (!name.value.trim() || nameError.value || storageError.value) return;
  try {
    const payload: NamespaceCreate = {
      name: name.value.trim(),
      parent_id: props.parentId,
      description: description.value.trim() || null,
    };
    if (!props.parentId && storageUri.value.trim()) {
      payload.storage_uri = storageUri.value.trim();
      payload.storage_connection_name = selectedConnection.value?.connectionName ?? null;
    }
    await CatalogApi.createNamespace(payload);
    emit("close");
    await Promise.all([catalogStore.loadTree(), catalogStore.loadStats()]);
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to create namespace");
  }
}
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

.storage-label {
  display: block;
  margin-bottom: var(--spacing-2);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
}
</style>
