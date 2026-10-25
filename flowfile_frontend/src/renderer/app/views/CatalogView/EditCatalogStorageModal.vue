<template>
  <div v-if="visible" class="modal-overlay" @click.self="emit('close')">
    <div class="modal-container">
      <div class="modal-header">
        <h3 class="modal-title">Edit storage of {{ catalog.name }}</h3>
      </div>
      <div class="modal-content">
        <p class="hint">
          New tables in this catalog are stored here. Its storage can change only while it holds no
          table; clear the URI to store tables locally.
        </p>
        <CatalogStorageFields
          v-model:uri="storageUri"
          v-model:connection="selectedConnection"
          :connections="connections"
          :connections-loading="connectionsLoading"
        />
      </div>
      <div class="modal-actions">
        <button class="btn btn-secondary" @click="emit('close')">Cancel</button>
        <button
          class="btn btn-primary"
          :disabled="saving || connectionsLoading || !!storageError || !changed"
          @click="save"
        >
          Save
        </button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from "vue";
import { ElMessage } from "element-plus";
import { CatalogApi } from "../../api/catalog.api";
import type { NamespaceTree } from "../../types/catalog.types";
import { useCatalogStore } from "../../stores/catalog-store";
import { fetchCloudStorageConnectionsInterfaces } from "../CloudConnectionView/api";
import type { FullCloudStorageConnectionInterface } from "../CloudConnectionView/CloudConnectionTypes";
import CatalogStorageFields from "./CatalogStorageFields.vue";
import { catalogStorageError } from "./catalogStorageEdit";

const props = defineProps<{ visible: boolean; catalog: NamespaceTree }>();
const emit = defineEmits(["close"]);

const catalogStore = useCatalogStore();
const storageUri = ref("");
const selectedConnection = ref<FullCloudStorageConnectionInterface | null>(null);
const connections = ref<FullCloudStorageConnectionInterface[]>([]);
const connectionsLoading = ref(false);
const saving = ref(false);

const connectionName = computed(() => selectedConnection.value?.connectionName ?? null);
const storageError = computed(() => catalogStorageError(storageUri.value, connectionName.value));
const changed = computed(
  () =>
    (storageUri.value.trim() || null) !== (props.catalog.storage_uri ?? null) ||
    connectionName.value !== (props.catalog.storage_connection_name ?? null),
);

async function load() {
  storageUri.value = props.catalog.storage_uri ?? "";
  selectedConnection.value = null;
  connectionsLoading.value = true;
  try {
    connections.value = await fetchCloudStorageConnectionsInterfaces();
  } catch {
    connections.value = [];
  } finally {
    connectionsLoading.value = false;
  }
  const current = props.catalog.storage_connection_name;
  selectedConnection.value = connections.value.find((c) => c.connectionName === current) ?? null;
}

watch(
  () => props.visible,
  (visible) => {
    if (visible) load();
  },
);

async function save() {
  if (storageError.value || !changed.value) return;
  saving.value = true;
  const uri = storageUri.value.trim() || null;
  try {
    await CatalogApi.updateNamespace(props.catalog.id, {
      storage_uri: uri,
      storage_connection_name: uri ? connectionName.value : null,
    });
    await Promise.all([catalogStore.loadTree(), catalogStore.loadStats()]);
    catalogStore.selectNamespace(props.catalog.id);
    ElMessage.success(
      uri ? `Tables of ${props.catalog.name} go to ${uri}` : "Tables are stored locally",
    );
    emit("close");
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Could not change the catalog's storage");
  } finally {
    saving.value = false;
  }
}
</script>

<style scoped>
.hint {
  margin: 0 0 var(--spacing-3);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}
</style>
