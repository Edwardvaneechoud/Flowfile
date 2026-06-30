<template>
  <div class="namespace-detail">
    <button class="back-btn" @click="emit('close')">
      <i class="fa-solid fa-arrow-left"></i> Back
    </button>

    <div class="detail-header">
      <div class="header-main">
        <div class="header-title">
          <i class="fa-solid fa-box-archive header-icon"></i>
          <h2>{{ namespace.name }}</h2>
          <SharedBadge :access="namespace.access" />
        </div>
        <p v-if="namespace.description" class="description">{{ namespace.description }}</p>
      </div>
    </div>

    <h3 class="section-title">Storage</h3>

    <div class="meta-grid">
      <div class="meta-card">
        <span class="meta-label">Backend</span>
        <span class="meta-value">
          <i :class="isCloud ? providerIcon : 'fa-solid fa-hard-drive'"></i>
          {{ isCloud ? "Object storage" : "Local" }}
        </span>
      </div>

      <template v-if="isCloud">
        <div class="meta-card">
          <span class="meta-label">Storage URI</span>
          <span class="meta-value mono">{{ namespace.storage_uri }}</span>
        </div>
        <div class="meta-card">
          <span class="meta-label">Connection</span>
          <span class="meta-value">{{ namespace.storage_connection_name || "--" }}</span>
        </div>
        <template v-if="connection">
          <div class="meta-card">
            <span class="meta-label">Provider</span>
            <span class="meta-value">{{ getStorageTypeLabel(connection.storageType) }}</span>
          </div>
          <div class="meta-card">
            <span class="meta-label">Auth method</span>
            <span class="meta-value">{{ getAuthMethodLabel(connection.authMethod) }}</span>
          </div>
          <div v-if="location" class="meta-card">
            <span class="meta-label">{{ location.label }}</span>
            <span class="meta-value">{{ location.value }}</span>
          </div>
          <div v-if="connection.endpointUrl" class="meta-card">
            <span class="meta-label">Endpoint</span>
            <span class="meta-value mono">{{ connection.endpointUrl }}</span>
          </div>
          <div class="meta-card">
            <span class="meta-label">SSL verification</span>
            <span class="meta-value">{{ connection.verifySsl ? "Enabled" : "Disabled" }}</span>
          </div>
        </template>
      </template>
    </div>

    <p v-if="!isCloud" class="storage-hint">
      Tables in this catalog are stored on the local filesystem under Flowfile's managed catalog
      directory.
    </p>
    <p v-else-if="loadingConnection" class="storage-hint">
      <i class="fa-solid fa-spinner fa-spin"></i> Loading connection details…
    </p>
    <p v-else-if="!connection" class="storage-hint muted">
      Connection details unavailable — it may be owned by another user or no longer exist.
    </p>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted } from "vue";
import type { NamespaceTree } from "../../types/catalog.types";
import SharedBadge from "../../components/sharing/SharedBadge.vue";
import { fetchCloudStorageConnectionsInterfaces } from "../CloudConnectionView/api";
import {
  getStorageTypeLabel,
  getAuthMethodLabel,
} from "../CloudConnectionView/cloudConnectionFormatters";
import type { FullCloudStorageConnectionInterface } from "../CloudConnectionView/CloudConnectionTypes";

const props = defineProps<{ namespace: NamespaceTree }>();
const emit = defineEmits(["close"]);

const isCloud = computed(() => !!props.namespace.storage_uri);
const connection = ref<FullCloudStorageConnectionInterface | null>(null);
const loadingConnection = ref(false);

const providerIcon = computed(() => {
  switch (connection.value?.storageType) {
    case "s3":
      return "fa-brands fa-aws";
    case "adls":
      return "fa-brands fa-microsoft";
    case "gcs":
      return "fa-brands fa-google";
    default:
      return "fa-solid fa-cloud";
  }
});

const location = computed<{ label: string; value: string } | null>(() => {
  const c = connection.value;
  if (!c) return null;
  if (c.storageType === "s3" && c.awsRegion) return { label: "Region", value: c.awsRegion };
  if (c.storageType === "adls" && c.azureAccountName)
    return { label: "Account", value: c.azureAccountName };
  if (c.storageType === "gcs" && c.gcsProjectId) return { label: "Project", value: c.gcsProjectId };
  return null;
});

async function loadConnection() {
  connection.value = null;
  const name = props.namespace.storage_connection_name;
  if (!isCloud.value || !name) return;
  loadingConnection.value = true;
  try {
    const all = await fetchCloudStorageConnectionsInterfaces();
    connection.value = all.find((c) => c.connectionName === name) ?? null;
  } catch {
    connection.value = null;
  } finally {
    loadingConnection.value = false;
  }
}

watch(() => props.namespace.id, loadConnection);
onMounted(loadConnection);
</script>

<style scoped>
.namespace-detail {
  max-width: 1000px;
  margin: 0 auto;
}

.detail-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: var(--spacing-6);
}

.header-title {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
}

.header-icon {
  font-size: 20px;
  color: var(--color-primary);
}

.header-title h2 {
  margin: 0;
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
}

.description {
  margin: var(--spacing-2) 0 0 0;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
}

.section-title {
  margin: 0 0 var(--spacing-3);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-secondary);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.meta-value i {
  margin-right: 6px;
  color: var(--color-text-muted);
}

.storage-hint {
  margin: var(--spacing-3) 0 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.storage-hint.muted {
  color: var(--color-text-muted);
}
</style>
