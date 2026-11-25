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
        <p v-if="isHub" class="description" data-testid="hub-managed-note">
          {{ HUB_MANAGED_NOTE }}
        </p>
      </div>
      <div v-if="!isHub && !external && canManage(namespace)" class="header-actions">
        <button
          class="btn btn-danger btn-sm"
          data-testid="catalog-delete"
          :disabled="!!deleteBlocked || deleting"
          :title="deleteBlocked ?? 'Delete catalog'"
          @click="deleteCatalog"
        >
          <i class="fa-solid fa-trash"></i>
          Delete
        </button>
      </div>
    </div>

    <template v-if="external">
      <div class="section-header">
        <h3 class="section-title">Kept elsewhere</h3>
        <div class="header-actions">
          <button
            v-if="canChangeStorage(namespace)"
            class="btn btn-secondary btn-sm"
            data-testid="external-panel-test"
            :disabled="testing"
            @click="testStored"
          >
            <i :class="testing ? 'fa-solid fa-spinner fa-spin' : 'fa-solid fa-plug'"></i>
            Test
          </button>
          <button
            class="btn btn-secondary btn-sm"
            data-testid="external-panel-refresh"
            :disabled="refreshing"
            @click="refresh(true)"
          >
            <i :class="refreshing ? 'fa-solid fa-spinner fa-spin' : 'fa-solid fa-rotate'"></i>
            Refresh
          </button>
          <button
            v-if="canChangeStorage(namespace)"
            class="btn btn-secondary btn-sm"
            data-testid="external-panel-edit"
            @click="showExternalEditor = true"
          >
            <i class="fa-solid fa-pen"></i>
            Edit
          </button>
          <button
            v-if="canChangeStorage(namespace)"
            class="btn btn-danger btn-sm"
            data-testid="external-panel-remove"
            :disabled="deleting"
            @click="removeExternal"
          >
            <i class="fa-solid fa-link-slash"></i>
            Remove from Flowfile
          </button>
        </div>
      </div>
      <div class="meta-grid" data-testid="external-panel">
        <div class="meta-card">
          <span class="meta-label">Address</span>
          <span class="meta-value mono">{{ external.uri }}</span>
        </div>
        <div class="meta-card">
          <span class="meta-label">Warehouse</span>
          <span class="meta-value">{{ external.warehouse || "--" }}</span>
        </div>
        <div class="meta-card">
          <span class="meta-label">Sign-in</span>
          <span class="meta-value">
            {{ signInLabel(external.sign_in) }}
            <template v-if="external.client_id"> ({{ external.client_id }})</template>
          </span>
        </div>
        <div class="meta-card">
          <span class="meta-label">Brought in line</span>
          <span class="meta-value">{{ syncedLine(external) }}</span>
        </div>
      </div>
      <p v-if="notShown" class="storage-hint muted">{{ notShown }}</p>
      <p v-if="external.sync_error" class="storage-hint error" data-testid="external-sync-error">
        {{ external.sync_error }} The schemas known before are kept.
      </p>
      <p
        v-if="testResult"
        class="storage-hint"
        :class="testResult.ok ? 'ok' : 'error'"
        data-testid="external-panel-test-result"
      >
        {{ testResult.message }}
      </p>
      <p class="storage-hint">{{ NO_KEY_TYPED }}</p>
      <ExternalCatalogModal
        :visible="showExternalEditor"
        :catalog="namespace"
        @close="showExternalEditor = false"
        @saved="onExternalSaved"
      />
    </template>

    <div v-if="!external" class="section-header">
      <h3 class="section-title">Storage</h3>
      <button
        v-if="!isHub && canChangeStorage(namespace)"
        class="btn btn-secondary btn-sm"
        data-testid="catalog-edit-storage"
        :disabled="!!storageLocked"
        :title="storageLocked ?? 'Edit storage'"
        @click="showStorageEditor = true"
      >
        <i class="fa-solid fa-pen"></i>
        Edit storage
      </button>
    </div>

    <div v-if="!external" class="meta-grid">
      <div class="meta-card">
        <span class="meta-label">Backend</span>
        <span class="meta-value">
          <i :class="isCloud ? providerIcon : 'fa-solid fa-hard-drive'"></i>
          {{ isCloud ? "Object storage" : isHub ? "Hub filesystem" : "Local" }}
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

    <template v-if="!isHub && !external">
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
      <p v-if="storageLocked && canChangeStorage(namespace)" class="storage-hint muted">
        {{ storageLocked }}
      </p>
    </template>

    <EditCatalogStorageModal
      :visible="showStorageEditor"
      :catalog="namespace"
      @close="showStorageEditor = false"
    />
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import type { NamespaceTree } from "../../types/catalog.types";
import { CatalogApi } from "../../api/catalog.api";
import { useCatalogStore } from "../../stores/catalog-store";
import { useResourceSharing } from "../../composables/useResourceSharing";
import SharedBadge from "../../components/sharing/SharedBadge.vue";
import EditCatalogStorageModal from "./EditCatalogStorageModal.vue";
import { HUB_MANAGED_NOTE, isHubRow } from "./hubRows";
import ExternalCatalogModal from "./ExternalCatalogModal.vue";
import {
  NO_KEY_TYPED,
  answerStillShown,
  catalogAfterSave,
  notShownNote,
  signInLabel,
  syncedLine,
} from "./externalCatalog";
import type { ExternalCatalogTestResult } from "../../types";
import { forgetCatalogSelection } from "./catalogLastLocation";
import { deleteBlockedReason, storageLockedReason } from "./catalogStorageEdit";
import { fetchCloudStorageConnectionsInterfaces } from "../CloudConnectionView/api";
import {
  getStorageTypeLabel,
  getAuthMethodLabel,
} from "../CloudConnectionView/cloudConnectionFormatters";
import type { FullCloudStorageConnectionInterface } from "../CloudConnectionView/CloudConnectionTypes";

const props = defineProps<{ namespace: NamespaceTree }>();
const emit = defineEmits(["close"]);

const catalogStore = useCatalogStore();
// Storage is the owner's (or an admin's) to change, as core refuses a manage-grantee
const { canManage, canManageGrants: canChangeStorage } = useResourceSharing();

const isCloud = computed(() => !!props.namespace.storage_uri);
const isHub = computed(() => isHubRow(props.namespace));
const connection = ref<FullCloudStorageConnectionInterface | null>(null);
const loadingConnection = ref(false);
const showStorageEditor = ref(false);
const deleting = ref(false);

const external = computed(() => props.namespace.external ?? null);
const notShown = computed(() => notShownNote(external.value?.not_shown ?? 0));
const showExternalEditor = ref(false);
const testing = ref(false);
const refreshing = ref(false);
const testResult = ref<ExternalCatalogTestResult | null>(null);

/** Whether the panel still shows the catalog an answer was asked for. */
const stillShown = (id: number) =>
  answerStillShown(id, catalogStore.selectedNamespaceId, props.namespace.id);

async function refresh(force: boolean) {
  if (!external.value) return;
  const id = props.namespace.id;
  refreshing.value = force;
  try {
    await catalogStore.syncExternalCatalog(id, force);
    if (stillShown(id)) catalogStore.selectedNamespace = catalogStore.findNamespaceInTree(id);
  } finally {
    if (stillShown(id)) refreshing.value = false;
  }
}

async function testStored() {
  const info = external.value;
  if (!info) return;
  const id = props.namespace.id;
  testing.value = true;
  let result: ExternalCatalogTestResult;
  try {
    result = await CatalogApi.testExternalCatalog({
      uri: info.uri,
      warehouse: info.warehouse,
      sign_in: info.sign_in,
      client_id: info.client_id,
      scope: info.scope,
      token_endpoint: info.token_endpoint,
      namespace_id: id,
    });
  } catch (e: any) {
    const detail = e?.response?.data?.detail;
    result = {
      ok: false,
      message: typeof detail === "string" ? detail : "The test could not be run.",
      schemas: null,
    };
  }
  if (!stillShown(id)) return;
  testResult.value = result;
  testing.value = false;
}

/** After Edit then Save: the panel, and so the next edit, hold what was saved. */
function onExternalSaved(saved: NamespaceTree) {
  if (!stillShown(saved.id)) return;
  catalogStore.selectedNamespace = catalogAfterSave(
    catalogStore.findNamespaceInTree(saved.id),
    props.namespace,
    saved,
  );
  testResult.value = null;
}

async function removeExternal() {
  const catalog = props.namespace;
  try {
    await ElMessageBox.confirm(
      `Remove "${catalog.name}" from Flowfile? Its schemas and the stored sign-in go from Flowfile; the other ` +
        "catalog and its tables are not touched.",
      "Remove from Flowfile",
      { confirmButtonText: "Remove", cancelButtonText: "Cancel", type: "warning" },
    );
  } catch {
    return;
  }
  deleting.value = true;
  try {
    await CatalogApi.deleteExternalCatalog(catalog.id);
    forgetCatalogSelection("namespaceId", catalog.id);
    ElMessage.success(`Removed ${catalog.name} from Flowfile`);
    if (stillShown(catalog.id)) {
      catalogStore.clearNamespaceSelection();
      emit("close");
    }
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Could not remove the catalog");
  } finally {
    await Promise.all([catalogStore.loadTree(), catalogStore.loadStats()]);
    deleting.value = false;
  }
}

watch(
  () => props.namespace.id,
  () => {
    testResult.value = null;
    testing.value = false;
    void refresh(false);
  },
  { immediate: true },
);

const storageLocked = computed(() => storageLockedReason(props.namespace));
const deleteBlocked = computed(() => deleteBlockedReason(props.namespace));

async function deleteCatalog() {
  if (deleteBlocked.value) return;
  const catalog = props.namespace;
  const schemas = catalog.children.map((s) => `"${s.name}"`).join(", ");
  const withSchemas = schemas
    ? ` and its empty schema${catalog.children.length > 1 ? "s" : ""} ${schemas}`
    : "";
  try {
    await ElMessageBox.confirm(
      `Delete the catalog "${catalog.name}"${withSchemas}? Nothing in its storage is touched.`,
      "Delete catalog",
      { confirmButtonText: "Delete", cancelButtonText: "Cancel", type: "warning" },
    );
  } catch {
    return;
  }
  deleting.value = true;
  try {
    for (const schema of catalog.children) await CatalogApi.deleteNamespace(schema.id);
    await CatalogApi.deleteNamespace(catalog.id);
    forgetCatalogSelection("namespaceId", catalog.id);
    ElMessage.success(`Deleted ${catalog.name}`);
    if (stillShown(catalog.id)) {
      catalogStore.clearNamespaceSelection();
      emit("close");
    }
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Could not delete the catalog");
  } finally {
    await Promise.all([catalogStore.loadTree(), catalogStore.loadStats()]);
    if (catalogStore.selectedNamespaceId !== null) {
      catalogStore.selectedNamespace = catalogStore.findNamespaceInTree(
        catalogStore.selectedNamespaceId,
      );
    }
    deleting.value = false;
  }
}

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
  loadingConnection.value = false;
  const id = props.namespace.id;
  const name = props.namespace.storage_connection_name;
  // A hub catalog's connection is the hub's; this desktop's connections never hold it.
  if (isHub.value || !isCloud.value || !name) return;
  const asked = () => stillShown(id) && props.namespace.storage_connection_name === name;
  loadingConnection.value = true;
  let found: FullCloudStorageConnectionInterface | null = null;
  try {
    const all = await fetchCloudStorageConnectionsInterfaces();
    found = all.find((c) => c.connectionName === name) ?? null;
  } catch {
    found = null;
  }
  if (!asked()) return;
  connection.value = found;
  loadingConnection.value = false;
}

watch(() => [props.namespace.id, props.namespace.storage_connection_name], loadConnection);
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

.header-actions {
  display: flex;
  gap: var(--spacing-2);
}

.section-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: var(--spacing-3);
}

.section-title {
  margin: 0;
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

.storage-hint.error {
  color: var(--el-color-danger, #f56c6c);
}

.storage-hint.ok {
  color: var(--el-color-success, #67c23a);
}
</style>
