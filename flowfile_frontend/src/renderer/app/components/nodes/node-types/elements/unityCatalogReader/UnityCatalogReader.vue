<template>
  <div v-if="dataLoaded && nodeUcReader" class="uc-reader-container">
    <generic-node-settings
      v-model="nodeUcReader"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <!-- UC Connection Selection -->
      <div class="listbox-wrapper">
        <div class="form-group">
          <label>Unity Catalog Connection</label>
          <div v-if="connectionsLoading" class="loading-state">
            <div class="loading-spinner"></div>
            <p>Loading connections...</p>
          </div>
          <div v-else>
            <select
              v-model="selectedConnectionName"
              class="form-control minimal-select"
              @change="handleConnectionChange"
            >
              <option :value="null">Select a connection...</option>
              <option
                v-for="conn in ucConnections"
                :key="conn.connectionName"
                :value="conn.connectionName"
              >
                {{ conn.connectionName }} ({{ conn.serverUrl }})
              </option>
            </select>
          </div>
        </div>
      </div>

      <!-- Table Browser -->
      <div v-if="selectedConnectionName" class="listbox-wrapper">
        <h4 class="section-subtitle">Table Selection</h4>

        <!-- Catalog -->
        <div class="form-group">
          <label>Catalog</label>
          <select
            v-model="nodeUcReader.unity_catalog_settings.table_ref.catalog_name"
            class="form-control"
            @change="handleCatalogChange"
          >
            <option value="">Select catalog...</option>
            <option v-for="cat in catalogs" :key="cat.name" :value="cat.name">
              {{ cat.name }}
              <template v-if="cat.comment"> - {{ cat.comment }}</template>
            </option>
          </select>
        </div>

        <!-- Schema -->
        <div v-if="nodeUcReader.unity_catalog_settings.table_ref.catalog_name" class="form-group">
          <label>Schema</label>
          <div v-if="schemasLoading" class="loading-inline">
            <div class="loading-spinner-sm"></div> Loading schemas...
          </div>
          <select
            v-else
            v-model="nodeUcReader.unity_catalog_settings.table_ref.schema_name"
            class="form-control"
            @change="handleSchemaChange"
          >
            <option value="">Select schema...</option>
            <option v-for="s in schemas" :key="s.name" :value="s.name">
              {{ s.name }}
              <template v-if="s.comment"> - {{ s.comment }}</template>
            </option>
          </select>
        </div>

        <!-- Table -->
        <div v-if="nodeUcReader.unity_catalog_settings.table_ref.schema_name" class="form-group">
          <label>Table</label>
          <div v-if="tablesLoading" class="loading-inline">
            <div class="loading-spinner-sm"></div> Loading tables...
          </div>
          <select
            v-else
            v-model="nodeUcReader.unity_catalog_settings.table_ref.table_name"
            class="form-control"
            @change="handleTableChange"
          >
            <option value="">Select table...</option>
            <option v-for="t in tables" :key="t.name" :value="t.name">
              {{ t.name }} ({{ t.data_source_format }})
              <template v-if="t.comment"> - {{ t.comment }}</template>
            </option>
          </select>
        </div>

        <!-- Selected Table Info -->
        <div v-if="selectedTable" class="table-info-box">
          <div class="table-info-header">
            <i class="fa-solid fa-table"></i>
            <strong>{{ selectedTable.catalog_name }}.{{ selectedTable.schema_name }}.{{ selectedTable.name }}</strong>
          </div>
          <div class="table-info-details">
            <span>Format: {{ selectedTable.data_source_format }}</span>
            <span>Type: {{ selectedTable.table_type }}</span>
            <span>Columns: {{ selectedTable.columns.length }}</span>
          </div>
          <div v-if="selectedTable.storage_location" class="table-info-location">
            <i class="fa-solid fa-hdd"></i>
            {{ selectedTable.storage_location }}
          </div>
          <div v-if="selectedTable.columns.length > 0" class="table-columns">
            <details>
              <summary>Column Schema ({{ selectedTable.columns.length }} columns)</summary>
              <div class="column-list">
                <div v-for="col in selectedTable.columns" :key="col.name" class="column-item">
                  <span class="col-name">{{ col.name }}</span>
                  <span class="col-type">{{ col.type_name }}</span>
                </div>
              </div>
            </details>
          </div>
        </div>

        <!-- Info -->
        <div class="info-box">
          <i class="fa-solid fa-info-circle"></i>
          <div>
            <p>
              Storage credentials are handled automatically via Unity Catalog's credential
              vending. You don't need to configure S3/ADLS keys separately.
            </p>
          </div>
        </div>
      </div>
    </generic-node-settings>
  </div>
  <code-loader v-else />
</template>

<script lang="ts" setup>
import { CodeLoader } from "vue-content-loader";
import { ref, computed } from "vue";
import { createNodeUnityCatalogReader } from "./utils";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import {
  fetchUcConnections,
  browseCatalogs,
  browseSchemas,
  browseTables,
} from "../../../../../views/UnityCatalogView/api";
import type {
  UnityCatalogConnectionInterface,
  CatalogInfo,
  SchemaInfo,
  TableInfo,
  NodeUnityCatalogReader,
} from "../../../../../views/UnityCatalogView/UnityCatalogTypes";
import { ElMessage } from "element-plus";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

interface Props {
  nodeId: number;
}

defineProps<Props>();
const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const nodeUcReader = ref<NodeUnityCatalogReader | null>(null);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeUcReader,
});

// Connection state
const ucConnections = ref<UnityCatalogConnectionInterface[]>([]);
const connectionsLoading = ref(false);
const selectedConnectionName = ref<string | null>(null);

// Browser state
const catalogs = ref<CatalogInfo[]>([]);
const schemas = ref<SchemaInfo[]>([]);
const tables = ref<TableInfo[]>([]);
const schemasLoading = ref(false);
const tablesLoading = ref(false);

const selectedTable = computed<TableInfo | null>(() => {
  if (!nodeUcReader.value) return null;
  const ref_ = nodeUcReader.value.unity_catalog_settings.table_ref;
  return tables.value.find((t) => t.name === ref_.table_name) || null;
});

const handleConnectionChange = async () => {
  if (!nodeUcReader.value || !selectedConnectionName.value) return;
  nodeUcReader.value.unity_catalog_settings.connection_name = selectedConnectionName.value;
  nodeUcReader.value.unity_catalog_settings.table_ref = {
    catalog_name: "",
    schema_name: "",
    table_name: "",
  };
  nodeUcReader.value.fields = [];
  schemas.value = [];
  tables.value = [];

  try {
    catalogs.value = await browseCatalogs(selectedConnectionName.value);
  } catch {
    ElMessage.error("Failed to browse catalogs");
    catalogs.value = [];
  }
};

const handleCatalogChange = async () => {
  if (!nodeUcReader.value || !selectedConnectionName.value) return;
  const catName = nodeUcReader.value.unity_catalog_settings.table_ref.catalog_name;
  nodeUcReader.value.unity_catalog_settings.table_ref.schema_name = "";
  nodeUcReader.value.unity_catalog_settings.table_ref.table_name = "";
  nodeUcReader.value.fields = [];
  tables.value = [];

  if (!catName) { schemas.value = []; return; }

  schemasLoading.value = true;
  try {
    schemas.value = await browseSchemas(selectedConnectionName.value, catName);
  } catch {
    ElMessage.error("Failed to browse schemas");
    schemas.value = [];
  } finally {
    schemasLoading.value = false;
  }
};

const handleSchemaChange = async () => {
  if (!nodeUcReader.value || !selectedConnectionName.value) return;
  const ref_ = nodeUcReader.value.unity_catalog_settings.table_ref;
  ref_.table_name = "";
  nodeUcReader.value.fields = [];

  if (!ref_.schema_name) { tables.value = []; return; }

  tablesLoading.value = true;
  try {
    tables.value = await browseTables(
      selectedConnectionName.value,
      ref_.catalog_name,
      ref_.schema_name,
    );
  } catch {
    ElMessage.error("Failed to browse tables");
    tables.value = [];
  } finally {
    tablesLoading.value = false;
  }
};

const handleTableChange = () => {
  if (!nodeUcReader.value) return;
  nodeUcReader.value.fields = [];
};

const loadNodeData = async (nodeId: number) => {
  try {
    connectionsLoading.value = true;
    const [nodeData] = await Promise.all([
      nodeStore.getNodeData(nodeId, false),
      fetchUcConnections().then((conns) => { ucConnections.value = conns; }),
    ]);
    connectionsLoading.value = false;

    if (nodeData) {
      const hasValidSetup = Boolean(nodeData.setting_input?.is_setup);
      nodeUcReader.value = hasValidSetup
        ? nodeData.setting_input
        : createNodeUnityCatalogReader(nodeStore.flow_id, nodeId);

      // Restore selection state
      if (nodeUcReader.value?.unity_catalog_settings.connection_name) {
        selectedConnectionName.value = nodeUcReader.value.unity_catalog_settings.connection_name;
        const ref_ = nodeUcReader.value.unity_catalog_settings.table_ref;
        try {
          catalogs.value = await browseCatalogs(selectedConnectionName.value);
          if (ref_.catalog_name) {
            schemas.value = await browseSchemas(selectedConnectionName.value, ref_.catalog_name);
            if (ref_.schema_name) {
              tables.value = await browseTables(selectedConnectionName.value, ref_.catalog_name, ref_.schema_name);
            }
          }
        } catch {
          // silently handle - connection may be unreachable
        }
      }
    }
    dataLoaded.value = true;
  } catch (error) {
    console.error("Error loading UC reader node data:", error);
    dataLoaded.value = false;
  }
};

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>

<style scoped>
.uc-reader-container {
  font-family: var(--font-family-base);
  max-width: 100%;
  color: var(--color-text-primary);
}
.section-subtitle { margin: 0 0 0.75rem 0; font-size: 0.95rem; font-weight: 600; color: #4a5568; }
.form-control {
  width: 100%; padding: 0.5rem; border: 1px solid #e2e8f0;
  border-radius: 4px; font-size: 0.875rem; box-sizing: border-box;
}
.form-group { margin-bottom: 0.75rem; width: 100%; }
label { display: block; margin-bottom: 0.25rem; font-size: 0.875rem; font-weight: 500; color: #4a5568; }
select.form-control {
  appearance: none;
  background-image: url("data:image/svg+xml;charset=utf-8,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' viewBox='0 0 24 24' fill='none' stroke='%234a5568' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpolyline points='6 9 12 15 18 9'/%3E%3C/svg%3E");
  background-repeat: no-repeat;
  background-position: right 0.5rem center;
  background-size: 1em;
  padding-right: 2rem;
}
.table-info-box {
  margin-top: 1rem; padding: 1rem; background-color: #f0fdf4;
  border: 1px solid #bbf7d0; border-radius: 6px;
}
.table-info-header { display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.5rem; font-size: 0.9rem; }
.table-info-header i { color: #16a34a; }
.table-info-details {
  display: flex; gap: 1rem; font-size: 0.8rem; color: #4a5568; margin-bottom: 0.5rem;
}
.table-info-location {
  display: flex; align-items: center; gap: 0.5rem; font-size: 0.75rem;
  color: #718096; font-family: monospace; word-break: break-all;
}
.table-columns { margin-top: 0.75rem; }
.table-columns summary { cursor: pointer; font-size: 0.8rem; color: #4a5568; font-weight: 500; }
.column-list { margin-top: 0.5rem; max-height: 200px; overflow-y: auto; }
.column-item {
  display: flex; justify-content: space-between; padding: 0.25rem 0.5rem;
  font-size: 0.75rem; border-bottom: 1px solid #e2e8f0;
}
.col-name { font-weight: 500; color: #2d3748; }
.col-type { color: #718096; font-family: monospace; }
.info-box {
  display: flex; gap: 0.75rem; padding: 0.75rem; background-color: #e6f7ff;
  border-left: 4px solid #1890ff; border-radius: 4px; margin-top: 1rem; font-size: 0.875rem;
}
.info-box i { color: #1890ff; font-size: 1.25rem; flex-shrink: 0; }
.info-box p { margin: 0; color: #4a5568; }
.loading-state {
  display: flex; flex-direction: column; align-items: center; gap: 0.5rem; padding: 1rem;
}
.loading-state p { margin: 0; color: #718096; font-size: 0.875rem; }
.loading-spinner {
  width: 2rem; height: 2rem; border: 2px solid #e2e8f0;
  border-top-color: #4299e1; border-radius: 50%;
  animation: spin 0.8s linear infinite;
}
.loading-inline { display: flex; align-items: center; gap: 0.5rem; color: #718096; font-size: 0.8rem; }
.loading-spinner-sm {
  width: 1rem; height: 1rem; border: 2px solid #e2e8f0;
  border-top-color: #4299e1; border-radius: 50%;
  animation: spin 0.8s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }
</style>
