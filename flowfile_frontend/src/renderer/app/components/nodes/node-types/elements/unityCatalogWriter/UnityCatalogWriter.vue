<template>
  <div v-if="dataLoaded && nodeUcWriter" class="uc-writer-container">
    <generic-node-settings
      v-model="nodeUcWriter"
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
          <select
            v-else
            v-model="nodeUcWriter.unity_catalog_settings.connection_name"
            class="form-control minimal-select"
            @change="handleConnectionChange"
          >
            <option :value="undefined">Select a connection...</option>
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

      <!-- Table Destination -->
      <div v-if="nodeUcWriter.unity_catalog_settings.connection_name" class="listbox-wrapper">
        <h4 class="section-subtitle">Table Destination</h4>

        <div class="form-group">
          <label>Catalog Name</label>
          <input
            v-model="nodeUcWriter.unity_catalog_settings.table_ref.catalog_name"
            type="text"
            class="form-control"
            placeholder="my_catalog"
          />
        </div>

        <div class="form-group">
          <label>Schema Name</label>
          <input
            v-model="nodeUcWriter.unity_catalog_settings.table_ref.schema_name"
            type="text"
            class="form-control"
            placeholder="my_schema"
          />
        </div>

        <div class="form-group">
          <label>Table Name</label>
          <input
            v-model="nodeUcWriter.unity_catalog_settings.table_ref.table_name"
            type="text"
            class="form-control"
            placeholder="my_table"
          />
        </div>

        <div class="form-group">
          <label>Format</label>
          <select
            v-model="nodeUcWriter.unity_catalog_settings.data_source_format"
            class="form-control"
          >
            <option value="DELTA">Delta Lake</option>
            <option value="PARQUET">Parquet</option>
          </select>
        </div>

        <div class="form-group">
          <label>Write Mode</label>
          <select
            v-model="nodeUcWriter.unity_catalog_settings.write_mode"
            class="form-control"
          >
            <option value="overwrite">Overwrite</option>
            <option value="append">Append</option>
          </select>
        </div>

        <div class="form-group">
          <div class="checkbox-container">
            <input
              id="register-table"
              v-model="nodeUcWriter.unity_catalog_settings.register_table"
              type="checkbox"
              class="checkbox-input"
            />
            <label for="register-table" class="checkbox-label">
              Register table in Unity Catalog
            </label>
          </div>
        </div>

        <div v-if="nodeUcWriter.unity_catalog_settings.register_table" class="form-group">
          <label>Table Comment (optional)</label>
          <input
            v-model="nodeUcWriter.unity_catalog_settings.table_comment"
            type="text"
            class="form-control"
            placeholder="Description of this table"
          />
        </div>
      </div>
    </generic-node-settings>
  </div>
  <code-loader v-else />
</template>

<script lang="ts" setup>
import { CodeLoader } from "vue-content-loader";
import { ref } from "vue";
import { createNodeUnityCatalogWriter } from "./utils";
import type { NodeUnityCatalogWriter } from "./utils";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { fetchUcConnections } from "../../../../../views/UnityCatalogView/api";
import type { UnityCatalogConnectionInterface } from "../../../../../views/UnityCatalogView/UnityCatalogTypes";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

interface Props {
  nodeId: number;
}

defineProps<Props>();
const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const nodeUcWriter = ref<NodeUnityCatalogWriter | null>(null);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeUcWriter,
});

const ucConnections = ref<UnityCatalogConnectionInterface[]>([]);
const connectionsLoading = ref(false);

const handleConnectionChange = () => {
  // Reset on connection change
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
      nodeUcWriter.value = hasValidSetup
        ? nodeData.setting_input
        : createNodeUnityCatalogWriter(nodeStore.flow_id, nodeId);
    }
    dataLoaded.value = true;
  } catch (error) {
    console.error("Error loading UC writer node data:", error);
    dataLoaded.value = false;
  }
};

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>

<style scoped>
.uc-writer-container {
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
.checkbox-container { display: flex; align-items: center; gap: 0.5rem; }
.checkbox-input { width: 1rem; height: 1rem; cursor: pointer; }
.checkbox-label { margin: 0; cursor: pointer; }
.loading-state {
  display: flex; flex-direction: column; align-items: center; gap: 0.5rem; padding: 1rem;
}
.loading-state p { margin: 0; color: #718096; font-size: 0.875rem; }
.loading-spinner {
  width: 2rem; height: 2rem; border: 2px solid #e2e8f0;
  border-top-color: #4299e1; border-radius: 50%;
  animation: spin 0.8s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }
</style>
