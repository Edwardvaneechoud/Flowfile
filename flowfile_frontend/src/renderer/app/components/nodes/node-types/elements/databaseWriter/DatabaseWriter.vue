// DatabaseWriterNode.vue
<template>
  <div v-if="dataLoaded && nodeData" class="db-container">
    <generic-node-settings v-model="nodeData">
      <!-- Connection Mode Selection -->
      <div class="listbox-wrapper">
        <div class="form-group">
          <label>Connection Mode</label>
          <el-radio-group v-model="nodeData.database_write_settings.connection_mode">
            <el-radio v-for="option in connectionModeOptions" :key="option" :label="option">
              {{ option }}
            </el-radio>
          </el-radio-group>
        </div>

        <!-- Connection Settings -->
        <div>
          <!-- Inline connection settings -->
          <div
            v-if="
              nodeData.database_write_settings.connection_mode === 'inline' &&
              nodeData.database_write_settings.database_connection
            "
          >
            <database-connection-settings
              v-model="nodeData.database_write_settings.database_connection"
              @change="resetFields"
            />
          </div>

          <!-- Existing connection selection -->
          <div v-else>
            <div v-if="connectionsAreLoading">
              <div class="loading-spinner"></div>
              <p>Loading connections...</p>
            </div>
            <div v-else>
              <select
                id="connection-select"
                v-model="nodeData.database_write_settings.database_connection_name"
                class="form-control minimal-select"
              >
                <option disabled value="">Choose a connection</option>
                <option
                  v-for="conn in connectionInterfaces"
                  :key="conn.connectionName"
                  :value="conn.connectionName"
                >
                  {{ conn.connectionName }} ({{ conn.databaseType }} - {{ conn.database }})
                </option>
              </select>
            </div>
          </div>
        </div>
      </div>

      <!-- Table Settings -->
      <div class="listbox-wrapper">
        <h4 class="section-subtitle">Table Settings</h4>
        <div class="form-row">
          <div class="form-group half">
            <label for="schema-name">Schema</label>
            <input
              id="schema-name"
              v-model="nodeData.database_write_settings.schema_name"
              type="text"
              class="form-control"
              placeholder="Enter schema name"
            />
          </div>

          <div class="form-group half">
            <label for="table-name">Table</label>
            <input
              id="table-name"
              v-model="nodeData.database_write_settings.table_name"
              type="text"
              class="form-control"
              placeholder="Enter table name"
            />
          </div>
        </div>
      </div>

      <!-- If Exists Options -->
      <div class="listbox-wrapper">
        <div class="form-group">
          <label for="if-exists-action">If Table Exists</label>
          <select
            id="if-exists-action"
            v-model="nodeData.database_write_settings.if_exists"
            class="form-control"
          >
            <option v-for="action in ifExistActions" :key="action" :value="action">
              {{ action.charAt(0).toUpperCase() + action.slice(1) }}
            </option>
          </select>
        </div>
        <div class="form-group">
          <p class="option-description">
            <strong>Append:</strong> Add new data to existing table<br />
            <strong>Replace:</strong> Delete existing table and create new one<br />
            <strong>Fail:</strong> Abort if table already exists
          </p>
        </div>
      </div>
    </generic-node-settings>
  </div>
  <code-loader v-else />
</template>

<script lang="ts" setup>
import { CodeLoader } from "vue-content-loader";
import { ref, onMounted } from "vue";
import {
  NodeDatabaseWriter,
  IfExistAction,
  ConnectionModeOption,
} from "../../../baseNode/nodeInput";
import { createNodeDatabaseWriter } from "./utils";
import { useNodeStore } from "../../../../../stores/column-store";
import { useNodeSettings } from "../../../../../composables";
import { fetchDatabaseConnectionsInterfaces } from "../../../../../views/DatabaseView/api";
import { FullDatabaseConnectionInterface } from "../../../../../views/DatabaseView/databaseConnectionTypes";
import { ElMessage, ElRadio } from "element-plus";
import DatabaseConnectionSettings from "../databaseReader/DatabaseConnectionSettings.vue";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

interface Props {
  nodeId: number;
}

const props = defineProps<Props>();
const nodeStore = useNodeStore();
const connectionModeOptions = ref<ConnectionModeOption[]>(["inline", "reference"]);
const ifExistActions = ref<IfExistAction[]>(["append", "replace", "fail"]);
const connectionInterfaces = ref<FullDatabaseConnectionInterface[]>([]);
const nodeData = ref<null | NodeDatabaseWriter>(null);
const dataLoaded = ref(false);
const connectionsAreLoading = ref(false);

const { saveSettings, pushNodeData } = useNodeSettings({
  nodeData: nodeData,
  beforeSave: () => {
    if (!nodeData.value) {
      throw new Error("Node data not available");
    }
    // Clean up settings based on connection_mode before saving
    if (nodeData.value.database_write_settings.connection_mode === "reference") {
      nodeData.value.database_write_settings.database_connection = undefined;
    } else {
      nodeData.value.database_write_settings.database_connection_name = undefined;
    }
    nodeData.value.is_setup = true;
  },
});

// Load node data from the store
const loadNodeData = async (nodeId: number) => {
  try {
    const fetchedNodeData = await nodeStore.getNodeData(nodeId, false);
    if (fetchedNodeData) {
      const hasValidSetup = Boolean(fetchedNodeData.setting_input?.is_setup);
      nodeData.value = hasValidSetup
        ? fetchedNodeData.setting_input
        : createNodeDatabaseWriter(nodeStore.flow_id, nodeId);
    }
    dataLoaded.value = true;
  } catch (error) {
    console.error("Error loading node data:", error);
    dataLoaded.value = false;
    ElMessage.error("Failed to load node data");
  }
};

// Simple reset fields function (minimal)
const resetFields = () => {
  // No fields to reset in this simplified version
};

// Fetch available database connections
const fetchConnections = async () => {
  connectionsAreLoading.value = true;
  try {
    connectionInterfaces.value = await fetchDatabaseConnectionsInterfaces();
  } catch (error) {
    console.error("Error fetching connections:", error);
    ElMessage.error("Failed to load database connections");
  } finally {
    connectionsAreLoading.value = false;
  }
};

onMounted(async () => {
  await fetchConnections();
  await loadNodeData(props.nodeId);
});

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});
</script>

<style scoped>
.db-container {
  font-family: var(--font-family-base);
  max-width: 100%;
  color: var(--color-text-primary);
}

.section-subtitle {
  margin: 0 0 0.75rem 0;
  font-size: 0.95rem;
  font-weight: 600;
  color: var(--color-text-secondary);
}

.form-row {
  display: flex;
  gap: 0.75rem;
  margin-bottom: 0.75rem;
  width: 100%;
  box-sizing: border-box;
}

.half {
  flex: 1;
  min-width: 0; /* Allow fields to shrink below their content size */
  max-width: calc(50% - 0.375rem); /* Account for the gap between items */
}

.form-control {
  width: 100%;
  padding: 0.5rem;
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  font-size: 0.875rem;
  box-sizing: border-box;
  background-color: var(--color-background-primary);
  color: var(--color-text-primary);
}

.form-group {
  margin-bottom: 0.75rem;
  width: 100%;
}

label {
  display: block;
  margin-bottom: 0.25rem;
  font-size: 0.875rem;
  font-weight: 500;
  color: var(--color-text-secondary);
}

select.form-control {
  appearance: none;
  background-image: url("data:image/svg+xml;charset=utf-8,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' viewBox='0 0 24 24' fill='none' stroke='%234a5568' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpolyline points='6 9 12 15 18 9'/%3E%3C/svg%3E");
  background-repeat: no-repeat;
  background-position: right 0.5rem center;
  background-size: 1em;
  padding-right: 2rem;
}

.loading-spinner {
  display: inline-block;
  width: 20px;
  height: 20px;
  border: 2px solid var(--color-info-light);
  border-radius: 50%;
  border-top-color: var(--color-info);
  animation: spin 1s ease-in-out infinite;
  margin-right: 8px;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

.option-description {
  font-size: 0.875rem;
  color: var(--color-text-tertiary);
  margin: 0.5rem 0;
  padding: 0.75rem;
  background-color: var(--color-gray-50);
  border-radius: 4px;
  border-left: 3px solid var(--color-info);
}

.el-radio-group {
  display: flex !important;
  flex-direction: row !important;
  gap: 1rem;
}

.el-radio {
  margin-right: 0 !important; /* Override any existing margins */
}

@media (max-width: 640px) {
  .form-row {
    flex-direction: column;
    gap: 0.5rem;
  }

  .half {
    max-width: 100%;
  }

  .el-radio-group {
    flex-direction: column !important;
    gap: 0.5rem;
  }
}
</style>
