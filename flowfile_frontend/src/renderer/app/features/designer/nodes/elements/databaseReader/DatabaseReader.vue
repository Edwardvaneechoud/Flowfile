// DatabaseReaderNode.vue
<template>
  <div v-if="dataLoaded && nodeDatabaseReader" class="db-container">
    <generic-node-settings v-model="nodeDatabaseReader">
      
      <div class="listbox-wrapper">
        <div class="form-group">
  <label>Connection Mode</label>
  <el-radio-group v-model="nodeDatabaseReader.database_settings.connection_mode">
    <el-radio 
      v-for="option in connectionModeOptions" 
      :key="option" 
      :label="option"
    >
      {{ option }}
    </el-radio>
  </el-radio-group>
</div>
<div>
        <div  v-if="nodeDatabaseReader.database_settings.connection_mode == 'inline' && nodeDatabaseReader.database_settings.database_connection" >
        <database-connection-settings
          v-model="nodeDatabaseReader.database_settings.database_connection"
          @change="resetFields"
        />
      </div>

      
      <div v-else>
        <div v-if="connectionsAreLoading">
          <div class="loading-spinner"></div>
          <p>Loading connections...</p>
        </div>
        <div v-else>
          <select 
      id="connection-select" 
      v-model="nodeDatabaseReader.database_settings.database_connection_name" 
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
        
        <div class="listbox-wrapper">
          <!-- Query Mode Toggle -->
          <div class="form-group">
            <label for="query-mode">Query Mode</label>
            <select
              id="query-mode"
              v-model="nodeDatabaseReader.database_settings.query_mode"
              class="form-control"
              @change="handleQueryModeChange"
            >
              <option value="table">Table</option>
              <option value="query">Query</option>
            </select>
          </div>

          <!-- Table Mode Fields -->
          <div
            v-if="nodeDatabaseReader.database_settings.query_mode === 'table'"
            class="query-section"
          >
            <h4 class="section-subtitle">Table Selection</h4>
            <div class="form-row">
              <div class="form-group half">
                <label for="schema-name">Schema</label>
                <input
                  id="schema-name"
                  v-model="nodeDatabaseReader.database_settings.schema_name"
                  type="text"
                  class="form-control"
                  placeholder="Enter schema name"
                  @input="resetFields"
                />
              </div>

              <div class="form-group half">
                <label for="table-name">Table</label>
                <input
                  id="table-name"
                  v-model="nodeDatabaseReader.database_settings.table_name"
                  type="text"
                  class="form-control"
                  placeholder="Enter table name"
                  @input="resetFields"
                />
              </div>
            </div>
          </div>

          <!-- SQL Query Component -->
          <sql-query-component
            v-if="nodeDatabaseReader.database_settings.query_mode === 'query'"
            v-model="nodeDatabaseReader.database_settings.query"
            @validate="validateQuery"
            @input="resetFields"
          />

          <!-- Validation Section -->
          <div class="validation-section">
            <button
              class="validate-button"
              :disabled="isValidating"
              @click="validateDatabaseSettings"
            >
              {{ isValidating ? "Validating..." : "Validate Settings" }}
            </button>

            <!-- Error Message Box -->
            <div v-if="validationError" class="error-box">
              <div class="error-title">Validation Error</div>
              <div class="error-message">{{ validationError }}</div>
            </div>

            <!-- Success Message Box -->
            <div v-if="validationSuccess" class="success-box">
              <div class="success-message">{{ validationSuccess }}</div>
            </div>
          </div>
        </div>
    </generic-node-settings>
  </div>
  <code-loader v-else />
</template>

<script lang="ts" setup>
import { CodeLoader } from "vue-content-loader";
import { ref, onMounted } from "vue";
import { NodeDatabaseReader, ConnectionModeOption } from "../../../baseNode/nodeInput";
import { createNodeDatabaseReader } from "./utils";
import { useNodeStore } from "../../../../../stores/column-store";
import { fetchDatabaseConnectionsInterfaces } from '../../../../../pages/databaseManager/api'
import { FullDatabaseConnectionInterface } from '../../../../../pages/databaseManager/databaseConnectionTypes'
import { ElMessage, ElRadio } from 'element-plus'
import DatabaseConnectionSettings from "./DatabaseConnectionSettings.vue";
import SqlQueryComponent from "./SQLQueryComponent.vue";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import axios from "axios";

interface Props {
  nodeId: number;
}

const connectionModeOptions = ref<ConnectionModeOption[]>(['inline', 'reference'])
const connectionInterfaces = ref<FullDatabaseConnectionInterface[]>([])
const props = defineProps<Props>();
const nodeStore = useNodeStore();
const nodeDatabaseReader = ref<null | NodeDatabaseReader>(null);
const dataLoaded = ref(false);
const validationError = ref<string | null>(null);
const validationSuccess = ref<string | null>(null);
const isValidating = ref(false);
const connectionsAreLoading = ref(false);

const handleQueryModeChange = (event: Event) => {
  const target = event.target as HTMLSelectElement;
  const selectedMode = target.value;

  validationError.value = null;
  validationSuccess.value = null;

  if (nodeDatabaseReader.value) {
    nodeDatabaseReader.value.fields = [];

    if (selectedMode === "table") {
      nodeDatabaseReader.value.database_settings.query = "";
    } else {
      nodeDatabaseReader.value.database_settings.schema_name = undefined;
      nodeDatabaseReader.value.database_settings.table_name = undefined;
    }
  }
};

const loadNodeData = async (nodeId: number) => {
  try {
    const nodeData = await nodeStore.getNodeData(nodeId, false);
    if (nodeData) {
      const hasValidSetup = Boolean(nodeData.setting_input?.is_setup);
      nodeDatabaseReader.value = hasValidSetup
        ? nodeData.setting_input
        : createNodeDatabaseReader(nodeStore.flow_id, nodeId);
    }
    dataLoaded.value = true;
  } catch (error) {
    console.error("Error loading node data:", error);
    dataLoaded.value = false;
  }
};

const validateQuery = () => {
  if (!nodeDatabaseReader.value?.database_settings?.query) {
    validationError.value = "Please enter a SQL query";
    validationSuccess.value = null;
    return;
  }

  // Now we'll use the API endpoint instead of just showing an alert
  validateDatabaseSettings();
};

const resetFields = () => {
  if (nodeDatabaseReader.value) {
    nodeDatabaseReader.value.fields = [];
  }
};

const validateDatabaseSettings = async () => {
  if (!nodeDatabaseReader.value?.database_settings) {
    validationError.value = "Database settings are incomplete";
    validationSuccess.value = null;
    return;
  }
  validationError.value = null;
  validationSuccess.value = null;
  isValidating.value = true;

  resetFields();

  try {
    const response = await axios.post(
      "/validate_db_settings",
      nodeDatabaseReader.value.database_settings,
    );

    // If we get here, validation was successful
    validationSuccess.value = response.data.message || "Settings are valid";
  } catch (error: any) {
    // Handle validation errors
    if (error.response && error.response.data && error.response.data.detail) {
      validationError.value = error.response.data.detail;
    } else {
      validationError.value = "An error occurred during validation";
      console.error("Validation error:", error);
    }
  } finally {
    isValidating.value = false;
  }
};

const pushNodeData = async () => {
  if (!nodeDatabaseReader.value || !nodeDatabaseReader.value.database_settings) {
    return;
  }
  nodeDatabaseReader.value.is_setup = true;
  nodeStore.updateSettings(nodeDatabaseReader);
  dataLoaded.value = false;
};

const fetchConnections = async () => {
  connectionsAreLoading.value = true
  try {
    connectionInterfaces.value = await fetchDatabaseConnectionsInterfaces()
  } catch (error) {
    console.error("Error fetching connections:", error)
    ElMessage.error('Failed to load database connections')
  } finally {
    connectionsAreLoading.value = false
  }
}


onMounted(async () => {
  await fetchConnections()
})


defineExpose({
  loadNodeData,
  pushNodeData,
});
</script>

<style scoped>
.db-container {
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
  max-width: 100%;
  color: #333;
}

.section-subtitle {
  margin: 0 0 0.75rem 0;
  font-size: 0.95rem;
  font-weight: 600;
  color: #4a5568;
}

.query-section {
  margin-top: 1rem;
  padding-top: 1rem;
  border-top: 1px solid #edf2f7;
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
  border: 1px solid #e2e8f0;
  border-radius: 4px;
  font-size: 0.875rem;
  box-sizing: border-box;
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
  color: #4a5568;
}

.form-control {
  width: 100%;
  padding: 0.5rem;
  border: 1px solid #e2e8f0;
  border-radius: 4px;
  font-size: 0.875rem;
}


/* Validation section styling */
.validation-section {
  margin-top: 1.5rem;
  padding-top: 1rem;
  border-top: 1px solid #edf2f7;
}

.validate-button {
  background-color: #4299e1;
  color: white;
  border: none;
  border-radius: 4px;
  padding: 0.5rem 1rem;
  font-size: 0.875rem;
  font-weight: 500;
  cursor: pointer;
  transition: background-color 0.2s;
}

.validate-button:hover {
  background-color: #3182ce;
}

.validate-button:disabled {
  background-color: #a0aec0;
  cursor: not-allowed;
}

.error-box {
  margin-top: 1rem;
  padding: 0.75rem;
  border-radius: 4px;
  background-color: #fff5f5;
  border: 1px solid #fed7d7;
}

.error-title {
  color: #e53e3e;
  font-weight: 600;
  margin-bottom: 0.25rem;
}

.error-message {
  color: #c53030;
  font-size: 0.875rem;
  white-space: pre-wrap;
  overflow-wrap: break-word;
}

.success-box {
  margin-top: 1rem;
  padding: 0.75rem;
  border-radius: 4px;
  background-color: #f0fff4;
  border: 1px solid #c6f6d5;
}

.success-message {
  color: #38a169;
  font-size: 0.875rem;
}

@media (max-width: 640px) {
  .form-row {
    flex-direction: column;
    gap: 0.5rem;
  }
}

.el-radio-group {
  display: flex !important;
  flex-direction: row !important;
  gap: 1rem;
}

.el-radio {
  margin-right: 0 !important; /* Override any existing margins */
}


</style>
