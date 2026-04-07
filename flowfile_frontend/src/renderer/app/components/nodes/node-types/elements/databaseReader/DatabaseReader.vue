// DatabaseReaderNode.vue
<template>
  <div v-if="dataLoaded && nodeDatabaseReader" class="db-container">
    <generic-node-settings
      v-model="nodeDatabaseReader"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="form-group">
          <label>Connection Mode</label>
          <el-radio-group v-model="nodeDatabaseReader.database_settings.connection_mode">
            <el-radio v-for="option in connectionModeOptions" :key="option" :label="option">
              {{ option }}
            </el-radio>
          </el-radio-group>
        </div>
        <div>
          <div
            v-if="
              nodeDatabaseReader.database_settings.connection_mode == 'inline' &&
              nodeDatabaseReader.database_settings.database_connection
            "
          >
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
              <div class="input-with-fetch">
                <el-select
                  ref="schemaSelectRef"
                  v-model="nodeDatabaseReader.database_settings.schema_name"
                  filterable
                  allow-create
                  clearable
                  default-first-option
                  :placeholder="schemasAreLoading ? 'Loading...' : 'Schema'"
                  class="flex-input"
                  @change="handleSchemaChange"
                  @blur="handleSchemaBlur"
                >
                  <el-option v-for="s in availableSchemas" :key="s" :label="s" :value="s" />
                </el-select>
                <button
                  type="button"
                  class="btn-fetch-icon"
                  :disabled="schemasAreLoading"
                  title="Refresh schemas"
                  @click="handleFetchSchemas()"
                >
                  <i v-if="schemasAreLoading" class="fa-solid fa-spinner fa-spin"></i>
                  <i v-else class="fa-solid fa-refresh"></i>
                </button>
              </div>
            </div>

            <div class="form-group half">
              <label for="table-name">Table</label>
              <div class="input-with-fetch">
                <el-select
                  ref="tableSelectRef"
                  v-model="nodeDatabaseReader.database_settings.table_name"
                  filterable
                  allow-create
                  clearable
                  default-first-option
                  :placeholder="tablesAreLoading ? 'Loading...' : 'Table'"
                  class="flex-input"
                  @change="handleTableSelect"
                  @blur="handleTableBlur"
                >
                  <el-option v-for="t in availableTables" :key="t" :label="t" :value="t" />
                </el-select>
                <button
                  type="button"
                  class="btn-fetch-icon"
                  :disabled="tablesAreLoading"
                  title="Refresh tables"
                  @click="handleFetchTables()"
                >
                  <i v-if="tablesAreLoading" class="fa-solid fa-spinner fa-spin"></i>
                  <i v-else class="fa-solid fa-refresh"></i>
                </button>
              </div>
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
import { ref, onMounted, watch } from "vue";
import { NodeDatabaseReader, ConnectionModeOption, DatabaseConnection } from "../../../baseNode/nodeInput";
import { createNodeDatabaseReader } from "./utils";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { fetchDatabaseConnectionsInterfaces } from "../../../../../views/DatabaseView/api";
import { FullDatabaseConnectionInterface} from "../../../../../views/DatabaseView/databaseConnectionTypes";
import { ElMessage, ElOption, ElRadio, ElSelect } from "element-plus";
import DatabaseConnectionSettings from "./DatabaseConnectionSettings.vue";
import SqlQueryComponent from "./SQLQueryComponent.vue";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import { fetchDbSchemas, fetchDbTables } from "./api";
import axios from "axios";

interface Props {
  nodeId: number;
}

const connectionModeOptions = ref<ConnectionModeOption[]>(["inline", "reference"]);
const connectionInterfaces = ref<FullDatabaseConnectionInterface[]>([]);
defineProps<Props>();
const nodeStore = useNodeStore();
const nodeDatabaseReader = ref<null | NodeDatabaseReader>(null);
const dataLoaded = ref(false);

const defaultDatabaseConnection: DatabaseConnection = {
  database_type: "postgresql",
  username: "",
  password_ref: "",
  host: "localhost",
  port: 5432,
  database: "",
  url: undefined,
};

// Use the standardized node settings composable
const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeDatabaseReader,
  onBeforeSave: () => {
    if (!nodeDatabaseReader.value || !nodeDatabaseReader.value.database_settings) {
      return false;
    }
    // Clean up settings based on connection_mode before saving
    if (nodeDatabaseReader.value.database_settings.connection_mode === "reference") {
      nodeDatabaseReader.value.database_settings.database_connection = undefined;
    } else {
      nodeDatabaseReader.value.database_settings.database_connection_name = undefined;
    }
    return true;
  },
});
const validationError = ref<string | null>(null);
const validationSuccess = ref<string | null>(null);
const isValidating = ref(false);
const connectionsAreLoading = ref(false);
const availableSchemas = ref<string[]>([]);
const schemasAreLoading = ref(false);
const availableTables = ref<string[]>([]);
const tablesAreLoading = ref(false);
const schemaSelectRef = ref();
const tableSelectRef = ref();

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
      dataLoaded.value = true;
    }
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
  availableSchemas.value = [];
  availableTables.value = [];
};

const handleSchemaChange = () => {
  availableTables.value = [];
  if (nodeDatabaseReader.value) {
    nodeDatabaseReader.value.fields = [];
  }
};

// Commit typed text on blur so custom values aren't discarded by el-select
const handleSchemaBlur = () => {
  if (!nodeDatabaseReader.value) return;
  const inputEl = schemaSelectRef.value?.$el?.querySelector("input");
  if (inputEl?.value) {
    nodeDatabaseReader.value.database_settings.schema_name = inputEl.value;
  }
};

const handleTableBlur = () => {
  if (!nodeDatabaseReader.value) return;
  const inputEl = tableSelectRef.value?.$el?.querySelector("input");
  if (inputEl?.value) {
    nodeDatabaseReader.value.database_settings.table_name = inputEl.value;
  }
};

const handleFetchSchemas = async (silent = false) => {
  if (!nodeDatabaseReader.value?.database_settings) return;
  schemasAreLoading.value = true;
  try {
    availableSchemas.value = await fetchDbSchemas(nodeDatabaseReader.value.database_settings);
  } catch (error: any) {
    if (!silent) {
      const detail = error.response?.data?.detail || "Failed to fetch schemas";
      ElMessage.error(detail);
    }
  } finally {
    schemasAreLoading.value = false;
  }
};

const handleFetchTables = async (silent = false) => {
  if (!nodeDatabaseReader.value?.database_settings) return;
  tablesAreLoading.value = true;
  try {
    availableTables.value = await fetchDbTables(nodeDatabaseReader.value.database_settings);
  } catch (error: any) {
    if (!silent) {
      const detail = error.response?.data?.detail || "Failed to fetch tables";
      ElMessage.error(detail);
    }
  } finally {
    tablesAreLoading.value = false;
  }
};

const handleTableSelect = (value: string) => {
  if (!nodeDatabaseReader.value) return;
  // When no schema was selected, tables come as "schema.table" — split them
  if (value && value.includes(".") && !nodeDatabaseReader.value.database_settings.schema_name) {
    const dotIndex = value.indexOf(".");
    nodeDatabaseReader.value.database_settings.schema_name = value.substring(0, dotIndex);
    nodeDatabaseReader.value.database_settings.table_name = value.substring(dotIndex + 1);
  }
  if (nodeDatabaseReader.value) {
    nodeDatabaseReader.value.fields = [];
  }
};

// Auto-fetch schemas/tables when connection details change.
// Reference mode: fetch immediately on selection.
// Inline mode: debounce so we don't fire on every keystroke, and only when the
// connection looks complete (all required fields filled).
const isInlineConnectionComplete = () => {
  const conn = nodeDatabaseReader.value?.database_settings?.database_connection;
  if (!conn) return false;
  if (conn.database_type === "sqlite") return !!conn.database;
  return !!(conn.host && conn.port && conn.database && conn.username && conn.password_ref);
};

let inlineDebounceTimer: ReturnType<typeof setTimeout> | null = null;

watch(
  () => nodeDatabaseReader.value?.database_settings?.database_connection_name,
  (name) => {
    if (name) {
      handleFetchSchemas(true);
      handleFetchTables(true);
    }
  },
);

watch(
  () => nodeDatabaseReader.value?.database_settings?.database_connection,
  () => {
    if (inlineDebounceTimer) clearTimeout(inlineDebounceTimer);
    if (!isInlineConnectionComplete()) return;
    inlineDebounceTimer = setTimeout(() => {
      handleFetchSchemas(true);
      handleFetchTables(true);
    }, 1500);
  },
  { deep: true },
);

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
    // Clean up settings based on connection_mode before sending
    const settings = { ...nodeDatabaseReader.value.database_settings };
    if (settings.connection_mode === "reference") {
      settings.database_connection = undefined;
    } else {
      settings.database_connection_name = undefined;
    }

    const response = await axios.post("/validate_db_settings", settings);

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

.query-section {
  margin-top: 1rem;
  padding-top: 1rem;
  border-top: 1px solid var(--color-border-light);
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

/* Validation section styling */
.validation-section {
  margin-top: 1.5rem;
  padding-top: 1rem;
  border-top: 1px solid var(--color-border-light);
}

.validate-button {
  background-color: var(--color-info);
  color: var(--color-text-inverse);
  border: none;
  border-radius: 4px;
  padding: 0.5rem 1rem;
  font-size: 0.875rem;
  font-weight: 500;
  cursor: pointer;
  transition: background-color 0.2s;
}

.validate-button:hover {
  background-color: var(--color-info-hover);
}

.validate-button:disabled {
  background-color: var(--color-text-muted);
  cursor: not-allowed;
}

.error-box {
  margin-top: 1rem;
  padding: 0.75rem;
  border-radius: 4px;
  background-color: var(--color-danger-light);
  border: 1px solid var(--color-danger);
}

.error-title {
  color: var(--color-danger);
  font-weight: 600;
  margin-bottom: 0.25rem;
}

.error-message {
  color: var(--color-danger-hover);
  font-size: 0.875rem;
  white-space: pre-wrap;
  overflow-wrap: break-word;
}

.success-box {
  margin-top: 1rem;
  padding: 0.75rem;
  border-radius: 4px;
  background-color: var(--color-success-light);
  border: 1px solid var(--color-success);
}

.success-message {
  color: var(--color-success);
  font-size: 0.875rem;
}

.input-with-fetch {
  display: flex;
  gap: 0.5rem;
  align-items: center;
}

.input-with-fetch .form-control,
.input-with-fetch .flex-input {
  flex: 1;
  min-width: 0;
}

.input-with-fetch :deep(.el-input__wrapper) {
  box-sizing: border-box;
}

.btn-fetch-icon {
  flex-shrink: 0;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  color: var(--color-text-secondary);
  cursor: pointer;
  transition:
    color 0.2s,
    border-color 0.2s,
    background-color 0.2s;
  font-size: 0.8rem;
  padding: 0;
}

.btn-fetch-icon:hover {
  color: var(--color-info);
  border-color: var(--color-info);
  background-color: var(--color-gray-50);
}

.btn-fetch-icon:disabled {
  color: var(--color-text-muted);
  border-color: var(--color-border-light);
  background-color: var(--color-background-primary);
  cursor: not-allowed;
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
