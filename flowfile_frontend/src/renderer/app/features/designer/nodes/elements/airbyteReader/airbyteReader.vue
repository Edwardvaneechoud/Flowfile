<template>
  <div v-if="nodeExternalSource">
    <generic-node-settings v-model="nodeExternalSource">
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">
          <img src="/images/airbyte.png" alt="Airbyte Icon" class="file-icon" />
          <span>Get data from Airbyte supported source</span>
        </div>
        <div class="attention-notice">
          <span class="warning-icon">⚠️</span>
          <span class="docker-notice">Running Docker instance required</span>
        </div>
      </div>

      <div class="listbox-wrapper to-front">
        <drop-down-generic
          v-model="selectedConnector"
          :option-list="availableConnectors"
          title="Load data from"
          :is-loading="!loadedConnectors"
          :allow-other="false"
          @change="selectConnector"
        />
      </div>

      <div v-if="sourceSelected">
        <div class="listbox-subtitle flex justify-between items-center">
          <div class="flex items-center gap-2">
            <span>Config settings</span>
            <button class="icon-button" @click="isConfigCollapsed = !isConfigCollapsed">
              <span class="material-icons">
                {{ isConfigCollapsed ? "expand_more" : "expand_less" }}
              </span>
            </button>
          </div>
          <div class="flex gap-2">
            <button
              v-if="airbyteConfig?.parsed_config"
              class="secondary-button"
              @click="resetConfig"
            >
              Reset settings
              <span class="material-icons">restart_alt</span>
            </button>
            <button
              v-if="airbyteConfig?.parsed_config"
              class="secondary-button"
              :disabled="isValidating"
              @click="validateConfig"
            >
              {{ isValidating ? "Validating..." : "Validate" }}
              <span class="material-icons" :class="{ spin: isValidating }">check_circle</span>
            </button>
          </div>
        </div>

        <div v-if="validationMessage" :class="['validation-banner', validationStatus]">
          <span class="material-icons">{{
            validationStatus === "success" ? "check_circle" : "warning"
          }}</span>
          {{ validationMessage }}
        </div>

        <div v-if="airbyteConfig?.parsed_config" class="config-section">
          <AirbyteForm
            v-if="!isConfigCollapsed"
            ref="airbyteForm"
            :parsed-config="airbyteConfig.parsed_config"
          />

          <div v-if="!airbyteConfigTemplate?.available_streams" class="stream-section">
            <button
              class="primary-button"
              :disabled="isFetchingStreams"
              @click="fetchAvailableStreams"
            >
              {{ isFetchingStreams ? "Loading streams..." : "Load available streams" }}
              <span class="material-icons" :class="{ spin: isFetchingStreams }">refresh</span>
            </button>
          </div>

          <div v-else class="stream-section">
            <div class="listbox-subtitle">
              <span>Select stream</span>
            </div>
            <el-select
              v-model="airbyteConfig.selected_stream"
              placeholder="Select a stream"
              size="small"
              class="stream-select"
            >
              <el-option
                v-for="stream in airbyteConfigTemplate.available_streams"
                :key="stream"
                :label="stream"
                :value="stream"
              />
            </el-select>
          </div>
        </div>

        <CodeLoader v-else />
      </div>

      <div v-else-if="connectorSelected" class="config-section">
        <button class="file-upload-label" @click="getConfig">
          Load settings
          <span class="material-icons file-icon">refresh</span>
        </button>
      </div>
    </generic-node-settings>
  </div>
</template>

<script setup lang="ts">
import { ref, defineExpose } from "vue";
import {
  getAirbyteConnectors,
  getAirbyteConnectorTemplate,
  computeSchema,
  getConfigSettings,
  setAirbyteConfigGetStreams,
  getAirbyteAvailableConfigs,
} from "./utils";
import AirbyteForm from "./airbyteConfigForm.vue";
import { SchemaProperty, AirbyteConfig, AirbyteConfigTemplate, NodeExternalSource } from "./types";
import { useNodeStore } from "../../../../../stores/column-store";
import { CodeLoader } from "vue-content-loader";
import DropDownGeneric from "../../../baseNode/page_objects/dropDownGeneric.vue";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

defineProps<{ nodeId?: number }>();

const nodeStore = useNodeStore();
const nodeExternalSource = ref<NodeExternalSource | null>(null);
const backupAirbyteConfig = ref<null | AirbyteConfig>(null);
const sourceSelected = ref<boolean>(false);
const availableConnectors = ref<string[]>([]);
const isConfigCollapsed = ref<boolean>(false);
const availableConfigs = ref<string[]>([]);
const loadedConnectors = ref<boolean>(false);
const connectorSelected = ref<boolean>(false);
const initialSelectedStream = ref<string | null>(null);
const inputValues = ref<Record<string, SchemaProperty> | null>(null);
const selectedConnector = ref<string>("");
const isFetchingStreams = ref<boolean>(false);
const isValidating = ref(false);
const validationMessage = ref("");
const validationStatus = ref<"success" | "error">("success");

const airbyteConfigTemplate = ref<AirbyteConfigTemplate | null>(null);
const airbyteConfig = ref<AirbyteConfig | null>(null);

const getConnectors = async () => {
  availableConnectors.value = await getAirbyteConnectors();
  loadedConnectors.value = true;
};

const getAvailableConfigs = async () => {
  availableConfigs.value = await getAirbyteAvailableConfigs();
};

getAvailableConfigs();
getConnectors();

const validateConfig = async () => {
  if (!airbyteConfig.value?.parsed_config) return;

  isValidating.value = true;
  validationMessage.value = "";

  try {
    await validateSelection();
    validationStatus.value = "success";
    validationMessage.value = "Configuration validated successfully";
  } catch (error) {
    validationStatus.value = "error";
    validationMessage.value = error instanceof Error ? error.message : "Validation failed";
  } finally {
    isValidating.value = false;
    setTimeout(() => {
      validationMessage.value = "";
    }, 5000);
  }
};

const resetConfig = async () => {
  if (
    !confirm("Are you sure you want to reset all settings? This cannot be undone.") ||
    !backupAirbyteConfig.value
  )
    return;

  if (!backupAirbyteConfig.value) {
    console.error("Backup config is missing");
    return;
  }

  airbyteConfig.value = { ...backupAirbyteConfig.value };
  selectedConnector.value = backupAirbyteConfig.value.source_name;
  initialSelectedStream.value = backupAirbyteConfig.value.selected_stream;

  const connectorInputData = await getAirbyteConnectorTemplate(selectedConnector.value);
  if (!connectorInputData) return;

  airbyteConfigTemplate.value = connectorInputData;
  sourceSelected.value = true;
};

const fetchAvailableStreams = async () => {
  if (!airbyteConfig.value?.parsed_config) return;

  isFetchingStreams.value = true;
  try {
    inputValues.value = getConfigSettings(airbyteConfig.value.parsed_config);
    airbyteConfig.value.mapped_config_spec = inputValues.value;
    await setAirbyteConfigGetStreams(airbyteConfig.value);
    airbyteConfigTemplate.value = await getAirbyteConnectorTemplate(selectedConnector.value);
  } catch (error) {
    console.error("Error fetching streams:", error);
  } finally {
    isFetchingStreams.value = false;
  }
};

const loadNodeData = async (nodeId: number) => {
  const nodeResult = await nodeStore.getNodeData(nodeId, false);
  nodeExternalSource.value = nodeResult?.setting_input;

  if (!nodeExternalSource.value?.is_setup || !nodeExternalSource.value.source_settings) return;
  airbyteConfig.value = nodeExternalSource.value.source_settings;
  backupAirbyteConfig.value = { ...airbyteConfig.value };

  selectedConnector.value = airbyteConfig.value.source_name;
  sourceSelected.value = true;
  initialSelectedStream.value = airbyteConfig.value.selected_stream;

  const connectorInputData = await getAirbyteConnectorTemplate(selectedConnector.value);
  if (!connectorInputData) return;

  airbyteConfigTemplate.value = connectorInputData;

  if (!airbyteConfig.value.parsed_config) {
    airbyteConfig.value.parsed_config = computeSchema(airbyteConfigTemplate.value.config_spec);
  }

  if (!connectorInputData.available_streams && airbyteConfig.value.parsed_config) {
    await fetchAvailableStreams();
  }
};

const selectConnector = () => {
  if (airbyteConfig.value?.source_name === selectedConnector.value) return;

  if (availableConfigs.value.includes("source-" + selectedConnector.value)) {
    getConfig();
    return;
  }

  sourceSelected.value = false;
  connectorSelected.value = true;
  airbyteConfigTemplate.value = null;
  airbyteConfig.value = null;
};

const getConfig = async () => {
  connectorSelected.value = false;
  sourceSelected.value = true;

  const connectorInputData = await getAirbyteConnectorTemplate(selectedConnector.value);
  if (!connectorInputData) return;

  airbyteConfigTemplate.value = connectorInputData;
  const parsed_config = computeSchema(airbyteConfigTemplate.value.config_spec);

  airbyteConfig.value = {
    parsed_config,
    mapped_config_spec: {},
    config_mode: "in_line",
    selected_stream: "",
    source_name: selectedConnector.value,
  };
};

const validateSelection = async () => {
  if (!nodeExternalSource.value || !airbyteConfig.value) throw new Error("Invalid configuration");

  nodeExternalSource.value.is_setup = true;
  nodeExternalSource.value.source_settings = airbyteConfig.value;
  nodeExternalSource.value.source_settings.mapped_config_spec = getConfigSettings(
    airbyteConfig.value.parsed_config,
  );

  if (initialSelectedStream.value != airbyteConfig.value.selected_stream) {
    nodeExternalSource.value.source_settings.fields = [];
  }

  await nodeStore.updateSettings(nodeExternalSource);
};

const pushNodeData = async () => {
  if (!nodeExternalSource.value || !airbyteConfig.value) return;

  nodeExternalSource.value.is_setup = true;
  nodeExternalSource.value.source_settings = airbyteConfig.value;
  nodeExternalSource.value.source_settings.mapped_config_spec = getConfigSettings(
    airbyteConfig.value.parsed_config,
  );

  if (initialSelectedStream.value != airbyteConfig.value.selected_stream) {
    nodeExternalSource.value.source_settings.fields = [];
  }

  await nodeStore.updateSettings(nodeExternalSource);
};

defineExpose({
  loadNodeData,
  pushNodeData,
});
</script>

<style scoped>
.to-front {
  z-index: 1000;
}

.config-section {
  margin-top: 16px;
}

.stream-section {
  margin-top: 16px;
}

.stream-select {
  width: 100%;
  max-width: 400px;
}
.icon-button {
  padding: 2px;
  border: none;
  background: none;
  cursor: pointer;
  color: #666;
}

.icon-button:hover {
  color: #333;
}

.primary-button {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  padding: 8px 16px;
  background-color: #7878ff5b;
  border: none;
  border-radius: 4px;
  font-size: 13px;
  transition: background-color 0.3s ease;
}

.primary-button:hover:not(:disabled) {
  background-color: #b3b5ba;
}

.primary-button:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.secondary-button {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 4px 8px;
  background-color: #f1f1f1;
  border: 1px solid #ddd;
  border-radius: 4px;
  font-size: 12px;
  color: #666;
  transition: all 0.2s ease;
}

.secondary-button:hover {
  background-color: #e4e4e4;
  color: #333;
}

.validation-banner {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 12px;
  margin-top: 12px;
  border-radius: 4px;
  font-size: 14px;
}

.validation-banner.success {
  background-color: #ecfdf5;
  color: #047857;
}

.validation-banner.error {
  background-color: #fef2f2;
  color: #dc2626;
}

.spin {
  animation: spin 1s linear infinite;
}

@keyframes spin {
  from {
    transform: rotate(0deg);
  }
  to {
    transform: rotate(360deg);
  }
}

.file-upload-label {
  display: flex;
  align-items: center;
  justify-content: flex-start;
  background-color: #f5f5f5;
  border: 1px solid #ddd;
  border-radius: 4px;
  padding: 10px 15px;
  color: #333;
  font-size: 16px;
  font-weight: 500;
  text-align: left;
  user-select: none;
  cursor: pointer;
  transition: background-color 0.3s ease;
}

.file-upload-label:hover {
  background-color: #e4e4e4;
}

.file-icon {
  margin-right: 10px;
  font-size: 20px;
  width: 24px;
  height: auto;
}

.attention-notice {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 4px 8px;
  border-radius: 4px;
  width: fit-content;
  margin-top: 4px;
}

.docker-notice {
  font-size: 12px;
  font-weight: 600;
}

.warning-icon {
  font-size: 14px;
  animation: pulse 2s infinite;
}

.flex {
  display: flex;
}

.justify-between {
  justify-content: space-between;
}

.items-center {
  align-items: center;
}

.gap-2 {
  gap: 8px;
}

@keyframes pulse {
  0% {
    opacity: 1;
  }
  50% {
    opacity: 0.5;
  }
  100% {
    opacity: 1;
  }
}
</style>
