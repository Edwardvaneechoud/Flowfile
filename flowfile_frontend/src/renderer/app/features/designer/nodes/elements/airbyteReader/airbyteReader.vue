<template>
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
      title="Data type"
      :allow-other="false"
      @change="selectConnector"
    />
  </div>
  <div class="listbox-wrapper">
    <div class="listbox-subtitle">
      <span>Config settings</span>
    </div>
    <button
      v-if="connectorSelected"
      class="file-upload-label"
      @click="getConfig"
    >
      Load settings
      <span class="material-icons file-icon">refresh</span>
    </button>
    <div v-else-if="sourceSelected" class="listbox-wrapper">
      <div v-if="airbyteConfig?.parsed_config" class="listbox">
        <AirbyteForm
          ref="airbyteForm"
          :parsed-config="airbyteConfig.parsed_config"
        ></AirbyteForm>
        <button @click="updateConfig">Get availalbe streams</button>
      </div>
      <CodeLoader v-else />
    </div>
    <div
      v-if="
        airbyteConfigTemplate &&
        airbyteConfigTemplate.available_streams &&
        airbyteConfig
      "
      class="listbox-wrapper"
    >
      <div class="listbox-subtitle">
        <span>Select stream</span>
      </div>
    </div>
    <el-select
      v-if="
        airbyteConfigTemplate &&
        airbyteConfigTemplate.available_streams &&
        airbyteConfig
      "
      v-model="airbyteConfig.selected_stream"
      placeholder="Select a connector"
      size="small"
    >
      <el-option
        v-for="connector in airbyteConfigTemplate.available_streams"
        :key="connector"
        :label="connector"
        :value="connector"
      />
    </el-select>
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
} from "./utils";
import AirbyteForm from "./airbyteConfigForm.vue";
import {
  SchemaProperty,
  AirbyteConfig,
  AirbyteConfigTemplate,
  NodeExternalSource,
} from "./types";
import { useNodeStore } from "../../../../../stores/column-store";
import { CodeLoader } from "vue-content-loader";
import DropDownGeneric from "../../../baseNode/page_objects/dropDownGeneric.vue";

defineProps<{ nodeId?: number }>();

const nodeStore = useNodeStore();
const nodeExternalSource = ref<null | NodeExternalSource>(null);
const sourceSelected = ref<boolean>(false);
const availableConnectors = ref<string[]>([]);
const connectorSelected = ref<boolean>(false);
const initialSelectedStream = ref<string | null>(null);
const getConnectors = async () => {
  availableConnectors.value = await getAirbyteConnectors();
};
const inputValues = ref<Record<string, SchemaProperty> | null>(null);
const selectedConnector = ref<string>("");
getConnectors();

const airbyteConfigTemplate = ref<AirbyteConfigTemplate | null>(null);
const airbyteConfig = ref<AirbyteConfig | null>(null);

const loadNodeData = async (nodeId: number) => {
  const nodeResult = await nodeStore.getNodeData(1, nodeId, false);
  nodeExternalSource.value = nodeResult?.setting_input;
  if (
    nodeExternalSource.value?.is_setup &&
    nodeExternalSource.value.source_settings &&
    nodeExternalSource.value.source_settings
  ) {
    airbyteConfig.value = nodeExternalSource.value.source_settings;
    selectedConnector.value = airbyteConfig.value.source_name;
    sourceSelected.value = true;
    initialSelectedStream.value = airbyteConfig.value.selected_stream;

    // Fetch and restore the configuration
    const connectorInputData = await getAirbyteConnectorTemplate(
      selectedConnector.value,
    );
    if (connectorInputData) {
      airbyteConfigTemplate.value = connectorInputData;
      // Preserve the existing parsed_config instead of computing a new one
      if (!airbyteConfig.value.parsed_config) {
        airbyteConfig.value.parsed_config = computeSchema(
          airbyteConfigTemplate.value.config_spec,
        );
      }

      // If we have streams available, restore them
      if (connectorInputData.available_streams) {
        airbyteConfigTemplate.value.available_streams =
          connectorInputData.available_streams;
      } else if (airbyteConfig.value.parsed_config) {
        // If no streams are available, try to fetch them
        inputValues.value = getConfigSettings(
          airbyteConfig.value.parsed_config,
        );
        airbyteConfig.value.mapped_config_spec = inputValues.value;
        await setAirbyteConfigGetStreams(airbyteConfig.value);
        airbyteConfigTemplate.value = await getAirbyteConnectorTemplate(
          selectedConnector.value,
        );
      }
    }
  }
};

const selectConnector = () => {
  // Only reset if selecting a different connector
  if (airbyteConfig.value?.source_name !== selectedConnector.value) {
    sourceSelected.value = false;
    connectorSelected.value = true;
    airbyteConfigTemplate.value = null;
    airbyteConfig.value = null;
  }
};

const getConfig = async () => {
  connectorSelected.value = false;
  sourceSelected.value = true;
  const connectorInputData = await getAirbyteConnectorTemplate(
    selectedConnector.value,
  );
  if (connectorInputData) {
    airbyteConfigTemplate.value = connectorInputData;
    let parsed_config = computeSchema(airbyteConfigTemplate.value.config_spec);
    airbyteConfig.value = {
      parsed_config: parsed_config,
      mapped_config_spec: {},
      config_mode: "in_line",
      selected_stream: "",
      source_name: selectedConnector.value,
    };
  }
};

const updateConfig = async () => {
  if (
    !airbyteConfigTemplate.value?.available_streams &&
    airbyteConfig.value &&
    airbyteConfig.value.parsed_config
  ) {
    inputValues.value = getConfigSettings(airbyteConfig.value.parsed_config);
    airbyteConfig.value.mapped_config_spec = inputValues.value;
    await setAirbyteConfigGetStreams(airbyteConfig.value);
    airbyteConfigTemplate.value = await getAirbyteConnectorTemplate(
      selectedConnector.value,
    );
  }
};

const pushNodeData = async () => {
  if (nodeExternalSource.value && airbyteConfig.value) {
    nodeExternalSource.value.is_setup = true;
    nodeExternalSource.value.source_settings = airbyteConfig.value;
    nodeExternalSource.value.source_settings.mapped_config_spec =
      getConfigSettings(airbyteConfig.value.parsed_config);
    if (initialSelectedStream.value != airbyteConfig.value.selected_stream) {
      nodeExternalSource.value.source_settings.fields = [];
    }
  }
  console.log(nodeExternalSource.value);
  await nodeStore.updateSettings(nodeExternalSource);
  if (nodeExternalSource.value) {
    await nodeStore.getNodeData(
      1,
      Number(nodeExternalSource.value.node_id),
      false,
    );
  }
  console.log("pushed");
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
  font-size: 20px; /* Slightly larger icon for better visual balance */
  width: 24px; /* or any size that fits your design */
  height: auto;
}

button {
  display: inline-flex; /* Aligns text and icon */
  align-items: center; /* Centers items vertically */
  justify-content: center; /* Centers items horizontally */
  cursor: pointer;
  padding: 4px;
  background-color: #7878ff5b;
  border: none;
  border-radius: 4px;
  font-size: 13px;
  transition: background-color 0.3s ease; /* Smooth transition for hover effect */
}

button:hover {
  background-color: #b3b5ba;
}

button .material-icons {
  margin-right: 8px; /* Space between icon and text */
  font-size: 20px; /* Icon size */
}

/* Specific overrides for el-select */
.el-select-dropdown .el-select-dropdown__item.selected {
  background-color: #f5f5f5 !important;
  color: #000000;
}
.dropdown-wrapper {
  position: relative; /* or absolute, depending on your layout */
  z-index: 1; /* make sure it is above the parent element */
}

.dropdown-wrapper .options-list {
  position: absolute;
  top: 100%;
  left: 0;
  z-index: 1;
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
