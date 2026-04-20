<template>
  <div v-if="dataLoaded && nodePredict" class="listbox-wrapper">
    <generic-node-settings
      :model-value="nodePredict"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Artefact name</div>
        <el-input
          v-model="nodePredict.data_science_predict_input.artefact_name"
          size="small"
          placeholder="name of the published artefact"
        />

        <div class="listbox-subtitle" style="margin-top: 12px">Artefact version (optional)</div>
        <el-input
          v-model="versionInput"
          size="small"
          type="number"
          placeholder="leave empty for latest"
          @change="syncVersion"
        />

        <div class="listbox-subtitle" style="margin-top: 12px">Feature columns</div>
        <el-input
          v-model="featureColsRaw"
          size="small"
          placeholder="comma-separated column names"
          @change="syncFeatureCols"
        />

        <div class="listbox-subtitle" style="margin-top: 12px">Prediction column</div>
        <el-input
          v-model="nodePredict.data_science_predict_input.prediction_col"
          size="small"
          placeholder="prediction"
        />
      </div>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref } from "vue";
import { CodeLoader } from "vue-content-loader";
import { ElMessage } from "element-plus";
import type { NodeDataSciencePredict } from "@/types/node.types";
import type { NodeData } from "@/components/nodes/baseNode/nodeInterfaces";
import { useNodeStore } from "@/stores/node-store";
import { useNodeSettings } from "@/composables/useNodeSettings";
import GenericNodeSettings from "@/components/nodes/baseNode/genericNodeSettings.vue";
import { createDataSciencePredictNode } from "./utils";

const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const nodePredict = ref<NodeDataSciencePredict | null>(null);
const nodeData = ref<NodeData | null>(null);

const featureColsRaw = ref("");
const versionInput = ref("");

const syncFeatureCols = () => {
  if (!nodePredict.value) return;
  nodePredict.value.data_science_predict_input.feature_cols = featureColsRaw.value
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
};

const syncVersion = () => {
  if (!nodePredict.value) return;
  const trimmed = versionInput.value.trim();
  if (trimmed === "") {
    nodePredict.value.data_science_predict_input.artefact_version = null;
  } else {
    const n = Number(trimmed);
    nodePredict.value.data_science_predict_input.artefact_version = Number.isNaN(n) ? null : n;
  }
};

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodePredict,
  onBeforeSave: () => {
    if (!nodePredict.value) return false;
    syncFeatureCols();
    syncVersion();
    if (!nodePredict.value.data_science_predict_input.artefact_name) {
      ElMessage.error("Artefact name is required");
      return false;
    }
    if (nodePredict.value.data_science_predict_input.feature_cols.length === 0) {
      ElMessage.error("At least one feature column is required");
      return false;
    }
    return true;
  },
});

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  const setting = nodeData.value?.setting_input as NodeDataSciencePredict | undefined;
  const hasValidSetup = Boolean(setting?.is_setup && setting?.data_science_predict_input);
  nodePredict.value = hasValidSetup
    ? (setting as NodeDataSciencePredict)
    : createDataSciencePredictNode(nodeStore.flow_id, nodeStore.node_id);
  featureColsRaw.value = nodePredict.value.data_science_predict_input.feature_cols.join(", ");
  const v = nodePredict.value.data_science_predict_input.artefact_version;
  versionInput.value = v === null || v === undefined ? "" : String(v);
  dataLoaded.value = true;
};

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>
