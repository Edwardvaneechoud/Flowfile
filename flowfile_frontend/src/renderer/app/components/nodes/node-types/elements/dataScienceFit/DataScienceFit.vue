<template>
  <div v-if="dataLoaded && nodeFit" class="listbox-wrapper">
    <generic-node-settings
      :model-value="nodeFit"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Estimator</div>
        <el-select
          v-model="nodeFit.data_science_fit_input.kind"
          size="small"
          style="width: 100%"
        >
          <el-option
            v-for="opt in kindOptions"
            :key="opt.value"
            :label="opt.label"
            :value="opt.value"
          />
        </el-select>

        <div class="listbox-subtitle" style="margin-top: 12px">Artefact name</div>
        <el-input
          v-model="nodeFit.data_science_fit_input.artefact_name"
          size="small"
          placeholder="e.g. churn_lr_v1"
        />

        <div class="listbox-subtitle" style="margin-top: 12px">Feature columns</div>
        <ColumnListSelector
          v-model="nodeFit.data_science_fit_input.feature_cols"
          :schema="tableSchema"
          data-type-filter="Numeric"
          placeholder="Select numeric feature columns"
        />

        <div class="listbox-subtitle" style="margin-top: 12px">Target column</div>
        <DropDownGeneric
          v-model="targetColInput"
          :option-list="numericColumnNames"
          :allow-other="false"
          placeholder="Select the target column"
          @change="syncTargetCol"
        />

        <div class="listbox-subtitle" style="margin-top: 12px">Prediction column</div>
        <el-input
          v-model="nodeFit.data_science_fit_input.prediction_col"
          size="small"
          placeholder="prediction"
        />

        <div class="listbox-subtitle" style="margin-top: 12px">
          Hyperparameters (JSON)
        </div>
        <el-input
          v-model="hyperparamsRaw"
          size="small"
          type="textarea"
          :rows="2"
          placeholder="{}"
          @change="syncHyperparams"
        />
        <div v-if="hyperparamsError" class="hp-error">{{ hyperparamsError }}</div>
      </div>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { computed, ref } from "vue";
import { CodeLoader } from "vue-content-loader";
import { ElMessage } from "element-plus";
import type { NodeDataScienceFit } from "@/types/node.types";
import type { NodeData } from "@/components/nodes/baseNode/nodeInterfaces";
import { useNodeStore } from "@/stores/node-store";
import { useNodeSettings } from "@/composables/useNodeSettings";
import GenericNodeSettings from "@/components/nodes/baseNode/genericNodeSettings.vue";
import ColumnListSelector from "@/components/nodes/baseNode/page_objects/columnListSelector.vue";
import DropDownGeneric from "@/components/nodes/baseNode/page_objects/dropDownGeneric.vue";
import { createDataScienceFitNode } from "./utils";

const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const nodeFit = ref<NodeDataScienceFit | null>(null);
const nodeData = ref<NodeData | null>(null);

const targetColInput = ref<string>("");
const hyperparamsRaw = ref<string>("{}");
const hyperparamsError = ref<string | null>(null);

const kindOptions = [
  { value: "linreg", label: "Linear Regression" },
  { value: "ridge", label: "Ridge Regression (coming soon)" },
  { value: "lasso", label: "Lasso Regression (coming soon)" },
  { value: "knn_cls", label: "KNN Classifier (coming soon)" },
  { value: "knn_reg", label: "KNN Regressor (coming soon)" },
  { value: "pca", label: "PCA (coming soon)" },
];

const tableSchema = computed(() => nodeData.value?.main_input?.table_schema ?? []);
const numericColumnNames = computed(() =>
  tableSchema.value.filter((c) => c.data_type_group === "Numeric").map((c) => c.name),
);

const syncTargetCol = () => {
  if (!nodeFit.value) return;
  const trimmed = (targetColInput.value ?? "").trim();
  nodeFit.value.data_science_fit_input.target_col = trimmed === "" ? null : trimmed;
};

const syncHyperparams = () => {
  if (!nodeFit.value) return;
  try {
    nodeFit.value.data_science_fit_input.hyperparams = hyperparamsRaw.value
      ? JSON.parse(hyperparamsRaw.value)
      : {};
    hyperparamsError.value = null;
  } catch {
    hyperparamsError.value = "Invalid JSON";
  }
};

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeFit,
  onBeforeSave: () => {
    if (!nodeFit.value) return false;
    syncTargetCol();
    syncHyperparams();
    if (hyperparamsError.value) {
      ElMessage.error(hyperparamsError.value);
      return false;
    }
    if (!nodeFit.value.data_science_fit_input.artefact_name) {
      ElMessage.error("Artefact name is required");
      return false;
    }
    if (nodeFit.value.data_science_fit_input.feature_cols.length === 0) {
      ElMessage.error("At least one feature column is required");
      return false;
    }
    return true;
  },
});

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  const setting = nodeData.value?.setting_input as NodeDataScienceFit | undefined;
  const hasValidSetup = Boolean(setting?.is_setup && setting?.data_science_fit_input);
  nodeFit.value = hasValidSetup
    ? (setting as NodeDataScienceFit)
    : createDataScienceFitNode(nodeStore.flow_id, nodeStore.node_id);
  targetColInput.value = nodeFit.value.data_science_fit_input.target_col ?? "";
  hyperparamsRaw.value = JSON.stringify(
    nodeFit.value.data_science_fit_input.hyperparams ?? {},
  );
  dataLoaded.value = true;
};

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>

<style scoped>
.hp-error {
  color: #b91c1c;
  font-size: 12px;
  margin-top: 4px;
}
</style>
