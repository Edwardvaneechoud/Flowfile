<template>
  <div v-if="dataLoaded && nodeFit" class="listbox-wrapper">
    <generic-node-settings
      :model-value="nodeFit"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Artefact name</div>
        <el-input
          v-model="nodeFit.linear_regression_fit_input.artefact_name"
          size="small"
          placeholder="e.g. churn_lr_v1"
        />

        <div class="listbox-subtitle" style="margin-top: 12px">Feature columns</div>
        <ColumnListSelector
          v-model="nodeFit.linear_regression_fit_input.feature_cols"
          :schema="tableSchema"
          data-type-filter="Numeric"
          placeholder="Select numeric feature columns"
        />

        <div class="listbox-subtitle" style="margin-top: 12px">Target column</div>
        <DropDownGeneric
          v-model="targetColInput"
          :option-list="candidateTargetColumns"
          :allow-other="false"
          placeholder="Select the target (must be numeric)"
          @change="syncTargetCol"
        />

        <div class="listbox-subtitle" style="margin-top: 12px">Prediction column</div>
        <el-input
          v-model="nodeFit.linear_regression_fit_input.prediction_col"
          size="small"
          placeholder="prediction"
        />

        <div class="listbox-subtitle" style="margin-top: 12px">Fit options</div>

        <div class="option-row">
          <label class="option-label">Fit intercept</label>
          <el-switch v-model="nodeFit.linear_regression_fit_input.fit_intercept" />
        </div>

        <div class="option-row">
          <label class="option-label">Null handling</label>
          <el-select
            v-model="nodeFit.linear_regression_fit_input.null_policy"
            size="small"
            style="flex: 1"
          >
            <el-option
              v-for="p in nullPolicyOptions"
              :key="p.value"
              :label="p.label"
              :value="p.value"
            />
          </el-select>
        </div>

        <div class="option-row">
          <label class="option-label">Solver</label>
          <el-select
            v-model="nodeFit.linear_regression_fit_input.solver"
            size="small"
            style="flex: 1"
          >
            <el-option label="QR (default, stable)" value="qr" />
            <el-option label="Cholesky (fast, well-conditioned)" value="cholesky" />
            <el-option label="SVD (rank-deficient OK)" value="svd" />
          </el-select>
        </div>
      </div>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { computed, ref } from "vue";
import { CodeLoader } from "vue-content-loader";
import { ElMessage } from "element-plus";
import type { NodeLinearRegressionFit } from "@/types/node.types";
import type { NodeData } from "@/components/nodes/baseNode/nodeInterfaces";
import { useNodeStore } from "@/stores/node-store";
import { useNodeSettings } from "@/composables/useNodeSettings";
import GenericNodeSettings from "@/components/nodes/baseNode/genericNodeSettings.vue";
import ColumnListSelector from "@/components/nodes/baseNode/page_objects/columnListSelector.vue";
import DropDownGeneric from "@/components/nodes/baseNode/page_objects/dropDownGeneric.vue";
import { createLinearRegressionFitNode } from "./utils";

const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const nodeFit = ref<NodeLinearRegressionFit | null>(null);
const nodeData = ref<NodeData | null>(null);

const targetColInput = ref<string>("");

const nullPolicyOptions = [
  { value: "skip", label: "Skip rows with null features" },
  { value: "raise", label: "Raise on null features" },
  { value: "zero", label: "Fill nulls with 0" },
  { value: "one", label: "Fill nulls with 1" },
  { value: "ignore", label: "Ignore (no null handling)" },
];

const tableSchema = computed(() => nodeData.value?.main_input?.table_schema ?? []);
const numericColumnNames = computed(() =>
  tableSchema.value.filter((c) => c.data_type_group === "Numeric").map((c) => c.name),
);
const candidateTargetColumns = computed(() => {
  const selected = new Set(nodeFit.value?.linear_regression_fit_input.feature_cols ?? []);
  return numericColumnNames.value.filter((n) => !selected.has(n));
});

const syncTargetCol = () => {
  if (!nodeFit.value) return;
  const trimmed = (targetColInput.value ?? "").trim();
  nodeFit.value.linear_regression_fit_input.target_col = trimmed === "" ? null : trimmed;
};

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeFit,
  onBeforeSave: () => {
    if (!nodeFit.value) return false;
    syncTargetCol();
    const input = nodeFit.value.linear_regression_fit_input;
    if (!input.artefact_name) {
      ElMessage.error("Artefact name is required");
      return false;
    }
    if (input.feature_cols.length === 0) {
      ElMessage.error("At least one feature column is required");
      return false;
    }
    if (!input.target_col) {
      ElMessage.error("Target column is required");
      return false;
    }
    if (input.feature_cols.includes(input.target_col)) {
      ElMessage.error("Target column must not be one of the feature columns");
      return false;
    }
    return true;
  },
});

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  const setting = nodeData.value?.setting_input as NodeLinearRegressionFit | undefined;
  const hasValidSetup = Boolean(setting?.is_setup && setting?.linear_regression_fit_input);
  nodeFit.value = hasValidSetup
    ? (setting as NodeLinearRegressionFit)
    : createLinearRegressionFitNode(nodeStore.flow_id, nodeStore.node_id);
  targetColInput.value = nodeFit.value.linear_regression_fit_input.target_col ?? "";
  dataLoaded.value = true;
};

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>

<style scoped>
.option-row {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-top: 6px;
}
.option-label {
  min-width: 110px;
  font-size: 12px;
  color: var(--color-text-secondary, #6b7280);
}
</style>
