<template>
  <div v-if="dataLoaded && nodeEvaluateModel" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeEvaluateModel"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Compare</div>
        <p class="hint">
          Pick the actual target column and the prediction column already on the input data. Connect
          an Apply Model node upstream and keep the target column in its output.
        </p>

        <el-row class="setting-row">
          <el-col :span="10" class="grid-content">Actual column</el-col>
          <el-col :span="14" class="grid-content">
            <el-select
              v-model="nodeEvaluateModel.evaluate_input.actual_column"
              placeholder="Select the true target column"
              filterable
            >
              <el-option v-for="c in inputColumns" :key="c" :label="c" :value="c" />
            </el-select>
          </el-col>
        </el-row>

        <el-row class="setting-row">
          <el-col :span="10" class="grid-content">Predicted column</el-col>
          <el-col :span="14" class="grid-content">
            <el-select
              v-model="nodeEvaluateModel.evaluate_input.predicted_column"
              placeholder="prediction"
              filterable
              allow-create
            >
              <el-option v-for="c in inputColumns" :key="c" :label="c" :value="c" />
            </el-select>
          </el-col>
        </el-row>
      </div>

      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Task type</div>
        <el-row class="setting-row">
          <el-col :span="10" class="grid-content">Task type</el-col>
          <el-col :span="14" class="grid-content">
            <el-select v-model="nodeEvaluateModel.evaluate_input.task_type">
              <el-option label="Auto (from upstream)" value="auto" />
              <el-option label="Regression" value="regression" />
              <el-option label="Classification" value="classification" />
            </el-select>
          </el-col>
        </el-row>

        <p v-if="nodeEvaluateModel.evaluate_input.task_type === 'auto'" class="hint">
          "Auto" reads the task type from a Train Model node upstream — leave the picker empty to
          fall back to regression metrics.
        </p>

        <el-row v-if="nodeEvaluateModel.evaluate_input.task_type === 'auto'" class="setting-row">
          <el-col :span="10" class="grid-content">Upstream train node</el-col>
          <el-col :span="14" class="grid-content">
            <el-select
              v-model="nodeEvaluateModel.evaluate_input.upstream_train_node_id"
              placeholder="Optional — pick a Train Model"
              :loading="loadingUpstream"
              clearable
              filterable
            >
              <el-option
                v-for="opt in upstreamOptions"
                :key="opt.node_id"
                :label="formatUpstreamLabel(opt)"
                :value="opt.node_id"
              />
            </el-select>
          </el-col>
        </el-row>
      </div>

      <div class="info-banner">
        Output is a long-form table with two columns: <code>metric</code> and <code>value</code>.
        Regression emits <code>mae, mse, rmse, r2, mape, n</code>; classification emits
        <code>accuracy, precision, recall, f1, n_correct, n_total</code> (macro-averaged).
      </div>
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed } from "vue";
import axios from "axios";
import type { NodeEvaluateModel, UpstreamTrainModelOption } from "../../../../../types/node.types";
import type { NodeData } from "../../../baseNode/nodeInterfaces";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const nodeEvaluateModel = ref<NodeEvaluateModel | null>(null);
const dataLoaded = ref(false);
const nodeData = ref<NodeData | null>(null);
const upstreamOptions = ref<UpstreamTrainModelOption[]>([]);
const loadingUpstream = ref(false);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeEvaluateModel,
});

// Pulled from the upstream node's example schema so the user gets a column
// dropdown instead of free-form typing. allow-create on the predicted picker
// keeps it usable when the upstream hasn't run yet (no example data).
const inputColumns = computed<string[]>(() => nodeData.value?.main_input?.columns ?? []);

function formatUpstreamLabel(opt: UpstreamTrainModelOption): string {
  const desc = opt.description?.trim();
  const target = opt.target_column ? `target=${opt.target_column}` : "";
  const algo = opt.model_type || "";
  const tail = [algo, target].filter(Boolean).join(", ");
  const head = desc || `Node ${opt.node_id}`;
  return tail ? `${head} (${tail})` : head;
}

async function loadUpstreamTrainNodes(flowId: number, nodeId: number) {
  loadingUpstream.value = true;
  try {
    const resp = await axios.get<UpstreamTrainModelOption[]>(
      `/ml/upstream-train-models?flow_id=${flowId}&node_id=${nodeId}`,
    );
    upstreamOptions.value = resp.data;
  } catch (e) {
    console.error("Failed to load /ml/upstream-train-models", e);
    upstreamOptions.value = [];
  } finally {
    loadingUpstream.value = false;
  }
}

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeEvaluateModel.value = nodeData.value?.setting_input as NodeEvaluateModel;
  const flowId = Number(nodeData.value?.flow_id);
  await loadUpstreamTrainNodes(flowId, nodeId);

  if (nodeEvaluateModel.value) {
    if (!nodeEvaluateModel.value.is_setup || !nodeEvaluateModel.value.evaluate_input) {
      nodeEvaluateModel.value.evaluate_input = {
        actual_column: "",
        predicted_column: "prediction",
        task_type: "auto",
        upstream_train_node_id: null,
      };
    } else {
      // Backfill defaults for older saved settings.
      const ei = nodeEvaluateModel.value.evaluate_input;
      if (!ei.predicted_column) ei.predicted_column = "prediction";
      if (!ei.task_type) ei.task_type = "auto";
      if (ei.upstream_train_node_id === undefined) ei.upstream_train_node_id = null;
    }
    // Drop a stale upstream_train_node_id that no longer matches a real
    // upstream Train Model — same UX guard ApplyModel uses.
    const savedUpstreamId = nodeEvaluateModel.value.evaluate_input.upstream_train_node_id;
    if (
      savedUpstreamId != null &&
      !upstreamOptions.value.some((opt) => opt.node_id === savedUpstreamId)
    ) {
      nodeEvaluateModel.value.evaluate_input.upstream_train_node_id = null;
    }
    dataLoaded.value = true;
  }
};

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>

<style scoped>
.setting-row {
  margin-bottom: var(--spacing-2);
}

.grid-content {
  font-size: var(--font-size-sm);
  align-items: center;
}

.hint {
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  line-height: 1.4;
  margin: var(--spacing-2) 0;
}

.info-banner {
  margin-top: var(--spacing-3);
  padding: var(--spacing-2) var(--spacing-3);
  background-color: var(--color-info-background, #f0f7ff);
  border-left: 3px solid var(--color-info, #1890ff);
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
}

code {
  background: var(--color-background-secondary, #f5f5f5);
  padding: 0 4px;
  border-radius: 3px;
  font-size: 0.9em;
}
</style>
