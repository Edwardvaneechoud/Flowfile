<template>
  <div v-if="dataLoaded && nodeRunFlow" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeRunFlow"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="list-wrapper">
        <div class="listbox-subtitle">Flow to run</div>
        <el-select
          v-model="nodeRunFlow.flow_registration_id"
          size="small"
          style="width: 100%"
          placeholder="Select a saved flow"
          filterable
          @change="onFlowSelected"
        >
          <el-option
            v-for="flow in runnableFlows"
            :key="flow.registration_id"
            :label="flow.file_exists ? flow.name : `${flow.name} (file missing)`"
            :value="flow.registration_id"
            :disabled="!flow.file_exists"
          />
        </el-select>
        <div class="hint-text">
          The sub-flow runs once per input row. Its single API Response node defines the output.
        </div>
      </div>

      <div v-if="nodeRunFlow.flow_registration_id" class="list-wrapper">
        <div class="listbox-subtitle">Map parameters to columns</div>
        <div v-if="flowParams.length === 0" class="hint-text">
          The selected flow has no parameters. Add ${name} parameters to the flow to map them here.
        </div>
        <table v-else class="mapping-table">
          <thead>
            <tr>
              <th>Parameter</th>
              <th>Input column</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="param in flowParams" :key="param.name">
              <td class="param-name">${{ param.name }}</td>
              <td>
                <el-select
                  v-model="mappingByParam[param.name]"
                  size="small"
                  style="width: 100%"
                  clearable
                  filterable
                  placeholder="Select column"
                >
                  <el-option
                    v-for="column in inputColumns"
                    :key="column"
                    :label="column"
                    :value="column"
                  />
                </el-select>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="list-wrapper">
        <div class="listbox-subtitle">Execution</div>
        <div class="field-row">
          <span class="field-label">Delay between runs (seconds)</span>
          <el-input-number
            v-model="nodeRunFlow.delay_seconds"
            :min="0"
            :step="0.5"
            size="small"
            controls-position="right"
          />
        </div>
        <div class="field-row">
          <span class="field-label">Max rows</span>
          <el-input-number
            v-model="nodeRunFlow.max_rows"
            :min="1"
            :step="100"
            size="small"
            controls-position="right"
          />
        </div>
        <div class="hint-text">Rows run sequentially. Inputs beyond max rows are skipped.</div>
      </div>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref, computed } from "vue";
import { CodeLoader } from "vue-content-loader";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import type { NodeRunFlow } from "../../../../../types/node.types";
import { FlowApiApi, type RunnableFlow, type FlowParamInfo } from "../../../../../api/flowApi.api";
import { createNodeRunFlow } from "./utils";

const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const nodeRunFlow = ref<null | NodeRunFlow>(null);
const nodeData = ref<null | NodeData>(null);
const runnableFlows = ref<RunnableFlow[]>([]);
const flowParams = ref<FlowParamInfo[]>([]);
const mappingByParam = ref<Record<string, string>>({});

const inputColumns = computed<string[]>(() => nodeData.value?.main_input?.columns ?? []);

const rebuildMappingState = () => {
  const existing = new Map(
    (nodeRunFlow.value?.parameter_mappings ?? []).map((m) => [m.param_name, m.input_column]),
  );
  const next: Record<string, string> = {};
  for (const param of flowParams.value) {
    next[param.name] = existing.get(param.name) ?? "";
  }
  mappingByParam.value = next;
};

const loadFlowParameters = async (registrationId: number) => {
  try {
    flowParams.value = await FlowApiApi.getFlowParameters(registrationId);
  } catch (error) {
    console.error("Failed to load sub-flow parameters:", error);
    flowParams.value = [];
  }
  rebuildMappingState();
};

const onFlowSelected = async (registrationId: number | null) => {
  if (!nodeRunFlow.value) return;
  const flow = runnableFlows.value.find((f) => f.registration_id === registrationId);
  nodeRunFlow.value.flow_registration_id = registrationId;
  nodeRunFlow.value.flow_reference = flow?.flow_path ?? null;
  if (registrationId == null) {
    flowParams.value = [];
    mappingByParam.value = {};
    return;
  }
  await loadFlowParameters(registrationId);
};

const syncMappings = () => {
  if (!nodeRunFlow.value) return;
  nodeRunFlow.value.parameter_mappings = flowParams.value
    .map((param) => ({
      param_name: param.name,
      input_column: mappingByParam.value[param.name] ?? "",
    }))
    .filter((mapping) => mapping.input_column !== "");
};

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeRunFlow,
  onBeforeSave: () => {
    syncMappings();
    return true;
  },
});

const loadData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  // Match RestApiReader.vue: use the seeded settings when configured, else a default
  // factory instance so the panel is never bound to a null/undefined setting_input.
  const hasValidSetup = Boolean(nodeData.value?.setting_input?.is_setup);
  nodeRunFlow.value = hasValidSetup
    ? (nodeData.value!.setting_input as NodeRunFlow)
    : createNodeRunFlow(nodeStore.flow_id, nodeId);
  try {
    runnableFlows.value = await FlowApiApi.listRunnableFlows();
  } catch (error) {
    console.error("Failed to load runnable flows:", error);
    runnableFlows.value = [];
  }
  if (nodeRunFlow.value?.flow_registration_id != null) {
    await loadFlowParameters(nodeRunFlow.value.flow_registration_id);
  }
  dataLoaded.value = true;
};

const loadNodeData = async (nodeId: number) => {
  await loadData(nodeId);
};

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});
</script>

<style scoped>
.hint-text {
  font-size: 11px;
  color: var(--el-text-color-secondary);
  margin-top: 4px;
}

.mapping-table {
  width: 100%;
  border-collapse: collapse;
}

.mapping-table th {
  text-align: left;
  font-size: 11px;
  color: var(--el-text-color-secondary);
  font-weight: 500;
  padding: 2px 4px;
}

.mapping-table td {
  padding: 2px 4px;
  vertical-align: middle;
}

.mapping-table .param-name {
  font-family: var(--el-font-family-monospace, monospace);
  font-size: 12px;
  white-space: nowrap;
}

.field-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  margin-bottom: 6px;
}

.field-label {
  font-size: 12px;
}
</style>
