<template>
  <div class="params-panel">
    <div v-if="!hasParameters" class="params-empty">
      <span class="material-icons params-empty-icon">tune</span>
      <p>No parameters defined.</p>
      <p class="params-empty-hint">
        Parameters let you reference dynamic values in node settings using
        <code>${param_name}</code> syntax.
      </p>
      <el-button size="small" type="primary" @click="addParameter">+ Add Parameter</el-button>
    </div>

    <div v-else class="params-list">
      <div class="params-list-header">
        <span class="col-name">Name</span>
        <span class="col-value">Value</span>
        <span class="col-desc">Description</span>
        <span class="col-actions" />
      </div>

      <div v-for="(param, index) in parameters" :key="index" class="param-row">
        <el-input
          v-model="param.name"
          placeholder="name"
          size="small"
          class="col-name"
          @change="save"
        />
        <el-input
          v-model="param.default_value"
          placeholder="value"
          size="small"
          class="col-value"
          @change="save"
        />
        <el-input
          v-model="param.description"
          placeholder="description"
          size="small"
          class="col-desc"
          @change="save"
        />
        <div class="col-actions">
          <el-button
            size="small"
            type="danger"
            :icon="Delete"
            circle
            @click="removeParameter(index)"
          />
        </div>
      </div>

      <el-button size="small" style="margin-top: 8px" @click="addParameter">
        + Add Parameter
      </el-button>
    </div>

    <div v-if="saveError" class="params-error">{{ saveError }}</div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted } from "vue";
import { Delete } from "@element-plus/icons-vue";
import { ElMessage } from "element-plus";
import { useNodeStore } from "../../../stores/column-store";
import { getFlowSettings, updateFlowSettings } from "../../nodes/nodeLogic";
import type { FlowSettings } from "../../../types/flow.types";
import type { FlowParameter } from "../../../types/flow.types";

const nodeStore = useNodeStore();

const flowSettings = ref<FlowSettings | null>(null);
const saveError = ref<string | null>(null);

const parameters = computed<FlowParameter[]>(() => flowSettings.value?.parameters ?? []);
const hasParameters = computed(() => parameters.value.length > 0);

async function load() {
  if (!nodeStore.flow_id || nodeStore.flow_id <= 0) return;
  flowSettings.value = await getFlowSettings(nodeStore.flow_id);
  if (flowSettings.value) {
    flowSettings.value.parameters = flowSettings.value.parameters ?? [];
  }
}

async function save() {
  if (!flowSettings.value) return;
  saveError.value = null;
  try {
    await updateFlowSettings(flowSettings.value);
  } catch (e: any) {
    saveError.value = e?.message ?? "Failed to save parameters";
    ElMessage.error("Failed to save parameters");
  }
}

function addParameter() {
  if (!flowSettings.value) return;
  flowSettings.value.parameters = [
    ...(flowSettings.value.parameters ?? []),
    { name: "", default_value: "", description: "" },
  ];
  save();
}

function removeParameter(index: number) {
  if (!flowSettings.value?.parameters) return;
  flowSettings.value.parameters = flowSettings.value.parameters.filter((_, i) => i !== index);
  save();
}

watch(
  () => nodeStore.flow_id,
  async (newId, oldId) => {
    if (newId !== oldId && newId > 0) {
      await load();
    }
  },
);

onMounted(load);
</script>

<style scoped>
.params-panel {
  display: flex;
  flex-direction: column;
  height: 100%;
  padding: var(--spacing-3);
  font-family: var(--font-family-base);
  overflow-y: auto;
}

.params-empty {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  height: 100%;
  text-align: center;
  color: var(--color-text-muted, #999);
  gap: var(--spacing-2);
  padding: var(--spacing-6);
}

.params-empty-icon {
  font-size: 40px;
  color: var(--color-border-secondary, #ccc);
}

.params-empty p {
  margin: 0;
  font-size: var(--font-size-sm);
}

.params-empty-hint {
  font-size: var(--font-size-xs, 11px) !important;
  max-width: 260px;
  line-height: 1.5;
}

.params-empty-hint code {
  background: var(--color-background-tertiary);
  padding: 1px 4px;
  border-radius: var(--border-radius-sm);
  font-family: var(--font-family-mono);
}

.params-list {
  display: flex;
  flex-direction: column;
  gap: 0;
}

.params-list-header {
  display: flex;
  gap: var(--spacing-2);
  padding: 0 0 var(--spacing-1) 0;
  font-size: var(--font-size-xs, 11px);
  font-weight: var(--font-weight-semibold, 600);
  color: var(--color-text-muted, #999);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  border-bottom: 1px solid var(--color-border-light);
  margin-bottom: var(--spacing-2);
}

.param-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  margin-bottom: var(--spacing-2);
}

.col-name {
  flex: 1.2;
  min-width: 0;
}

.col-value {
  flex: 1.8;
  min-width: 0;
}

.col-desc {
  flex: 2;
  min-width: 0;
}

.col-actions {
  flex: 0 0 32px;
  display: flex;
  align-items: center;
  justify-content: center;
}

.params-error {
  margin-top: var(--spacing-2);
  font-size: var(--font-size-xs, 11px);
  color: var(--color-danger, #f56c6c);
}
</style>
