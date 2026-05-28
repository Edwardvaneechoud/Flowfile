<template>
  <div v-if="dataLoaded && nodeRandomSplit" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeRandomSplit"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Splits</div>
        <div class="splits-editor">
          <div v-for="(split, index) in splits" :key="index" class="split-row">
            <span class="split-letter">{{ outputLabel(index) }}</span>
            <el-input
              v-model="split.name"
              size="small"
              placeholder="name"
              class="split-name"
              @input="syncOutputs"
            />
            <el-input
              v-model.number="split.percentage"
              size="small"
              type="number"
              :min="0"
              :max="100"
              :step="1"
              class="split-percentage"
            />
            <span class="split-pct-suffix">%</span>
            <button
              class="icon-button icon-button--danger"
              :disabled="splits.length <= 1"
              title="Remove split"
              @click="removeSplit(index)"
            >
              <i class="fa-solid fa-minus"></i>
            </button>
          </div>
          <button class="add-output-button" :disabled="splits.length >= 10" @click="addSplit">
            <i class="fa-solid fa-plus"></i> Add Split
          </button>
        </div>
        <div class="split-total" :class="{ 'split-total--bad': !totalIsValid }">
          Total: {{ totalPercentage.toFixed(2) }}%
          <span v-if="!totalIsValid"> (must equal 100)</span>
        </div>

        <div class="listbox-subtitle" style="margin-top: 12px">Seed (optional)</div>
        <el-row>
          <el-col :span="12">
            <el-input
              v-model.number="seedInput"
              size="small"
              type="number"
              placeholder="leave blank for random"
            />
          </el-col>
        </el-row>
        <div class="seed-hint">
          Leave blank to generate a fresh seed on every run. Set a fixed seed for reproducible
          partitions across runs.
        </div>
      </div>
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed, nextTick } from "vue";
import { Position } from "@vue-flow/core";
import { ElMessage } from "element-plus";
import { outputHandle, outputLabel } from "@/utils/outputHandle";
import type { NodeRandomSplit, RandomSplitGroup } from "@/types/node.types";
import type { NodeData } from "@/components/nodes/baseNode/nodeInterfaces";
import { useNodeStore } from "@/stores/node-store";
import { useFlowStore } from "@/stores/flow-store";
import { useNodeSettings } from "@/composables/useNodeSettings";
import GenericNodeSettings from "@/components/nodes/baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const flowStore = useFlowStore();
const dataLoaded = ref(false);
const nodeRandomSplit = ref<null | NodeRandomSplit>(null);
const nodeData = ref<null | NodeData>(null);

const splits = ref<RandomSplitGroup[]>([
  { name: "train", percentage: 80 },
  { name: "test", percentage: 20 },
]);
const seedInput = ref<number | null>(null);

const totalPercentage = computed(() =>
  splits.value.reduce((acc, s) => acc + (Number(s.percentage) || 0), 0),
);
const totalIsValid = computed(() => Math.abs(totalPercentage.value - 100) <= 0.01);

const namesAreValid = computed(() => {
  const names = splits.value.map((s) => s.name);
  if (names.some((n) => !n)) return false;
  if (new Set(names).size !== names.length) return false;
  return names.every((n) => /^[A-Za-z][A-Za-z0-9_]*$/.test(n));
});

const updateNodeOutputHandles = () => {
  const vfInstance = flowStore.vueFlowInstance;
  if (!vfInstance || !nodeRandomSplit.value) return;
  const vfNode = vfInstance.findNode(String(nodeRandomSplit.value.node_id));
  if (!vfNode) return;
  const multi = splits.value.length > 1;
  vfNode.data.outputs = splits.value.map((s, i) => ({
    id: outputHandle(i),
    position: Position.Right,
    label: multi ? outputLabel(i) : undefined,
    title: multi ? s.name : undefined,
  }));
};

const syncOutputs = () => {
  updateNodeOutputHandles();
};

const addSplit = () => {
  if (splits.value.length >= 10) return;
  splits.value.push({ name: `split_${splits.value.length + 1}`, percentage: 0 });
  syncOutputs();
};

const removeSplit = (index: number) => {
  if (splits.value.length <= 1) return;
  splits.value.splice(index, 1);
  syncOutputs();
};

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeRandomSplit,
  onBeforeSave: () => {
    if (!nodeRandomSplit.value) return false;
    if (splits.value.length === 0) {
      ElMessage.error("At least one split is required");
      return false;
    }
    if (!totalIsValid.value) {
      ElMessage.error("Split percentages must sum to 100");
      return false;
    }
    if (!namesAreValid.value) {
      ElMessage.error(
        "Split names must be unique and start with a letter (alphanumeric / underscore only)",
      );
      return false;
    }
    nodeRandomSplit.value.splits = splits.value.map((s) => ({
      name: s.name,
      percentage: Number(s.percentage),
    }));
    nodeRandomSplit.value.seed =
      seedInput.value === null || Number.isNaN(seedInput.value as number)
        ? null
        : Number(seedInput.value);
    return true;
  },
});

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeRandomSplit.value = nodeData.value?.setting_input;
  if (nodeRandomSplit.value) {
    if (nodeRandomSplit.value.is_setup && nodeRandomSplit.value.splits?.length) {
      splits.value = nodeRandomSplit.value.splits.map((s) => ({ ...s }));
      seedInput.value = nodeRandomSplit.value.seed ?? null;
    } else {
      nodeRandomSplit.value.splits = splits.value.map((s) => ({ ...s }));
      nodeRandomSplit.value.seed = null;
    }
    dataLoaded.value = true;
    await nextTick();
    updateNodeOutputHandles();
  }
};

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});
</script>

<style scoped>
.splits-editor {
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.split-row {
  display: flex;
  align-items: center;
  gap: 6px;
}
.split-letter {
  display: inline-flex;
  justify-content: center;
  align-items: center;
  width: 22px;
  height: 22px;
  background: #f1f5f9;
  border-radius: 4px;
  font-weight: 600;
  font-size: 12px;
  color: #475569;
}
.split-name {
  flex: 1;
}
.split-percentage {
  width: 80px;
}
.split-pct-suffix {
  color: #64748b;
  font-size: 12px;
}
.split-total {
  margin-top: 8px;
  font-size: 12px;
  color: #475569;
}
.split-total--bad {
  color: #dc2626;
}
.seed-hint {
  margin-top: 4px;
  font-size: 11px;
  color: #64748b;
}
</style>
