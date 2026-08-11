<template>
  <div v-if="dataLoaded && nodeSample" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeSample"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Method</div>
        <el-radio-group v-model="sampleMethod" class="method-group">
          <el-radio label="first">First rows</el-radio>
          <el-radio label="random">Random rows</el-radio>
          <el-radio label="random_fraction">Random %</el-radio>
        </el-radio-group>
        <div class="method-hint">{{ methodHint }}</div>

        <div class="listbox-subtitle" style="margin-top: 12px">Size</div>
        <el-row v-if="sampleMethod === 'random_fraction'" align="middle">
          <el-col :span="10" class="grid-content">Percentage</el-col>
          <el-col :span="8">
            <el-input v-model.number="fraction" size="small" type="number" :min="0" :max="100" />
          </el-col>
          <el-col :span="2" class="pct-suffix">%</el-col>
        </el-row>
        <el-row v-else align="middle">
          <el-col :span="10" class="grid-content">Number of rows</el-col>
          <el-col :span="8">
            <el-input v-model.number="sampleSize" size="small" type="number" :min="1" :step="1" />
          </el-col>
        </el-row>

        <template v-if="sampleMethod !== 'first'">
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
            Leave blank to draw a fresh sample on every run. Set a fixed seed for a reproducible
            sample across runs.
          </div>
        </template>
      </div>
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed } from "vue";
import { ElMessage } from "element-plus";
import type { NodeSample, SampleMethod } from "@/types/node.types";
import type { NodeData } from "@/components/nodes/baseNode/nodeInterfaces";
import { useNodeStore } from "@/stores/node-store";
import { useNodeSettings } from "@/composables/useNodeSettings";
import GenericNodeSettings from "@/components/nodes/baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const nodeSample = ref<null | NodeSample>(null);
const nodeData = ref<null | NodeData>(null);

const sampleMethod = ref<SampleMethod>("first");
const sampleSize = ref<number>(1000);
const fraction = ref<number>(10);
const seedInput = ref<number | null>(null);

const methodHint = computed(() => {
  if (sampleMethod.value === "random") return "A uniform random selection of rows.";
  if (sampleMethod.value === "random_fraction") return "A uniform random share of all rows.";
  return "The first rows, in their existing order.";
});

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeSample,
  onBeforeSave: () => {
    if (!nodeSample.value) return false;
    if (sampleMethod.value === "random_fraction") {
      const pct = Number(fraction.value);
      if (!Number.isFinite(pct) || pct <= 0 || pct > 100) {
        ElMessage.error("Percentage must be greater than 0 and at most 100");
        return false;
      }
      nodeSample.value.fraction = pct;
    } else {
      const n = Number(sampleSize.value);
      if (!Number.isFinite(n) || n < 1) {
        ElMessage.error("Number of rows must be at least 1");
        return false;
      }
      nodeSample.value.sample_size = n;
    }
    nodeSample.value.sample_method = sampleMethod.value;
    nodeSample.value.seed =
      seedInput.value === null || seedInput.value === undefined || Number.isNaN(seedInput.value)
        ? null
        : Number(seedInput.value);
    return true;
  },
});

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeSample.value = nodeData.value?.setting_input;
  if (nodeSample.value) {
    if (nodeSample.value.is_setup) {
      sampleMethod.value = nodeSample.value.sample_method ?? "first";
      sampleSize.value = nodeSample.value.sample_size ?? sampleSize.value;
      fraction.value = nodeSample.value.fraction ?? fraction.value;
      seedInput.value = nodeSample.value.seed ?? null;
    } else {
      nodeSample.value.sample_method = sampleMethod.value;
      nodeSample.value.sample_size = sampleSize.value;
      nodeSample.value.fraction = fraction.value;
      nodeSample.value.seed = null;
    }
    dataLoaded.value = true;
  }
};

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});
</script>

<style scoped>
.method-group {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 2px;
}
.method-hint {
  margin-top: 4px;
  font-size: 11px;
  color: #64748b;
}
.pct-suffix {
  color: #64748b;
  font-size: 12px;
  padding-left: 6px;
}
.seed-hint {
  margin-top: 4px;
  font-size: 11px;
  color: #64748b;
}
</style>
