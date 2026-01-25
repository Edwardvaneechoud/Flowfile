<template>
  <div v-if="dataLoaded && nodeSample" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeSample"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Settings</div>
        <el-row>
          <el-col :span="10" class="grid-content">Offset</el-col>
          <el-col :span="8" class="grid-content"
            ><input v-model="sampleSize" type="number" min="0" step="1"
          /></el-col>
        </el-row>
      </div>
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import { ref, onMounted, onUnmounted, nextTick } from "vue";
import { NodeSample } from "../../../baseNode/nodeInput";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const showContextMenu = ref(false);
const showContextMenuRemove = ref(false);
const dataLoaded = ref(false);
const contextMenuColumn = ref<string | null>(null);
const contextMenuRef = ref<HTMLElement | null>(null);
const nodeSample = ref<null | NodeSample>(null);
const nodeData = ref<null | NodeData>(null);
const sampleSize = ref<number>(1000);

// Use the standardized node settings composable
const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeSample,
  onBeforeSave: () => {
    // Sync sample size before saving
    if (nodeSample.value) {
      nodeSample.value.sample_size = sampleSize.value;
    }
    return true;
  },
});

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeSample.value = nodeData.value?.setting_input;
  if (nodeSample.value) {
    if (!nodeSample.value.is_setup) {
      nodeSample.value.sample_size = sampleSize.value;
    } else {
      sampleSize.value = nodeSample.value.sample_size;
    }
    dataLoaded.value = true;
  }
};

const handleClickOutside = (event: MouseEvent) => {
  if (!contextMenuRef.value?.contains(event.target as Node)) {
    showContextMenu.value = false;
    contextMenuColumn.value = null;
    showContextMenuRemove.value = false;
  }
};

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});

onMounted(async () => {
  await nextTick();
  window.addEventListener("click", handleClickOutside);
});

onUnmounted(() => {
  window.removeEventListener("click", handleClickOutside);
});
</script>
