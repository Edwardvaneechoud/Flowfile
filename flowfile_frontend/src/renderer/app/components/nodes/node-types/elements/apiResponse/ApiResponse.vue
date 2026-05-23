<template>
  <div v-if="dataLoaded && node" class="listbox-wrapper">
    <div class="main-part">
      <p class="hint">
        This node marks its input as the body of the flow's HTTP API response. Publish the flow as
        an API from the catalog to get a URL and keys. Query parameters map to the flow's
        <code>${parameter}</code> values and are configured when publishing.
      </p>

      <div class="row">
        <label>Response shape</label>
        <el-select v-model="node.orientation" size="small">
          <el-option label="Records (list of row objects)" value="records" />
          <el-option label="Columns (column name -> values)" value="columns" />
        </el-select>
      </div>

      <div class="row">
        <label>Max rows (optional)</label>
        <el-input-number
          v-model="node.max_rows"
          :min="1"
          size="small"
          controls-position="right"
          placeholder="all rows"
        />
      </div>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref } from "vue";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { createDefaultApiResponse, type NodeApiResponse } from "./defaultValues";

const nodeStore = useNodeStore();
const node = ref<NodeApiResponse | null>(null);
const dataLoaded = ref(false);

const { saveSettings, pushNodeData } = useNodeSettings({ nodeRef: node });

async function loadNodeData(nodeId: number) {
  const nodeResult = await nodeStore.getNodeData(nodeId, false);
  if (nodeResult?.setting_input && nodeResult.setting_input.is_setup) {
    node.value = nodeResult.setting_input as NodeApiResponse;
  } else {
    node.value = createDefaultApiResponse(nodeStore.flow_id, nodeId);
  }
  dataLoaded.value = true;
}

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>

<style scoped>
.main-part {
  display: flex;
  flex-direction: column;
  gap: 16px;
  padding: 20px;
  border: 1px solid var(--color-border-primary);
  border-radius: 8px;
  background-color: var(--color-background-primary);
  margin-top: 20px;
}

.row {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.hint {
  font-size: 12px;
  line-height: 1.5;
  color: var(--color-text-secondary);
}

.hint code {
  font-family: var(--font-mono, monospace);
  background-color: var(--color-background-secondary);
  padding: 1px 4px;
  border-radius: 3px;
}
</style>
