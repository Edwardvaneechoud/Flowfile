<template>
  <div v-show="tableVisible" class="table-preview">
    <DataPreview
      ref="dataPreview"
      :show-file-stats="false"
      :hide-title="true"
      :flow-id="1"
      :node-id="selectedNodeId"
    />
  </div>
</template>

<script setup lang="ts">
import { ref, defineExpose } from "vue";
import DataPreview from "../../dataPreview.vue";

const tableVisible = ref(true);
const selectedNodeId = ref(0);
const dataPreview = ref<InstanceType<typeof DataPreview>>();

const setSelectedNodeId = (nodeId: number) => {
  selectedNodeId.value = nodeId;
  if (dataPreview.value) {
    dataPreview.value.downloadData(nodeId);
  }
};

defineExpose({
  showTablePreview: () => {
    tableVisible.value = true;
  },
  hideTablePreview: () => {
    tableVisible.value = false;
  },
  setSelectedNodeId,
});
</script>
