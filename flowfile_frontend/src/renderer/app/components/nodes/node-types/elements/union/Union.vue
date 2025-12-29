<template>
  <div v-if="dataLoaded && nodeUnion" class="listbox-wrapper">
    <generic-node-settings v-model="nodeUnion">
      'Union multiple tables into one table, this node does not have settings'
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import { ref, onMounted, onUnmounted, nextTick, defineProps } from "vue";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { UnionInput, NodeUnion } from "../../../baseNode/nodeInput";
import { useNodeStore } from "../../../../../stores/column-store";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const showContextMenu = ref(false);
const dataLoaded = ref(false);
const nodeData = ref<null | NodeData>(null);
const unionInput = ref<UnionInput>({ mode: "relaxed" });
const nodeUnion = ref<NodeUnion | null>(null);

const loadNodeData = async (nodeId: number) => {
  console.log("loadNodeData from union ");
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeUnion.value = nodeData.value?.setting_input as NodeUnion;
  if (nodeData.value) {
    if (nodeUnion.value) {
      if (nodeUnion.value.union_input) {
        unionInput.value = nodeUnion.value.union_input;
      } else {
        nodeUnion.value.union_input = unionInput.value;
      }
    }
  }
  dataLoaded.value = true;
  console.log("loadNodeData from groupby");
};

const handleClickOutside = (event: MouseEvent) => {
  const targetEvent = event.target as HTMLElement;
  if (targetEvent.id === "pivot-context-menu") return;
  showContextMenu.value = false;
};

const pushNodeData = async () => {
  if (unionInput.value) {
    nodeStore.updateSettings(nodeUnion);
  }
};

defineExpose({
  loadNodeData,
  pushNodeData,
});

onMounted(async () => {
  await nextTick();
  window.addEventListener("click", handleClickOutside);
});

onUnmounted(() => {
  window.removeEventListener("click", handleClickOutside);
});
</script>

<style scoped>
.context-menu {
  position: fixed;
  z-index: 1000;
  border: 1px solid #ccc;
  background-color: white;
  padding: 8px;
  box-shadow: 0 2px 10px rgba(0, 0, 0, 0.2);
  border-radius: 4px;
}

.context-menu ul {
  list-style: none;
  padding: 0;
  margin: 0;
}

.context-menu li {
  padding: 8px 16px;
  cursor: pointer;
}

.context-menu li:hover {
  background-color: #f0f0f0;
}

.switch-container {
  display: flex;
  align-items: center;
  margin: 12px;
}

.switch-container span {
  margin-right: 10px;
}
</style>
