<template>
  <div v-for="(child, index) in menuContents.children" :key="index">
    <div class="cool-button-container">
      <button class="cool-button" @click="handleButtonClick(child)">
        {{ child.name }}
      </button>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, watch, onMounted, defineEmits } from "vue";
import { MenuContents, ColumnSelectorInterface } from "./types";
import { useNodeStore } from "../../../../stores/column-store";
import { FileColumn } from "../../../../components/nodes/baseNode/nodeInterfaces";

const emit = defineEmits<{
  (event: "value-selected", payload: string): void;
}>();

const ranVal = ref(0);
const showOptions = ref(false);
const nodeStore = useNodeStore();
const menuContents = ref<MenuContents>({
  title: "Existing fields",
  icon: "room",
  children: [],
});

watch(
  () => nodeStore.nodeData?.main_input?.table_schema,
  (newColumns) => {
    if (newColumns) {
      updateColumnData(newColumns);
    }
  },
  { deep: true },
);

const handleButtonClick = (columnSelector: ColumnSelectorInterface) => {
  const val = columnSelector.node_type === "c" ? `[${columnSelector.label}]` : columnSelector.label;
  ranVal.value++;
  emit("value-selected", val);
};

const updateColumnData = (columns: FileColumn[]) => {
  const childrenNodes: ColumnSelectorInterface[] = columns.map((col) => ({
    label: col.name,
    hasAction: true,
    node_type: "c",
    name: col.name + "(" + col.data_type + ")",
  }));
  if (menuContents.value) {
    menuContents.value.children = childrenNodes;
  }
};

onMounted(async () => {
  if (nodeStore.nodeData?.main_input?.columns) {
    updateColumnData(nodeStore.nodeData.main_input.table_schema);
  }
});

defineExpose({ showOptions });
</script>

<style scoped>
.cool-button-container {
  display: flex;
  flex-direction: column;
  align-items: start;
}

.cool-button {
  width: 100%; /* Ensure the button takes full width of the container */
  max-width: 200px; /* Set a max-width if you want to control the maximum button size */
  border: none;
  color: rgb(0, 0, 0);
  padding: 3px 3px;
  text-align: left;
  display: inline-block;
  margin: 1px 0;
  cursor: pointer;
  border-radius: 4px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis; /* Add ellipsis for overflow text */
}

.cool-button:hover {
  background-color: #bcbcbc;
}

.sidebar-title {
  font-size: 16px;
  font-weight: bold;
  padding: 10px;
}
</style>
