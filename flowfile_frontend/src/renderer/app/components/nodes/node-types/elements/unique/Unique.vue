<template>
  <div v-if="dataLoaded && nodeUnique" class="listbox-wrapper">
    <generic-node-settings v-model="nodeUnique">
      <select-dynamic
        :select-inputs="selection"
        :show-keep-option="true"
        :show-data-type="false"
        :show-new-columns="false"
        :show-old-columns="true"
        :show-headers="true"
        :show-title="false"
        :show-data="true"
        title="Select data"
        original-column-header="Column"
        @update-select-inputs="calculateSelects"
      />
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref, onMounted, onUnmounted, computed, nextTick } from "vue";
import {
  UniqueInput,
  NodeUnique,
  createSelectInputFromName,
  SelectInput,
} from "../../../baseNode/nodeInput";
import { CodeLoader } from "vue-content-loader";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { useNodeStore } from "../../../../../stores/column-store";
import selectDynamic from "../../../baseNode/selectComponents/selectDynamic.vue";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const showContextMenu = ref(false);
const showContextMenuRemove = ref(false);
const dataLoaded = ref(false);
const contextMenuColumn = ref<string | null>(null);
const contextMenuRef = ref<HTMLElement | null>(null);
const nodeUnique = ref<null | NodeUnique>(null);
const nodeData = ref<null | NodeData>(null);
const selection = ref<SelectInput[]>([]);
const uniqueInput = ref<UniqueInput>({
  columns: [],
  strategy: "any",
});

const loadSelection = (nodeData: NodeData, columnsToKeep: string[]) => {
  if (nodeData.main_input?.columns) {
    // Map over nodeData.main_input.columns and create SelectInput for each column
    selection.value = nodeData.main_input.columns.map((column) => {
      // If the column is in columnsToKeep, keep it, otherwise set keep to false
      const keep = columnsToKeep.includes(column);
      return createSelectInputFromName(column, keep);
    });
  }
};

const loadData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeUnique.value = nodeData.value?.setting_input;
  dataLoaded.value = true;

  if (nodeData.value) {
    if (nodeUnique.value) {
      if (nodeUnique.value.unique_input) {
        uniqueInput.value = nodeUnique.value.unique_input;
      } else {
        nodeUnique.value.unique_input = uniqueInput.value;
      }
      loadSelection(nodeData.value, uniqueInput.value.columns);
    }
  }
};

const calculateSelects = (updatedInputs: SelectInput[]) => {
  console.log(updatedInputs);
  selection.value = updatedInputs;
  uniqueInput.value.columns = updatedInputs
    .filter((input) => input.keep)
    .map((input) => input.old_name);
};

const setUniqueColumns = () => {
  uniqueInput.value.columns = selection.value
    .filter((input) => input.keep)
    .map((input) => input.old_name);
};

const loadNodeData = async (nodeId: number) => {
  loadData(nodeId);
  dataLoaded.value = true;
};

const handleClickOutside = (event: MouseEvent) => {
  if (!contextMenuRef.value?.contains(event.target as Node)) {
    showContextMenu.value = false;
    contextMenuColumn.value = null;
    showContextMenuRemove.value = false;
  }
};

const getMissingColumns = (availableColumns: string[], usedColumns: string[]): string[] => {
  const availableSet = new Set(availableColumns);
  return Array.from(new Set(usedColumns.filter((usedColumn) => !availableSet.has(usedColumn))));
};

const missingColumns = computed(() => {
  if (nodeData.value && nodeData.value.main_input?.columns) {
    return getMissingColumns(nodeData.value.main_input?.columns, uniqueInput.value.columns);
  }
  return [];
});

const calculateMissingColumns = (): string[] => {
  if (nodeData.value && nodeData.value.main_input?.columns) {
    return getMissingColumns(nodeData.value.main_input?.columns, uniqueInput.value.columns);
  }
  return [];
};

const validateNode = async () => {
  if (nodeUnique.value?.unique_input) {
    await loadData(Number(nodeUnique.value.node_id));
  }
  const missingColumnsLocal = calculateMissingColumns();
  if (missingColumnsLocal.length > 0 && nodeUnique.value) {
    nodeStore.setNodeValidation(nodeUnique.value.node_id, {
      isValid: false,
      error: `The fields ${missingColumns.value.join(", ")} are missing in the available columns.`,
    });
  } else if (nodeUnique.value?.unique_input.columns.length == 0) {
    nodeStore.setNodeValidation(nodeUnique.value.node_id, {
      isValid: false,
      error: "Please select at least one field.",
    });
  } else if (nodeUnique.value) {
    nodeStore.setNodeValidation(nodeUnique.value.node_id, {
      isValid: true,
      error: "",
    });
  }
};

const instantValidate = async () => {
  if (missingColumns.value.length > 0 && nodeUnique.value) {
    nodeStore.setNodeValidation(nodeUnique.value.node_id, {
      isValid: false,
      error: `The fields ${missingColumns.value.join(", ")} are missing in the available columns.`,
    });
  } else if (nodeUnique.value?.unique_input.columns.length == 0) {
    nodeStore.setNodeValidation(nodeUnique.value.node_id, {
      isValid: false,
      error: "Please select at least one field.",
    });
  } else if (nodeUnique.value) {
    nodeStore.setNodeValidation(nodeUnique.value.node_id, {
      isValid: true,
      error: "",
    });
  }
};

const pushNodeData = async () => {
  dataLoaded.value = false;
  setUniqueColumns();
  console.log("doing this");
  console.log(nodeUnique.value?.is_setup);
  console.log(nodeUnique.value);
  if (nodeUnique.value?.is_setup) {
    nodeUnique.value.is_setup = true;
  }

  nodeStore.updateSettings(nodeUnique);
  await instantValidate();
  if (nodeUnique.value?.unique_input) {
    nodeStore.setNodeValidateFunc(nodeUnique.value?.node_id, validateNode);
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
/* Context menu styles are now centralized in styles/components/_context-menu.css */
/* No component-specific styles needed */
</style>
