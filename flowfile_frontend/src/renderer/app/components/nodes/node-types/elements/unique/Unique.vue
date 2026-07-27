<template>
  <div v-if="dataLoaded && nodeUnique" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeUnique"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="list-wrapper">
        <div class="listbox-subtitle">Keep strategy</div>
        <el-select v-model="uniqueInput.strategy" size="small" style="width: 100%">
          <el-option
            v-for="option in strategyOptions"
            :key="option.value"
            :label="option.label"
            :value="option.value"
          />
        </el-select>
      </div>
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
  UniqueSorttrategy,
  NodeUnique,
  createSelectInputFromName,
  SelectInput,
} from "../../../baseNode/nodeInput";
import { CodeLoader } from "vue-content-loader";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
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

const strategyOptions: { value: UniqueSorttrategy; label: string }[] = [
  { value: "first", label: "First" },
  { value: "last", label: "Last" },
  { value: "any", label: "Any" },
  { value: "none", label: "None" },
];

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

// Missing-column warnings come from the backend settings validation; this only
// covers configuration completeness the backend does not check.
const validateConfig = () => {
  if (!nodeUnique.value) return;
  if (nodeUnique.value.unique_input.columns.length == 0) {
    nodeStore.setNodeValidation(nodeUnique.value.node_id, {
      isValid: false,
      error: "Please select at least one field.",
    });
  } else {
    nodeStore.setNodeValidation(nodeUnique.value.node_id, {
      isValid: true,
      error: "",
    });
  }
};

const setUniqueColumns = () => {
  uniqueInput.value.columns = selection.value
    .filter((input) => input.keep)
    .map((input) => input.old_name);
};

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeUnique,
  onBeforeSave: () => {
    setUniqueColumns();
    return true;
  },
  onAfterSave: async () => {
    validateConfig();
  },
});

const loadSelection = (nodeData: NodeData, columnsToKeep: string[]) => {
  if (nodeData.main_input?.columns) {
    selection.value = nodeData.main_input.columns.map((column) => {
      const keep = columnsToKeep.includes(column);
      return createSelectInputFromName(column, keep);
    });
  }
};

const calculateSelects = (updatedInputs: SelectInput[]) => {
  selection.value = updatedInputs;
  uniqueInput.value.columns = updatedInputs
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

<style scoped>
/* Context menu styles are now centralized in styles/components/_context-menu.css */
/* No component-specific styles needed */
</style>
