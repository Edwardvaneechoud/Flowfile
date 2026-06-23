<template>
  <div v-if="dataLoaded && nodeSort" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeSort"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Columns</div>
        <ul v-if="dataLoaded" class="listbox">
          <li
            v-for="(col_schema, index) in nodeData?.main_input?.table_schema"
            :key="col_schema.name"
            :class="{ 'is-selected': selectedColumns.includes(col_schema.name) }"
            @click="handleItemClick(index, col_schema.name, $event)"
            @contextmenu="openContextMenu(index, col_schema.name, $event)"
          >
            {{ col_schema.name }} ({{ col_schema.data_type }})
          </li>
        </ul>
      </div>
      <div
        v-if="showContextMenu"
        ref="contextMenuRef"
        class="context-menu"
        :style="{
          top: contextMenuPosition.y + 'px',
          left: contextMenuPosition.x + 'px',
        }"
      >
        <button v-if="!singleColumnSelected" @click="setSortSettings('ascending', selectedColumns)">
          Ascending
        </button>
        <button v-if="singleColumnSelected" @click="setSortSettings('ascending', selectedColumns)">
          Ascending
        </button>
        <button v-if="singleColumnSelected" @click="setSortSettings('descending', selectedColumns)">
          Descending
        </button>
      </div>

      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Settings</div>

        <div v-if="dataLoaded" class="table-wrapper">
          <table class="styled-table">
            <thead>
              <tr>
                <th>Field</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              <div v-if="nodeSort">
                <tr
                  v-for="(item, index) in nodeSort.sort_input"
                  :key="index"
                  @contextmenu.prevent="openRowContextMenu($event, index)"
                >
                  <td>{{ item.column }}</td>
                  <td>
                    <el-select v-model="item.how" size="small">
                      <el-option
                        v-for="aggOption in sortOptions"
                        :key="aggOption.value"
                        :label="aggOption.label"
                        :value="aggOption.value"
                      />
                    </el-select>
                  </td>
                </tr>
              </div>
            </tbody>
          </table>
        </div>
        <div
          v-if="showContextMenuRemove"
          class="context-menu"
          :style="{
            top: contextMenuPosition.y + 'px',
            left: contextMenuPosition.x + 'px',
          }"
        >
          <button @click="removeRow">Remove</button>
        </div>
      </div>
    </generic-node-settings>
  </div>
  <code-loader v-else />
</template>

<script lang="ts" setup>
import { ref, computed } from "vue";
import { NodeSort } from "../../../baseNode/nodeInput";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { CodeLoader } from "vue-content-loader";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const nodeSort = ref<null | NodeSort>(null);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeSort,
});
const showContextMenu = ref(false);
const showContextMenuRemove = ref(false);
const dataLoaded = ref(false);
const contextMenuPosition = ref({ x: 0, y: 0 });
const contextMenuColumn = ref<string | null>(null);
const contextMenuRef = ref<HTMLElement | null>(null);
const selectedColumns = ref<string[]>([]);
const nodeData = ref<null | NodeData>(null);
const sortOptions = [
  { label: "Ascending", value: "ascending" },
  { label: "Descending", value: "descending" },
];
const firstSelectedIndex = ref<number | null>(null);

const openRowContextMenu = (event: MouseEvent, index: number) => {
  event.preventDefault();
  contextMenuPosition.value = { x: event.clientX, y: event.clientY };
  contextMenuRowIndex.value = index;
  showContextMenuRemove.value = true;
};

const removeRow = () => {
  if (contextMenuRowIndex.value !== null) {
    nodeSort.value?.sort_input.splice(contextMenuRowIndex.value, 1);
  }
  showContextMenuRemove.value = false;
  contextMenuRowIndex.value = null;
};

const contextMenuRowIndex = ref<number | null>(null);

const singleColumnSelected = computed(() => selectedColumns.value.length == 1);

const openContextMenu = (clickedIndex: number, columnName: string, event: MouseEvent) => {
  event.preventDefault();
  event.stopPropagation();
  if (!selectedColumns.value.includes(columnName)) {
    selectedColumns.value = [columnName];
  }
  contextMenuPosition.value = { x: event.clientX, y: event.clientY };
  showContextMenu.value = true;
};

const setSortSettings = (sortType: string, columns: string[] | null) => {
  if (columns) {
    columns.forEach((column) => {
      nodeSort.value?.sort_input.push({ column: column, how: sortType });
    });
  }
  showContextMenu.value = false;
  contextMenuColumn.value = null;
};

const handleItemClick = (clickedIndex: number, columnName: string, event: MouseEvent) => {
  if (event.shiftKey && firstSelectedIndex.value !== null) {
    const range = getRange(firstSelectedIndex.value, clickedIndex);
    selectedColumns.value = range
      .map((index) => nodeData.value?.main_input?.columns[index])
      .filter((col): col is string => col !== undefined);
  } else {
    if (firstSelectedIndex.value === clickedIndex) {
      selectedColumns.value = [];
    } else {
      firstSelectedIndex.value = clickedIndex;
      selectedColumns.value = [columnName];
    }
  }
};

const getRange = (start: number, end: number) => {
  return start < end
    ? [...Array(end - start + 1).keys()].map((i) => i + start)
    : [...Array(start - end + 1).keys()].map((i) => i + end);
};

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeSort.value = nodeData.value?.setting_input;
  if (nodeSort.value) {
    if (!nodeSort.value.is_setup) {
      nodeSort.value.sort_input = [];
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
/* Context menu styles are now centralized in styles/components/_context-menu.css */

.table-wrapper {
  max-height: 300px;
  box-shadow: var(--shadow-sm);
  border-radius: var(--border-radius-lg);
  overflow: auto;
  margin: var(--spacing-1);
}
</style>
