<template>
  <div v-if="dataLoaded && nodeSort" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeSort"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper column-list">
        <div class="listbox-subtitle">Columns</div>
        <ul v-if="dataLoaded" class="listbox">
          <li
            v-for="(col_schema, index) in nodeData?.main_input?.table_schema"
            :key="col_schema.name"
            :class="{ 'is-selected': selectedColumns.includes(col_schema.name) }"
            @mousedown="handleItemMouseDown"
            @click="handleItemClick(index, col_schema.name, $event)"
            @contextmenu="openContextMenu(index, col_schema.name, $event)"
          >
            {{ col_schema.name }} ({{ col_schema.data_type }})
          </li>
        </ul>
      </div>
      <context-menu
        v-if="activeMenu"
        :position="contextMenuPosition"
        :options="menuOptions"
        @select="onMenuSelect"
        @close="activeMenu = null"
      />

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
              <tr
                v-for="(item, index) in nodeSort.sort_input"
                :key="index"
                @contextmenu="openRowContextMenu($event, index)"
              >
                <td>{{ item.column }}</td>
                <td>
                  <el-select v-model="item.how" size="small">
                    <el-option
                      v-for="aggOption in sortOptions"
                      :key="aggOption"
                      :label="aggOption"
                      :value="aggOption"
                    />
                  </el-select>
                </td>
              </tr>
            </tbody>
          </table>
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
import { ContextMenu } from "../../../../common";
import type { ContextMenuOption } from "../../../../common";

const nodeStore = useNodeStore();
const nodeSort = ref<null | NodeSort>(null);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeSort,
});
// One menu at a time: the column menu and the sort-row menu are mutually
// exclusive by construction (a single enum instead of two booleans).
const activeMenu = ref<"column" | "row" | null>(null);
const dataLoaded = ref(false);
const contextMenuPosition = ref({ x: 0, y: 0 });
const selectedColumns = ref<string[]>([]);
const nodeData = ref<null | NodeData>(null);
const sortOptions = ["Ascending", "Descending"];
const firstSelectedIndex = ref<number | null>(null);

const openRowContextMenu = (event: MouseEvent, index: number) => {
  // Right-clicks on the row's editable fields keep the native menu (paste).
  const el = event.target as HTMLElement | null;
  if (el?.closest?.("input, textarea")) return;
  event.preventDefault();
  contextMenuPosition.value = { x: event.clientX, y: event.clientY };
  contextMenuRowIndex.value = index;
  activeMenu.value = "row";
};

const removeRow = () => {
  if (contextMenuRowIndex.value !== null) {
    nodeSort.value?.sort_input.splice(contextMenuRowIndex.value, 1);
  }
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
  activeMenu.value = "column";
};

const menuOptions = computed<ContextMenuOption[]>(() => {
  if (activeMenu.value === "row") {
    return [{ label: "Remove", action: "remove", danger: true }];
  }
  return [
    { label: "Ascending", action: "Ascending" },
    ...(singleColumnSelected.value ? [{ label: "Descending", action: "Descending" }] : []),
  ];
});

const onMenuSelect = (action: string) => {
  if (activeMenu.value === "row") {
    if (action === "remove") removeRow();
    return;
  }
  setSortSettings(action, selectedColumns.value);
};

const setSortSettings = (sortType: string, columns: string[] | null) => {
  if (columns) {
    columns.forEach((column) => {
      nodeSort.value?.sort_input.push({ column: column, how: sortType });
    });
  }
};

const handleItemMouseDown = (event: MouseEvent) => {
  // Stop the browser extending its own text selection across the drawer.
  if (event.shiftKey) {
    event.preventDefault();
    window.getSelection()?.removeAllRanges();
  }
};

const handleItemClick = (clickedIndex: number, columnName: string, event: MouseEvent) => {
  if (event.shiftKey && firstSelectedIndex.value !== null) {
    const range = getRange(firstSelectedIndex.value, clickedIndex);
    // Index against table_schema — the array the template renders.
    selectedColumns.value = range
      .map((index) => nodeData.value?.main_input?.table_schema[index]?.name)
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
