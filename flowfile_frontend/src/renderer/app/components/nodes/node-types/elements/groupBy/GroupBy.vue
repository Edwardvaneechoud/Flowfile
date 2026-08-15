<template>
  <div v-if="dataLoaded && nodeGroupBy">
    <generic-node-settings
      v-model="nodeGroupBy"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper column-list">
        <ul v-if="dataLoaded" class="listbox">
          <template
            v-for="(col_schema, index) in nodeData?.main_input?.table_schema"
            :key="col_schema.name"
          >
            <li
              :class="{ 'is-selected': selectedColumns.includes(col_schema.name) }"
              draggable="true"
              @mousedown="handleItemMouseDown"
              @click="handleItemClick(index, col_schema.name, $event)"
              @contextmenu="openContextMenu(index, col_schema.name, $event)"
              @dragstart="onDragStart(col_schema.name, $event)"
              @dragover.prevent
              @drop="onDrop(index)"
            >
              {{ col_schema.name }} ({{ col_schema.data_type }})
            </li>
          </template>
        </ul>
      </div>
      <context-menu
        v-if="activeMenu"
        :position="contextMenuPosition"
        :options="menuOptions"
        @select="onMenuSelect"
        @close="activeMenu = null"
      />

      <div class="listbox-subtitle">Settings</div>

      <div v-if="dataLoaded" class="table-wrapper" @dragover.prevent @drop="onDropInTable">
        <table class="styled-table">
          <thead>
            <tr>
              <th>Field</th>
              <th>Action</th>
              <th>Output Field Name</th>
            </tr>
          </thead>
          <tbody>
            <template v-for="(item, index) in groupByInput.agg_cols" :key="index">
              <tr @contextmenu="openRowContextMenu($event, index)">
                <td>{{ item.old_name }}</td>
                <td>
                  <el-select v-model="item.agg" size="small">
                    <el-option
                      v-for="aggOption in aggOptions"
                      :key="aggOption"
                      :label="aggOption"
                      :value="aggOption"
                    />
                  </el-select>
                </td>
                <td>
                  <el-input
                    v-model="item.new_name"
                    class="w-50 m-2"
                    size="small"
                    v-bind="NO_AUTOFILL"
                  />
                </td>
              </tr>
            </template>
          </tbody>
        </table>
      </div>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref, computed } from "vue";
import { GroupByInput, NodeGroupBy, AggOption, GroupByOption } from "../../../baseNode/nodeInput";
import { CodeLoader } from "vue-content-loader";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { NO_AUTOFILL } from "../../../../../utils/noAutofill";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import { ContextMenu } from "../../../../common";
import type { ContextMenuOption } from "../../../../common";

const nodeStore = useNodeStore();
const nodeGroupBy = ref<null | NodeGroupBy>(null);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeGroupBy,
  onAfterSave: async () => {
    validateConfig();
  },
});
// One menu at a time: the column menu and the agg-row menu are mutually
// exclusive by construction (a single enum instead of two booleans).
const activeMenu = ref<"column" | "row" | null>(null);
const dataLoaded = ref(false);
const contextMenuPosition = ref({ x: 0, y: 0 });
const selectedColumns = ref<string[]>([]);
const nodeData = ref<null | NodeData>(null);
const aggOptions: (AggOption | GroupByOption)[] = [
  "groupby",
  "sum",
  "max",
  "mean",
  "median",
  "min",
  "count",
  "n_unique",
  "first",
  "last",
  "concat",
];
const firstSelectedIndex = ref<number | null>(null);
const draggedColumnName = ref<string | null>(null);

const groupByInput = ref<GroupByInput>({
  agg_cols: [],
});

const onDragStart = (columnName: string, event: DragEvent) => {
  draggedColumnName.value = columnName;
  event.dataTransfer?.setData("text/plain", columnName);
};

const onDrop = (index: number) => {
  if (draggedColumnName.value) {
    const colSchema = nodeData.value?.main_input?.table_schema;
    if (colSchema) {
      const fromIndex = colSchema.findIndex((col) => col.name === draggedColumnName.value);
      if (fromIndex !== -1 && fromIndex !== index) {
        const [movedColumn] = colSchema.splice(fromIndex, 1);
        colSchema.splice(index, 0, movedColumn);
      }
    }
    draggedColumnName.value = null;
  }
};

const onDropInTable = () => {
  if (!draggedColumnName.value) return;
  const column = draggedColumnName.value;
  const alreadyGroupedBy = groupByInput.value.agg_cols.some(
    (row) => row.old_name === column && row.agg === "groupby",
  );
  if (!alreadyGroupedBy) {
    groupByInput.value.agg_cols.push({
      old_name: column,
      agg: "groupby",
      new_name: column,
    });
  }
  draggedColumnName.value = null;
};

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
    groupByInput.value.agg_cols.splice(contextMenuRowIndex.value, 1);
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
    { label: "Group by", action: "groupby" },
    ...(singleColumnSelected.value
      ? singleColumnAggOptions.map((option) => ({ label: option.label, action: option.value }))
      : []),
  ];
});

const onMenuSelect = (action: string) => {
  if (activeMenu.value === "row") {
    if (action === "remove") removeRow();
    return;
  }
  setAggregations(action as AggOption | GroupByOption, selectedColumns.value);
};

const setAggregations = (aggType: AggOption | GroupByOption, columns: string[] | null) => {
  if (columns) {
    columns.forEach((column) => {
      const new_column_name = aggType !== "groupby" ? column + "_" + aggType : column;

      groupByInput.value.agg_cols.push({
        old_name: column,
        agg: aggType,
        new_name: new_column_name,
      });
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
    // Index against table_schema — the array the template renders and onDrop reorders.
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

interface SingleColumnAggOption {
  value: AggOption;
  label: string;
}

const singleColumnAggOptions: SingleColumnAggOption[] = [
  { value: "count", label: "Count" },
  { value: "max", label: "Max" },
  { value: "mean", label: "Mean" },
  { value: "median", label: "Median" },
  { value: "min", label: "Min" },
  { value: "sum", label: "Sum" },
  { value: "n_unique", label: "N_unique" },
  { value: "first", label: "First" },
  { value: "last", label: "Last" },
  { value: "concat", label: "Concat" },
];

const getRange = (start: number, end: number) => {
  return start < end
    ? [...Array(end - start + 1).keys()].map((i) => i + start)
    : [...Array(start - end + 1).keys()].map((i) => i + end);
};

const loadData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeGroupBy.value = nodeData.value?.setting_input;
  if (nodeData.value) {
    if (nodeGroupBy.value) {
      if (nodeGroupBy.value.groupby_input) {
        groupByInput.value = nodeGroupBy.value.groupby_input;
      } else {
        nodeGroupBy.value.groupby_input = groupByInput.value;
      }
    }
  }
};

const loadNodeData = async (nodeId: number) => {
  loadData(nodeId);
  dataLoaded.value = true;
};

// Missing-column warnings come from the backend settings validation; this only
// covers configuration completeness the backend does not check.
const validateConfig = () => {
  if (!nodeGroupBy.value) return;
  if (nodeGroupBy.value.groupby_input.agg_cols.length == 0) {
    nodeStore.setNodeValidation(nodeGroupBy.value.node_id, {
      isValid: false,
      error: "Please select at least one field.",
    });
  } else {
    nodeStore.setNodeValidation(nodeGroupBy.value.node_id, {
      isValid: true,
      error: "",
    });
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
