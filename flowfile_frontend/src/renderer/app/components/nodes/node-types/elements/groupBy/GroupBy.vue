<template>
  <div v-if="dataLoaded && nodeGroupBy">
    <generic-node-settings
      v-model="nodeGroupBy"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <ul v-if="dataLoaded" class="listbox">
          <template
            v-for="(col_schema, index) in nodeData?.main_input?.table_schema"
            :key="col_schema.name"
          >
            <li
              :class="{ 'is-selected': selectedColumns.includes(col_schema.name) }"
              @click="handleItemClick(index, col_schema.name, $event)"
              @contextmenu="openContextMenu(index, col_schema.name, $event)"
            >
              {{ col_schema.name }} ({{ col_schema.data_type }})
            </li>
          </template>
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
        <button @click="setAggregations('groupby', selectedColumns)">Group by</button>
        <template v-for="option in singleColumnAggOptions" :key="option.value">
          <button
            v-if="singleColumnSelected"
            @click="setAggregations(option.value, selectedColumns)"
          >
            {{ option.label }}
          </button>
        </template>
      </div>

      <div class="listbox-subtitle">Settings</div>

      <div v-if="dataLoaded" class="table-wrapper">
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
              <tr @contextmenu.prevent="openRowContextMenu($event, index)">
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
                  <el-input v-model="item.new_name" class="w-50 m-2" size="small" />
                </td>
              </tr>
            </template>
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
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref, onMounted, onUnmounted, computed, nextTick } from "vue";
import { GroupByInput, NodeGroupBy, AggOption, GroupByOption } from "../../../baseNode/nodeInput";
import { CodeLoader } from "vue-content-loader";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const nodeGroupBy = ref<null | NodeGroupBy>(null);

// Use the standardized node settings composable
const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeGroupBy,
  onAfterSave: async () => {
    await instantValidate();
  },
  getValidationFunc: () => {
    if (nodeGroupBy.value?.groupby_input) {
      return validateNode;
    }
    return undefined;
  },
});
const showContextMenu = ref(false);
const showContextMenuRemove = ref(false);
const dataLoaded = ref(false);
const contextMenuPosition = ref({ x: 0, y: 0 });
const contextMenuColumn = ref<string | null>(null);
const contextMenuRef = ref<HTMLElement | null>(null);
const selectedColumns = ref<string[]>([]);
const nodeData = ref<null | NodeData>(null);
const aggOptions: (AggOption | GroupByOption)[] = [
  "groupby",
  "sum",
  "max",
  "median",
  "min",
  "count",
  "n_unique",
  "first",
  "last",
  "concat",
];
const firstSelectedIndex = ref<number | null>(null);

const groupByInput = ref<GroupByInput>({
  agg_cols: [],
});

const openRowContextMenu = (event: MouseEvent, index: number) => {
  event.preventDefault();
  contextMenuPosition.value = { x: event.clientX, y: event.clientY };
  contextMenuRowIndex.value = index; // Save the index of the row where the context menu was opened
  showContextMenuRemove.value = true;
};

const removeRow = () => {
  if (contextMenuRowIndex.value !== null) {
    groupByInput.value.agg_cols.splice(contextMenuRowIndex.value, 1);
  }
  showContextMenuRemove.value = false; // Hide the context menu after removing the row
  contextMenuRowIndex.value = null; // Reset the saved index
};

const contextMenuRowIndex = ref<number | null>(null); // New ref to keep track of which row is being interacted with

const singleColumnSelected = computed(() => selectedColumns.value.length == 1);

const openContextMenu = (clickedIndex: number, columnName: string, event: MouseEvent) => {
  event.preventDefault();
  event.stopPropagation(); // Stop click event from propagating
  if (!selectedColumns.value.includes(columnName)) {
    selectedColumns.value = [columnName];
  }
  contextMenuPosition.value = { x: event.clientX, y: event.clientY };
  showContextMenu.value = true;
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
  showContextMenu.value = false; // Hide the context menu after selection
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

interface SingleColumnAggOption {
  value: AggOption;
  label: string;
}

const singleColumnAggOptions: SingleColumnAggOption[] = [
  { value: "count", label: "Count" },
  { value: "max", label: "Max" },
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
    return getMissingColumns(
      nodeData.value.main_input?.columns,
      groupByInput.value.agg_cols.map((col) => col.old_name),
    );
  }
  return [];
});

const calculateMissingColumns = (): string[] => {
  if (nodeData.value && nodeData.value.main_input?.columns) {
    return getMissingColumns(
      nodeData.value.main_input?.columns,
      groupByInput.value.agg_cols.map((col) => col.old_name),
    );
  }
  return [];
};

const validateNode = async () => {
  if (nodeGroupBy.value?.groupby_input) {
    await loadData(Number(nodeGroupBy.value.node_id));
  }
  const missingColumnsLocal = calculateMissingColumns();
  if (missingColumnsLocal.length > 0 && nodeGroupBy.value) {
    nodeStore.setNodeValidation(nodeGroupBy.value.node_id, {
      isValid: false,
      error: `The fields ${missingColumns.value.join(", ")} are missing in the available columns.`,
    });
  } else if (nodeGroupBy.value?.groupby_input.agg_cols.length == 0) {
    nodeStore.setNodeValidation(nodeGroupBy.value.node_id, {
      isValid: false,
      error: "Please select at least one field.",
    });
  } else if (nodeGroupBy.value) {
    nodeStore.setNodeValidation(nodeGroupBy.value.node_id, {
      isValid: true,
      error: "",
    });
  }
};

const instantValidate = async () => {
  if (missingColumns.value.length > 0 && nodeGroupBy.value) {
    nodeStore.setNodeValidation(nodeGroupBy.value.node_id, {
      isValid: false,
      error: `The fields ${missingColumns.value.join(", ")} are missing in the available columns.`,
    });
  } else if (nodeGroupBy.value?.groupby_input.agg_cols.length == 0) {
    nodeStore.setNodeValidation(nodeGroupBy.value.node_id, {
      isValid: false,
      error: "Please select at least one field.",
    });
  } else if (nodeGroupBy.value) {
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

.table-wrapper {
  max-height: 300px;
  box-shadow: var(--shadow-sm);
  border-radius: var(--border-radius-lg);
  overflow: auto;
  margin: var(--spacing-1);
}
</style>
