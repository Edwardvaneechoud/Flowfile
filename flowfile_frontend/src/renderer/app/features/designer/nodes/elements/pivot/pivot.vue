<template>
  <div v-if="dataLoaded && nodePivot" class="listbox-wrapper">
    <generic-node-settings v-model="nodePivot">
      <div class="listbox-wrapper">
        <ul class="listbox">
          <li
            v-for="(col_schema, index) in nodeData?.main_input?.table_schema"
            :key="col_schema.name"
            :class="getColumnClass(col_schema.name)"
            draggable="true"
            @click="handleItemClick(col_schema.name)"
            @contextmenu.prevent="openContextMenu(col_schema.name, $event)"
            @dragstart="onDragStart(col_schema.name, $event)"
            @dragover.prevent
            @drop="onDrop(index)"
          >
            {{ col_schema.name }} ({{ col_schema.data_type }})
          </li>
        </ul>
      </div>

      <ContextMenu
        v-if="showContextMenu"
        id="pivot-context-menu"
        ref="contextMenuRef"
        :position="contextMenuPosition"
        :options="contextMenuOptions"
        @select="handleContextMenuSelect"
        @close="closeContextMenu"
      />

      <div class="listbox-wrapper">
        <SettingsSection
          title="Index Keys"
          :items="pivotInput.index_columns"
          droppable="true"
          @remove-item="removeColumn('index', $event)"
          @dragover.prevent
          @drop="onDropInSection('index')"
        />
        <SettingsSection
          title="Pivot Column"
          :items="[pivotInput.pivot_column ?? '']"
          droppable="true"
          @remove-item="removeColumn('pivot', $event)"
          @dragover.prevent
          @drop="onDropInSection('pivot')"
        />
        <SettingsSection
          title="Value Column"
          :items="[pivotInput.value_col ?? '']"
          droppable="true"
          @remove-item="removeColumn('value', $event)"
          @dragover.prevent
          @drop="onDropInSection('value')"
        />
        <div class="list-wrapper">
          <div class="listbox-subtitle">Select aggregations</div>
          <el-select
            v-model="pivotInput.aggregations"
            multiple
            placeholder="Select"
            size="small"
            style="width: 100%"
          >
            <el-option
              v-for="item in aggOptions"
              :key="item"
              :label="item"
              :value="item"
              style="width: 400px"
            />
          </el-select>
        </div>
        <PivotValidation :pivot-input="pivotInput" />
      </div>
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import { ref, onMounted, onUnmounted, computed, nextTick } from "vue";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { PivotInput, NodePivot, PivotAggOption } from "../../../baseNode/nodeInput";
import { useNodeStore } from "../../../../../stores/column-store";
import ContextMenu from "./ContextMenu.vue";
import SettingsSection from "./SettingsSection.vue";
import PivotValidation from "./PivotValidation.vue";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const showContextMenu = ref(false);
const dataLoaded = ref(false);
const contextMenuPosition = ref({ x: 0, y: 0 });
const selectedColumns = ref<string[]>([]);
const contextMenuOptions = ref<{ label: string; action: string; disabled: boolean }[]>([]);
const contextMenuRef = ref<HTMLElement | null>(null);
const nodeData = ref<null | NodeData>(null);
const draggedColumnName = ref<string | null>(null);
const aggOptions: PivotAggOption[] = ["sum", "count", "min", "max", "n_unique", "mean", "median", 'first', 'last', 'concat'];

const pivotInput = ref<PivotInput>({
  index_columns: [],
  pivot_column: null,
  value_col: null,
  aggregations: [],
});

const nodePivot = ref<NodePivot | null>(null);

const singleColumnSelected = computed(() => selectedColumns.value.length === 1);

const getColumnClass = (columnName: string): string => {
  return selectedColumns.value.includes(columnName) ? "is-selected" : "";
};

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

const onDropInSection = (section: "index" | "pivot" | "value") => {
  if (draggedColumnName.value) {
    // Remove column from any existing assignments
    removeColumnIfExists(draggedColumnName.value);

    // Assign the dragged column to the appropriate section
    if (section === "index" && !pivotInput.value.index_columns.includes(draggedColumnName.value)) {
      pivotInput.value.index_columns.push(draggedColumnName.value);
    } else if (section === "pivot") {
      pivotInput.value.pivot_column = draggedColumnName.value;
    } else if (section === "value") {
      pivotInput.value.value_col = draggedColumnName.value;
    }

    draggedColumnName.value = null;
  }
};

const openContextMenu = (columnName: string, event: MouseEvent) => {
  selectedColumns.value = [columnName];
  contextMenuPosition.value = { x: event.clientX, y: event.clientY };

  contextMenuOptions.value = [
    {
      label: "Add to Index",
      action: "index",
      disabled: isColumnAssigned(columnName),
    },
    {
      label: "Set as Pivot",
      action: "pivot",
      disabled: isColumnAssigned(columnName) || !singleColumnSelected.value,
    },
    {
      label: "Set as Value",
      action: "value",
      disabled: isColumnAssigned(columnName) || !singleColumnSelected.value,
    },
  ];

  showContextMenu.value = true;
};

const handleContextMenuSelect = (action: string) => {
  const column = selectedColumns.value[0];
  if (action === "index" && !pivotInput.value.index_columns.includes(column)) {
    removeColumnIfExists(column);
    pivotInput.value.index_columns.push(column);
  } else if (action === "pivot") {
    removeColumnIfExists(column);
    pivotInput.value.pivot_column = column;
  } else if (action === "value") {
    removeColumnIfExists(column);
    pivotInput.value.value_col = column;
  }
  closeContextMenu();
};

const isColumnAssigned = (columnName: string): boolean => {
  return (
    pivotInput.value.index_columns.includes(columnName) ||
    pivotInput.value.pivot_column === columnName ||
    pivotInput.value.value_col === columnName
  );
};

const removeColumnIfExists = (column: string) => {
  pivotInput.value.index_columns = pivotInput.value.index_columns.filter((col) => col !== column);
  if (pivotInput.value.pivot_column === column) pivotInput.value.pivot_column = null;
  if (pivotInput.value.value_col === column) pivotInput.value.value_col = null;
};

const removeColumn = (type: "index" | "pivot" | "value", column: string) => {
  if (type === "index") {
    pivotInput.value.index_columns = pivotInput.value.index_columns.filter((col) => col !== column);
  } else if (type === "pivot") {
    pivotInput.value.pivot_column = null;
  } else if (type === "value") {
    pivotInput.value.value_col = null;
  }
};

const handleItemClick = (columnName: string) => {
  selectedColumns.value = [columnName];
};

const loadNodeData = async (nodeId: number) => {
  console.log("loadNodeData from groupby");
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodePivot.value = nodeData.value?.setting_input as NodePivot;
  if (nodeData.value) {
    if (nodePivot.value) {
      if (nodePivot.value.pivot_input) {
        pivotInput.value = nodePivot.value.pivot_input;
      } else {
        nodePivot.value.pivot_input = pivotInput.value;
      }
    }
  }
  dataLoaded.value = true;
  nodeStore.isDrawerOpen = true;
  console.log("loadNodeData from groupby");
};

const handleClickOutside = (event: MouseEvent) => {
  const targetEvent = event.target as HTMLElement;
  if (targetEvent.id === "pivot-context-menu") return;
  showContextMenu.value = false;
};

const closeContextMenu = () => {
  showContextMenu.value = false;
};

const pushNodeData = async () => {
  if (pivotInput.value) {
    nodeStore.updateSettings(nodePivot);
  }
  nodeStore.isDrawerOpen = false;
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
</style>
