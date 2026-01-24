<template>
  <div v-if="dataLoaded && nodeUnpivot" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeUnpivot"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
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
          :items="unpivotInput.index_columns"
          droppable="true"
          @remove-item="removeColumn('index', $event)"
          @dragover.prevent
          @drop="onDropInSection('index')"
        />
      </div>
      <div class="listbox-wrapper">
        <div class="switch-container">
          <span>Value selector</span>
          <el-switch
            v-model="unpivotInput.data_type_selector_mode"
            active-value="column"
            inactive-value="data_type"
            active-text="Column"
            inactive-text="Data Type"
            inline-prompt
          />
        </div>

        <SettingsSection
          v-if="unpivotInput.data_type_selector_mode === 'column'"
          title="Columns to unpivot"
          title-font-size="14px"
          :items="unpivotInput.value_columns"
          droppable="true"
          @remove-item="removeColumn('value', $event)"
          @dragover.prevent
          @drop="onDropInSection('value')"
        />
        <div v-else class="listbox-wrapper">
          <div class="listbox-subtitle">Dynamic data type selector</div>
          <div class="listbox-wrapper">
            <el-select
              v-model="unpivotInput.data_type_selector"
              placeholder="Select"
              size="small"
              style="width: 100%"
            >
              <el-option
                v-for="item in dataTypeSelectorOptions"
                :key="item"
                :label="item"
                :value="item"
                style="width: 400px"
              />
            </el-select>
          </div>
        </div>
      </div>
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import { ref, onMounted, onUnmounted, nextTick } from "vue";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { NodeUnpivot, DataTypeSelector, UnpivotInput } from "../../../baseNode/nodeInput";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import ContextMenu from "./ContextMenu.vue";
import SettingsSection from "./SettingsSection.vue";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const nodeUnpivot = ref<NodeUnpivot | null>(null);

// Use the standardized node settings composable
const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeUnpivot,
  onBeforeSave: () => {
    if (unpivotInput.value) {
      if (unpivotInput.value.data_type_selector_mode === "data_type") {
        unpivotInput.value.value_columns = [];
      } else {
        unpivotInput.value.data_type_selector = null;
      }
      nodeUnpivot.value!.unpivot_input = unpivotInput.value;
    }
    return true;
  },
});
const showContextMenu = ref(false);
const dataLoaded = ref(false);
const contextMenuPosition = ref({ x: 0, y: 0 });
const selectedColumns = ref<string[]>([]);
const contextMenuOptions = ref<{ label: string; action: string; disabled: boolean }[]>([]);
const contextMenuRef = ref<HTMLElement | null>(null);
const nodeData = ref<null | NodeData>(null);
const draggedColumnName = ref<string | null>(null);

const dataTypeSelectorOptions: DataTypeSelector[] = ["all", "numeric", "string", "date", "all"];

const unpivotInput = ref<UnpivotInput>({
  index_columns: [],
  value_columns: [],
  data_type_selector: null,
  data_type_selector_mode: "column",
});

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

const onDropInSection = (section: "index" | "value") => {
  if (draggedColumnName.value) {
    // Remove column from any existing assignments
    removeColumnIfExists(draggedColumnName.value);
    // Assign the dragged column to the appropriate section
    if (
      section === "index" &&
      !unpivotInput.value.index_columns.includes(draggedColumnName.value)
    ) {
      unpivotInput.value.index_columns.push(draggedColumnName.value);
    } else if (
      section === "value" &&
      !unpivotInput.value.value_columns.includes(draggedColumnName.value)
    ) {
      unpivotInput.value.value_columns.push(draggedColumnName.value);
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
      label: "Add to Value",
      action: "value",
      disabled:
        isColumnAssigned(columnName) || !(unpivotInput.value.data_type_selector_mode === "column"),
    },
  ];

  showContextMenu.value = true;
};

const handleContextMenuSelect = (action: string) => {
  const column = selectedColumns.value[0];
  if (action === "index" && !unpivotInput.value.index_columns.includes(column)) {
    removeColumnIfExists(column);
    unpivotInput.value.index_columns.push(column);
  } else if (action === "value" && !unpivotInput.value.index_columns.includes(column)) {
    removeColumnIfExists(column);
    unpivotInput.value.value_columns.push(column);
  }
  closeContextMenu();
};

const isColumnAssigned = (columnName: string): boolean => {
  return (
    unpivotInput.value.index_columns.includes(columnName) ||
    unpivotInput.value.value_columns.includes(columnName)
  );
};

const removeColumnIfExists = (column: string) => {
  unpivotInput.value.index_columns = unpivotInput.value.index_columns.filter(
    (col) => col !== column,
  );
  unpivotInput.value.value_columns = unpivotInput.value.value_columns.filter(
    (col) => col !== column,
  );
};

const removeColumn = (type: "index" | "value", column: string) => {
  if (type === "index") {
    unpivotInput.value.index_columns = unpivotInput.value.index_columns.filter(
      (col) => col !== column,
    );
  } else if (type === "value") {
    unpivotInput.value.value_columns = unpivotInput.value.value_columns.filter(
      (col) => col !== column,
    );
  }
};

const handleItemClick = (columnName: string) => {
  selectedColumns.value = [columnName];
};

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeUnpivot.value = nodeData.value?.setting_input as NodeUnpivot;
  if (nodeData.value) {
    if (nodeUnpivot.value) {
      if (nodeUnpivot.value.unpivot_input) {
        unpivotInput.value = nodeUnpivot.value.unpivot_input;
      } else {
        nodeUnpivot.value.unpivot_input = unpivotInput.value;
      }
    }
  }
  dataLoaded.value = true;
};

const handleClickOutside = (event: MouseEvent) => {
  const targetEvent = event.target as HTMLElement;
  if (targetEvent.id === "pivot-context-menu") return;
  showContextMenu.value = false;
};

const closeContextMenu = () => {
  showContextMenu.value = false;
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
.context-menu {
  position: fixed;
  z-index: 1000;
  border: 1px solid #ccc;
  background-color: var(--color-background-primary);
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
