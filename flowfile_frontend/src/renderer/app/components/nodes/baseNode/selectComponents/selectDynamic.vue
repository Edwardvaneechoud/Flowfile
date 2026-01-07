<template>
  <div>
    <div v-if="dataLoaded">
      <div v-if="hasMissingFields" class="remove-missing-fields" @click="removeMissingFields">
        <UnavailableField tooltip-text="Field not available click for removing them for memory" />
        <span>Remove Missing Fields</span>
      </div>

      <div v-if="props.showTitle" class="listbox-subtitle">
        {{ props.title }}
      </div>

      <div class="listbox-wrapper">
        <div class="table-wrapper">
          <table class="styled-table">
            <thead>
              <tr v-if="props.showHeaders">
                <th
                  v-if="props.showOldColumns"
                  :style="{ width: standardColumnWidth }"
                  @click="toggleSort"
                >
                  {{ originalColumnHeader }}
                  <span v-if="props.sortedBy === 'asc'">▲</span>
                  <span v-else-if="props.sortedBy === 'desc'">▼</span>
                </th>
                <th v-if="props.showNewColumns" :style="{ width: standardColumnWidth }">
                  New column name
                </th>
                <th v-if="props.showDataType" :style="{ width: standardColumnWidth }">Data type</th>
                <th v-if="props.showKeepOption" :style="{ width: selectColumnWidth }">Select</th>
              </tr>
            </thead>
            <tbody id="selectable-container">
              <tr
                v-for="(column, index) in localSelectInputs"
                :key="column.old_name"
                :class="{ 'drag-over': dragOverIndex === index }"
                :style="{ opacity: column.is_available ? 1 : 0.6 }"
                draggable="true"
                @dragstart="handleDragStart(index, $event)"
                @dragover.prevent="handleDragOver(index)"
                @drop="handleDrop(index)"
              >
                <td
                  v-if="props.showOldColumns"
                  :class="{ 'highlight-row': isSelected(column.old_name) }"
                  @click="handleItemClick(index, column.old_name, $event)"
                  @contextmenu.prevent="openContextMenu(index, column.old_name, $event)"
                >
                  <div v-if="!column.is_available" class="unavailable-field">
                    <UnavailableField />
                    <span style="margin-left: 20px">{{ column.old_name }}</span>
                  </div>
                  <div v-else>
                    {{ column.old_name }}
                  </div>
                </td>

                <td
                  v-if="props.showNewColumns"
                  :class="{ 'highlight-row': isSelected(column.old_name) }"
                >
                  <el-input v-model="column.new_name" size="small" class="smaller-el-input" />
                </td>

                <td
                  v-if="props.showDataType"
                  :class="{ 'highlight-row': isSelected(column.old_name) }"
                >
                  <el-select v-model="column.data_type" size="small">
                    <el-option
                      v-for="dataType in dataTypes"
                      :key="dataType"
                      :label="dataType"
                      :value="dataType"
                    />
                  </el-select>
                </td>

                <td
                  v-if="props.showKeepOption"
                  :class="{ 'highlight-row': isSelected(column.old_name) }"
                >
                  <el-checkbox v-model="column.keep" />
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <div
      v-if="showContextMenu"
      class="context-menu"
      :style="{ top: contextMenuPosition.y + 'px', left: contextMenuPosition.x + 'px' }"
    >
      <button @click="selectAllSelected">Select</button>
      <button @click="deselectAllSelected">Deselect</button>
    </div>
  </div>
</template>

<script lang="ts" setup>

import { ref, computed, onMounted, onUnmounted, watchEffect } from "vue";
import { SelectInput } from "../nodeInput";
import { useNodeStore } from "../../../../stores/column-store";
import UnavailableField from "./UnavailableFields.vue";

const sortState = ref<"none" | "asc" | "desc">("none");

const initializeOrder = () => {
  const sortedInputs = [...props.selectInputs].sort((a, b) =>
    a.is_available === b.is_available ? 0 : a.is_available ? -1 : 1,
  );
  if (sortState.value === "none") {
    localSelectInputs.value = [...sortedInputs];
  }
};

const toggleSort = () => {
  if (props.sortedBy === "none") {
    emit("update:sortedBy", "asc");
    localSelectInputs.value.sort((a, b) => a.old_name.localeCompare(b.old_name));
  } else if (props.sortedBy === "asc") {
    emit("update:sortedBy", "desc");
    localSelectInputs.value.sort((a, b) => b.old_name.localeCompare(a.old_name));
  } else {
    emit("update:sortedBy", "none");
    localSelectInputs.value.sort((a, b) => a.original_position - b.original_position);
  }
  localSelectInputs.value.forEach((input, i) => (input.position = i));
};

const props = defineProps({
  selectInputs: {
    type: Array as () => SelectInput[],
    default: () => [],
  },
  showOldColumns: { type: Boolean, default: true },
  showNewColumns: { type: Boolean, default: true },
  showKeepOption: { type: Boolean, default: false },
  showDataType: { type: Boolean, default: false },
  title: { type: String, default: "Select columns" },
  showOptionKeepUnseen: { type: Boolean, default: false },
  showHeaders: { type: Boolean, default: true },
  showData: { type: Boolean, default: true },
  showTitle: { type: Boolean, default: true },
  draggable: { type: Boolean, default: false },
  showMissing: { type: Boolean, default: true },
  originalColumnHeader: { type: String, default: "Original column name" },
  sortedBy: { type: String, default: "none" },
});

// State and Store
const dataLoaded = ref(true);
const selectedColumns = ref<string[]>([]);
const firstSelectedIndex = ref<number | null>(null);
const contextMenuPosition = ref({ x: 0, y: 0 });
const showContextMenu = ref(false);
const draggingIndex = ref<number>(-1);
const dragOverIndex = ref<number>(-1);
const nodeStore = useNodeStore();
const dataTypes = nodeStore.getDataTypes();

// Local sorted select inputs (by availability)
const localSelectInputs = ref<SelectInput[]>(
  [...props.selectInputs].sort((a, b) =>
    a.is_available === b.is_available ? 0 : a.is_available ? -1 : 1,
  ),
);

watchEffect(() => {
  localSelectInputs.value = [...props.selectInputs].sort((a, b) =>
    a.is_available === b.is_available ? 0 : a.is_available ? -1 : 1,
  );
});

// Computed properties for column widths
const standardColumnCount = computed(
  () => [props.showOldColumns, props.showNewColumns, props.showDataType].filter(Boolean).length,
);

const standardColumnWidth = computed(() => {
  const totalColumns = standardColumnCount.value + 0.5;
  return totalColumns > 0 ? 100 / totalColumns + "%" : "0%";
});

const selectColumnWidth = computed(() =>
  standardColumnCount.value > 0 ? 50 / (standardColumnCount.value + 0.5) + "%" : "0%",
);

// Helper Methods
const isSelected = (columnName: string) => selectedColumns.value.includes(columnName);

const getRange = (start: number, end: number) =>
  start < end
    ? Array.from({ length: end - start + 1 }, (_, i) => i + start)
    : Array.from({ length: start - end + 1 }, (_, i) => i + end);

// Drag & Drop Handlers
const handleDragStart = (index: number, event: DragEvent) => {
  draggingIndex.value = index;
  event.dataTransfer?.setData("text", "");
  if (event.dataTransfer) event.dataTransfer.effectAllowed = "move";
};

const handleDragOver = (index: number) => {
  dragOverIndex.value = index;
};

const handleDrop = (index: number) => {
  const itemToMove = localSelectInputs.value.splice(draggingIndex.value, 1)[0];
  localSelectInputs.value.splice(index, 0, itemToMove);
  draggingIndex.value = -1;
  dragOverIndex.value = -1;
  localSelectInputs.value.forEach((input, i) => (input.position = i));
};

// Item Selection and Context Menu
const handleItemClick = (clickedIndex: number, columnName: string, event: MouseEvent) => {
  if (event.shiftKey && firstSelectedIndex.value !== null) {
    const range = getRange(firstSelectedIndex.value, clickedIndex);
    selectedColumns.value = range
      .map((index) => localSelectInputs.value[index].old_name)
      .filter(Boolean);
  } else {
    firstSelectedIndex.value = clickedIndex;
    selectedColumns.value = [localSelectInputs.value[clickedIndex].old_name];
  }
};

const openContextMenu = (clickedIndex: number, columnName: string, event: MouseEvent) => {
  showContextMenu.value = true;
  event.stopPropagation();
  if (!selectedColumns.value.includes(columnName)) {
    handleItemClick(clickedIndex, columnName, event);
  }
  contextMenuPosition.value = { x: event.clientX, y: event.clientY };
};

const selectAllSelected = () => {
  localSelectInputs.value.forEach((column) => {
    if (selectedColumns.value.includes(column.old_name)) {
      column.keep = true;
    }
  });
};

const deselectAllSelected = () => {
  localSelectInputs.value.forEach((column) => {
    if (selectedColumns.value.includes(column.old_name)) {
      column.keep = false;
    }
  });
};

// Check for Missing Fields
const hasMissingFields = computed(() =>
  localSelectInputs.value.some((column) => !column.is_available),
);

// Click Outside Handler for Context Menu
const handleClickOutside = (event: MouseEvent) => {
  const container = document.getElementById("selectable-container");
  if (container && !container.contains(event.target as Node)) {
    selectedColumns.value = [];
    showContextMenu.value = false;
  }
};

onMounted(() => {
  window.addEventListener("click", handleClickOutside);

  initializeOrder();
});

onUnmounted(() => {
  window.removeEventListener("click", handleClickOutside);
});

// Emit and Expose
const emit = defineEmits(["updateSelectInputs", "update:sortedBy"]);

const removeMissingFields = () => {
  const availableColumns = localSelectInputs.value.filter((column) => column.is_available);
  localSelectInputs.value = availableColumns;
  emit("updateSelectInputs", availableColumns);
};

defineExpose({ localSelectInputs });
</script>

<style scoped>
/* Context menu styling */
.context-menu {
  position: fixed;
  z-index: 1000;
  border: 1px solid var(--color-border-primary);
  background-color: var(--color-background-primary);
  padding: 8px;
  box-shadow: var(--shadow-lg);
  border-radius: 4px;
  user-select: none;
}

.context-menu button {
  display: block;
  background: none;
  border: none;
  padding: 4px 8px;
  text-align: left;
  width: 100%;
  cursor: pointer;
  color: var(--color-text-primary);
}

.context-menu button:hover {
  background-color: var(--color-background-hover);
}

/* Table wrapper for scrolling */
.table-wrapper {
  max-height: 700px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
  border-radius: 8px;
  overflow: auto;
  margin: 5px;
}

/* Adjusted font size for input fields */
:deep(.smaller-el-input .el-input__inner) {
  padding: 2px 10px;
  font-size: 12px;
  height: 24px;
  line-height: 20px;
}

.smaller-el-input {
  line-height: 20px;
}

/* Highlight selected rows */
.highlight-row {
  background-color: #e7e6e6 !important;
}

/* Greyed out styling for unavailable fields */
.greyed-out {
  background-color: #dcdcdc !important;
  color: #888 !important;
  z-index: 1;
}

/* Unavailable field styling */
.unavailable-field {
  display: flex;
  align-items: center;
  position: relative;
  pointer-events: auto;
}

/* Remove missing fields button styling */
.remove-missing-fields {
  display: flex;
  align-items: center;
  background-color: #d9534f;
  padding: 4px 8px;
  margin: 10px 20px;
  text-align: center;
  cursor: pointer;
  border-radius: 4px;
  font-weight: bold;
  font-size: 0.9em;
}

.remove-missing-fields:hover {
  background-color: #c9302c;
}

.remove-missing-fields > unavailable-field {
  margin-right: 4px;
}

/* Adjustments for el-checkbox component */
:deep(.el-checkbox) {
  font-size: 12px;
  height: 20px;
  line-height: 20px;
  display: flex;
  align-items: center;
}

:deep(.el-checkbox .el-checkbox__input) {
  height: 16px;
  width: 16px;
  margin: 0;
}

:deep(.el-checkbox .el-checkbox__inner) {
  height: 16px;
  width: 16px;
}

:deep(.el-checkbox .el-checkbox__label) {
  font-size: 12px;
  line-height: 20px;
  margin-left: 4px;
}
</style>
