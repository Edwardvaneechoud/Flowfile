<template>
  <div v-if="dataLoaded">
    <div v-if="hasMissingFields" class="remove-missing-fields" @click="removeMissingFields">
      <unavailable-field tooltip-text="Field not available click for removing them for memory" />
      <span>Remove Missing Fields</span>
    </div>
    <div v-if="props.showTitle" class="listbox-subtitle">{{ props.title }}</div>
    <div class="listbox-wrapper">
      <div class="table-wrapper">
        <table class="styled-table">
          <thead>
            <tr v-if="props.showHeaders">
              <th v-if="props.showOldColumns" :style="{ width: standardColumnWidth }">
                {{ originalColumnHeader }}
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
              :class="[{ 'drag-over': dragOverIndex === index }]"
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
                  <unavailable-field />
                  <span style="margin-left: 20px">{{ column.old_name }}</span>
                </div>
                <div v-else @click="handleItemClick(index, column.old_name, $event)">
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
    :style="{
      top: contextMenuPosition.y + 'px',
      left: contextMenuPosition.x + 'px',
    }"
  >
    <button @click="selectAllSelected">Select</button>
    <button @click="deselectAllSelected">Deselect</button>
  </div>
</template>

<script lang="ts" setup>
import { ref, defineProps, computed, onMounted, onUnmounted, watchEffect } from "vue";
import { SelectInput } from "../nodeInput";
import { useNodeStore } from "../../../../stores/column-store";
import unavailableField from "./UnavailableFields.vue";

const props = defineProps({
  selectInputs: {
    type: Object as () => SelectInput[],
    default: () => ({}),
  },
  showOldColumns: {
    type: Boolean,
    default: true,
  },
  showNewColumns: {
    type: Boolean,
    default: true,
  },
  showKeepOption: {
    type: Boolean,
    default: false,
  },
  showDataType: {
    type: Boolean,
    default: false,
  },
  title: {
    type: String,
    default: "Select columns",
  },
  showOptionKeepUnseen: {
    type: Boolean,
    default: false,
  },
  showHeaders: {
    type: Boolean,
    default: true,
  },
  showData: {
    type: Boolean,
    default: true,
  },
  showTitle: {
    type: Boolean,
    default: true,
  },
  draggable: {
    type: Boolean,
    default: false,
  },
  showMissing: {
    type: Boolean,
    default: true,
  },
  originalColumnHeader: {
    type: String,
    default: "Original column name",
  },
});
// Refs and Computed
const dataLoaded = ref(true);
const dataTypes = useNodeStore().getDataTypes();
const selectedColumns = ref<string[]>([]);
const firstSelectedIndex = ref<number | null>(null);
const contextMenuPosition = ref({ x: 0, y: 0 });
const showContextMenu = ref(false);
const draggingIndex = ref<number>(-1);
const dragOverIndex = ref<number>(-1);

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

const standardColumnCount = computed(() => {
  return [props.showOldColumns, props.showNewColumns, props.showDataType].filter(Boolean).length;
});

const standardColumnWidth = computed(() => {
  const totalColumns = standardColumnCount.value + 0.5;
  return totalColumns > 0 ? 100 / totalColumns + "%" : "0%";
});

const selectColumnWidth = computed(() => {
  return standardColumnCount.value > 0 ? 50 / (standardColumnCount.value + 0.5) + "%" : "0%";
});

// Methods
const isSelected = (columnName: string) => selectedColumns.value.includes(columnName);

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

const getRange = (start: number, end: number) => {
  const range =
    start < end
      ? Array.from({ length: end - start + 1 }, (_, i) => i + start)
      : Array.from({ length: start - end + 1 }, (_, i) => i + end);
  return range;
};

const openContextMenu = (clickedIndex: number, columnName: string, event: MouseEvent) => {
  showContextMenu.value = true;
  event.stopPropagation();
  handleItemClick(clickedIndex, columnName, event, true);
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

const handleItemClick = (clickedIndex: number, columnName: string, event: MouseEvent, full = false,) => {
  if (event.button === 2 || !localSelectInputs.value[clickedIndex].is_available) return;
  if ((event.shiftKey || full) && firstSelectedIndex.value !== null) {
    const range = getRange(firstSelectedIndex.value, clickedIndex);
      const s = range
      .map((index) => localSelectInputs.value[index].old_name)
      .filter((col): col is string => col !== undefined);
    selectedColumns.value = s 
  } else {
    firstSelectedIndex.value = clickedIndex;
    selectedColumns.value = [localSelectInputs.value[clickedIndex].old_name];
  }
};

const hasMissingFields = computed(() => {
  return localSelectInputs.value.some((column) => !column.is_available);
});

// Lifecycle Hooks
onMounted(() => {
  window.addEventListener("click", handleClickOutside);
});

onUnmounted(() => {
  window.removeEventListener("click", handleClickOutside);
});

const handleClickOutside = (event: MouseEvent) => {
  const container = document.getElementById('selectable-container');
  if (container && !container.contains(event.target as Node)) {
    selectedColumns.value = [];
    showContextMenu.value = false;
  }
};


const emit = defineEmits(["updateSelectInputs"]);

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
  border: 1px solid #ccc;
  background-color: white;
  padding: 8px;
  box-shadow: 0 2px 10px rgba(0, 0, 0, 0.2);
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
}

.context-menu button:hover {
  background-color: #f0f0f0;
}

/* Table wrapper to allow scrolling */
.table-wrapper {
  max-height: 700px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
  border-radius: 8px;
  overflow: auto;
  margin: 5px;
}

/* Adjusted font size for input fields within the table */
::v-deep(.smaller-el-input .el-input__inner) {
  padding: 2px 10px;
  font-size: 12px;
  height: 24px; /* Adjusted height to match row height */
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
/* Adjust the el-checkbox component size */
::v-deep .el-checkbox {
  font-size: 12px; /* Match the table's font size */
  height: 20px; /* Adjust height as needed */
  line-height: 20px; /* Ensure vertical alignment */
  display: flex;
  align-items: center;
}

/* Adjust the checkbox input */
::v-deep .el-checkbox .el-checkbox__input {
  height: 16px; /* Adjust to fit the checkbox size */
  width: 16px;
  margin: 0; /* Remove default margins */
}

/* Adjust the checkbox inner square */
::v-deep .el-checkbox .el-checkbox__inner {
  height: 16px;
  width: 16px;
}

/* Adjust the label of the checkbox, if any */
::v-deep .el-checkbox .el-checkbox__label {
  font-size: 12px;
  line-height: 20px;
  margin-left: 4px; /* Space between checkbox and label */
}
</style>
