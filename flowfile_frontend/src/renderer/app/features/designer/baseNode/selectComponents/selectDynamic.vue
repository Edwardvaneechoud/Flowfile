<template>
  <div v-if="dataLoaded">
    <!-- Remove Missing Fields Button -->
    <div v-if="hasMissingFields" class="remove-missing-fields" @click="removeMissingFields">
      <UnavailableField tooltip-text="Field not available click for removing them for memory" />
      <span>Remove Missing Fields</span>
    </div>

    <!-- Title -->
    <div v-if="props.showTitle" class="listbox-subtitle">
      {{ props.title }}
    </div>

    <!-- Table Wrapper -->
    <div class="listbox-wrapper">
      <div class="table-wrapper">
        <table class="styled-table">
          <thead>
            <tr v-if="props.showHeaders">
              <th
                v-if="props.showOldColumns"
                :style="{ width: columnWidths.standardWidth }"
                @click="toggleSort"
              >
                {{ props.originalColumnHeader }}
                <span v-if="props.sortedBy === 'asc'">▲</span>
                <span v-else-if="props.sortedBy === 'desc'">▼</span>
              </th>
              <th v-if="props.showNewColumns" :style="{ width: columnWidths.standardWidth }">
                New column name
              </th>
              <th v-if="props.showDataType" :style="{ width: columnWidths.standardWidth }">Data type</th>
              <th v-if="props.showKeepOption" :style="{ width: columnWidths.selectWidth }">Select</th>
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
              <!-- Old Column -->
              <td
                v-if="props.showOldColumns"
                :class="{ 'highlight-row': selectedSet.has(column.old_name) }"
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

              <!-- New Column -->
              <td
                v-if="props.showNewColumns"
                :class="{ 'highlight-row': selectedSet.has(column.old_name) }"
              >
                <el-input v-model="column.new_name" size="small" class="smaller-el-input" />
              </td>

              <!-- Data Type -->
              <td
                v-if="props.showDataType"
                :class="{ 'highlight-row': selectedSet.has(column.old_name) }"
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

              <!-- Keep Option -->
              <td
                v-if="props.showKeepOption"
                :class="{ 'highlight-row': selectedSet.has(column.old_name) }"
              >
                <el-checkbox v-model="column.keep" />
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- Context Menu -->
  <div
    v-if="showContextMenu"
    class="context-menu"
    :style="{ top: contextMenuPosition.y + 'px', left: contextMenuPosition.x + 'px' }"
  >
    <button @click="selectAllSelected">Select</button>
    <button @click="deselectAllSelected">Deselect</button>
  </div> 
</template>

<script lang="ts" setup>
import {
  ref,
  defineProps,
  computed,
  onMounted,
  onUnmounted,
  watch,
  defineEmits,
  defineExpose,
  nextTick,
  shallowRef
} from "vue";
import { SelectInput } from "../nodeInput";
import { useNodeStore } from "../../../../stores/column-store";
import UnavailableField from "./UnavailableFields.vue";

// State variables
const sortState = ref<"none" | "asc" | "desc">("none");
const dataLoaded = ref(false); // Start with false to defer rendering
const selectedColumns = ref<string[]>([]);
const selectedSet = computed(() => new Set(selectedColumns.value)); // For faster lookups
const firstSelectedIndex = ref<number | null>(null);
const contextMenuPosition = ref({ x: 0, y: 0 });
const showContextMenu = ref(false);
const draggingIndex = ref<number>(-1);
const dragOverIndex = ref<number>(-1);
const nodeStore = useNodeStore();
const dataTypes = shallowRef(nodeStore.getDataTypes()); // Using shallowRef for non-reactive data

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

const emit = defineEmits(["updateSelectInputs", "update:sortedBy"]);

// Optimized reactive state with initialization
const localSelectInputs = shallowRef<SelectInput[]>([]);

// More efficient initialization
const initializeLocalInputs = () => {
  // Don't sort initially if not needed, just copy
  if (props.selectInputs && props.selectInputs.length > 0) {
    // Only sort if necessary (if there are unavailable items)
    const hasUnavailable = props.selectInputs.some(input => !input.is_available);
    
    if (hasUnavailable && sortState.value === "none") {
      // Sort only if there are unavailable items
      localSelectInputs.value = [...props.selectInputs].sort((a, b) =>
        a.is_available === b.is_available ? 0 : a.is_available ? -1 : 1
      );
    } else {
      // Just clone without sorting
      localSelectInputs.value = [...props.selectInputs];
    }
  }
  // Mark as loaded after initialization to avoid flash of unsorted content
  nextTick(() => {
    dataLoaded.value = true;
  });
};

// Unified computed property for column widths (calculated once)
const columnWidths = computed(() => {
  const standardCount = [props.showOldColumns, props.showNewColumns, props.showDataType]
    .filter(Boolean).length;
  
  const totalColumns = standardCount + 0.5;
  return {
    standardWidth: totalColumns > 0 ? 100 / totalColumns + "%" : "0%",
    selectWidth: standardCount > 0 ? 50 / (standardCount + 0.5) + "%" : "0%",
    standardCount
  };
});

// Optimized sort toggle function
const toggleSort = () => {
  let newSortValue: "none" | "asc" | "desc" = "none";
  let sortedInputs: SelectInput[] = [];
  
  if (props.sortedBy === "none") {
    newSortValue = "asc";
    sortedInputs = [...localSelectInputs.value]
      .sort((a, b) => a.old_name.localeCompare(b.old_name));
  } else if (props.sortedBy === "asc") {
    newSortValue = "desc";
    sortedInputs = [...localSelectInputs.value]
      .sort((a, b) => b.old_name.localeCompare(a.old_name));
  } else {
    newSortValue = "none";
    sortedInputs = [...localSelectInputs.value]
      .sort((a, b) => a.original_position - b.original_position);
  }
  
  // Update positions in one go to avoid reactivity triggers
  localSelectInputs.value = sortedInputs.map((input, i) => ({
    ...input,
    position: i
  }));
  
  emit("update:sortedBy", newSortValue);
};

// Helper Methods
const getRange = (start: number, end: number) =>
  start < end
    ? Array.from({ length: end - start + 1 }, (_, i) => i + start)
    : Array.from({ length: start - end + 1 }, (_, i) => i + end);

// Optimized Drag & Drop Handlers
const handleDragStart = (index: number, event: DragEvent) => {
  draggingIndex.value = index;
  event.dataTransfer?.setData("text", "");
  if (event.dataTransfer) event.dataTransfer.effectAllowed = "move";
};

const handleDragOver = (index: number) => {
  dragOverIndex.value = index;
};

const handleDrop = (index: number) => {
  // Create a new array to minimize reactivity overhead
  const items = [...localSelectInputs.value];
  const itemToMove = items.splice(draggingIndex.value, 1)[0];
  items.splice(index, 0, itemToMove);
  
  // Update positions in one batch operation
  localSelectInputs.value = items.map((item, i) => ({
    ...item,
    position: i
  }));
  
  draggingIndex.value = -1;
  dragOverIndex.value = -1;
};

// Optimized selection handlers
const handleItemClick = (clickedIndex: number, columnName: string, event: MouseEvent) => {
  if (event.shiftKey && firstSelectedIndex.value !== null) {
    const range = getRange(firstSelectedIndex.value, clickedIndex);
    selectedColumns.value = range
      .map((index) => localSelectInputs.value[index]?.old_name)
      .filter(Boolean);
  } else {
    firstSelectedIndex.value = clickedIndex;
    selectedColumns.value = [columnName];
  }
};

const openContextMenu = (clickedIndex: number, columnName: string, event: MouseEvent) => {
  event.stopPropagation();
  if (!selectedSet.value.has(columnName)) {
    handleItemClick(clickedIndex, columnName, event);
  }
  contextMenuPosition.value = { x: event.clientX, y: event.clientY };
  showContextMenu.value = true;
};

// Optimized selection functions
const selectAllSelected = () => {
  if (selectedColumns.value.length === 0) return;
  
  localSelectInputs.value = localSelectInputs.value.map(column => {
    if (selectedSet.value.has(column.old_name)) {
      return { ...column, keep: true };
    }
    return column;
  });
  
  showContextMenu.value = false;
};

const deselectAllSelected = () => {
  if (selectedColumns.value.length === 0) return;
  
  localSelectInputs.value = localSelectInputs.value.map(column => {
    if (selectedSet.value.has(column.old_name)) {
      return { ...column, keep: false };
    }
    return column;
  });
  
  showContextMenu.value = false;
};

// Computed property for missing fields
const hasMissingFields = computed(() => {
  // Use some() instead of filter().length for better performance
  return localSelectInputs.value.some(column => !column.is_available);
});

// Click Outside Handler for Context Menu
const handleClickOutside = (event: MouseEvent) => {
  const container = document.getElementById("selectable-container");
  if (container && !container.contains(event.target as Node)) {
    selectedColumns.value = [];
    showContextMenu.value = false;
  }
};

// Optimized function to remove missing fields
const removeMissingFields = () => {
  const availableColumns = localSelectInputs.value.filter(column => column.is_available);
  localSelectInputs.value = availableColumns;
  emit("updateSelectInputs", availableColumns);
};

// Use a more specific watcher instead of watchEffect
watch(
  () => props.selectInputs,
  (newInputs) => {
    if (dataLoaded.value && newInputs?.length > 0) {
      // Only resort if necessary based on sort state
      if (sortState.value === "none") {
        const hasUnavailable = newInputs.some(input => !input.is_available);
        if (hasUnavailable) {
          localSelectInputs.value = [...newInputs].sort((a, b) =>
            a.is_available === b.is_available ? 0 : a.is_available ? -1 : 1
          );
        } else {
          localSelectInputs.value = [...newInputs];
        }
      } else if (sortState.value === "asc") {
        localSelectInputs.value = [...newInputs].sort((a, b) => 
          a.old_name.localeCompare(b.old_name)
        );
      } else if (sortState.value === "desc") {
        localSelectInputs.value = [...newInputs].sort((a, b) => 
          b.old_name.localeCompare(a.old_name)
        );
      }
    }
  },
  { 
    deep: false,  // Only watch for reference changes to avoid deep comparisons
    immediate: false // Don't trigger immediately, we'll initialize manually
  }
);

// Lifecycle hooks
onMounted(() => {
  window.addEventListener("click", handleClickOutside);
  
  // Defer initial data loading to improve perceived performance
  setTimeout(() => {
    initializeLocalInputs();
  }, 0);
});

onUnmounted(() => {
  window.removeEventListener("click", handleClickOutside);
});

// Expose necessary properties/methods
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

/* Table wrapper for scrolling */
.table-wrapper {
  max-height: 700px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
  border-radius: 8px;
  overflow: auto;
  margin: 5px;
}

/* Adjusted font size for input fields */
::v-deep(.smaller-el-input .el-input__inner) {
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
::v-deep .el-checkbox {
  font-size: 12px;
  height: 20px;
  line-height: 20px;
  display: flex;
  align-items: center;
}

::v-deep .el-checkbox .el-checkbox__input {
  height: 16px;
  width: 16px;
  margin: 0;
}

::v-deep .el-checkbox .el-checkbox__inner {
  height: 16px;
  width: 16px;
}

::v-deep .el-checkbox .el-checkbox__label {
  font-size: 12px;
  line-height: 20px;
  margin-left: 4px;
}
</style>