<template>
  <div class="component-container">
    <label class="listbox-subtitle">{{ schema.label }}</label>

    <!-- Group By and Order By Configuration -->
    <div class="config-section">
      <div class="config-row">
        <label class="config-label">Group By (optional)</label>
        <el-select
          v-model="localValue.group_by_columns"
          multiple
          filterable
          placeholder="Select columns to group by..."
          style="width: 100%"
          size="default"
          @change="emitUpdate"
        >
          <el-option
            v-for="column in allColumns"
            :key="column.name"
            :label="column.name"
            :value="column.name"
          >
            <span>{{ column.name }}</span>
            <span class="column-type">{{ column.data_type }}</span>
          </el-option>
        </el-select>
      </div>

      <div class="config-row">
        <label class="config-label">Order By (optional)</label>
        <el-select
          v-model="localValue.order_by_column"
          filterable
          clearable
          placeholder="Select column to order by..."
          style="width: 100%"
          size="default"
          @change="emitUpdate"
        >
          <el-option
            v-for="column in allColumns"
            :key="column.name"
            :label="column.name"
            :value="column.name"
          >
            <span>{{ column.name }}</span>
            <span class="column-type">{{ column.data_type }}</span>
          </el-option>
        </el-select>
      </div>
    </div>

    <!-- Column List for Selection -->
    <div class="column-list-wrapper">
      <div class="listbox-subtitle">Available Columns</div>
      <ul class="listbox">
        <li
          v-for="(column, index) in filteredColumns"
          :key="column.name"
          :class="{ 'is-selected': selectedColumns.includes(column.name) }"
          @click="handleColumnClick(index, column.name, $event)"
          @contextmenu.prevent="openContextMenu(column.name, $event)"
        >
          {{ column.name }} ({{ column.data_type }})
        </li>
      </ul>
    </div>

    <!-- Context Menu -->
    <div
      v-if="showContextMenu"
      ref="contextMenuRef"
      class="context-menu"
      :style="{
        top: contextMenuPosition.y + 'px',
        left: contextMenuPosition.x + 'px',
      }"
    >
      <button
        v-for="func in schema.available_functions"
        :key="func"
        @click="addOperation(func)"
      >
        Rolling {{ func }}
      </button>
    </div>

    <!-- Operations Table -->
    <div class="listbox-subtitle">Rolling Operations</div>
    <div v-if="localValue.operations.length > 0" class="table-wrapper">
      <table class="styled-table">
        <thead>
          <tr>
            <th>Column</th>
            <th>Function</th>
            <th>Window Size</th>
            <th>Min Periods</th>
            <th>Output Name</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="(operation, index) in localValue.operations"
            :key="index"
          >
            <td>{{ operation.column }}</td>
            <td>
              <el-select
                v-model="operation.function"
                size="small"
                @change="updateOutputName(index)"
              >
                <el-option
                  v-for="func in schema.available_functions"
                  :key="func"
                  :label="func"
                  :value="func"
                />
              </el-select>
            </td>
            <td>
              <el-input-number
                v-model="operation.window_size"
                :min="1"
                :max="1000"
                size="small"
                style="width: 100px"
                @change="updateOutputName(index)"
              />
            </td>
            <td>
              <el-input-number
                v-model="operation.min_periods"
                :min="1"
                :max="operation.window_size"
                size="small"
                style="width: 100px"
                placeholder="Default"
              />
            </td>
            <td>
              <el-input
                v-model="operation.output_name"
                size="small"
                placeholder="Auto-generated"
              />
            </td>
            <td>
              <el-button
                type="danger"
                size="small"
                :icon="Delete"
                circle
                @click="removeOperation(index)"
              />
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <div v-else class="empty-state">
      <p>No rolling operations configured.</p>
      <p class="hint">Right-click on a column above to add a rolling operation.</p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, onUnmounted, PropType } from "vue";
import { Delete } from "@element-plus/icons-vue";
import type { RollingWindowInputComponent, RollingOperationValue } from "../interface";
import type { FileColumn } from "../../../../baseNode/nodeInterfaces";

interface RollingWindowValue {
  operations: RollingOperationValue[];
  group_by_columns: string[];
  order_by_column: string | null;
}

const props = defineProps({
  schema: {
    type: Object as PropType<RollingWindowInputComponent>,
    required: true,
  },
  modelValue: {
    type: Object as PropType<RollingWindowValue>,
    default: () => ({
      operations: [],
      group_by_columns: [],
      order_by_column: null,
    }),
  },
  incomingColumns: {
    type: Array as PropType<FileColumn[]>,
    default: () => [],
  },
});

const emit = defineEmits(["update:modelValue"]);

// Local state
const localValue = ref<RollingWindowValue>({
  operations: [],
  group_by_columns: [],
  order_by_column: null,
});

const selectedColumns = ref<string[]>([]);
const showContextMenu = ref(false);
const contextMenuPosition = ref({ x: 0, y: 0 });
const contextMenuRef = ref<HTMLElement | null>(null);
const firstSelectedIndex = ref<number | null>(null);

// Computed properties
const allColumns = computed(() => props.incomingColumns);

const filteredColumns = computed(() => {
  if (!props.schema.data_types || props.schema.data_types === "ALL") {
    return props.incomingColumns;
  }
  if (Array.isArray(props.schema.data_types)) {
    return props.incomingColumns.filter((column) =>
      props.schema.data_types.includes(column.data_type)
    );
  }
  return props.incomingColumns;
});

// Initialize local value from props
watch(
  () => props.modelValue,
  (newValue) => {
    if (newValue) {
      localValue.value = {
        operations: newValue.operations || [],
        group_by_columns: newValue.group_by_columns || [],
        order_by_column: newValue.order_by_column || null,
      };
    }
  },
  { immediate: true, deep: true }
);

// Emit updates
const emitUpdate = () => {
  emit("update:modelValue", { ...localValue.value });
};

// Column selection handlers
const handleColumnClick = (clickedIndex: number, columnName: string, event: MouseEvent) => {
  if (event.shiftKey && firstSelectedIndex.value !== null) {
    const start = Math.min(firstSelectedIndex.value, clickedIndex);
    const end = Math.max(firstSelectedIndex.value, clickedIndex);
    selectedColumns.value = filteredColumns.value
      .slice(start, end + 1)
      .map((col) => col.name);
  } else {
    if (selectedColumns.value.length === 1 && selectedColumns.value[0] === columnName) {
      selectedColumns.value = [];
      firstSelectedIndex.value = null;
    } else {
      firstSelectedIndex.value = clickedIndex;
      selectedColumns.value = [columnName];
    }
  }
};

const openContextMenu = (columnName: string, event: MouseEvent) => {
  event.preventDefault();
  if (!selectedColumns.value.includes(columnName)) {
    selectedColumns.value = [columnName];
  }
  contextMenuPosition.value = { x: event.clientX, y: event.clientY };
  showContextMenu.value = true;
};

// Operation management
const addOperation = (func: string) => {
  selectedColumns.value.forEach((column) => {
    const windowSize = 3;
    const newOperation: RollingOperationValue = {
      column,
      function: func,
      window_size: windowSize,
      output_name: `${column}_rolling_${func}_${windowSize}`,
      min_periods: null,
    };
    localValue.value.operations.push(newOperation);
  });
  showContextMenu.value = false;
  selectedColumns.value = [];
  emitUpdate();
};

const removeOperation = (index: number) => {
  localValue.value.operations.splice(index, 1);
  emitUpdate();
};

const updateOutputName = (index: number) => {
  const op = localValue.value.operations[index];
  op.output_name = `${op.column}_rolling_${op.function}_${op.window_size}`;
  emitUpdate();
};

// Click outside handler
const handleClickOutside = (event: MouseEvent) => {
  if (!contextMenuRef.value?.contains(event.target as Node)) {
    showContextMenu.value = false;
  }
};

onMounted(() => {
  window.addEventListener("click", handleClickOutside);
});

onUnmounted(() => {
  window.removeEventListener("click", handleClickOutside);
});
</script>

<style scoped>
.component-container {
  display: flex;
  flex-direction: column;
  gap: 1rem;
}

.config-section {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
  padding: 1rem;
  background-color: var(--color-background-secondary, #f5f7fa);
  border-radius: 8px;
}

.config-row {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
}

.config-label {
  font-size: 0.875rem;
  font-weight: 500;
  color: var(--color-text-secondary, #606266);
}

.column-list-wrapper {
  margin-top: 0.5rem;
}

.listbox {
  max-height: 200px;
  overflow-y: auto;
  border: 1px solid var(--color-border-primary, #dcdfe6);
  border-radius: 8px;
  padding: 0;
  margin: 0;
  list-style: none;
}

.listbox li {
  padding: 8px 12px;
  cursor: pointer;
  transition: background-color 0.2s;
}

.listbox li:hover {
  background-color: var(--color-background-hover, #f5f7fa);
}

.listbox li.is-selected {
  background-color: var(--color-primary-light, #ecf5ff);
  color: var(--color-primary, #409eff);
}

.column-type {
  font-size: 0.75rem;
  color: var(--color-text-tertiary, #909399);
  margin-left: 8px;
}

.context-menu {
  position: fixed;
  z-index: 1000;
  border: 1px solid #ccc;
  background-color: var(--color-background-primary, #fff);
  padding: 8px;
  box-shadow: 0 2px 10px rgba(0, 0, 0, 0.2);
  border-radius: 4px;
  user-select: none;
}

.context-menu button {
  display: block;
  background: none;
  border: none;
  padding: 6px 12px;
  text-align: left;
  width: 100%;
  cursor: pointer;
  border-radius: 4px;
  font-size: 0.875rem;
}

.context-menu button:hover {
  background-color: var(--color-background-hover, #f0f0f0);
}

.table-wrapper {
  max-height: 300px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
  border-radius: 8px;
  overflow: auto;
}

.styled-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.875rem;
}

.styled-table thead {
  background-color: var(--color-background-secondary, #f5f7fa);
  position: sticky;
  top: 0;
}

.styled-table th,
.styled-table td {
  padding: 10px 12px;
  text-align: left;
  border-bottom: 1px solid var(--color-border-primary, #ebeef5);
}

.styled-table th {
  font-weight: 600;
  color: var(--color-text-secondary, #606266);
}

.styled-table tbody tr:hover {
  background-color: var(--color-background-hover, #f5f7fa);
}

.empty-state {
  padding: 2rem;
  text-align: center;
  color: var(--color-text-secondary, #909399);
  background-color: var(--color-background-secondary, #f5f7fa);
  border-radius: 8px;
}

.empty-state p {
  margin: 0;
}

.empty-state .hint {
  font-size: 0.875rem;
  margin-top: 0.5rem;
  color: var(--color-text-tertiary, #c0c4cc);
}
</style>
