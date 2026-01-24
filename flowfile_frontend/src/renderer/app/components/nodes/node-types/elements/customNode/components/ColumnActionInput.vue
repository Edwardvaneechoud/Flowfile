<template>
  <div class="component-container">
    <label class="listbox-subtitle">{{ schema.label }}</label>

    <!-- Optional Group By and Order By Configuration -->
    <div v-if="schema.show_group_by || schema.show_order_by" class="config-section">
      <div v-if="schema.show_group_by" class="config-row">
        <label class="config-label">Group By (optional)</label>
        <el-select
          v-model="localValue.group_by_columns"
          multiple
          filterable
          placeholder="Select columns to group by..."
          style="width: 100%"
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

      <div v-if="schema.show_order_by" class="config-row">
        <label class="config-label">Order By (optional)</label>
        <el-select
          v-model="localValue.order_by_column"
          filterable
          clearable
          placeholder="Select column to order by..."
          style="width: 100%"
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
      <button v-for="action in schema.actions" :key="action.value" @click="addRow(action.value)">
        {{ action.label }}
      </button>
    </div>

    <!-- Configured Rows Table -->
    <div class="listbox-subtitle">Settings</div>
    <div v-if="localValue.rows.length > 0" class="table-wrapper">
      <table class="styled-table">
        <thead>
          <tr>
            <th>Field</th>
            <th>Action</th>
            <th>Output Field Name</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(row, index) in localValue.rows" :key="index">
            <td>{{ row.column }}</td>
            <td>
              <el-select v-model="row.action" size="small" @change="updateOutputName(index)">
                <el-option
                  v-for="action in schema.actions"
                  :key="action.value"
                  :label="action.label"
                  :value="action.value"
                />
              </el-select>
            </td>
            <td>
              <el-input v-model="row.output_name" size="small" @change="emitUpdate" />
            </td>
            <td class="action-cell">
              <el-button type="danger" circle @click="removeRow(index)">
                <el-icon><Delete /></el-icon>
              </el-button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <div v-else class="empty-state">
      <p>No rows configured.</p>
      <p class="hint">Right-click on a column above to add a row.</p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, onUnmounted, PropType } from "vue";
import { ElIcon } from "element-plus";
import { Delete } from "@element-plus/icons-vue";
import type { ColumnActionInputComponent, ColumnActionRow } from "../interface";
import type { FileColumn } from "../../../../baseNode/nodeInterfaces";

interface ColumnActionValue {
  rows: ColumnActionRow[];
  group_by_columns: string[];
  order_by_column: string | null;
}

const props = defineProps({
  schema: {
    type: Object as PropType<ColumnActionInputComponent>,
    required: true,
  },
  modelValue: {
    type: Object as PropType<ColumnActionValue>,
    default: () => ({
      rows: [],
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
const localValue = ref<ColumnActionValue>({
  rows: [],
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
      props.schema.data_types.includes(column.data_type),
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
        rows: newValue.rows || [],
        group_by_columns: newValue.group_by_columns || [],
        order_by_column: newValue.order_by_column || null,
      };
    }
  },
  { immediate: true, deep: true },
);

// Emit updates
const emitUpdate = () => {
  emit("update:modelValue", { ...localValue.value });
};

// Generate output name from template
const generateOutputName = (column: string, action: string): string => {
  return props.schema.output_name_template.replace("{column}", column).replace("{action}", action);
};

// Column selection handlers
const handleColumnClick = (clickedIndex: number, columnName: string, event: MouseEvent) => {
  if (event.shiftKey && firstSelectedIndex.value !== null) {
    const start = Math.min(firstSelectedIndex.value, clickedIndex);
    const end = Math.max(firstSelectedIndex.value, clickedIndex);
    selectedColumns.value = filteredColumns.value.slice(start, end + 1).map((col) => col.name);
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

// Row management
const addRow = (action: string) => {
  selectedColumns.value.forEach((column) => {
    const newRow: ColumnActionRow = {
      column,
      action,
      output_name: generateOutputName(column, action),
    };
    localValue.value.rows.push(newRow);
  });
  showContextMenu.value = false;
  selectedColumns.value = [];
  emitUpdate();
};

const removeRow = (index: number) => {
  localValue.value.rows.splice(index, 1);
  emitUpdate();
};

const updateOutputName = (index: number) => {
  const row = localValue.value.rows[index];
  row.output_name = generateOutputName(row.column, row.action);
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
  gap: var(--spacing-4);
}

.config-section {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
  padding: var(--spacing-4);
  background-color: var(--color-background-secondary);
  border-radius: var(--border-radius-lg);
}

.config-row {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
}

.config-label {
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
}

.column-list-wrapper {
  margin-top: var(--spacing-2);
}

.listbox-subtitle {
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
  margin-bottom: var(--spacing-2);
}

.listbox {
  max-height: 200px;
  overflow-y: auto;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-lg);
  padding: 0;
  margin: 0;
  list-style: none;
  background-color: var(--color-background-primary);
}

.listbox li {
  padding: var(--spacing-2) var(--spacing-3);
  cursor: pointer;
  transition: background-color var(--transition-normal) var(--transition-timing);
  color: var(--color-text-primary);
}

.listbox li:hover {
  background-color: var(--color-background-hover);
}

.listbox li.is-selected {
  background-color: var(--color-background-selected);
  color: var(--color-accent);
}

.column-type {
  font-size: var(--font-size-sm);
  color: var(--color-text-tertiary);
  margin-left: var(--spacing-2);
}

.context-menu {
  position: fixed;
  z-index: var(--z-index-dropdown);
  border: 1px solid var(--color-border-primary);
  background-color: var(--color-background-primary);
  padding: var(--spacing-2);
  box-shadow: var(--shadow-lg);
  border-radius: var(--border-radius-sm);
  user-select: none;
}

.context-menu button {
  display: block;
  background: none;
  border: none;
  padding: var(--spacing-1-5) var(--spacing-3);
  text-align: left;
  width: 100%;
  cursor: pointer;
  border-radius: var(--border-radius-sm);
  font-size: var(--font-size-base);
  color: var(--color-text-primary);
  transition: background-color var(--transition-fast) var(--transition-timing);
}

.context-menu button:hover {
  background-color: var(--color-background-hover);
}

.table-wrapper {
  max-height: 300px;
  box-shadow: var(--shadow-sm);
  border-radius: var(--border-radius-lg);
  overflow: auto;
  border: 1px solid var(--color-border-primary);
}

.styled-table {
  width: 100%;
  border-collapse: collapse;
  font-size: var(--font-size-base);
  background-color: var(--color-background-primary);
}

.styled-table thead {
  background-color: var(--table-header-bg);
  position: sticky;
  top: 0;
}

.styled-table th,
.styled-table td {
  padding: var(--spacing-2-5) var(--spacing-3);
  text-align: left;
  border-bottom: 1px solid var(--table-border);
}

.styled-table th {
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-secondary);
}

.styled-table td {
  color: var(--color-text-primary);
}

.styled-table tbody tr:hover {
  background-color: var(--table-row-hover);
}

.styled-table th:nth-child(1),
.styled-table td:nth-child(1) {
  width: 25%;
}

.styled-table th:nth-child(2),
.styled-table td:nth-child(2) {
  width: 25%;
}

.styled-table th:nth-child(3),
.styled-table td:nth-child(3) {
  width: 40%;
}

.styled-table th:nth-child(4),
.styled-table td:nth-child(4) {
  width: 10%;
  text-align: center;
}

.action-cell {
  text-align: center;
}

.action-cell .el-button {
  padding: var(--spacing-1);
}

.empty-state {
  padding: var(--spacing-8);
  text-align: center;
  color: var(--color-text-secondary);
  background-color: var(--color-background-secondary);
  border-radius: var(--border-radius-lg);
}

.empty-state p {
  margin: 0;
}

.empty-state .hint {
  font-size: var(--font-size-base);
  margin-top: var(--spacing-2);
  color: var(--color-text-tertiary);
}
</style>
