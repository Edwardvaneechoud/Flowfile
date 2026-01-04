<template>
  <div v-if="dataLoaded && nodeManualInput">
    <generic-node-settings v-model="nodeManualInput">
      <div class="settings-section">
        <!-- Table Controls - Moved to top for better visibility -->
        <div class="controls-section controls-top">
          <div class="button-group">
            <el-button type="primary" size="small" @click="addColumn">
              <template #icon><i class="fas fa-plus" /></template>
              Add Column
            </el-button>
            <el-button type="primary" size="small" @click="addRow">
              <template #icon><i class="fas fa-plus" /></template>
              Add Row
            </el-button>
            <el-button size="small" @click="toggleRawData">
              <template #icon>
                <i :class="showRawData ? 'fas fa-eye-slash' : 'fas fa-code'" />
              </template>
              {{ showRawData ? "Hide JSON" : "Edit JSON" }}
            </el-button>
          </div>
          <div class="table-info">
            <span class="info-badge">{{ columns.length }} columns</span>
            <span class="info-badge">{{ rows.length }} rows</span>
          </div>
        </div>

        <div class="table-container">
          <table class="modern-table">
            <thead>
              <tr class="header-row">
                <th class="row-number-header">#</th>
                <th
                  v-for="col in columns"
                  :key="col.id"
                  class="column-header-cell"
                >
                  <div class="column-header">
                    <div class="header-top">
                      <input
                        v-model="col.name"
                        class="input-header"
                        type="text"
                        :placeholder="`Column ${col.id}`"
                        @focus="selectAll($event)"
                      />
                      <button
                        class="delete-column-btn"
                        title="Delete column"
                        @click="deleteColumn(col.id)"
                      >
                        <i class="fas fa-times" />
                      </button>
                    </div>
                    <div class="header-type">
                      <el-select
                        v-model="col.dataType"
                        size="small"
                        class="type-select"
                        :teleported="false"
                      >
                        <el-option
                          v-for="dtype in dataTypes"
                          :key="dtype"
                          :label="dtype"
                          :value="dtype"
                        />
                      </el-select>
                    </div>
                  </div>
                </th>
                <th class="actions-header"></th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(row, rowIndex) in rows" :key="row.id" class="data-row">
                <td class="row-number">{{ rowIndex + 1 }}</td>
                <td v-for="col in columns" :key="col.id" class="data-cell">
                  <input
                    v-model="row.values[col.id]"
                    class="input-cell"
                    type="text"
                    @focus="selectAll($event)"
                    @keydown="handleCellKeydown($event, row, col)"
                  />
                </td>
                <td class="row-actions">
                  <button
                    class="delete-row-btn"
                    title="Delete row"
                    @click="deleteRow(row.id)"
                  >
                    <i class="fas fa-times" />
                  </button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- Raw Data Editor -->
        <el-collapse-transition>
          <div v-if="showRawData" class="raw-data-section">
            <div class="raw-data-header">
              <span class="raw-data-title">JSON Editor</span>
              <span class="raw-data-hint">Edit the data as JSON array</span>
            </div>
            <el-input
              v-model="rawDataString"
              type="textarea"
              :rows="10"
              :placeholder="JSON.stringify([{ column1: 'value1' }], null, 2)"
              class="json-editor"
            />
            <div class="raw-data-controls">
              <el-button type="primary" size="small" @click="updateTableFromRawData">
                <template #icon><i class="fas fa-sync" /></template>
                Apply JSON to Table
              </el-button>
            </div>
          </div>
        </el-collapse-transition>
      </div>
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed, watch } from "vue";
import { useNodeStore } from "../../../../../stores/column-store";
import { createManualInput } from "./manualInputLogic";
import type {
  NodeManualInput,
  MinimalFieldInput,
  RawDataFormat,
} from "../../../baseNode/nodeInput";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import { ElNotification } from "element-plus";

interface Column {
  id: number;
  name: string;
  dataType?: string;
}

interface Row {
  id: number;
  values: Record<number, string>;
}

const nodeStore = useNodeStore();

const dataLoaded = ref(false);
const nodeManualInput = ref<NodeManualInput | null>(null);
const columns = ref<Column[]>([]);
const rows = ref<Row[]>([]);
const showRawData = ref(false);
const rawDataString = ref("");

let nextColumnId = 1;
let nextRowId = 1;

const dataTypes = nodeStore.getDataTypes();
const rawData = computed(() => {
  return rows.value.map((row) => {
    const obj: Record<string, string> = {};
    for (const col of columns.value) {
      obj[col.name] = row.values[col.id];
    }
    return obj;
  });
});

const rawDataFormat = computed((): RawDataFormat => {
  const formattedColumns: MinimalFieldInput[] = columns.value.map((col) => ({
    name: col.name,
    data_type: col.dataType || "String",
  }));

  const data: unknown[][] = columns.value.map((col) =>
    rows.value.map((row) => row.values[col.id] || ""),
  );

  return {
    columns: formattedColumns,
    data: data,
  };
});

const initializeEmptyTable = () => {
  rows.value = [{ id: 1, values: { 1: "" } }];
  columns.value = [{ id: 1, name: "Column 1", dataType: "String" }];
  nextColumnId = 2;
  nextRowId = 2;
};

const populateTableFromData = (data: Record<string, string>[]) => {
  rows.value = [];
  columns.value = [];

  data.forEach((item, rowIndex) => {
    const row: Row = { id: rowIndex + 1, values: {} };
    Object.keys(item).forEach((key, colIndex) => {
      if (rowIndex === 0) {
        columns.value.push({ id: colIndex + 1, name: key, dataType: "String" });
      }
      row.values[colIndex + 1] = item[key];
    });
    rows.value.push(row);
  });

  nextColumnId = columns.value.length + 1;
  nextRowId = rows.value.length + 1;
};

const populateTableFromRawDataFormat = (rawDataFormat: RawDataFormat) => {
  rows.value = [];
  columns.value = [];

  if (rawDataFormat.columns) {
    rawDataFormat.columns.forEach((col, index) => {
      columns.value.push({
        id: index + 1,
        name: col.name,
        dataType: col.data_type || "String",
      });
    });
  }

  const numRows = rawDataFormat.data[0]?.length || 0;
  for (let rowIndex = 0; rowIndex < numRows; rowIndex++) {
    const row: Row = { id: rowIndex + 1, values: {} };
    rawDataFormat.data.forEach((colData, colIndex) => {
      row.values[colIndex + 1] = String(colData[rowIndex] || "");
    });
    rows.value.push(row);
  }

  if (numRows === 0 && columns.value.length > 0) {
    const emptyRow: Row = { id: 1, values: {} };
    columns.value.forEach((col) => {
      emptyRow.values[col.id] = "";
    });
    rows.value.push(emptyRow);
    nextRowId = 2;
  } else {
    nextRowId = numRows + 1;
  }

  nextColumnId = columns.value.length + 1;
};

const loadNodeData = async (nodeId: number) => {
  const nodeResult = await nodeStore.getNodeData(nodeId, false);

  if (nodeResult?.setting_input) {
    nodeManualInput.value = nodeResult.setting_input;
    console.log("nodeManualInput.value from input", nodeManualInput.value);
    if (
      nodeResult.setting_input.raw_data_format &&
      nodeResult.setting_input.raw_data_format.columns &&
      nodeResult.setting_input.raw_data_format.data
    ) {
      populateTableFromRawDataFormat(nodeResult.setting_input.raw_data_format);
    } else if (nodeResult.setting_input.raw_data) {
      populateTableFromData(nodeResult.setting_input.raw_data);
    } else {
      initializeEmptyTable();
    }
  } else {
    nodeManualInput.value = createManualInput(nodeStore.flow_id, nodeStore.node_id).value;
    console.log("nodeManualInput.value no data available", nodeManualInput.value);
    initializeEmptyTable();
  }

  rawDataString.value = JSON.stringify(rawData.value, null, 2);
  dataLoaded.value = true;
};

const addColumn = () => {
  columns.value.push({
    id: nextColumnId,
    name: `Column ${nextColumnId}`,
    dataType: "String",
  });
  nextColumnId++;
};

const addRow = () => {
  const newRow: Row = { id: nextRowId, values: {} };
  columns.value.forEach((col) => {
    newRow.values[col.id] = "";
  });
  rows.value.push(newRow);
  nextRowId++;
};

const deleteColumn = (id: number) => {
  const index = columns.value.findIndex((col) => col.id === id);
  if (index !== -1) {
    columns.value.splice(index, 1);
    rows.value.forEach((row) => {
      delete row.values[id];
    });
  }
};

const deleteRow = (id: number) => {
  const index = rows.value.findIndex((row) => row.id === id);
  if (index !== -1) {
    rows.value.splice(index, 1);
  }
};

const toggleRawData = () => {
  showRawData.value = !showRawData.value;
};

const selectAll = (event: FocusEvent) => {
  const target = event.target as HTMLInputElement;
  target.select();
};

const handleCellKeydown = (event: KeyboardEvent, row: Row, col: Column) => {
  if (event.key === "Tab" && !event.shiftKey) {
    const colIndex = columns.value.findIndex((c) => c.id === col.id);
    const rowIndex = rows.value.findIndex((r) => r.id === row.id);

    // If last column and last row, add new row
    if (colIndex === columns.value.length - 1 && rowIndex === rows.value.length - 1) {
      event.preventDefault();
      addRow();
      // Focus first cell of new row after Vue updates DOM
      setTimeout(() => {
        const newRowCells = document.querySelectorAll('.data-row:last-child .input-cell');
        if (newRowCells.length > 0) {
          (newRowCells[0] as HTMLInputElement).focus();
        }
      }, 0);
    }
  }
};

const updateTableFromRawData = () => {
  try {
    const newData = JSON.parse(rawDataString.value);
    if (!Array.isArray(newData)) {
      ElNotification({
        title: "Error",
        message: "Data must be an array of objects",
        type: "error",
      });
      return;
    }
    populateTableFromData(newData);
    ElNotification({
      title: "Success",
      message: "Table updated successfully",
      type: "success",
    });
  } catch (error) {
    ElNotification({
      title: "Error",
      message: "Invalid JSON format. Please check your input.",
      type: "error",
    });
  }
};

const pushNodeData = async () => {
  if (nodeManualInput.value) {
    // Always save in the new format
    nodeManualInput.value.raw_data_format = rawDataFormat.value;
    await nodeStore.updateSettings(nodeManualInput);
  }
  dataLoaded.value = false;
};

// Watchers
watch(rawData, (newVal) => {
  rawDataString.value = JSON.stringify(newVal, null, 2);
});

defineExpose({
  loadNodeData,
  pushNodeData,
});
</script>

<style scoped>
.settings-section {
  padding: var(--spacing-4);
  background: var(--color-background-primary);
  border-radius: var(--border-radius-lg);
}

/* Controls Section */
.controls-section {
  margin-bottom: var(--spacing-3);
}

.controls-top {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: var(--spacing-2);
}

.button-group {
  display: flex;
  gap: var(--spacing-2);
  flex-wrap: wrap;
}

.table-info {
  display: flex;
  gap: var(--spacing-2);
}

.info-badge {
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  background: var(--color-background-muted);
  padding: var(--spacing-1) var(--spacing-2);
  border-radius: var(--border-radius-sm);
}

/* Table Container */
.table-container {
  max-height: 350px;
  overflow: auto;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-secondary);
}

/* Modern Table */
.modern-table {
  width: 100%;
  border-collapse: collapse;
  table-layout: fixed;
}

/* Header Row */
.header-row {
  position: sticky;
  top: 0;
  z-index: 10;
  background: var(--color-background-muted);
}

.row-number-header {
  width: 40px;
  min-width: 40px;
  max-width: 40px;
  text-align: center;
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
  border-bottom: 2px solid var(--color-border-light);
  border-right: 1px solid var(--color-border-light);
  background: var(--color-background-muted);
  position: sticky;
  left: 0;
  z-index: 11;
}

.column-header-cell {
  min-width: 140px;
  width: 140px;
  padding: 0;
  border-bottom: 2px solid var(--color-border-light);
  border-right: 1px solid var(--color-border-light);
  vertical-align: top;
}

.column-header {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.header-top {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  padding: var(--spacing-2);
  padding-bottom: var(--spacing-1);
}

.input-header {
  flex: 1;
  min-width: 0;
  border: none;
  background: transparent;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
  padding: var(--spacing-1);
  border-radius: var(--border-radius-sm);
  outline: none;
}

.input-header:focus {
  background: var(--color-background-primary);
  box-shadow: 0 0 0 1px var(--color-primary);
}

.input-header::placeholder {
  color: var(--color-text-muted);
}

.delete-column-btn {
  width: 20px;
  height: 20px;
  padding: 0;
  border: none;
  background: transparent;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--color-text-muted);
  border-radius: var(--border-radius-sm);
  transition: all var(--transition-fast);
  flex-shrink: 0;
}

.delete-column-btn:hover {
  color: var(--color-danger);
  background: var(--color-danger-bg, rgba(239, 68, 68, 0.1));
}

.delete-column-btn i {
  font-size: 10px;
}

.header-type {
  padding: 0 var(--spacing-2) var(--spacing-2);
}

.type-select {
  width: 100%;
}

.type-select :deep(.el-input__wrapper) {
  background: var(--color-background-primary);
  box-shadow: none;
  border: 1px solid var(--color-border-light);
}

.type-select :deep(.el-input__inner) {
  font-size: var(--font-size-xs);
  height: 24px;
}

.type-select :deep(.el-select__caret) {
  font-size: 10px;
}

.actions-header {
  width: 36px;
  min-width: 36px;
  max-width: 36px;
  border-bottom: 2px solid var(--color-border-light);
  background: var(--color-background-muted);
}

/* Data Rows */
.data-row {
  transition: background var(--transition-fast);
}

.data-row:hover {
  background: var(--color-background-hover);
}

.data-row:hover .row-number {
  background: var(--color-background-hover);
}

.row-number {
  width: 40px;
  min-width: 40px;
  max-width: 40px;
  text-align: center;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  border-bottom: 1px solid var(--color-border-light);
  border-right: 1px solid var(--color-border-light);
  background: var(--color-background-muted);
  position: sticky;
  left: 0;
  z-index: 5;
}

.data-cell {
  min-width: 140px;
  width: 140px;
  padding: 0;
  border-bottom: 1px solid var(--color-border-light);
  border-right: 1px solid var(--color-border-light);
}

.input-cell {
  width: 100%;
  height: 100%;
  border: none;
  background: transparent;
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  padding: var(--spacing-2);
  outline: none;
}

.input-cell:focus {
  background: var(--color-background-primary);
  box-shadow: inset 0 0 0 2px var(--color-primary);
}

.row-actions {
  width: 36px;
  min-width: 36px;
  max-width: 36px;
  text-align: center;
  border-bottom: 1px solid var(--color-border-light);
  padding: var(--spacing-1);
}

.delete-row-btn {
  width: 24px;
  height: 24px;
  padding: 0;
  border: none;
  background: transparent;
  cursor: pointer;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  color: var(--color-text-muted);
  border-radius: var(--border-radius-sm);
  transition: all var(--transition-fast);
  opacity: 0;
}

.data-row:hover .delete-row-btn {
  opacity: 1;
}

.delete-row-btn:hover {
  color: var(--color-danger);
  background: var(--color-danger-bg, rgba(239, 68, 68, 0.1));
}

.delete-row-btn i {
  font-size: 10px;
}

/* Raw Data Section */
.raw-data-section {
  margin-top: var(--spacing-4);
  padding: var(--spacing-4);
  background: var(--color-background-muted);
  border-radius: var(--border-radius-lg);
  border: 1px solid var(--color-border-light);
}

.raw-data-header {
  display: flex;
  align-items: baseline;
  gap: var(--spacing-2);
  margin-bottom: var(--spacing-3);
}

.raw-data-title {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.raw-data-hint {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.json-editor :deep(.el-textarea__inner) {
  font-family: var(--font-mono);
  font-size: var(--font-size-xs);
  line-height: 1.5;
  background: var(--color-background-primary);
}

.raw-data-controls {
  margin-top: var(--spacing-3);
  display: flex;
  justify-content: flex-end;
}

/* Custom Scrollbar */
.table-container::-webkit-scrollbar {
  width: 8px;
  height: 8px;
}

.table-container::-webkit-scrollbar-track {
  background: var(--color-background-muted);
  border-radius: 4px;
}

.table-container::-webkit-scrollbar-thumb {
  background-color: var(--color-gray-400);
  border-radius: 4px;
}

.table-container::-webkit-scrollbar-thumb:hover {
  background-color: var(--color-gray-500);
}

.table-container::-webkit-scrollbar-corner {
  background: var(--color-background-muted);
}
</style>
