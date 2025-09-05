<template>
  <div v-if="dataLoaded && nodeManualInput">
    <generic-node-settings v-model="nodeManualInput">
      <div class="settings-section">
        <div class="table-container">
          <table class="modern-table">
            <thead>
              <tr>
                <td v-for="col in columns" :key="'delete-' + col.id">
                  <button class="delete-button" @click="deleteColumn(col.id)" />
                </td>
              </tr>
              <tr>
                <th v-for="col in columns" :key="col.id">
                  <div class="column-header">
                    <input v-model="col.name" class="input-header" type="text" />
                    <el-select v-model="col.dataType" size="small" class="type-select">
                      <el-option
                        v-for="dtype in dataTypes"
                        :key="dtype"
                        :label="dtype"
                        :value="dtype"
                      />
                    </el-select>
                  </div>
                </th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="row in rows" :key="row.id">
                <td v-for="col in columns" :key="col.id">
                  <input v-model="row.values[col.id]" class="input-cell" type="text" />
                </td>
                <td>
                  <button class="delete-button" @click="deleteRow(row.id)" />
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- Table Controls -->
        <div class="controls-section">
          <div class="button-group">
            <el-button type="primary" size="small" @click="addColumn">
              <template #icon><i class="fas fa-plus" /></template>
              Add Column
            </el-button>
            <el-button type="primary" size="small" @click="addRow">
              <template #icon><i class="fas fa-plus" /></template>
              Add Row
            </el-button>
            <el-button type="primary" size="small" @click="toggleRawData">
              <template #icon>
                <i :class="showRawData ? 'fas fa-eye-slash' : 'fas fa-eye'" />
              </template>
              {{ showRawData ? "Hide" : "Show" }} Raw Data
            </el-button>
          </div>
        </div>

        <!-- Raw Data Editor -->
        <el-collapse-transition>
          <div v-if="showRawData" class="raw-data-section">
            <el-input
              v-model="rawDataString"
              type="textarea"
              :rows="8"
              :placeholder="JSON.stringify({ column1: 'value1' }, null, 2)"
            />
            <div class="raw-data-controls">
              <el-button type="primary" size="small" @click="updateTableFromRawData">
                Update Table
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
    console.log("nodeManualInput.value from input", nodeManualInput.value)
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
    console.log("nodeManualInput.value no data available", nodeManualInput.value)
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
  padding: 1.25rem;
  background: var(--el-bg-color);
  border-radius: 8px;
  margin-bottom: 1rem;
}

.table-container {
  max-height: 400px;
  overflow: auto;
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 8px;
  margin-bottom: 1rem;
}

.modern-table {
  width: 100%;
  border-collapse: separate;
  border-spacing: 0;
  background: white;
}

.modern-table th,
.modern-table td {
  padding: 0.5rem;
  border: 1px solid var(--el-border-color-lighter);
}

.column-header {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
}

.type-select {
  width: 100%;
}

.type-select :deep(.el-input__inner) {
  font-size: 0.75rem;
  padding: 0 0.5rem;
}

.input-header,
.input-cell {
  width: 100%;
  border: none;
  padding: 0.25rem;
  font-size: 0.875rem;
  background: transparent;
}

.input-header {
  font-weight: 500;
}

.controls-section {
  margin: 1rem 0;
}

.button-group {
  display: flex;
  gap: 0.5rem;
}

.delete-button {
  width: 20px;
  height: 20px;
  padding: 0;
  border: none;
  background: transparent;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #cecece;
  transition: color 0.2s;
  position: relative;
  margin: 0 auto;
}

.delete-button::before,
.delete-button::after {
  content: "";
  position: absolute;
  width: 10px;
  height: 2px;
  background-color: currentColor;
  transform: rotate(45deg);
}

.delete-button::after {
  transform: rotate(-45deg);
}

.delete-button:hover {
  color: #ff4d4f;
}

.raw-data-section {
  margin-top: 1rem;
  padding: 1rem;
  background: var(--el-bg-color-page);
  border-radius: 8px;
}

.raw-data-controls {
  margin-top: 0.5rem;
  display: flex;
  justify-content: flex-end;
}
</style>
