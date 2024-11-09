<template>
  <div id="page-container">
    <div class="listbox-wrapper">
      <!-- Table -->
      <div v-if="dataLoaded" class="table-container">
        <table class="modern-table">
          <thead>
            <tr>
              <td v-for="col in columns" :key="'delete-' + col.id">
                <button class="delete-button" @click="deleteColumn(col.id)" />
              </td>
            </tr>
            <tr>
              <th v-for="col in columns" :key="col.id">
                <input v-model="col.name" class="input-header" type="text" />
              </th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in rows" :key="row.id">
              <td v-for="col in columns" :key="col.id">
                <input
                  v-model="row.values[col.id]"
                  class="input-cell"
                  type="text"
                />
              </td>
              <td>
                <button class="delete-button" @click="deleteRow(row.id)" />
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- Controls between table and textarea -->
      <div class="controls">
        <button class="control-button" @click="addColumn">Add Column</button>
        <button class="control-button" @click="addRow">Add Row</button>
        <button class="control-button" @click="hideShowRaw">
          {{ showDataText }} raw data
        </button>
      </div>
      <!-- Display and Edit Raw Data -->
      <div v-if="showRawData">
        <textarea v-model="rawDataString" class="raw-data-box"></textarea>

        <!-- Button below textarea -->
        <div>
          <button class="control-button" @click="updateTableFromRawData">
            Update Table
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, nextTick, defineProps } from "vue";
import { useNodeStore } from "../../../../../stores/column-store";
import { createManualInput } from "./manualInputLogic";
import { onMounted } from "vue";
const nodeStore = useNodeStore();
const manualInput = createManualInput();
const dataLoaded = ref(false);
const columns = ref<Column[]>([]);
const rows = ref<Row[]>([]);
let nextColumnId = 1;
let nextRowId = 1;
const showRawData = ref<boolean>(false);
const showDataText = ref<string>("Show");
const props = defineProps({ nodeId: { type: Number, required: true } });

const rawData = computed(() => {
  return rows.value.map((row) => {
    const obj: { [key: string]: string } = {};
    for (const col of columns.value) {
      obj[col.name] = row.values[col.id];
    }
    return obj;
  });
});

interface Column {
  id: number;
  name: string;
}

interface Row {
  id: number;
  values: { [key: number]: string };
}

interface RawDataItem {
  [key: string]: string; // or whatever type you expect
}
const loadNodeData = async (nodeId: number) => {
  const nodeResult = await nodeStore.getNodeData(1, nodeId, false);
  console.log("nodeResult", nodeResult);
  if (nodeResult) {
    if (nodeResult.setting_input && nodeResult.setting_input.raw_data) {
      manualInput.value = nodeResult.setting_input;
      const newData = nodeResult.setting_input.raw_data;
      rows.value = [];
      columns.value = [];
      // Populate new columns and rows
      newData.forEach((item: RawDataItem, rowIndex: number) => {
        const row: Row = { id: rowIndex + 1, values: {} };
        Object.keys(item).forEach((key, colIndex) => {
          if (rowIndex === 0) {
            columns.value.push({ id: colIndex + 1, name: key });
          }
          row.values[colIndex + 1] = item[key];
        });
        rows.value.push(row);
      });
    } else {
      manualInput.value = createManualInput(
        nodeStore.flow_id,
        nodeStore.node_id,
      ).value;
      rows.value = [{ id: 1, values: { "1": "" } }];
      columns.value = [{ id: 1, name: "Column 1" }];
      manualInput.value.raw_data = [];
    }
    nextColumnId = columns.value.length + 1;
    nextRowId = rows.value.length + 1;
  }
  dataLoaded.value = true;
  nodeStore.isDrawerOpen = true;
};

const addColumn = () => {
  columns.value.push({ id: nextColumnId, name: `Column ${nextColumnId}` });
  nextColumnId++;
};

const hideShowRaw = () => {
  showRawData.value = !showRawData.value;
  showDataText.value = showRawData.value ? "Hide" : "Show"; // Corrected this logic
};

const addRow = () => {
  const newRow: Row = { id: nextRowId, values: {} };
  for (const col of columns.value) {
    newRow.values[col.id] = "";
  }
  rows.value.push(newRow);
  nextRowId++;
};

const deleteColumn = (id: number) => {
  const index = columns.value.findIndex((col) => col.id === id);
  if (index !== -1) {
    columns.value.splice(index, 1);
    for (const row of rows.value) {
      delete row.values[id];
    }
  }
};

const deleteRow = (id: number) => {
  const index = rows.value.findIndex((row) => row.id === id);
  if (index !== -1) {
    rows.value.splice(index, 1);
  }
};

const rawDataString = ref(JSON.stringify(rawData.value, null, 2));

watch(rawData, (newVal) => {
  rawDataString.value = JSON.stringify(newVal, null, 2);
});

const updateTableFromRawData = () => {
  try {
    const newData = JSON.parse(rawDataString.value);

    if (!Array.isArray(newData)) {
      alert("Invalid format: Must be an array of objects.");
      return;
    }
    rows.value = [];
    columns.value = [];
    newData.forEach((item, rowIndex) => {
      const row: Row = { id: rowIndex + 1, values: {} };
      Object.keys(item).forEach((key, colIndex) => {
        if (rowIndex === 0) {
          columns.value.push({ id: colIndex + 1, name: key });
        }
        row.values[colIndex + 1] = item[key];
      });
      rows.value.push(row);
    });

    nextColumnId = columns.value.length + 1;
    nextRowId = rows.value.length + 1;
  } catch (e) {
    alert("Invalid JSON format");
  }
};

const pushNodeData = async () => {
  dataLoaded.value = false;
  manualInput.value.raw_data = rawData;
  nodeStore.updateSettings(manualInput);
};

onMounted(async () => {
  await nextTick();
});

defineExpose({
  loadNodeData,
  pushNodeData,
});
</script>

<style scoped>
/* Base Table */
.modern-table {
  width: 100%;
  border-collapse: separate;
  border-spacing: 0;
  margin-bottom: 20px;
  font-family: var(--font-family-base);
}

.table-container {
  max-width: 100%;
  max-height: 600px;
  overflow: auto;
  border-radius: 8px;
  box-shadow: var(--shadow-default);
  background: white;
}

/* Header and Cell Styles */
.modern-table th,
.modern-table td {
  border: 1px solid var(--border-color);
  padding: 8px;
  text-align: center;
  background-color: white;
  position: relative;
  min-width: 120px;
}

/* Special handling for delete column */
.modern-table td:last-child,
.modern-table th:last-child {
  min-width: 20px; /* Narrower width for delete column */
  border: none;
  background-color: transparent;
}

.modern-table th {
  background-color: #fafafa;
  color: var(--primary-blue);
  position: sticky;
  top: 0;
  z-index: 2;
  box-shadow: 0 2px 2px -1px rgba(0, 0, 0, 0.05);
}

/* Input Styles */
.input-header,
.input-cell {
  width: 100%;
  border: none;
  outline: none;
  text-align: center;
  background: transparent;
  font-size: 12px;
  transition: all 0.2s ease;
  /* Override browser defaults */
  padding: 0;
  margin: 0;
  writing-mode: horizontal-tb;
  -webkit-writing-mode: horizontal-tb !important;
  padding-block: 0;
  padding-inline: 0;
}

/* Add specific padding we want */
.input-header {
  font-weight: 500;
  color: var(--primary-blue);
}

.input-cell {
  color: #333;
}

.input-header:focus,
.input-cell:focus {
  background-color: var(--hover-blue);
}

/* Compact Delete Button */
.delete-button {
  background-color: transparent;
  color: #cecece;
  border: none;
  padding: 4px;
  cursor: pointer;
  width: 16px;
  height: 16px;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.2s ease;
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
  transform: scale(1.1);
}

/* Control Buttons */
.control-button {
  background-color: var(--primary-blue);
  color: white;
  border: none;
  padding: 8px 16px;
  margin: 5px;
  cursor: pointer;
  border-radius: 6px;
  font-size: 12px;
  transition: all 0.2s ease;
}

.control-button:hover {
  background-color: var(--primary-blue-hover);
}

/* Custom Scrollbar */
.table-container::-webkit-scrollbar {
  width: 6px;
  height: 6px;
}

.table-container::-webkit-scrollbar-track {
  background: transparent;
}

.table-container::-webkit-scrollbar-thumb {
  background-color: #e0e0e0;
  border-radius: 3px;
}

.table-container::-webkit-scrollbar-thumb:hover {
  background-color: #cecece;
}
</style>
