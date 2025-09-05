<template>
  <div v-if="dataLoaded && nodeSort" class="listbox-wrapper">
    <generic-node-settings v-model="nodeSort">
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Columns</div>
        <ul v-if="dataLoaded" class="listbox">
          <li
            v-for="(col_schema, index) in nodeData?.main_input?.table_schema"
            :key="col_schema.name"
            :class="{ 'is-selected': selectedColumns.includes(col_schema.name) }"
            @click="handleItemClick(index, col_schema.name, $event)"
            @contextmenu="openContextMenu(index, col_schema.name, $event)"
          >
            {{ col_schema.name }} ({{ col_schema.data_type }})
          </li>
        </ul>
      </div>
      <div
        v-if="showContextMenu"
        ref="contextMenuRef"
        class="context-menu"
        :style="{
          top: contextMenuPosition.y + 'px',
          left: contextMenuPosition.x + 'px',
        }"
      >
        <button v-if="!singleColumnSelected" @click="setSortSettings('Ascending', selectedColumns)">
          Ascending
        </button>
        <button v-if="singleColumnSelected" @click="setSortSettings('Ascending', selectedColumns)">
          Ascending
        </button>
        <button v-if="singleColumnSelected" @click="setSortSettings('Descending', selectedColumns)">
          Descending
        </button>
      </div>

      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Settings</div>

        <div v-if="dataLoaded" class="table-wrapper">
          <table class="styled-table">
            <thead>
              <tr>
                <th>Field</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              <div v-if="nodeSort">
                <tr
                  v-for="(item, index) in nodeSort.sort_input"
                  :key="index"
                  @contextmenu.prevent="openRowContextMenu($event, index)"
                >
                  <td>{{ item.column }}</td>
                  <td>
                    <el-select v-model="item.how" size="small">
                      <el-option
                        v-for="aggOption in sortOptions"
                        :key="aggOption"
                        :label="aggOption"
                        :value="aggOption"
                      />
                    </el-select>
                  </td>
                </tr>
              </div>
            </tbody>
          </table>
        </div>
        <div
          v-if="showContextMenuRemove"
          class="context-menu"
          :style="{
            top: contextMenuPosition.y + 'px',
            left: contextMenuPosition.x + 'px',
          }"
        >
          <button @click="removeRow">Remove</button>
        </div>
      </div>
    </generic-node-settings>
  </div>
  <code-loader v-else />
</template>

<script lang="ts" setup>
import { ref, onMounted, onUnmounted, computed, nextTick, defineProps } from "vue";
import { NodeSort } from "../../../baseNode/nodeInput";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { useNodeStore } from "../../../../../stores/column-store";
import { CodeLoader } from "vue-content-loader";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const showContextMenu = ref(false);
const showContextMenuRemove = ref(false);
const dataLoaded = ref(false);
const contextMenuPosition = ref({ x: 0, y: 0 });
const contextMenuColumn = ref<string | null>(null);
const contextMenuRef = ref<HTMLElement | null>(null);
const selectedColumns = ref<string[]>([]);
const nodeSort = ref<null | NodeSort>(null);
const nodeData = ref<null | NodeData>(null);
const sortOptions = ["Ascending", "Descending"];
const firstSelectedIndex = ref<number | null>(null);

const openRowContextMenu = (event: MouseEvent, index: number) => {
  event.preventDefault();
  contextMenuPosition.value = { x: event.clientX, y: event.clientY };
  contextMenuRowIndex.value = index; // Save the index of the row where the context menu was opened
  showContextMenuRemove.value = true;
};

const removeRow = () => {
  if (contextMenuRowIndex.value !== null) {
    nodeSort.value?.sort_input.splice(contextMenuRowIndex.value, 1);
  }
  showContextMenuRemove.value = false; // Hide the context menu after removing the row
  contextMenuRowIndex.value = null; // Reset the saved index
};

const contextMenuRowIndex = ref<number | null>(null); // New ref to keep track of which row is being interacted with

const singleColumnSelected = computed(() => selectedColumns.value.length == 1);

const openContextMenu = (clickedIndex: number, columnName: string, event: MouseEvent) => {
  event.preventDefault();
  event.stopPropagation(); // Stop click event from propagating
  if (!selectedColumns.value.includes(columnName)) {
    selectedColumns.value = [columnName];
  }
  contextMenuPosition.value = { x: event.clientX, y: event.clientY };
  showContextMenu.value = true;
};

const setSortSettings = (sortType: string, columns: string[] | null) => {
  if (columns) {
    columns.forEach((column) => {
      nodeSort.value?.sort_input.push({ column: column, how: sortType });
    });
  }
  showContextMenu.value = false; // Hide the context menu after selection
  contextMenuColumn.value = null;
};

const handleItemClick = (clickedIndex: number, columnName: string, event: MouseEvent) => {
  if (event.shiftKey && firstSelectedIndex.value !== null) {
    const range = getRange(firstSelectedIndex.value, clickedIndex);
    selectedColumns.value = range
      .map((index) => nodeData.value?.main_input?.columns[index])
      .filter((col): col is string => col !== undefined);
  } else {
    if (firstSelectedIndex.value === clickedIndex) {
      selectedColumns.value = [];
    } else {
      firstSelectedIndex.value = clickedIndex;
      selectedColumns.value = [columnName];
    }
  }
};

const getRange = (start: number, end: number) => {
  return start < end
    ? [...Array(end - start + 1).keys()].map((i) => i + start)
    : [...Array(start - end + 1).keys()].map((i) => i + end);
};

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeSort.value = nodeData.value?.setting_input;
  if (!nodeData.value?.setting_input.is_setup && nodeSort.value) {
    nodeSort.value.sort_input = [];
  }
  dataLoaded.value = true;
  if (nodeSort.value?.is_setup) {
    nodeSort.value.is_setup = true;
  }
};

const pushNodeData = async () => {
  nodeStore.updateSettings(nodeSort);
};

defineExpose({
  loadNodeData,
  pushNodeData,
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
  z-index: 100;
}

.context-menu button:hover {
  background-color: #f0f0f0;
}

.table-wrapper {
  max-height: 300px; /* Adjust this value as needed */
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1); /* subtle shadow for depth */
  border-radius: 8px; /* rounded corners */
  overflow: auto; /* ensures the rounded corners are applied to the child elements */
  margin: 5px; /* adds a small margin around the table */
}

.context-menu {
  position: fixed;
  z-index: 1000;
  border: 1px solid #ccc;
  background-color: white;
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
