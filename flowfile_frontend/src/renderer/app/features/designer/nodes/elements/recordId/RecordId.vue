<template>
  <div v-if="dataLoaded && nodeRecordId" class="listbox-wrapper">
    <generic-node-settings v-model="nodeRecordId">
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Settings</div>
        <el-row>
          <el-col :span="10" class="grid-content">Offset</el-col>
          <el-col :span="8" class="grid-content"
            ><input v-model="nodeRecordId.record_id_input.offset" type="number" min="0" step="1"
          /></el-col>
        </el-row>
        <el-row>
          <el-col :span="10" class="grid-content">Output name</el-col>
          <el-col :span="8" class="grid-content"
            ><input v-model="nodeRecordId.record_id_input.output_column_name" type="text"
          /></el-col>
        </el-row>
      </div>
      <div class="listbox-wrapper">
        <el-checkbox
          v-model="nodeRecordId.record_id_input.group_by"
          label="Assign record id by group"
          size="large"
        />
        <div v-if="nodeRecordId.record_id_input.group_by">
          <div class="listbox-subtitle">Optional Settings</div>
          <ul class="listbox">
            <div class="column-options">
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
            </div>
            <ContextMenu
              v-if="showContextMenu"
              id="record-id-menu"
              ref="contextMenuRef"
              :position="contextMenuPosition"
              :options="contextMenuOptions"
              @select="handleContextMenuSelect"
              @close="closeContextMenu"
            />
            <SettingsSection
              title="Group by columns"
              :items="nodeRecordId.record_id_input.group_by_columns"
              droppable="true"
              @remove-item="removeColumn('add', $event)"
              @dragover.prevent
              @drop="onDropInSection('add')"
            />
          </ul>
        </div>
      </div>
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import { ref, onMounted, onUnmounted, nextTick, defineExpose } from "vue";
import { NodeRecordId } from "../../../baseNode/nodeInput";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { useNodeStore } from "../../../../../stores/column-store";
import ContextMenu from "../pivot/ContextMenu.vue";
import SettingsSection from "../pivot/SettingsSection.vue";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const showContextMenu = ref(false);
const contextMenuPosition = ref({ x: 0, y: 0 });
const dataLoaded = ref(false);
const contextMenuRef = ref<HTMLElement | null>(null);
const nodeRecordId = ref<null | NodeRecordId>(null);
const nodeData = ref<null | NodeData>(null);
const contextMenuOptions = ref<{ label: string; action: string; disabled: boolean }[]>([]);
const draggedColumnName = ref<string | null>(null);
const selectedColumns = ref<string[]>([]);

const getColumnClass = (columnName: string): string => {
  return selectedColumns.value.includes(columnName) ? "is-selected" : "";
};

const onDropInSection = (section: "add") => {
  if (draggedColumnName.value) {
    if (
      section === "add" &&
      !nodeRecordId.value?.record_id_input.group_by_columns.includes(draggedColumnName.value)
    ) {
      nodeRecordId.value?.record_id_input.group_by_columns.push(draggedColumnName.value);
    }
    draggedColumnName.value = null;
  }
};

const closeContextMenu = () => {
  showContextMenu.value = false;
};

const handleItemClick = (columnName: string) => {
  selectedColumns.value = [columnName];
};

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeRecordId.value = nodeData.value?.setting_input;
  if (!nodeData.value?.setting_input.is_setup && nodeRecordId.value) {
    nodeRecordId.value.record_id_input = {
      offset: 1,
      output_column_name: "record_id",
      group_by: false,
      group_by_columns: [],
    };
  }
  dataLoaded.value = true;
  if (nodeRecordId.value?.is_setup) {
    nodeRecordId.value.is_setup = true;
  }
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

const openContextMenu = (columnName: string, event: MouseEvent) => {
  selectedColumns.value = [columnName];
  contextMenuPosition.value = { x: event.clientX, y: event.clientY };

  contextMenuOptions.value = [
    {
      label: "Group by",
      action: "add",
      disabled: isColumnAssigned(columnName),
    },
  ];
  showContextMenu.value = true;
};

const handleClickOutside = (event: MouseEvent) => {
  if (!contextMenuRef.value?.contains(event.target as Node)) {
    showContextMenu.value = false;
  }
};

const isColumnAssigned = (columnName: string): boolean => {
  if (nodeRecordId.value) {
    return nodeRecordId.value.record_id_input.group_by_columns.includes(columnName);
  }
  return false;
};

const handleContextMenuSelect = (action: "add") => {
  const nodeRecord = nodeRecordId.value;

  if (nodeRecord && action === "add") {
    selectedColumns.value
      .filter((col) => !isColumnAssigned(col))
      .forEach((col) => {
        nodeRecord.record_id_input.group_by_columns.push(col);
      });
  }
};

const removeColumn = (type: "add", column: string) => {
  if (nodeRecordId.value)
    if (type === "add") {
      nodeRecordId.value.record_id_input.group_by_columns =
        nodeRecordId.value.record_id_input.group_by_columns.filter((col) => col !== column);
    }
};

const pushNodeData = async () => {
  nodeStore.updateSettings(nodeRecordId);
};

defineExpose({
  loadNodeData,
  pushNodeData,
});

onMounted(async () => {
  await nextTick();
  window.addEventListener("click", handleClickOutside);
});

onUnmounted(() => {
  window.removeEventListener("click", handleClickOutside);
});
</script>
