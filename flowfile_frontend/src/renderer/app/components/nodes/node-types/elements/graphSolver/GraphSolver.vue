<template>
  <div v-if="dataLoaded && nodeGraphSolver" class="listbox-wrapper">
    <generic-node-settings v-model="nodeGraphSolver">
      <div class="listbox-wrapper">
        <ul class="listbox">
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
        </ul>
      </div>

      <ContextMenu
        v-if="showContextMenu"
        id="pivot-context-menu"
        ref="contextMenuRef"
        :position="contextMenuPosition"
        :options="contextMenuOptions"
        @select="handleContextMenuSelect"
        @close="closeContextMenu"
      />

      <div class="listbox-wrapper">
        <SettingsSection
          title="From Column"
          :item="graphSolverInput.col_from ?? ''"
          droppable="true"
          @remove-item="removeColumn('from')"
          @dragover.prevent
          @drop="onDropInSection('from')"
        />
        <SettingsSection
          title="To Column"
          :item="graphSolverInput.col_to ?? ''"
          droppable="true"
          @remove-item="removeColumn('to')"
          @dragover.prevent
          @drop="onDropInSection('to')"
        />
        <div class="listbox-wrapper">
          <div class="listbox-subtitle">Select Output column name</div>
          <el-input
            v-model="graphSolverInput.output_column_name"
            style="width: 240px"
            placeholder="Please input"
          />
        </div>
      </div>
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import { ref, onMounted, onUnmounted, computed, nextTick, defineProps } from "vue";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { GraphSolverInput, NodeGraphSolver } from "../../../baseNode/nodeInput";
import { useNodeStore } from "../../../../../stores/column-store";
import { useNodeSettings } from "../../../../../composables";
import ContextMenu from "./ContextMenu.vue";
import SettingsSection from "./SettingsSection.vue";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const showContextMenu = ref(false);
const dataLoaded = ref(false);
const contextMenuPosition = ref({ x: 0, y: 0 });
const selectedColumns = ref<string[]>([]);
const contextMenuOptions = ref<{ label: string; action: string; disabled: boolean }[]>([]);
const contextMenuRef = ref<HTMLElement | null>(null);
const nodeData = ref<null | NodeData>(null);
const draggedColumnName = ref<string | null>(null);

const graphSolverInput = ref<GraphSolverInput>({
  col_from: "",
  col_to: "",
  output_column_name: "group_column",
});

const nodeGraphSolver = ref<NodeGraphSolver | null>(null);

defineProps({ nodeId: { type: Number, required: true } });

const singleColumnSelected = computed(() => selectedColumns.value.length === 1);

const getColumnClass = (columnName: string): string => {
  return selectedColumns.value.includes(columnName) ? "is-selected" : "";
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

const onDropInSection = (section: "from" | "to") => {
  if (draggedColumnName.value) {
    // Remove column from any existing assignments
    removeColumnIfExists(draggedColumnName.value);

    // Assign the dragged column to the appropriate section
    if (section === "from" && graphSolverInput.value.col_from !== draggedColumnName.value) {
      graphSolverInput.value.col_from = draggedColumnName.value;
    } else if (section === "to") {
      graphSolverInput.value.col_to = draggedColumnName.value;
    }

    draggedColumnName.value = null;
  }
};

const openContextMenu = (columnName: string, event: MouseEvent) => {
  selectedColumns.value = [columnName];
  contextMenuPosition.value = { x: event.clientX, y: event.clientY };

  contextMenuOptions.value = [
    {
      label: "Assign as From",
      action: "from",
      disabled: isColumnAssigned(columnName) || !singleColumnSelected.value,
    },
    {
      label: "Assign as To",
      action: "to",
      disabled: isColumnAssigned(columnName) || !singleColumnSelected.value,
    },
  ];
  showContextMenu.value = true;
};

const handleContextMenuSelect = (action: string) => {
  const column = selectedColumns.value[0];
  if (action === "from" && graphSolverInput.value.col_from !== column) {
    removeColumnIfExists(column);
    graphSolverInput.value.col_from = column;
  } else if (action === "to") {
    removeColumnIfExists(column);
    graphSolverInput.value.col_to = column;
  }
  closeContextMenu();
};

const isColumnAssigned = (columnName: string): boolean => {
  return (
    graphSolverInput.value.col_from === columnName || graphSolverInput.value.col_to === columnName
  );
};

const removeColumnIfExists = (columnName: string) => {
  if (graphSolverInput.value.col_from === columnName) {
    graphSolverInput.value.col_from = "";
  } else if (graphSolverInput.value.col_to === columnName) {
    graphSolverInput.value.col_to = "";
  }
};

const removeColumn = (type: "from" | "to") => {
  if (type === "from") {
    graphSolverInput.value.col_from = "";
  } else if (type === "to") {
    graphSolverInput.value.col_to = "";
  }
};

const handleItemClick = (columnName: string) => {
  selectedColumns.value = [columnName];
};

const loadNodeData = async (nodeId: number) => {
  console.log("loadNodeData from groupby");
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeGraphSolver.value = nodeData.value?.setting_input as NodeGraphSolver;
  if (nodeData.value) {
    if (nodeGraphSolver.value) {
      if (nodeGraphSolver.value.graph_solver_input) {
        graphSolverInput.value = nodeGraphSolver.value.graph_solver_input;
      } else {
        nodeGraphSolver.value.graph_solver_input = graphSolverInput.value;
      }
    }
  }
  dataLoaded.value = true;
};

const handleClickOutside = (event: MouseEvent) => {
  const targetEvent = event.target as HTMLElement;
  if (targetEvent.id === "pivot-context-menu") return;
  showContextMenu.value = false;
};

const closeContextMenu = () => {
  showContextMenu.value = false;
};

const { saveSettings, pushNodeData } = useNodeSettings({
  nodeData: nodeGraphSolver,
});

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});

onMounted(async () => {
  await nextTick();
  window.addEventListener("click", handleClickOutside);
});

onUnmounted(() => {
  window.removeEventListener("click", handleClickOutside);
});
</script>

<style scoped>
.context-menu {
  position: fixed;
  z-index: 1000;
  border: 1px solid #ccc;
  background-color: var(--color-background-primary);
  padding: 8px;
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
