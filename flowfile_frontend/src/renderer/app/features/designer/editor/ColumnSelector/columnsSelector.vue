<template>
  <div v-if="filteredChildren.length === 0" class="empty-hint">
    {{ menuContents.children?.length ? "No matching fields" : "No input fields" }}
  </div>
  <div v-for="(child, index) in filteredChildren" :key="index" class="cool-button-container">
    <button class="cool-button" :title="child.label" @click="handleButtonClick(child)">
      <span class="col-name">{{ child.label }}</span>
      <span v-if="child.data_type" class="type-badge" :class="badgeClass(child.data_type_group)">
        {{ child.data_type }}
      </span>
    </button>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed, watch, onMounted, defineEmits } from "vue";
import { MenuContents, ColumnSelectorInterface } from "./types";
import { useNodeStore } from "../../../../stores/column-store";
import { FileColumn } from "../../../../components/nodes/baseNode/nodeInterfaces";

const props = defineProps<{ filterText?: string }>();

const emit = defineEmits<{
  (event: "value-selected", payload: string): void;
}>();

const ranVal = ref(0);
const showOptions = ref(false);
const nodeStore = useNodeStore();
const menuContents = ref<MenuContents>({
  title: "Existing fields",
  icon: "room",
  children: [],
});

const filteredChildren = computed<ColumnSelectorInterface[]>(() => {
  const children = menuContents.value.children ?? [];
  const q = props.filterText?.trim().toLowerCase() ?? "";
  if (!q) return children;
  return children.filter((c) => c.label.toLowerCase().includes(q));
});

watch(
  () => nodeStore.nodeData?.main_input?.table_schema,
  (newColumns) => {
    if (newColumns) {
      updateColumnData(newColumns);
    }
  },
  { deep: true },
);

const handleButtonClick = (columnSelector: ColumnSelectorInterface) => {
  const val = columnSelector.node_type === "c" ? `[${columnSelector.label}]` : columnSelector.label;
  ranVal.value++;
  emit("value-selected", val);
};

const badgeClass = (group?: string): string => {
  switch (group) {
    case "Numeric":
      return "badge-numeric";
    case "String":
      return "badge-string";
    case "Date":
      return "badge-date";
    case "Boolean":
      return "badge-boolean";
    case "Complex":
      return "badge-complex";
    case "Binary":
      return "badge-binary";
    default:
      return "badge-other";
  }
};

const updateColumnData = (columns: FileColumn[]) => {
  const childrenNodes: ColumnSelectorInterface[] = columns.map((col) => ({
    label: col.name,
    hasAction: true,
    node_type: "c",
    name: col.name,
    data_type: col.data_type,
    data_type_group: col.data_type_group,
  }));
  if (menuContents.value) {
    menuContents.value.children = childrenNodes;
  }
};

onMounted(async () => {
  if (nodeStore.nodeData?.main_input?.columns) {
    updateColumnData(nodeStore.nodeData.main_input.table_schema);
  }
});

defineExpose({ showOptions });
</script>

<style scoped>
.cool-button-container {
  display: flex;
  flex-direction: column;
  align-items: stretch;
}

.cool-button {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 6px;
  width: 100%;
  border: none;
  color: var(--color-text-primary);
  background-color: transparent;
  padding: 3px 5px;
  text-align: left;
  margin: 1px 0;
  cursor: pointer;
  border-radius: 4px;
}

.cool-button:hover {
  background-color: var(--color-background-hover);
}

.col-name {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.type-badge {
  flex-shrink: 0;
  font-size: 10px;
  font-weight: 500;
  line-height: 1.6;
  padding: 0 6px;
  border-radius: 999px;
  white-space: nowrap;
}

.badge-numeric {
  color: var(--color-info-hover);
  background-color: var(--color-info-light);
}

.badge-string {
  color: var(--color-success-hover);
  background-color: var(--color-success-light);
}

.badge-date {
  color: var(--color-warning-dark);
  background-color: var(--color-warning-light);
}

.badge-boolean {
  color: var(--color-accent-purple-hover);
  background-color: var(--color-focus-ring-purple-light);
}

.badge-complex {
  color: var(--color-accent-hover);
  background-color: var(--color-accent-subtle);
}

.badge-binary,
.badge-other {
  color: var(--color-text-tertiary);
  background-color: var(--color-background-tertiary);
}

.empty-hint {
  padding: 6px 5px;
  font-size: 12px;
  font-style: italic;
  color: var(--color-text-muted);
}

.sidebar-title {
  font-size: 16px;
  font-weight: bold;
  padding: 10px;
  color: var(--color-text-primary);
}
</style>
