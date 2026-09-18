<template>
  <div v-if="isFolded" class="rail-strip">
    <button
      v-for="option in radioOptions"
      :key="option.value"
      type="button"
      class="rail-strip-button"
      :title="option.text"
      @mousedown.prevent
      @click="expandTo(option.value)"
    >
      <i :class="option.icon" />
    </button>
  </div>
  <div v-else-if="showSideBar" class="options-container" :style="{ width: treeNodeWidth }">
    <button
      v-if="collapsible"
      type="button"
      class="rail-fold"
      title="Hide the field panel"
      @mousedown.prevent
      @click="emit('update:collapsed', true)"
    >
      «
    </button>
    <sidebar v-model="optionSelection" :options="radioOptions" />
    <div class="divider" />
    <div class="search-box">
      <i class="fas fa-search search-icon" />
      <input
        v-model="filterText"
        class="search-input"
        type="text"
        :placeholder="searchPlaceholder"
      />
      <i v-if="filterText" class="fas fa-times clear-icon" @click="filterText = ''" />
    </div>
    <div class="selector">
      <column-selector
        v-if="optionSelection === 'fields'"
        :filter-text="filterText"
        :table-schema="tableSchema"
        @value-selected="emit('value-selected', $event)"
      />
      <param-selector
        v-else-if="optionSelection === 'parameters'"
        :filter-text="filterText"
        :parameters="parameters"
        @value-selected="emit('value-selected', $event)"
      />
      <func-selector
        v-else
        :filter-text="filterText"
        @value-selected="emit('value-selected', $event)"
      />
    </div>
  </div>
  <div v-if="!isFolded" class="resizer" @mousedown="initResize"></div>
</template>

<script lang="ts" setup>
import { ref, computed, onBeforeUnmount } from "vue";
import ColumnSelector from "./ColumnSelector/columnsSelector.vue";
import ParamSelector from "./ParamSelector/ParamSelector.vue";
import Sidebar from "./Sidebar/Sidebar.vue";
import FuncSelector from "./FuncSelector/FuncSelector.vue";
import type { FlowParameter } from "../../../types/flow.types";
import type { EditorSchemaColumn } from "./ColumnSelector/types";

// The fields/functions/parameters rail shared by the formula-style editors.
// `tableSchema` left undefined keeps columnsSelector's default main-input read.
const props = defineProps<{
  tableSchema?: EditorSchemaColumn[] | null;
  parameters?: FlowParameter[];
  // Opt-in: hosts that can spare the width (Filter/Gate) keep the rail always open.
  collapsible?: boolean;
  collapsed?: boolean;
}>();

const emit = defineEmits<{
  (event: "value-selected", payload: string): void;
  (event: "update:collapsed", payload: boolean): void;
}>();

const isFolded = computed(() => !!props.collapsible && !!props.collapsed);

const expandTo = (value: string) => {
  optionSelection.value = value;
  emit("update:collapsed", false);
};

const optionSelection = ref("fields");
const filterText = ref("");
const startX = ref(0);
const startWidth = ref(0);
const treeNodeWidth = ref("200px");

const radioOptions = [
  { value: "fields", text: "Fields", icon: "fa fa-columns" },
  { value: "functions", text: "Functions", icon: "fas fa-atom" },
  { value: "parameters", text: "Parameters", icon: "fas fa-sliders-h" },
];

const showSideBar = computed(() => parseInt(treeNodeWidth.value.replace("px", "")) > 50);

const searchPlaceholder = computed(() => {
  if (optionSelection.value === "fields") return "Filter fields";
  if (optionSelection.value === "parameters") return "Filter parameters";
  return "Filter functions";
});

const tableSchema = computed(() => props.tableSchema);
const parameters = computed(() => props.parameters ?? []);

const resizeWidth = (event: MouseEvent) => {
  const deltaX = event.clientX - startX.value;
  const newWidth = startWidth.value + deltaX;
  treeNodeWidth.value = Math.max(50, Math.min(newWidth, 300)) + "px";
};

const initResize = (event: MouseEvent) => {
  startX.value = event.clientX;
  startWidth.value = parseInt(treeNodeWidth.value.replace("px", ""));
  document.addEventListener("mousemove", resizeWidth);
  document.addEventListener("mouseup", stopResize);
};

const stopResize = () => {
  document.removeEventListener("mousemove", resizeWidth);
  document.removeEventListener("mouseup", stopResize);
};
onBeforeUnmount(stopResize);
</script>

<style scoped>
.selector {
  overflow-y: auto;
  min-height: 0;
  flex: 1;
}

.rail-strip {
  display: flex;
  flex-direction: column;
  flex-shrink: 0;
  gap: 2px;
  width: 36px;
  padding: 6px 0;
  border-right: 1px solid var(--color-border-primary);
  background-color: var(--color-background-primary);
}

.rail-strip-button {
  width: 100%;
  height: 28px;
  color: var(--color-text-tertiary);
  background: transparent;
  border: none;
  cursor: pointer;
}

.rail-strip-button:hover {
  color: var(--color-accent);
  background: var(--color-background-hover);
}

.rail-fold {
  width: 100%;
  padding: 2px 0;
  font-size: 0.8rem;
  color: var(--color-text-tertiary);
  background: transparent;
  border: none;
  text-align: right;
  cursor: pointer;
}

.rail-fold:hover {
  color: var(--color-text-primary);
}

.options-container {
  display: flex;
  flex-direction: column;
  flex-shrink: 0;
  min-width: 50px;
  max-height: 300px;
  padding-left: 5px;
  padding-right: 5px;
  z-index: 1;
  overflow-y: auto;
  background-color: var(--color-background-primary);
  color: var(--color-text-primary);
}

.resizer {
  width: 5px;
  cursor: ew-resize;
  background-color: var(--color-border-primary);
  border-right: 0.5px solid var(--color-border-secondary);
  flex-shrink: 0;
}

.divider {
  border-top: 1px solid var(--color-border-primary);
  padding-bottom: 10px;
}

.search-box {
  flex-shrink: 0;
  position: relative;
  display: flex;
  align-items: center;
  margin-bottom: 6px;
}

.search-box .search-icon {
  position: absolute;
  left: 7px;
  font-size: 10px;
  color: var(--color-text-muted);
  pointer-events: none;
}

.search-input {
  width: 100%;
  box-sizing: border-box;
  padding: 4px 22px 4px 22px;
  font-size: 12px;
  color: var(--color-text-primary);
  background-color: var(--color-background-secondary);
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  outline: none;
}

.search-input:focus {
  border-color: var(--color-border-focus);
  box-shadow: 0 0 0 2px var(--color-focus-ring-accent);
}

.search-input::placeholder {
  color: var(--color-text-muted);
}

.search-box .clear-icon {
  position: absolute;
  right: 7px;
  font-size: 10px;
  color: var(--color-text-muted);
  cursor: pointer;
}

.search-box .clear-icon:hover {
  color: var(--color-text-secondary);
}
</style>
