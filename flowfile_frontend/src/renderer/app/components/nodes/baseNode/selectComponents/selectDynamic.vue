<template>
  <div ref="rootRef" class="select-dynamic-root">
    <div v-if="dataLoaded" class="select-dynamic-content">
      <div v-if="hasMissingFields" class="remove-missing-fields" @click="removeMissingFields">
        <UnavailableField tooltip-text="Field not available click for removing them for memory" />
        <span>Remove Missing Fields</span>
      </div>

      <div v-if="props.showTitle" class="listbox-subtitle">
        {{ props.title }}
      </div>

      <div class="listbox-wrapper select-dynamic-listbox">
        <div class="column-list-toolbar">
          <!-- Acts on the selection, so kept apart from the global controls -->
          <div v-if="canReorderSelection" class="column-list-selection">
            <span class="column-list-selection-count">
              {{ selection.selectedIndices.length }} selected
            </span>
            <button
              v-for="action in reorderActions"
              :key="action.command"
              class="btn btn-sm btn-ghost"
              type="button"
              :title="action.title"
              :aria-label="action.title"
              :disabled="!canMove(action.command)"
              @mousedown.prevent
              @click="moveSelectedTo(action.command)"
            >
              {{ action.glyph }}
            </button>
            <button
              class="btn btn-sm btn-ghost column-list-clear"
              type="button"
              title="Clear selection"
              aria-label="Clear selection"
              @mousedown.prevent
              @click="clearSelection"
            >
              ✕
            </button>
          </div>
          <span v-else class="column-list-count">
            {{ keptCount }} of {{ localSelectInputs.length }} kept
          </span>

          <div class="search-container column-list-search">
            <input
              v-model="filterText"
              class="search-input"
              type="search"
              placeholder="Filter columns…"
              aria-label="Filter columns"
              v-bind="NO_AUTOFILL"
            />
          </div>
          <div class="column-list-actions">
            <template v-if="props.showKeepOption">
              <button
                class="btn btn-sm btn-ghost"
                type="button"
                title="Keep all columns"
                @mousedown.prevent
                @click="setKeepForAll(true)"
              >
                ☑
              </button>
              <button
                class="btn btn-sm btn-ghost"
                type="button"
                title="Keep no columns"
                @mousedown.prevent
                @click="setKeepForAll(false)"
              >
                ☐
              </button>
            </template>
          </div>
        </div>

        <div class="table-wrapper">
          <table class="styled-table column-list">
            <colgroup>
              <col style="width: 22px" />
              <col v-if="props.showOldColumns" />
              <col v-if="props.showNewColumns" />
              <col v-if="props.showDataType" style="width: 130px" />
              <col v-if="props.showKeepOption" style="width: 52px" />
            </colgroup>
            <thead>
              <tr v-if="props.showHeaders">
                <th aria-label="Reorder" />
                <th
                  v-if="props.showOldColumns"
                  class="is-sortable"
                  :title="sortHint"
                  :aria-sort="ariaSort"
                  @click="toggleSort"
                >
                  <span class="th-inner">
                    <span class="th-label">{{ originalColumnHeader }}</span>
                    <span class="sort-glyph">{{
                      sortDirection === "asc" ? "▲" : sortDirection === "desc" ? "▼" : "⇅"
                    }}</span>
                  </span>
                </th>
                <th v-if="props.showNewColumns">New column name</th>
                <th v-if="props.showDataType">Data type</th>
                <th v-if="props.showKeepOption" class="is-centered">Keep</th>
              </tr>
            </thead>
            <tbody ref="selectableContainerRef" @dragend="handleDragEnd">
              <tr
                v-for="{ column, index } in visibleRows"
                :key="column.old_name"
                :class="rowClasses(index, column)"
                :draggable="!isFiltered"
                @dragstart="handleDragStart(index, $event)"
                @dragover.prevent="handleDragOver(index, $event)"
                @drop="handleDrop"
              >
                <td class="drag-handle-cell" :title="dragHandleTitle">⠿</td>
                <td
                  v-if="props.showOldColumns"
                  class="column-name-cell"
                  :title="column.old_name"
                  @mousedown="handleItemMouseDown($event)"
                  @click="handleItemClick(index, $event)"
                  @contextmenu.prevent="openContextMenu(index, $event)"
                >
                  <div v-if="!column.is_available" class="unavailable-field">
                    <UnavailableField />
                    <span class="unavailable-name">{{ column.old_name }}</span>
                  </div>
                  <div v-else class="column-name">
                    {{ column.old_name }}
                  </div>
                </td>

                <td v-if="props.showNewColumns">
                  <input
                    v-model="column.new_name"
                    class="inline-input"
                    type="text"
                    :placeholder="column.old_name"
                    :aria-label="`New name for ${column.old_name}`"
                    v-bind="NO_AUTOFILL"
                  />
                </td>

                <td v-if="props.showDataType" :class="{ 'is-changed': isTypeChanged(column) }">
                  <div class="cell-with-marker">
                    <el-select v-model="column.data_type" size="small">
                      <el-option
                        v-for="dataType in dataTypes"
                        :key="dataType"
                        :label="dataType"
                        :value="dataType"
                      />
                    </el-select>
                    <span
                      v-if="isTypeChanged(column)"
                      class="change-marker"
                      :title="`Data type changed from ${originalTypeOf(column) ?? 'the source type'}`"
                      aria-label="Data type changed"
                      >●</span
                    >
                  </div>
                </td>

                <td v-if="props.showKeepOption" class="is-centered">
                  <el-checkbox v-model="column.keep" :aria-label="`Keep ${column.old_name}`" />
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <div
      v-if="showContextMenu"
      ref="contextMenuRef"
      class="context-menu"
      :style="{ top: contextMenuPosition.y + 'px', left: contextMenuPosition.x + 'px' }"
      @click.stop
    >
      <button @click="setKeepForSelected(true)">Select</button>
      <button @click="setKeepForSelected(false)">Deselect</button>
      <div class="context-menu-divider" />
      <button :disabled="!canMove('top')" @click="moveSelectedTo('top')">Move to top</button>
      <button :disabled="!canMove('up')" @click="moveSelectedTo('up')">Move up</button>
      <button :disabled="!canMove('down')" @click="moveSelectedTo('down')">Move down</button>
      <button :disabled="!canMove('bottom')" @click="moveSelectedTo('bottom')">
        Move to bottom
      </button>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed, onMounted, onUnmounted, watch } from "vue";
import { SelectInput } from "../../../../types/node.types";
import { useNodeStore } from "../../../../stores/column-store";
import { NO_AUTOFILL } from "../../../../utils/noAutofill";
import UnavailableField from "./UnavailableFields.vue";
import {
  EMPTY_SELECTION,
  applyClick,
  applyReorder,
  assignPositions,
  clampSelection,
  ensureSelected,
  insertIndexFromPointer,
  isMacPlatform,
  moveItems,
  nextSortDirection,
  partitionAvailable,
  readModifiers,
  reconcileOrder,
  reorderInsertIndex,
  sortForDirection,
  type ReorderCommand,
  type SelectionState,
  type SortDirection,
} from "./columnSelection";

const props = withDefaults(
  defineProps<{
    selectInputs?: SelectInput[];
    showOldColumns?: boolean;
    showNewColumns?: boolean;
    showKeepOption?: boolean;
    showDataType?: boolean;
    title?: string;
    showHeaders?: boolean;
    showTitle?: boolean;
    originalColumnHeader?: string;
  }>(),
  {
    selectInputs: () => [],
    showOldColumns: true,
    showNewColumns: true,
    showKeepOption: false,
    showDataType: false,
    title: "Select columns",
    showHeaders: true,
    showTitle: true,
    originalColumnHeader: "Original column name",
  },
);

/**
 * defineModel rather than a prop+emit pair: only the Select node binds
 * v-model:sortedBy, so every other instance used to be stuck on a permanently
 * "none" prop and its header re-sorted ascending on every click. The local
 * fallback gives those instances a working three-state toggle.
 */
const sortedBy = defineModel<SortDirection>("sortedBy", { default: "none" });

/** NodeSelect.sorted_by is optional, so the model can legitimately arrive undefined. */
const sortDirection = computed<SortDirection>(() => sortedBy.value ?? "none");

/**
 * Only the Select node populates original_position; Join/CrossJoin/FuzzyMatch/
 * Unique leave it null, so "restore original order" falls back to the order the
 * parent still holds — which the component never reorders, only re-stamps.
 */
const originalPositionOf = (input: SelectInput): number => {
  if (Number.isFinite(input.original_position)) return input.original_position;
  const index = props.selectInputs.findIndex((s) => s.old_name === input.old_name);
  return index === -1 ? Number.MAX_SAFE_INTEGER : index;
};

const toggleSort = () => {
  sortedBy.value = nextSortDirection(sortDirection.value);
  localSelectInputs.value = sortForDirection(
    localSelectInputs.value,
    sortDirection.value,
    (input) => input.old_name,
    originalPositionOf,
  );
  assignPositions(localSelectInputs.value);
  // Row indices no longer mean what the selection recorded.
  clearSelection();
};

/**
 * Source data type per column, captured the first time the component sees it, so
 * an unsaved edit shows up immediately. A column that arrives already carrying a
 * saved type change has no recoverable source type here — `data_type_change` is
 * the persisted signal for those. (`is_altered` is not: the backend also sets it
 * for a plain rename.)
 */
const sourceDataTypes = new Map<string, string | undefined>();

const rememberSourceTypes = (inputs: readonly SelectInput[]) => {
  inputs.forEach((input) => {
    if (!sourceDataTypes.has(input.old_name) && !input.data_type_change) {
      sourceDataTypes.set(input.old_name, input.data_type);
    }
  });
};

const originalTypeOf = (column: SelectInput) => sourceDataTypes.get(column.old_name);

const isTypeChanged = (column: SelectInput) => {
  const source = sourceDataTypes.get(column.old_name);
  return source === undefined ? Boolean(column.data_type_change) : source !== column.data_type;
};

// State and Store
const dataLoaded = ref(true);
const selection = ref<SelectionState>({ ...EMPTY_SELECTION, selectedIndices: [] });

const clearSelection = () => {
  selection.value = { anchorIndex: null, selectedIndices: [] };
};
const contextMenuPosition = ref({ x: 0, y: 0 });
const showContextMenu = ref(false);
/** Indices captured at dragstart — the block that will move on drop. */
const draggingIndices = ref<number[]>([]);
/** Insertion gap in 0..n, or null when no drag is in flight. */
const dropIndex = ref<number | null>(null);
const contextMenuRef = ref<HTMLElement | null>(null);
const rootRef = ref<HTMLElement | null>(null);
const isMac = isMacPlatform();
// Ref on the <tbody> instead of a global id — avoids collisions if more than
// one selectDynamic instance is mounted at the same time.
const selectableContainerRef = ref<HTMLElement | null>(null);
const nodeStore = useNodeStore();
const dataTypes = nodeStore.getDataTypes();

rememberSourceTypes(props.selectInputs);

const localSelectInputs = ref<SelectInput[]>(
  partitionAvailable(props.selectInputs, (input) => input.is_available),
);

/**
 * Merge rather than replace. The previous watchEffect re-derived the list from
 * props on any tracked change — including a parent handing back an equivalent
 * array — which silently threw away whatever order the user had dragged into
 * place. Skipped while a drag is in flight.
 */
watch(
  () => props.selectInputs,
  (incoming) => {
    rememberSourceTypes(incoming);
    if (dropIndex.value !== null) return;
    const merged = reconcileOrder(localSelectInputs.value, incoming, (input) => input.old_name);
    localSelectInputs.value =
      sortDirection.value === "none"
        ? partitionAvailable(merged, (input) => input.is_available)
        : merged;
    selection.value = clampSelection(selection.value, localSelectInputs.value.length);
  },
  { deep: true },
);

const filterText = ref("");

const isFiltered = computed(() => filterText.value.trim().length > 0);

/**
 * Rows carry their index in the unfiltered list, because every selection and
 * reorder operation is expressed against that list. Reordering is disabled while
 * a filter is active — a drop position between two visible rows is ambiguous.
 */
const visibleRows = computed(() => {
  const query = filterText.value.trim().toLowerCase();
  return localSelectInputs.value
    .map((column, index) => ({ column, index }))
    .filter(
      ({ column }) =>
        !query ||
        column.old_name.toLowerCase().includes(query) ||
        (column.new_name ?? "").toLowerCase().includes(query),
    );
});

const keptCount = computed(() => localSelectInputs.value.filter((c) => c.keep).length);

const canReorderSelection = computed(
  () => !isFiltered.value && selection.value.selectedIndices.length > 0,
);

const reorderActions: { command: ReorderCommand; glyph: string; title: string }[] = [
  { command: "top", glyph: "⤒", title: "Move selected to top" },
  { command: "up", glyph: "↑", title: "Move selected up" },
  { command: "down", glyph: "↓", title: "Move selected down" },
  { command: "bottom", glyph: "⤓", title: "Move selected to bottom" },
];

const dragHandleTitle = computed(() =>
  isFiltered.value ? "Clear the filter to reorder columns" : "Drag to reorder",
);

const sortHint = computed(() =>
  sortDirection.value === "none"
    ? "Sort A→Z"
    : sortDirection.value === "asc"
      ? "Sort Z→A"
      : "Restore original order",
);

const ariaSort = computed(() =>
  sortDirection.value === "asc"
    ? "ascending"
    : sortDirection.value === "desc"
      ? "descending"
      : "none",
);

const setKeepForAll = (keep: boolean) => {
  localSelectInputs.value.forEach((column) => {
    column.keep = keep;
  });
};

// Helper Methods
const isSelected = (index: number) => selection.value.selectedIndices.includes(index);

const rowClasses = (index: number, column: SelectInput) => ({
  "is-selected": isSelected(index),
  "is-unavailable": !column.is_available,
  "is-dragging": draggingIndices.value.includes(index),
  "is-drop-before": dropIndex.value === index,
  "is-drop-after":
    dropIndex.value === localSelectInputs.value.length &&
    index === localSelectInputs.value.length - 1,
});

/** Commit a reordered array, keeping object identity so `position` reaches the parent. */
const commitOrder = (items: SelectInput[], selectedIndices: number[]) => {
  localSelectInputs.value = assignPositions(items);
  selection.value = { anchorIndex: selectedIndices[0] ?? null, selectedIndices };
};

// Drag & Drop Handlers
const handleDragStart = (index: number, event: DragEvent) => {
  // Dragging a row outside the selection collapses onto it first, so the block
  // that moves is always the block the user can see highlighted.
  selection.value = ensureSelected(selection.value, index);
  draggingIndices.value = [...selection.value.selectedIndices];
  event.dataTransfer?.setData("text", "");
  if (event.dataTransfer) event.dataTransfer.effectAllowed = "move";
};

const handleDragOver = (index: number, event: DragEvent) => {
  if (event.dataTransfer) event.dataTransfer.dropEffect = "move";
  const rect = (event.currentTarget as HTMLElement).getBoundingClientRect();
  dropIndex.value = insertIndexFromPointer(rect.top, rect.height, event.clientY, index);
};

const handleDrop = () => {
  if (dropIndex.value !== null && draggingIndices.value.length > 0) {
    const result = moveItems(localSelectInputs.value, draggingIndices.value, dropIndex.value);
    if (result.changed) commitOrder(result.items, result.selectedIndices);
  }
  handleDragEnd();
};

const handleDragEnd = () => {
  draggingIndices.value = [];
  dropIndex.value = null;
};

// Item Selection and Context Menu
const handleItemMouseDown = (event: MouseEvent) => {
  // Without this the browser extends its own text selection from wherever the
  // caret happens to sit, painting the whole drawer blue.
  if (event.shiftKey) {
    event.preventDefault();
    window.getSelection()?.removeAllRanges();
  }
};

const handleItemClick = (clickedIndex: number, event: MouseEvent) => {
  selection.value = applyClick(selection.value, clickedIndex, readModifiers(event, isMac));
};

const openContextMenu = (clickedIndex: number, event: MouseEvent) => {
  event.stopPropagation();
  selection.value = ensureSelected(selection.value, clickedIndex);
  contextMenuPosition.value = { x: event.clientX, y: event.clientY };
  showContextMenu.value = true;
};

const setKeepForSelected = (keep: boolean) => {
  selection.value.selectedIndices.forEach((index) => {
    const column = localSelectInputs.value[index];
    if (column) column.keep = keep;
  });
  showContextMenu.value = false;
  clearSelection();
};

const canMove = (command: ReorderCommand) =>
  reorderInsertIndex(selection.value.selectedIndices, localSelectInputs.value.length, command) !==
  null;

const moveSelectedTo = (command: ReorderCommand) => {
  const result = applyReorder(localSelectInputs.value, selection.value.selectedIndices, command);
  if (result.changed) commitOrder(result.items, result.selectedIndices);
  showContextMenu.value = false;
};

const hasMissingFields = computed(() =>
  localSelectInputs.value.some((column) => !column.is_available),
);

const handleClickOutside = (event: MouseEvent) => {
  const target = event.target as HTMLElement;

  // The menu floats over the table, so there is no "outside the tbody" to click.
  if (showContextMenu.value && contextMenuRef.value && !contextMenuRef.value.contains(target)) {
    showContextMenu.value = false;
  }

  // Scoped to this instance: Join/CrossJoin/FuzzyMatch mount two pickers at once.
  const insideThisPicker = rootRef.value?.contains(target) ?? false;
  const keepsSelection =
    insideThisPicker &&
    Boolean(target.closest?.("tbody tr") || target.closest?.(".column-list-selection"));
  if (!keepsSelection) {
    clearSelection();
  }
};

onMounted(() => {
  window.addEventListener("click", handleClickOutside);
});

onUnmounted(() => {
  window.removeEventListener("click", handleClickOutside);
});

// Emit and Expose
const emit = defineEmits<{ (e: "updateSelectInputs", inputs: SelectInput[]): void }>();

const removeMissingFields = () => {
  // Re-stamp like commitOrder does: only Select re-stamps again on save, so the
  // other pickers would ship the gaps left by the removed rows.
  const availableColumns = assignPositions(
    localSelectInputs.value.filter((column) => column.is_available),
  );
  localSelectInputs.value = availableColumns;
  selection.value = clampSelection(selection.value, availableColumns.length);
  emit("updateSelectInputs", availableColumns);
};
</script>

<style scoped>
/* Context menu styling */
.context-menu {
  position: fixed;
  z-index: 1000;
  border: 1px solid var(--color-border-primary);
  background-color: var(--color-background-primary);
  padding: 8px;
  box-shadow: var(--shadow-lg);
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
  color: var(--color-text-primary);
}

.context-menu button:hover {
  background-color: var(--color-background-hover);
}

/* Overrides only — shadow/radius/margin come from the shared .table-wrapper */
.table-wrapper {
  max-height: 700px;
}

/* Adjusted font size for input fields */
:deep(.smaller-el-input .el-input__inner) {
  padding: 2px 10px;
  font-size: 12px;
  height: 24px;
  line-height: 20px;
}

.smaller-el-input {
  line-height: 20px;
}

/* Row selection / drag / unavailable states live in styles/components/_column-list.css */

/* Unavailable field styling */
.unavailable-field {
  display: flex;
  align-items: center;
  position: relative;
  pointer-events: auto;
}

/* Tinted rather than a solid slab, matching FuzzyMatch's .rule-card.is-invalid.
   The old #d9534f set no text colour at all. */
.remove-missing-fields {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  margin: var(--spacing-2-5) var(--spacing-5);
  padding: var(--spacing-1) var(--spacing-2);
  border: 1px solid var(--color-danger);
  border-radius: var(--border-radius-sm);
  background-color: var(--color-danger-light);
  color: var(--color-danger);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  cursor: pointer;
  transition: all var(--transition-fast) var(--transition-timing);
}

.remove-missing-fields:hover {
  background-color: var(--color-danger);
  border-color: var(--color-danger-hover);
  color: var(--color-text-inverse);
}

/* Adjustments for el-checkbox component */
:deep(.el-checkbox) {
  font-size: 12px;
  height: 20px;
  line-height: 20px;
  display: flex;
  align-items: center;
}

:deep(.el-checkbox .el-checkbox__input) {
  height: 16px;
  width: 16px;
  margin: 0;
}

:deep(.el-checkbox .el-checkbox__inner) {
  height: 16px;
  width: 16px;
}

:deep(.el-checkbox .el-checkbox__label) {
  font-size: 12px;
  line-height: 20px;
  margin-left: 4px;
}
</style>
