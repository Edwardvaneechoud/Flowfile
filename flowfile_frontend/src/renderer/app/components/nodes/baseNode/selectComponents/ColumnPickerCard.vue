<template>
  <div
    ref="rootRef"
    class="listbox-wrapper picker-card column-list"
    @dragover="onCardDragOver"
    @drop.prevent="onCardDrop"
  >
    <section class="picker-columns" aria-label="Input columns">
      <div class="column-list-toolbar">
        <div v-if="selectedNames.length > 0" class="column-list-selection">
          <span class="column-list-selection-count">{{ selectedNames.length }} selected</span>
          <slot
            name="selection-actions"
            :selected-names="selectedNames"
            :open-menu="openMenuUnder"
          />
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
        <span v-else class="column-list-count">{{ columnCountLabel }}</span>

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
      </div>

      <div ref="columnsScrollRef" class="picker-scroll">
        <table class="styled-table column-list picker-column-table">
          <colgroup>
            <col style="width: 22px" />
            <col />
            <col style="width: 82px" />
            <col style="width: 112px" />
            <col style="width: 56px" />
          </colgroup>
          <thead>
            <tr>
              <th aria-label="Drag" />
              <th class="is-sortable" :title="sortHint" :aria-sort="ariaSort" @click="toggleSort">
                <span class="th-inner">
                  <span class="th-label">Column</span>
                  <span class="sort-glyph">{{ sortGlyph }}</span>
                </span>
              </th>
              <th>Type</th>
              <th>Used as</th>
              <th aria-label="Actions" />
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="{ column, index } in visibleColumns"
              :key="column.name"
              :class="columnRowClasses(index, column.name)"
              draggable="true"
              @dragstart="onColumnDragStart(index, $event)"
              @dragend="onColumnDragEnd"
              @mousedown="onRowMouseDown"
              @click="onColumnClick(index, $event)"
              @contextmenu.prevent="openColumnContextMenu(index, $event)"
            >
              <td class="drag-handle-cell" title="Drag onto the settings to add">⠿</td>
              <td class="column-name-cell" :title="column.name">
                <div class="column-name">{{ column.name }}</div>
              </td>
              <td class="column-type" :title="column.data_type">{{ column.data_type }}</td>
              <td class="usage-cell">
                <button
                  v-for="chip in usageChips(column.name)"
                  :key="chip.label"
                  type="button"
                  class="usage-chip"
                  :class="{ 'is-key': chip.isKey }"
                  :title="chip.title"
                  @mousedown.prevent
                  @click.stop="reveal(chip.rows)"
                >
                  {{ chip.label }}
                </button>
              </td>
              <td class="row-tools-cell">
                <span class="row-tools">
                  <slot name="row-actions" :column="column" :open-menu="openMenuUnder" />
                </span>
              </td>
            </tr>
            <tr v-if="visibleColumns.length === 0" class="is-empty">
              <td colspan="5" class="empty-cell">
                {{
                  columns.length === 0 ? "No input columns yet" : `No columns match “${filterText}”`
                }}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <section
      ref="settingsPaneRef"
      class="picker-settings"
      :class="settingsClasses"
      :style="settingsStyle"
      :aria-label="settingsLabel"
      tabindex="-1"
      @keydown="onSettingsKeydown"
    >
      <div
        class="picker-settings-header"
        title="Drag to resize · double-click to reset"
        @pointerdown="onSashPointerDown"
        @dblclick="resetSplit"
      >
        <span class="picker-settings-title">Settings</span>
        <span v-if="selectedRowCount > 0" class="column-list-selection picker-row-selection">
          <span class="column-list-selection-count">{{ selectedRowCount }} selected</span>
          <button
            class="btn btn-sm btn-ghost picker-remove-selected"
            type="button"
            :title="`Remove the selected ${pluralize(selectedRowCount, 'row')}`"
            @mousedown.prevent
            @click="removeRows(rowSelection.selectedIndices)"
          >
            Remove
          </button>
          <button
            class="btn btn-sm btn-ghost column-list-clear"
            type="button"
            title="Clear selection"
            aria-label="Clear row selection"
            @mousedown.prevent
            @click="clearRowSelection"
          >
            ✕
          </button>
        </span>
        <span v-else-if="settingsCount" class="picker-settings-count" :title="settingsCount">
          {{ settingsCount }}
        </span>
        <span class="picker-sash-grip" aria-hidden="true" />
        <span class="picker-header-tools" @dblclick.stop>
          <span v-if="isDragging" class="drop-pills" aria-hidden="true">
            <span
              v-for="zone in dropZones"
              :key="zone.value"
              class="drop-pill"
              :class="{ 'is-active': dropZone === zone.value, 'is-disabled': zone.disabled }"
            >
              {{ zone.label }}
            </span>
          </span>
          <slot v-else name="strip" />
          <button
            type="button"
            class="btn btn-sm btn-ghost btn-icon picker-fold"
            :title="settingsCollapsed ? 'Show the settings' : 'Hide the settings'"
            :aria-label="settingsCollapsed ? 'Show the settings' : 'Hide the settings'"
            :aria-expanded="!settingsCollapsed"
            @mousedown.prevent
            @click.stop="toggleSettingsCollapsed"
          >
            <span class="material-icons" aria-hidden="true">
              {{ settingsCollapsed ? "expand_less" : "expand_more" }}
            </span>
          </button>
        </span>
      </div>

      <template v-if="!settingsCollapsed">
        <div v-if="rowCount === 0" class="picker-empty">
          <slot name="empty" />
        </div>
        <div v-else ref="settingsScrollRef" class="picker-scroll">
          <slot
            name="settings"
            :is-row-selected="isRowSelected"
            :is-dragging="isDragging"
            :on-row-click="onRowClick"
            :on-row-context-menu="openRowContextMenu"
            :on-row-mouse-down="onRowMouseDown"
          />
        </div>
        <div v-if="$slots.footer" class="picker-footer">
          <slot name="footer" />
        </div>
      </template>
    </section>

    <context-menu
      v-if="menuTarget"
      :position="contextMenuPosition"
      :options="menuOptions"
      @select="onMenuSelect"
      @close="menuTarget = null"
    />
  </div>
</template>

<script lang="ts">
// Session-scoped on purpose: the drawer remounts the picker on every node switch.
let splitPreference: { height: number | null; collapsed: boolean } = {
  height: null,
  collapsed: false,
};
</script>

<script lang="ts" setup>
/**
 * The two-pane column picker shared by Group By, Pivot and Unpivot: a sortable,
 * filterable, multi-selectable column list above a settings section, split by a
 * strip that is also the sash. The host renders its own settings rows through
 * the `settings` slot and decides what a drop or a menu action means; this
 * component owns selection, drag and drop, the split and row removal.
 */
import { computed, nextTick, onMounted, onUnmounted, ref, watch } from "vue";
import type { FileColumn } from "../nodeInput";
import { useDragAutoScroll, findScrollParent } from "../../../../composables/useDragAutoScroll";
import { NO_AUTOFILL } from "../../../../utils/noAutofill";
import { ContextMenu } from "../../../common";
import type { ContextMenuOption } from "../../../common";
import {
  EMPTY_SELECTION,
  applyClick,
  clampSelection,
  ensureSelected,
  isMacPlatform,
  nextSortDirection,
  readModifiers,
  sortForDirection,
  type SelectionState,
  type SortDirection,
} from "./columnSelection";
import {
  clampSettingsHeight,
  dropZoneAt,
  pluralize,
  shiftAfterRemoval,
  type DropZoneSpec,
  type UsageChip,
} from "./columnPicker";

const props = withDefaults(
  defineProps<{
    columns: FileColumn[];
    /** Chips shown in the "Used as" cell; each reveals its settings rows when clicked. */
    usageChips: (name: string) => UsageChip[];
    usedCount: number;
    /** Right-click and "▾" menus on columns; `variant` tells the two apart. */
    columnMenuOptions: (names: string[], variant: string) => ContextMenuOption[];
    /** Equal horizontal bands of the settings pane, left to right. */
    dropZones: DropZoneSpec[];
    rowCount: number;
    settingsCount?: string;
    settingsLabel?: string;
  }>(),
  { settingsCount: "", settingsLabel: "Settings" },
);

const emit = defineEmits<{
  (event: "drop", names: string[], zone: string): void;
  (event: "column-action", action: string, names: string[]): void;
  (event: "remove-rows", indices: number[]): void;
  (event: "reveal", indices: number[]): void;
  (event: "drag-change", names: string[]): void;
}>();

const rootRef = ref<HTMLElement | null>(null);
const columnsScrollRef = ref<HTMLElement | null>(null);
const settingsPaneRef = ref<HTMLElement | null>(null);
const settingsScrollRef = ref<HTMLElement | null>(null);
const isMac = isMacPlatform();

// ----- columns -----

const columns = computed(() => props.columns);
const filterText = ref("");
const sortDirection = ref<SortDirection>("none");

/** Display order; the selection is expressed as indices into this list. */
const displayColumns = computed(() =>
  sortForDirection(
    columns.value,
    sortDirection.value,
    (column) => column.name,
    (column) => columns.value.indexOf(column),
  ),
);

const visibleColumns = computed(() => {
  const query = filterText.value.trim().toLowerCase();
  return displayColumns.value
    .map((column, index) => ({ column, index }))
    .filter(({ column }) => !query || column.name.toLowerCase().includes(query));
});

const sortHint = computed(() =>
  sortDirection.value === "none"
    ? "Sort A→Z"
    : sortDirection.value === "asc"
      ? "Sort Z→A"
      : "Restore original order",
);

const sortGlyph = computed(() =>
  sortDirection.value === "asc" ? "▲" : sortDirection.value === "desc" ? "▼" : "⇅",
);

const ariaSort = computed(() =>
  sortDirection.value === "asc"
    ? "ascending"
    : sortDirection.value === "desc"
      ? "descending"
      : "none",
);

const columnCountLabel = computed(() => {
  const total = pluralize(columns.value.length, "column");
  return props.usedCount > 0 ? `${total} · ${props.usedCount} in use` : total;
});

const selection = ref<SelectionState>({ ...EMPTY_SELECTION, selectedIndices: [] });

const selectedNames = computed(() =>
  selection.value.selectedIndices
    .map((index) => displayColumns.value[index]?.name)
    .filter((name): name is string => name !== undefined),
);

const clearSelection = () => {
  selection.value = { anchorIndex: null, selectedIndices: [] };
};

const toggleSort = () => {
  sortDirection.value = nextSortDirection(sortDirection.value);
  // Row indices no longer mean what the selection recorded.
  clearSelection();
};

watch(
  () => columns.value.length,
  (length) => {
    selection.value = clampSelection(selection.value, length);
  },
);

const onRowMouseDown = (event: MouseEvent) => {
  // Stop the browser extending its own text selection across the drawer.
  if (event.shiftKey) {
    event.preventDefault();
    window.getSelection()?.removeAllRanges();
  }
};

const onColumnClick = (index: number, event: MouseEvent) => {
  selection.value = applyClick(selection.value, index, readModifiers(event, isMac), {
    clearOnRepeatClick: true,
  });
};

const columnRowClasses = (index: number, name: string) => ({
  "is-selected": selection.value.selectedIndices.includes(index),
  "is-dragging": draggingNames.value.includes(name),
});

const handleClickOutside = (event: MouseEvent) => {
  const target = event.target as HTMLElement | null;
  const within = (selector: string) => Boolean(target?.closest?.(selector));
  const shared = ".column-list-selection, .context-menu, .el-popper";
  if (!within(`.picker-column-table tbody tr, ${shared}`)) clearSelection();
  if (!within(`.picker-settings tbody tr, ${shared}`)) clearRowSelection();
};

// ----- settings rows -----

const rowSelection = ref<SelectionState>({ ...EMPTY_SELECTION, selectedIndices: [] });
const selectedRowCount = computed(() => rowSelection.value.selectedIndices.length);
const isRowSelected = (index: number) => rowSelection.value.selectedIndices.includes(index);

const clearRowSelection = () => {
  rowSelection.value = { anchorIndex: null, selectedIndices: [] };
};

watch(
  () => props.rowCount,
  (length) => {
    rowSelection.value = clampSelection(rowSelection.value, length);
  },
);

/** Clicks on a row's own controls edit the row; only the rest of it selects. */
const isRowControl = (target: EventTarget | null) =>
  Boolean((target as HTMLElement | null)?.closest?.("input, button, .el-select"));

const onRowClick = (index: number, event: MouseEvent) => {
  if (isRowControl(event.target)) return;
  rowSelection.value = applyClick(rowSelection.value, index, readModifiers(event, isMac), {
    clearOnRepeatClick: true,
  });
  settingsPaneRef.value?.focus({ preventScroll: true });
};

const removeRows = (indices: readonly number[]) => {
  if (indices.length === 0) return;
  emit("remove-rows", [...indices]);
  rowSelection.value = {
    anchorIndex: null,
    selectedIndices: shiftAfterRemoval(rowSelection.value.selectedIndices, indices),
  };
};

const onSettingsKeydown = (event: KeyboardEvent) => {
  if (isRowControl(event.target)) return;
  if (event.key === "Escape") {
    clearRowSelection();
  } else if ((event.key === "Delete" || event.key === "Backspace") && selectedRowCount.value > 0) {
    event.preventDefault();
    removeRows(rowSelection.value.selectedIndices);
  }
};

/** Unfolds the settings and scrolls the last row into view; the host flashes them on `reveal`. */
const reveal = async (indices: number[]) => {
  if (indices.length === 0) return;
  settingsCollapsed.value = false;
  emit("reveal", indices);
  await nextTick();
  const rowEls = settingsScrollRef.value?.querySelectorAll("tbody tr");
  rowEls?.[indices[indices.length - 1]]?.scrollIntoView({ block: "nearest" });
};

// ----- context menus -----

type MenuTarget =
  | { kind: "columns"; names: string[]; variant: string }
  | { kind: "rows"; indices: number[] };

const menuTarget = ref<MenuTarget | null>(null);
const contextMenuPosition = ref({ x: 0, y: 0 });

const menuOptions = computed<ContextMenuOption[]>(() => {
  const target = menuTarget.value;
  if (!target) return [];
  if (target.kind === "rows") {
    const count = target.indices.length;
    const label = count === 1 ? "Remove" : `Remove ${count} rows`;
    return [{ label, action: "remove", danger: true }];
  }
  return props.columnMenuOptions(target.names, target.variant);
});

const openColumnContextMenu = (index: number, event: MouseEvent) => {
  event.stopPropagation();
  selection.value = ensureSelected(selection.value, index);
  contextMenuPosition.value = { x: event.clientX, y: event.clientY };
  menuTarget.value = { kind: "columns", names: [...selectedNames.value], variant: "context" };
};

/** Anchored under the button that opened it, unlike the pointer-anchored right-click menu. */
const openMenuUnder = (names: string[], event: MouseEvent, variant = "button") => {
  const rect = (event.currentTarget as HTMLElement).getBoundingClientRect();
  contextMenuPosition.value = { x: rect.left, y: rect.bottom + 4 };
  menuTarget.value = { kind: "columns", names: [...names], variant };
};

const openRowContextMenu = (event: MouseEvent, index: number) => {
  // Right-clicks on the row's editable fields keep the native menu (paste).
  const el = event.target as HTMLElement | null;
  if (el?.closest?.("input, textarea")) return;
  event.preventDefault();
  rowSelection.value = ensureSelected(rowSelection.value, index);
  contextMenuPosition.value = { x: event.clientX, y: event.clientY };
  menuTarget.value = { kind: "rows", indices: [...rowSelection.value.selectedIndices] };
};

const onMenuSelect = (action: string) => {
  const target = menuTarget.value;
  if (!target) return;
  if (target.kind === "rows") {
    if (action === "remove") removeRows(target.indices);
  } else {
    emit("column-action", action, target.names);
  }
  menuTarget.value = null;
};

// ----- drag & drop -----

const draggingNames = ref<string[]>([]);
const dropZone = ref<string | null>(null);
const isDragging = computed(() => draggingNames.value.length > 0);

const autoScroll = useDragAutoScroll({
  containers: () => [
    columnsScrollRef.value,
    settingsScrollRef.value,
    findScrollParent(rootRef.value?.parentElement ?? null),
  ],
});

/** The default ghost shows one row; a multi-column drag says how many are coming along. */
const setDragBadge = (event: DragEvent, count: number) => {
  if (!event.dataTransfer || count < 2) return;
  const badge = document.createElement("div");
  badge.textContent = `${count} columns`;
  Object.assign(badge.style, {
    position: "fixed",
    top: "-100px",
    left: "-100px",
    padding: "4px 10px",
    borderRadius: "9999px",
    background: "var(--color-accent)",
    color: "var(--color-text-inverse)",
    fontSize: "12px",
    fontWeight: "600",
  });
  document.body.appendChild(badge);
  event.dataTransfer.setDragImage(badge, 12, 12);
  setTimeout(() => badge.remove(), 0);
};

const onColumnDragStart = (index: number, event: DragEvent) => {
  // Dragging a row outside the selection collapses onto it first, so the block
  // that moves is always the block the user can see highlighted.
  selection.value = ensureSelected(selection.value, index);
  draggingNames.value = [...selectedNames.value];
  if (event.dataTransfer) {
    event.dataTransfer.effectAllowed = "copy";
    event.dataTransfer.setData("text/plain", draggingNames.value.join(", "));
  }
  setDragBadge(event, draggingNames.value.length);
  emit("drag-change", [...draggingNames.value]);
  autoScroll.start();
};

const onColumnDragEnd = () => {
  draggingNames.value = [];
  dropZone.value = null;
  emit("drag-change", []);
  autoScroll.stop();
};

const zoneAt = (event: DragEvent): string | null => {
  const pane = settingsPaneRef.value;
  if (!pane) return null;
  const rect = pane.getBoundingClientRect();
  const inside =
    event.clientX >= rect.left &&
    event.clientX <= rect.right &&
    event.clientY >= rect.top &&
    event.clientY <= rect.bottom;
  return inside ? (dropZoneAt(rect, event.clientX, props.dropZones)?.value ?? null) : null;
};

const onCardDragOver = (event: DragEvent) => {
  if (!isDragging.value) return;
  const zone = zoneAt(event);
  dropZone.value = zone;
  // Only the settings section accepts a drop, so the cursor says "no" elsewhere.
  if (zone) {
    event.preventDefault();
    if (event.dataTransfer) event.dataTransfer.dropEffect = "copy";
  }
};

const onCardDrop = (event: DragEvent) => {
  const names = [...draggingNames.value];
  const zone = zoneAt(event);
  onColumnDragEnd();
  if (zone) emit("drop", names, zone);
};

// ----- split -----

const settingsHeight = ref<number | null>(splitPreference.height);
const settingsCollapsed = ref(splitPreference.collapsed);
const isResizing = ref(false);

const settingsClasses = computed(() => ({
  "is-drop-target": isDragging.value,
  "is-collapsed": settingsCollapsed.value,
  "is-sized": !settingsCollapsed.value && settingsHeight.value !== null,
  "is-resizing": isResizing.value,
}));

const settingsStyle = computed(() =>
  settingsHeight.value !== null && !settingsCollapsed.value
    ? { "--picker-settings-height": `${settingsHeight.value}px` }
    : undefined,
);

/** The height both sections share: the card's content box. */
const splitAvailable = (): number => {
  const card = rootRef.value;
  if (!card) return 0;
  const style = getComputedStyle(card);
  return card.clientHeight - parseFloat(style.paddingTop) - parseFloat(style.paddingBottom);
};

let endResize: (() => void) | null = null;

/** The whole strip is the sash; its buttons keep their own clicks. */
const onSashPointerDown = (event: PointerEvent) => {
  const pane = settingsPaneRef.value;
  if (event.button !== 0 || !pane || (event.target as HTMLElement).closest("button")) return;
  event.preventDefault();
  (event.currentTarget as HTMLElement).setPointerCapture?.(event.pointerId);
  const startY = event.clientY;
  const startHeight = pane.offsetHeight;
  const available = splitAvailable();
  const onMove = (move: PointerEvent) => {
    const delta = startY - move.clientY;
    if (!isResizing.value && Math.abs(delta) < 3) return;
    isResizing.value = true;
    settingsCollapsed.value = false;
    settingsHeight.value = clampSettingsHeight(startHeight + delta, available);
  };
  const onEnd = () => {
    window.removeEventListener("pointermove", onMove);
    window.removeEventListener("pointerup", onEnd);
    window.removeEventListener("pointercancel", onEnd);
    document.body.style.cursor = "";
    isResizing.value = false;
    endResize = null;
  };
  endResize = onEnd;
  document.body.style.cursor = "row-resize";
  window.addEventListener("pointermove", onMove);
  window.addEventListener("pointerup", onEnd);
  window.addEventListener("pointercancel", onEnd);
};

const resetSplit = () => {
  settingsHeight.value = null;
  settingsCollapsed.value = false;
};

const toggleSettingsCollapsed = () => {
  settingsCollapsed.value = !settingsCollapsed.value;
};

watch([settingsHeight, settingsCollapsed], ([height, collapsed]) => {
  splitPreference = { height, collapsed };
});

// ----- lifecycle -----

onMounted(() => {
  window.addEventListener("click", handleClickOutside);
});

onUnmounted(() => {
  window.removeEventListener("click", handleClickOutside);
  endResize?.();
});

defineExpose({ clearSelection, clearRowSelection, removeRows, reveal, openMenuUnder });
</script>

<style scoped>
/* One card, like the Select picker, filling the tab pane so both sections
   share the drawer height instead of the columns pushing the settings below
   the fold. The .listbox-wrapper chrome (shadow, radius, padding) is shared. */
.picker-card {
  flex: 1 1 auto;
  min-height: 0;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

/* Both sections are content-sized and scroll inside their own box. Flex
   shrinks items in proportion to basis × flex-shrink, so the huge shrink
   factor makes the column list give up all the space first; the settings only
   shrink once the columns are frozen at their toolbar-plus-one-row floor. */
.picker-columns {
  display: flex;
  flex-direction: column;
  flex: 0 1000 auto;
  min-height: 84px;
}

.picker-settings {
  display: flex;
  flex-direction: column;
  flex: 0 1 auto;
  min-height: 30px;
  max-height: 55%;
  transition: box-shadow var(--transition-fast) var(--transition-timing);
}

/* A dragged split pins the settings; the max keeps the column list its floor. */
.picker-settings.is-sized {
  flex: 0 0 var(--picker-settings-height);
  max-height: calc(100% - 84px);
}

.picker-settings.is-collapsed {
  flex: 0 0 auto;
  max-height: none;
}

.picker-settings.is-drop-target {
  box-shadow: inset 0 0 0 1px var(--color-accent);
  border-radius: var(--border-radius-sm);
}

/* Focusable only so Delete and Escape reach the selected rows. */
.picker-settings:focus {
  outline: none;
}

.picker-scroll {
  flex: 1 1 auto;
  min-height: 0;
  overflow: auto;
}

.picker-scroll::-webkit-scrollbar {
  width: 6px;
}

.picker-scroll::-webkit-scrollbar-track {
  background: transparent;
}

.picker-scroll::-webkit-scrollbar-thumb {
  background-color: var(--color-gray-300);
  border-radius: var(--border-radius-full);
}

/* ----- density ----- */

/* Two tables share one card, so rows run denser than the shared 32px default.
   :deep() reaches the host's settings table, which is slot content. */
.picker-card .column-list-toolbar {
  flex-shrink: 0;
  height: 30px;
}

.picker-card :deep(.column-list-selection .btn) {
  height: 22px;
  padding-top: 0;
  padding-bottom: 0;
}

.picker-card :deep(.styled-table thead th) {
  height: 26px;
  padding-top: 0;
  padding-bottom: 0;
}

.picker-card :deep(.styled-table tr) {
  height: 26px;
}

.picker-card :deep(.styled-table td) {
  height: 26px;
  padding-top: 2px;
  padding-bottom: 2px;
}

/* Baseline-aligned inline-flex would add descender space under the 22px buttons. */
.picker-card :deep(.styled-table td.row-tools-cell) {
  padding-top: 1px;
  padding-bottom: 1px;
}

/* The 24px select needs a little more than the text rows. */
.picker-card :deep(.picker-settings .styled-table tbody tr),
.picker-card :deep(.picker-settings .styled-table tbody td) {
  height: 28px;
  padding-top: 1px;
  padding-bottom: 1px;
}

.picker-card :deep(tbody tr) {
  cursor: default;
}

/* ----- columns table ----- */

.column-type {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.usage-cell {
  white-space: nowrap;
  overflow: hidden;
}

.usage-chip {
  display: inline-block;
  margin-right: var(--spacing-0-5);
  padding: 0 var(--spacing-1-5);
  border: none;
  border-radius: var(--border-radius-full);
  background-color: var(--color-background-soft);
  color: var(--color-text-secondary);
  font-family: inherit;
  font-size: var(--font-size-2xs);
  line-height: 16px;
  cursor: pointer;
  transition: background-color var(--transition-fast) var(--transition-timing);
}

.usage-chip:hover {
  background-color: var(--color-background-tertiary);
  color: var(--color-text-primary);
}

.usage-chip.is-key {
  background-color: var(--color-accent-subtle);
  color: var(--color-accent-hover);
}

.usage-chip.is-key:hover {
  background-color: var(--color-focus-ring-accent-strong);
}

/* Row tools follow the drag grip: invisible until the row is hovered, selected
   or focused, so a 100-row list reads as a list rather than a button grid. */
.picker-card :deep(.row-tools-cell) {
  text-align: right;
  white-space: nowrap;
}

.picker-card :deep(.row-tools) {
  display: inline-flex;
  vertical-align: middle;
  gap: var(--spacing-0-5);
  opacity: 0;
  transition: opacity var(--transition-fast) var(--transition-timing);
}

.picker-card :deep(tbody tr:hover .row-tools),
.picker-card :deep(tbody tr.is-selected .row-tools),
.picker-card :deep(.row-tools:focus-within) {
  opacity: 1;
}

.picker-card :deep(.row-tools .btn) {
  width: 22px;
  min-width: 22px;
  height: 22px;
  padding: 0;
  border-radius: var(--border-radius-sm);
}

.picker-card :deep(.row-tools .material-icons) {
  font-size: 16px;
}

.picker-card :deep(.row-tools .btn:hover:not(:disabled)) {
  background-color: var(--color-accent-subtle);
  color: var(--color-accent-hover);
}

.picker-card :deep(.row-tools .row-remove:hover:not(:disabled)) {
  background-color: var(--color-danger-light);
  color: var(--color-danger);
}

.empty-cell {
  text-align: center;
  color: var(--color-text-tertiary);
}

/* ----- settings section ----- */

/* The strip doubles as the sash between the two sections. */
.picker-settings-header {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  flex-shrink: 0;
  height: 30px;
  padding: 0 var(--spacing-2);
  border-top: 1px solid var(--color-border-light);
  border-bottom: 1px solid var(--color-border-light);
  background-color: var(--color-background-muted);
  font-size: var(--font-size-xs);
  user-select: none;
  cursor: row-resize;
  touch-action: none;
  transition: border-color var(--transition-fast) var(--transition-timing);
}

.picker-settings.is-resizing .picker-settings-header {
  border-top-color: var(--color-accent);
}

/* A flex item, not pinned to the centre, so a long count never runs under it. */
.picker-sash-grip {
  flex: 0 0 28px;
  height: 3px;
  margin: 0 auto;
  border-radius: var(--border-radius-full);
  background-color: var(--color-border-secondary);
  pointer-events: none;
  transition: background-color var(--transition-fast) var(--transition-timing);
}

.picker-settings-header:hover .picker-sash-grip,
.picker-settings.is-resizing .picker-sash-grip {
  background-color: var(--color-text-muted);
}

.picker-header-tools {
  display: inline-flex;
  align-items: center;
  flex-shrink: 0;
  gap: var(--spacing-1);
  margin-left: auto;
  cursor: default;
}

.picker-fold {
  width: 22px;
  min-width: 22px;
  height: 22px;
  padding: 0;
  border-radius: var(--border-radius-sm);
}

.picker-fold .material-icons {
  font-size: 18px;
}

.picker-settings-title {
  flex-shrink: 0;
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.picker-settings-count {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  color: var(--color-text-tertiary);
  white-space: nowrap;
}

.picker-row-selection {
  cursor: default;
}

.picker-card .picker-remove-selected {
  color: var(--color-danger);
}

.picker-card .picker-remove-selected:hover:not(:disabled) {
  background-color: var(--color-danger-light);
  color: var(--color-danger);
}

.drop-pills {
  display: inline-flex;
  gap: var(--spacing-1);
}

.drop-pill {
  display: inline-flex;
  align-items: center;
  height: 20px;
  padding: 0 var(--spacing-2);
  border: 1px dashed var(--color-border-secondary);
  border-radius: var(--border-radius-full);
  color: var(--color-text-secondary);
  white-space: nowrap;
  transition:
    border-color var(--transition-fast) var(--transition-timing),
    background-color var(--transition-fast) var(--transition-timing);
}

.drop-pill.is-active {
  border-style: solid;
  border-color: var(--color-accent);
  background-color: var(--color-accent-subtle);
  color: var(--color-accent-hover);
  font-weight: var(--font-weight-medium);
}

.drop-pill.is-disabled {
  opacity: 0.4;
}

/* Hosts flag problems in the strip with this pill. */
.picker-card :deep(.picker-flag) {
  padding: 1px var(--spacing-2);
  border: none;
  border-radius: var(--border-radius-full);
  background-color: var(--color-danger-light);
  color: var(--color-danger-dark);
  font-family: inherit;
  font-size: var(--font-size-2xs);
  font-weight: var(--font-weight-medium);
  line-height: 1.4;
  white-space: nowrap;
  cursor: pointer;
}

.picker-empty {
  display: flex;
  align-items: center;
  justify-content: center;
  flex-wrap: wrap;
  gap: var(--spacing-1);
  padding: var(--spacing-2) var(--spacing-3);
  color: var(--color-text-tertiary);
  font-size: var(--font-size-xs);
  text-align: center;
}

.picker-empty :deep(.material-icons) {
  font-size: 16px;
  color: var(--color-text-secondary);
}

.picker-footer {
  display: flex;
  align-items: center;
  flex-shrink: 0;
  gap: var(--spacing-2);
  min-height: 32px;
  padding: var(--spacing-1) var(--spacing-2);
  border-top: 1px solid var(--color-border-light);
  font-size: var(--font-size-xs);
}

.picker-card :deep(.picker-footer-label) {
  flex-shrink: 0;
  color: var(--color-text-secondary);
  font-weight: var(--font-weight-medium);
}

/* Hosts name their first cell this way to get the same ellipsis as the column list. */
.picker-card :deep(.picker-field-cell) {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.picker-card :deep(.picker-note-cell) {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.picker-card :deep(.picker-settings tbody tr.is-new > td) {
  animation: picker-flash 1.2s var(--transition-timing);
}

@keyframes picker-flash {
  from {
    background-color: var(--color-accent-subtle);
  }
  to {
    background-color: var(--color-background-primary);
  }
}

@media (prefers-reduced-motion: reduce) {
  .picker-card :deep(.picker-settings tbody tr.is-new > td) {
    animation: none;
    background-color: var(--color-accent-subtle);
  }
}
</style>
