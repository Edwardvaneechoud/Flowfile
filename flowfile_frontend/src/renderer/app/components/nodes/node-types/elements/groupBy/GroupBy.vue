<template>
  <div v-if="dataLoaded && nodeGroupBy" class="group-by-root">
    <generic-node-settings
      v-model="nodeGroupBy"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div
        ref="rootRef"
        class="listbox-wrapper group-by-card column-list"
        @dragover="onCardDragOver"
        @drop.prevent="onCardDrop"
      >
        <section class="group-by-columns" aria-label="Input columns">
          <div class="column-list-toolbar">
            <div v-if="selectedNames.length > 0" class="column-list-selection">
              <span class="column-list-selection-count">{{ selectedNames.length }} selected</span>
              <button
                class="btn btn-sm btn-ghost"
                type="button"
                title="Add the selected columns as group-by keys"
                @mousedown.prevent
                @click="addRows(selectedNames, 'groupby')"
              >
                Group by
              </button>
              <button
                class="btn btn-sm btn-ghost"
                type="button"
                title="Aggregate the selected columns"
                @mousedown.prevent
                @click="openAggMenu(selectedNames, $event)"
              >
                Aggregate ▾
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

          <div ref="columnsScrollRef" class="group-by-scroll">
            <table class="styled-table column-list group-by-column-table">
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
                  <th
                    class="is-sortable"
                    :title="sortHint"
                    :aria-sort="ariaSort"
                    @click="toggleSort"
                  >
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
                  @mousedown="onColumnMouseDown"
                  @click="onColumnClick(index, $event)"
                  @contextmenu.prevent="openColumnMenu(index, $event)"
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
                      @click.stop="revealRows(chip.rows)"
                    >
                      {{ chip.label }}
                    </button>
                  </td>
                  <td class="row-tools-cell">
                    <span class="row-tools">
                      <button
                        type="button"
                        class="btn btn-sm btn-ghost btn-icon"
                        :title="`Group by ${column.name}`"
                        :aria-label="`Group by ${column.name}`"
                        @mousedown.prevent
                        @click.stop="addRows([column.name], 'groupby')"
                      >
                        <span class="material-icons" aria-hidden="true">add</span>
                      </button>
                      <button
                        type="button"
                        class="btn btn-sm btn-ghost btn-icon"
                        :title="`Aggregate ${column.name}…`"
                        :aria-label="`Aggregate ${column.name}`"
                        @mousedown.prevent
                        @click.stop="openAggMenu([column.name], $event)"
                      >
                        <span class="material-icons" aria-hidden="true">functions</span>
                      </button>
                    </span>
                  </td>
                </tr>
                <tr v-if="visibleColumns.length === 0" class="is-empty">
                  <td colspan="5" class="empty-cell">
                    {{
                      columns.length === 0
                        ? "No input columns yet"
                        : `No columns match “${filterText}”`
                    }}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>

        <section
          ref="settingsPaneRef"
          class="group-by-settings"
          :class="settingsClasses"
          :style="settingsStyle"
          aria-label="Group by settings"
          tabindex="-1"
          @keydown="onSettingsKeydown"
        >
          <div
            class="group-by-settings-header"
            title="Drag to resize · double-click to reset"
            @pointerdown="onSashPointerDown"
            @dblclick="resetSplit"
          >
            <span class="group-by-settings-title">Settings</span>
            <span v-if="selectedRowCount > 0" class="column-list-selection group-by-row-selection">
              <span class="column-list-selection-count">{{ selectedRowCount }} selected</span>
              <button
                class="btn btn-sm btn-ghost group-by-remove-selected"
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
            <span v-else-if="settingsCountLabel" class="group-by-settings-count">
              {{ settingsCountLabel }}
            </span>
            <span class="group-by-sash-grip" aria-hidden="true" />
            <span class="group-by-header-tools" @dblclick.stop>
              <span v-if="isDragging" class="drop-pills" aria-hidden="true">
                <span class="drop-pill" :class="{ 'is-active': dropZone === 'groupby' }"
                  >Group by</span
                >
                <span class="drop-pill" :class="{ 'is-active': dropZone === 'aggregate' }">
                  Aggregate · {{ dragAggHint }}
                </span>
              </span>
              <button
                v-else-if="duplicateNames.size > 0"
                type="button"
                class="group-by-flag"
                :title="`Output names must be unique: ${[...duplicateNames].join(', ')}`"
                @mousedown.prevent
                @click="revealDuplicates"
              >
                {{ pluralize(duplicateNames.size, "duplicate name") }}
              </button>
              <button
                type="button"
                class="btn btn-sm btn-ghost btn-icon group-by-fold"
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
            <div v-if="rows.length === 0" class="group-by-empty">
              Drag columns here, or hover a column and use
              <span class="material-icons" aria-label="add">add</span> /
              <span class="material-icons" aria-label="aggregate">functions</span>.
            </div>
            <div v-else ref="settingsScrollRef" class="group-by-scroll">
              <table class="styled-table column-list group-by-agg-table">
                <colgroup>
                  <col />
                  <col style="width: 112px" />
                  <col />
                  <col style="width: 34px" />
                </colgroup>
                <thead>
                  <tr>
                    <th>Field</th>
                    <th>Action</th>
                    <th>Output name</th>
                    <th aria-label="Remove" />
                  </tr>
                </thead>
                <tbody>
                  <tr
                    v-for="(item, index) in rows"
                    :key="index"
                    :class="{
                      'is-new': flashedRows.has(index),
                      'is-selected': rowSelection.selectedIndices.includes(index),
                      'is-drop-after': isDragging && index === rows.length - 1,
                    }"
                    @mousedown="onColumnMouseDown"
                    @click="onRowClick(index, $event)"
                    @contextmenu="openRowContextMenu($event, index)"
                  >
                    <td class="agg-field-cell" :title="item.old_name">{{ item.old_name }}</td>
                    <td>
                      <el-select
                        :model-value="item.agg"
                        size="small"
                        @update:model-value="setAgg(item, $event)"
                      >
                        <el-option
                          v-for="aggOption in AGG_OPTIONS"
                          :key="aggOption"
                          :label="aggLabel(aggOption)"
                          :value="aggOption"
                        />
                      </el-select>
                    </td>
                    <td>
                      <input
                        v-model="item.new_name"
                        class="inline-input"
                        :class="{ 'is-duplicate': isDuplicateName(item) }"
                        type="text"
                        :placeholder="outputNameFor(item.old_name, item.agg)"
                        :aria-label="`Output name for ${item.old_name}`"
                        :title="
                          isDuplicateName(item)
                            ? 'Another row already uses this output name'
                            : undefined
                        "
                        v-bind="NO_AUTOFILL"
                      />
                    </td>
                    <td class="row-tools-cell">
                      <span class="row-tools">
                        <button
                          type="button"
                          class="btn btn-sm btn-ghost btn-icon row-remove"
                          :title="`Remove ${item.old_name}`"
                          :aria-label="`Remove ${aggLabel(item.agg)} of ${item.old_name}`"
                          @mousedown.prevent
                          @click="removeRow(index)"
                        >
                          <span class="material-icons" aria-hidden="true">close</span>
                        </button>
                      </span>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </template>
        </section>
      </div>

      <context-menu
        v-if="menuTarget"
        :position="contextMenuPosition"
        :options="menuOptions"
        @select="onMenuSelect"
        @close="menuTarget = null"
      />
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts">
// Session-scoped on purpose: the drawer remounts this component on every node switch.
let splitPreference: { height: number | null; collapsed: boolean } = {
  height: null,
  collapsed: false,
};
</script>

<script lang="ts" setup>
import { ref, computed, nextTick, onMounted, onUnmounted, watch } from "vue";
import { CodeLoader } from "vue-content-loader";
import type { AggColl, FileColumn, NodeGroupBy } from "../../../baseNode/nodeInput";
import type { NodeData } from "../../../baseNode/nodeInterfaces";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { useDragAutoScroll, findScrollParent } from "../../../../../composables/useDragAutoScroll";
import { NO_AUTOFILL } from "../../../../../utils/noAutofill";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import { ContextMenu } from "../../../../common";
import type { ContextMenuOption } from "../../../../common";
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
} from "../../../baseNode/selectComponents/columnSelection";
import {
  AGG_OPTIONS,
  AGGREGATE_OPTIONS,
  addAggRows,
  aggLabel,
  clampSettingsHeight,
  defaultAggFor,
  dropZoneAt,
  duplicateOutputNames,
  outputNameFor,
  pluralize,
  renamedForAgg,
  shiftAfterRemoval,
  usageByColumn,
  withoutRows,
  type AggKind,
  type ColumnUsage,
  type DropZone,
} from "./groupByLogic";

const nodeStore = useNodeStore();
const nodeGroupBy = ref<null | NodeGroupBy>(null);
const nodeData = ref<null | NodeData>(null);
const dataLoaded = ref(false);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeGroupBy,
  onAfterSave: async () => {
    validateConfig();
  },
});

const rootRef = ref<HTMLElement | null>(null);
const columnsScrollRef = ref<HTMLElement | null>(null);
const settingsPaneRef = ref<HTMLElement | null>(null);
const settingsScrollRef = ref<HTMLElement | null>(null);
const isMac = isMacPlatform();

// ----- columns -----

const columns = computed<FileColumn[]>(() => nodeData.value?.main_input?.table_schema ?? []);
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

const onColumnMouseDown = (event: MouseEvent) => {
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
  if (!within(".group-by-column-table tbody tr, .column-list-selection, .context-menu")) {
    clearSelection();
  }
  if (!within(".group-by-agg-table tbody tr, .column-list-selection, .context-menu")) {
    clearRowSelection();
  }
};

// ----- settings rows -----

const rows = computed<AggColl[]>(() => nodeGroupBy.value?.groupby_input?.agg_cols ?? []);
const usage = computed(() => usageByColumn(rows.value));
const duplicateNames = computed(() => duplicateOutputNames(rows.value));
const isDuplicateName = (item: AggColl) =>
  Boolean(item.new_name && duplicateNames.value.has(item.new_name));

const columnCountLabel = computed(() => {
  const total = pluralize(columns.value.length, "column");
  return usage.value.size > 0 ? `${total} · ${usage.value.size} in use` : total;
});

const settingsCountLabel = computed(() => {
  if (rows.value.length === 0) return "";
  const keys = rows.value.filter((row) => row.agg === "groupby").length;
  return `${pluralize(keys, "key")} · ${pluralize(rows.value.length - keys, "aggregation")}`;
});

interface UsageChip {
  label: string;
  title: string;
  isKey: boolean;
  rows: number[];
}

const MAX_USAGE_CHIPS = 2;

/** At most two chips per row plus a "+N" overflow, so a heavily used column can't widen the table. */
const usageChips = (name: string): UsageChip[] => {
  const uses: ColumnUsage[] = usage.value.get(name) ?? [];
  const chips = uses.slice(0, MAX_USAGE_CHIPS).map((use) => ({
    label: use.agg === "groupby" ? "key" : use.agg,
    title: `Show the ${aggLabel(use.agg)} row`,
    isKey: use.agg === "groupby",
    rows: [use.index],
  }));
  const hidden = uses.slice(MAX_USAGE_CHIPS);
  if (hidden.length > 0) {
    chips.push({
      label: `+${hidden.length}`,
      title: hidden.map((use) => aggLabel(use.agg)).join(", "),
      isKey: false,
      rows: hidden.map((use) => use.index),
    });
  }
  return chips;
};

const flashedRows = ref(new Set<number>());
let flashTimer: ReturnType<typeof setTimeout> | null = null;

/** Highlight rows briefly and scroll the last one into view — the feedback for "where did it go?". */
const revealRows = async (indices: number[]) => {
  if (indices.length === 0) return;
  flashedRows.value = new Set(indices);
  if (flashTimer) clearTimeout(flashTimer);
  flashTimer = setTimeout(() => {
    flashedRows.value = new Set();
  }, 1200);
  settingsCollapsed.value = false;
  await nextTick();
  const rowEls = settingsScrollRef.value?.querySelectorAll("tbody tr");
  rowEls?.[indices[indices.length - 1]]?.scrollIntoView({ block: "nearest" });
};

const revealDuplicates = () => {
  const indices = rows.value.flatMap((row, index) => (isDuplicateName(row) ? [index] : []));
  void revealRows(indices);
};

const addRows = (names: string[], agg: AggKind) => {
  const input = nodeGroupBy.value?.groupby_input;
  if (!input || names.length === 0) return;
  const result = addAggRows(input.agg_cols, names, agg);
  input.agg_cols = result.rows;
  void revealRows([...result.added, ...result.existing]);
  clearSelection();
};

const addDefaultAggregations = (names: string[]) => {
  const input = nodeGroupBy.value?.groupby_input;
  if (!input) return;
  const touched: number[] = [];
  for (const name of names) {
    const column = columns.value.find((col) => col.name === name);
    const result = addAggRows(input.agg_cols, [name], defaultAggFor(column?.data_type));
    input.agg_cols = result.rows;
    touched.push(...result.added, ...result.existing);
  }
  void revealRows(touched);
  clearSelection();
};

const setAgg = (item: AggColl, next: string) => {
  item.new_name = renamedForAgg(item, next);
  item.agg = next;
};

// ----- row selection -----

const rowSelection = ref<SelectionState>({ ...EMPTY_SELECTION, selectedIndices: [] });
const selectedRowCount = computed(() => rowSelection.value.selectedIndices.length);

const clearRowSelection = () => {
  rowSelection.value = { anchorIndex: null, selectedIndices: [] };
};

watch(
  () => rows.value.length,
  (length) => {
    rowSelection.value = clampSelection(rowSelection.value, length);
  },
);

/** Clicks on the row's own controls edit the row; only the rest of it selects. */
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
  const input = nodeGroupBy.value?.groupby_input;
  if (!input || indices.length === 0) return;
  input.agg_cols = withoutRows(input.agg_cols, indices);
  rowSelection.value = {
    anchorIndex: null,
    selectedIndices: shiftAfterRemoval(rowSelection.value.selectedIndices, indices),
  };
};

const removeRow = (index: number) => removeRows([index]);

const onSettingsKeydown = (event: KeyboardEvent) => {
  if (isRowControl(event.target)) return;
  if (event.key === "Escape") {
    clearRowSelection();
  } else if ((event.key === "Delete" || event.key === "Backspace") && selectedRowCount.value > 0) {
    event.preventDefault();
    removeRows(rowSelection.value.selectedIndices);
  }
};

// ----- context menus -----

type MenuTarget =
  | { kind: "columns"; names: string[]; withGroupBy: boolean }
  | { kind: "rows"; indices: number[] };

const menuTarget = ref<MenuTarget | null>(null);
const contextMenuPosition = ref({ x: 0, y: 0 });

const aggregateMenuOptions: ContextMenuOption[] = AGGREGATE_OPTIONS.map((option) => ({
  label: option.label,
  action: option.value,
}));

const menuOptions = computed<ContextMenuOption[]>(() => {
  const target = menuTarget.value;
  if (!target) return [];
  if (target.kind === "rows") {
    const count = target.indices.length;
    const label = count === 1 ? "Remove" : `Remove ${count} rows`;
    return [{ label, action: "remove", danger: true }];
  }
  return target.withGroupBy
    ? [{ label: "Group by", action: "groupby" }, ...aggregateMenuOptions]
    : aggregateMenuOptions;
});

const openColumnMenu = (index: number, event: MouseEvent) => {
  event.stopPropagation();
  selection.value = ensureSelected(selection.value, index);
  contextMenuPosition.value = { x: event.clientX, y: event.clientY };
  menuTarget.value = { kind: "columns", names: [...selectedNames.value], withGroupBy: true };
};

/** Anchored under the button that opened it, unlike the pointer-anchored right-click menu. */
const openAggMenu = (names: string[], event: MouseEvent) => {
  const rect = (event.currentTarget as HTMLElement).getBoundingClientRect();
  contextMenuPosition.value = { x: rect.left, y: rect.bottom + 4 };
  menuTarget.value = { kind: "columns", names: [...names], withGroupBy: false };
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
    addRows(target.names, action as AggKind);
  }
  menuTarget.value = null;
};

// ----- drag & drop -----

const draggingNames = ref<string[]>([]);
const dropZone = ref<DropZone | null>(null);
const isDragging = computed(() => draggingNames.value.length > 0);

const autoScroll = useDragAutoScroll({
  containers: () => [
    columnsScrollRef.value,
    settingsScrollRef.value,
    findScrollParent(rootRef.value?.parentElement ?? null),
  ],
});

const dragAggHint = computed(() => {
  const aggs = new Set(
    draggingNames.value.map((name) =>
      defaultAggFor(columns.value.find((col) => col.name === name)?.data_type),
    ),
  );
  return [...aggs].join(" / ");
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
  autoScroll.start();
};

const onColumnDragEnd = () => {
  draggingNames.value = [];
  dropZone.value = null;
  autoScroll.stop();
};

const zoneAt = (event: DragEvent): DropZone | null => {
  const pane = settingsPaneRef.value;
  if (!pane) return null;
  const rect = pane.getBoundingClientRect();
  const inside =
    event.clientX >= rect.left &&
    event.clientX <= rect.right &&
    event.clientY >= rect.top &&
    event.clientY <= rect.bottom;
  return inside ? dropZoneAt(rect, event.clientX) : null;
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
  if (zone === "groupby") addRows(names, "groupby");
  else if (zone === "aggregate") addDefaultAggregations(names);
};

// ----- split -----

const settingsHeight = ref<number | null>(null);
const settingsCollapsed = ref(false);
const isResizing = ref(false);

const settingsClasses = computed(() => ({
  "is-drop-target": isDragging.value,
  "is-collapsed": settingsCollapsed.value,
  "is-sized": !settingsCollapsed.value && settingsHeight.value !== null,
  "is-resizing": isResizing.value,
}));

const settingsStyle = computed(() =>
  settingsHeight.value !== null && !settingsCollapsed.value
    ? { "--group-by-settings-height": `${settingsHeight.value}px` }
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

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  const settings = nodeData.value?.setting_input ?? null;
  if (settings) {
    if (!settings.groupby_input) settings.groupby_input = { agg_cols: [] };
    if (!settings.groupby_input.agg_cols) settings.groupby_input.agg_cols = [];
  }
  nodeGroupBy.value = settings;
  filterText.value = "";
  sortDirection.value = "none";
  clearSelection();
  clearRowSelection();
  settingsHeight.value = splitPreference.height;
  settingsCollapsed.value = splitPreference.collapsed;
  dataLoaded.value = true;
};

// Missing-column warnings come from the backend settings validation; this only
// covers configuration completeness the backend does not check.
const validateConfig = () => {
  if (!nodeGroupBy.value) return;
  const nodeId = nodeGroupBy.value.node_id;
  if (rows.value.length === 0) {
    nodeStore.setNodeValidation(nodeId, {
      isValid: false,
      error: "Please select at least one field.",
    });
  } else if (duplicateNames.value.size > 0) {
    nodeStore.setNodeValidation(nodeId, {
      isValid: false,
      error: `Output names must be unique: ${[...duplicateNames.value].join(", ")}`,
    });
  } else {
    nodeStore.setNodeValidation(nodeId, { isValid: true, error: "" });
  }
};

onMounted(() => {
  window.addEventListener("click", handleClickOutside);
});

onUnmounted(() => {
  window.removeEventListener("click", handleClickOutside);
  if (flashTimer) clearTimeout(flashTimer);
  endResize?.();
});

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});
</script>

<style scoped>
.group-by-root {
  height: 100%;
  display: flex;
  flex-direction: column;
  min-height: 0;
}

/* One card, like the Select picker, filling the tab pane so both sections
   share the drawer height instead of the columns pushing the settings below
   the fold. The .listbox-wrapper chrome (shadow, radius, padding) is shared. */
.group-by-card {
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
.group-by-columns {
  display: flex;
  flex-direction: column;
  flex: 0 1000 auto;
  min-height: 84px;
}

.group-by-settings {
  display: flex;
  flex-direction: column;
  flex: 0 1 auto;
  min-height: 30px;
  max-height: 55%;
  transition: box-shadow var(--transition-fast) var(--transition-timing);
}

/* A dragged split pins the settings; the max keeps the column list its floor. */
.group-by-settings.is-sized {
  flex: 0 0 var(--group-by-settings-height);
  max-height: calc(100% - 84px);
}

.group-by-settings.is-collapsed {
  flex: 0 0 auto;
  max-height: none;
}

.group-by-settings.is-drop-target {
  box-shadow: inset 0 0 0 1px var(--color-accent);
  border-radius: var(--border-radius-sm);
}

/* Focusable only so Delete and Escape reach the selected rows. */
.group-by-settings:focus {
  outline: none;
}

.group-by-scroll {
  flex: 1 1 auto;
  min-height: 0;
  overflow: auto;
}

.group-by-scroll::-webkit-scrollbar {
  width: 6px;
}

.group-by-scroll::-webkit-scrollbar-track {
  background: transparent;
}

.group-by-scroll::-webkit-scrollbar-thumb {
  background-color: var(--color-gray-300);
  border-radius: var(--border-radius-full);
}

/* ----- density ----- */

/* Two tables share one card, so rows run denser than the shared 32px default. */
.group-by-card .column-list-toolbar {
  flex-shrink: 0;
  height: 30px;
}

.group-by-card .column-list-selection .btn {
  height: 22px;
  padding-top: 0;
  padding-bottom: 0;
}

.group-by-card .styled-table thead th {
  height: 26px;
  padding-top: 0;
  padding-bottom: 0;
}

.group-by-card .styled-table tr {
  height: 26px;
}

.group-by-card .styled-table td {
  height: 26px;
  padding-top: 2px;
  padding-bottom: 2px;
}

/* Baseline-aligned inline-flex would add descender space under the 22px buttons. */
.group-by-card .styled-table td.row-tools-cell {
  padding-top: 1px;
  padding-bottom: 1px;
}

.group-by-card .row-tools {
  vertical-align: middle;
}

/* The 24px select needs a little more than the text rows. */
.group-by-card .group-by-agg-table tbody tr,
.group-by-card .group-by-agg-table tbody td {
  height: 28px;
  padding-top: 1px;
  padding-bottom: 1px;
}

/* ----- columns table ----- */

.group-by-column-table tbody tr,
.group-by-agg-table tbody tr {
  cursor: default;
}

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
.row-tools-cell {
  text-align: right;
  white-space: nowrap;
}

.row-tools {
  display: inline-flex;
  gap: var(--spacing-0-5);
  opacity: 0;
  transition: opacity var(--transition-fast) var(--transition-timing);
}

tbody tr:hover .row-tools,
tbody tr.is-selected .row-tools,
.row-tools:focus-within {
  opacity: 1;
}

.row-tools .btn {
  width: 22px;
  min-width: 22px;
  height: 22px;
  padding: 0;
  border-radius: var(--border-radius-sm);
}

.row-tools .material-icons {
  font-size: 16px;
}

.row-tools .btn:hover:not(:disabled) {
  background-color: var(--color-accent-subtle);
  color: var(--color-accent-hover);
}

.row-tools .row-remove:hover:not(:disabled) {
  background-color: var(--color-danger-light);
  color: var(--color-danger);
}

.empty-cell {
  text-align: center;
  color: var(--color-text-tertiary);
}

/* ----- settings section ----- */

/* The strip doubles as the sash between the two sections. */
.group-by-settings-header {
  position: relative;
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

.group-by-settings.is-resizing .group-by-settings-header {
  border-top-color: var(--color-accent);
}

.group-by-sash-grip {
  position: absolute;
  top: 50%;
  left: 50%;
  width: 28px;
  height: 3px;
  transform: translate(-50%, -50%);
  border-radius: var(--border-radius-full);
  background-color: var(--color-border-secondary);
  pointer-events: none;
  transition: background-color var(--transition-fast) var(--transition-timing);
}

.group-by-settings-header:hover .group-by-sash-grip,
.group-by-settings.is-resizing .group-by-sash-grip {
  background-color: var(--color-text-muted);
}

.group-by-header-tools {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  margin-left: auto;
  cursor: default;
}

.group-by-fold {
  width: 22px;
  min-width: 22px;
  height: 22px;
  padding: 0;
  border-radius: var(--border-radius-sm);
}

.group-by-fold .material-icons {
  font-size: 18px;
}

.group-by-settings-title {
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.group-by-settings-count {
  color: var(--color-text-tertiary);
  white-space: nowrap;
}

.group-by-row-selection {
  cursor: default;
}

.group-by-card .group-by-remove-selected {
  color: var(--color-danger);
}

.group-by-card .group-by-remove-selected:hover:not(:disabled) {
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

.group-by-flag {
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

.group-by-empty {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-1);
  padding: var(--spacing-2) var(--spacing-3);
  color: var(--color-text-tertiary);
  font-size: var(--font-size-xs);
  text-align: center;
}

.group-by-empty .material-icons {
  font-size: 16px;
  color: var(--color-text-secondary);
}

.agg-field-cell {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.group-by-card .inline-input.is-duplicate {
  border-color: var(--color-danger);
  box-shadow: 0 0 0 2px var(--color-focus-ring-error);
}

.group-by-agg-table tbody tr.is-new > td {
  animation: group-by-flash 1.2s var(--transition-timing);
}

@keyframes group-by-flash {
  from {
    background-color: var(--color-accent-subtle);
  }
  to {
    background-color: var(--color-background-primary);
  }
}

@media (prefers-reduced-motion: reduce) {
  .group-by-agg-table tbody tr.is-new > td {
    animation: none;
    background-color: var(--color-accent-subtle);
  }
}
</style>
