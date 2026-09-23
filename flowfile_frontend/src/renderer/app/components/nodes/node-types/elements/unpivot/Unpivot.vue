<template>
  <div v-if="dataLoaded && nodeUnpivot && input" class="column-picker-host">
    <generic-node-settings
      v-model="nodeUnpivot"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <column-picker-card
        ref="picker"
        :columns="columns"
        :usage-chips="usageChips"
        :used-count="usedCount"
        :column-menu-options="columnMenuOptions"
        :drop-zones="dropZones"
        :row-count="rows.length"
        :settings-count="settingsCountLabel"
        settings-label="Unpivot settings"
        @drop="assign"
        @column-action="onColumnAction"
        @remove-rows="removeRows"
        @reveal="flashRows"
      >
        <template #selection-actions="{ selectedNames }">
          <button
            class="btn btn-sm btn-ghost"
            type="button"
            title="Keep the selected columns on every row"
            @mousedown.prevent
            @click="assign(selectedNames, 'index')"
          >
            Index
          </button>
          <button
            v-if="byColumns"
            class="btn btn-sm btn-ghost"
            type="button"
            title="Unpivot the selected columns into rows"
            @mousedown.prevent
            @click="assign(selectedNames, 'value')"
          >
            Unpivot
          </button>
        </template>

        <template #row-actions="{ column }">
          <button
            type="button"
            class="btn btn-sm btn-ghost btn-icon"
            :title="`Keep ${column.name} on every row`"
            :aria-label="`Index by ${column.name}`"
            @mousedown.prevent
            @click.stop="assign([column.name], 'index')"
          >
            <span class="material-icons" aria-hidden="true">add</span>
          </button>
          <button
            v-if="byColumns"
            type="button"
            class="btn btn-sm btn-ghost btn-icon"
            :title="`Unpivot ${column.name} into rows`"
            :aria-label="`Unpivot ${column.name}`"
            @mousedown.prevent
            @click.stop="assign([column.name], 'value')"
          >
            <span class="material-icons" aria-hidden="true">unfold_more</span>
          </button>
        </template>

        <template #strip>
          <span
            v-if="missing.length > 0"
            class="picker-flag"
            title="Required before this node does anything"
          >
            Missing: {{ missing.join(", ") }}
          </span>
        </template>

        <template
          #settings="{ isRowSelected, activeZone, onRowClick, onRowContextMenu, onRowMouseDown }"
        >
          <div class="unpivot-layout">
            <div class="unpivot-groups">
              <section
                class="unpivot-group"
                :class="{ 'is-active-zone': activeZone === 'index' }"
                data-drop-zone="index"
                aria-label="Index keys"
              >
                <div class="unpivot-group-header">
                  <span class="unpivot-group-title">Index keys</span>
                  <span class="unpivot-group-hint">kept on every row</span>
                </div>
                <div class="unpivot-well" data-picker-scroll>
                  <table v-if="indexRows.length > 0" class="styled-table column-list unpivot-rows">
                    <colgroup>
                      <col />
                      <col style="width: 64px" />
                    </colgroup>
                    <tbody>
                      <tr
                        v-for="{ row, index } in indexRows"
                        :key="row.name"
                        :class="{
                          'is-new': flashedRows.has(index),
                          'is-selected': isRowSelected(index),
                          'is-stale': !columnNames.has(row.name),
                        }"
                        @mousedown="onRowMouseDown"
                        @click="onRowClick(index, $event)"
                        @contextmenu="onRowContextMenu($event, index)"
                      >
                        <td
                          class="picker-field-cell"
                          :title="
                            columnNames.has(row.name)
                              ? row.name
                              : `${row.name} is no longer in the input`
                          "
                        >
                          {{ row.name }}
                        </td>
                        <td class="row-tools-cell">
                          <span class="row-tools">
                            <button
                              v-if="byColumns"
                              type="button"
                              class="btn btn-sm btn-ghost btn-icon"
                              :title="`Unpivot ${row.name} instead`"
                              :aria-label="`Unpivot ${row.name} instead`"
                              @mousedown.prevent
                              @click="assign([row.name], 'value')"
                            >
                              <span class="material-icons" aria-hidden="true">unfold_more</span>
                            </button>
                            <button
                              type="button"
                              class="btn btn-sm btn-ghost btn-icon row-remove"
                              :title="`Remove ${row.name}`"
                              :aria-label="`Remove ${row.name}`"
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
                  <div v-else class="unpivot-well-empty">
                    Drop columns here to keep them on every row
                  </div>
                </div>
              </section>

              <section
                class="unpivot-group"
                :class="{ 'is-active-zone': activeZone === 'value' }"
                data-drop-zone="value"
                aria-label="Columns to unpivot"
              >
                <div class="unpivot-group-header">
                  <span class="unpivot-group-title">Columns to unpivot</span>
                  <span
                    class="seg-control"
                    role="group"
                    aria-label="How the columns to unpivot are chosen"
                  >
                    <button
                      type="button"
                      class="seg-option"
                      :class="{ 'is-on': byColumns }"
                      :aria-pressed="byColumns"
                      @mousedown.prevent
                      @click="setMode('column')"
                    >
                      Chosen
                    </button>
                    <button
                      type="button"
                      class="seg-option"
                      :class="{ 'is-on': !byColumns }"
                      :aria-pressed="!byColumns"
                      @mousedown.prevent
                      @click="setMode('data_type')"
                    >
                      By type
                    </button>
                  </span>
                </div>
                <div v-if="byColumns" class="unpivot-well" data-picker-scroll>
                  <table v-if="valueRows.length > 0" class="styled-table column-list unpivot-rows">
                    <colgroup>
                      <col />
                      <col style="width: 64px" />
                    </colgroup>
                    <tbody>
                      <tr
                        v-for="{ row, index } in valueRows"
                        :key="row.name"
                        :class="{
                          'is-new': flashedRows.has(index),
                          'is-selected': isRowSelected(index),
                          'is-stale': !columnNames.has(row.name),
                        }"
                        @mousedown="onRowMouseDown"
                        @click="onRowClick(index, $event)"
                        @contextmenu="onRowContextMenu($event, index)"
                      >
                        <td
                          class="picker-field-cell"
                          :title="
                            columnNames.has(row.name)
                              ? row.name
                              : `${row.name} is no longer in the input`
                          "
                        >
                          {{ row.name }}
                        </td>
                        <td class="row-tools-cell">
                          <span class="row-tools">
                            <button
                              type="button"
                              class="btn btn-sm btn-ghost btn-icon"
                              :title="`Keep ${row.name} on every row instead`"
                              :aria-label="`Keep ${row.name} on every row instead`"
                              @mousedown.prevent
                              @click="assign([row.name], 'index')"
                            >
                              <span class="material-icons" aria-hidden="true">add</span>
                            </button>
                            <button
                              type="button"
                              class="btn btn-sm btn-ghost btn-icon row-remove"
                              :title="`Remove ${row.name}`"
                              :aria-label="`Remove ${row.name}`"
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
                  <div v-else class="unpivot-well-empty">Drop the columns to unpivot here</div>
                </div>
                <div v-else class="unpivot-well unpivot-by-type">
                  <el-select
                    v-model="input.data_type_selector"
                    size="small"
                    class="unpivot-type-select"
                    placeholder="Choose a data type"
                    aria-label="Data type to unpivot"
                  >
                    <el-option
                      v-for="option in DATA_TYPE_OPTIONS"
                      :key="option.value"
                      :label="option.label"
                      :value="option.value"
                    />
                  </el-select>
                  <span class="unpivot-group-hint"
                    >every matching column that is not an index key</span
                  >
                </div>
              </section>
            </div>
          </div>
        </template>
      </column-picker-card>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref, computed, onUnmounted } from "vue";
import { CodeLoader } from "vue-content-loader";
import type { DataSelectorMode, FileColumn, NodeUnpivot } from "../../../baseNode/nodeInput";
import type { NodeData } from "../../../baseNode/nodeInterfaces";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import ColumnPickerCard from "../../../baseNode/selectComponents/ColumnPickerCard.vue";
import type { ContextMenuOption } from "../../../../common";
import {
  pluralize,
  withoutRows,
  type DropZoneSpec,
  type UsageChip,
} from "../../../baseNode/selectComponents/columnPicker";
import { assignRole, type RoleRow } from "../../../baseNode/selectComponents/columnRoles";
import {
  DATA_TYPE_OPTIONS,
  UNPIVOT_ROLES,
  dataTypeLabel,
  missingUnpivotParts,
  rowsFromUnpivot,
  writeUnpivotRows,
} from "./unpivotLogic";

const nodeStore = useNodeStore();
const nodeUnpivot = ref<NodeUnpivot | null>(null);
const nodeData = ref<null | NodeData>(null);
const dataLoaded = ref(false);
const picker = ref<InstanceType<typeof ColumnPickerCard> | null>(null);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeUnpivot,
  onBeforeSave: () => {
    const settings = input.value;
    if (settings) {
      if (settings.data_type_selector_mode === "data_type") settings.value_columns = [];
      else settings.data_type_selector = null;
    }
    return true;
  },
  onAfterSave: async () => {
    validateConfig();
  },
});

const columns = computed<FileColumn[]>(() => nodeData.value?.main_input?.table_schema ?? []);
const input = computed(() => nodeUnpivot.value?.unpivot_input ?? null);
const byColumns = computed(() => input.value?.data_type_selector_mode !== "data_type");
/** Index keys first, then value columns; the picker addresses rows by this flat index. */
const rows = computed<RoleRow[]>(() => (input.value ? rowsFromUnpivot(input.value) : []));
const indexed = computed(() => rows.value.map((row, index) => ({ row, index })));
const indexRows = computed(() => indexed.value.filter(({ row }) => row.role === "index"));
const valueRows = computed(() => indexed.value.filter(({ row }) => row.role === "value"));
const missing = computed(() => (input.value ? missingUnpivotParts(input.value) : []));
const columnNames = computed(() => new Set(columns.value.map((column) => column.name)));
const usedCount = computed(
  () => rows.value.filter((row) => columnNames.value.has(row.name)).length,
);

const settingsCountLabel = computed(() => {
  if (!input.value) return "";
  const keys = pluralize(input.value.index_columns.length, "index key");
  return byColumns.value
    ? `${keys} · ${pluralize(input.value.value_columns.length, "column")} to unpivot`
    : `${keys} · unpivot ${dataTypeLabel(input.value.data_type_selector).toLowerCase()}`;
});

const usageChips = (name: string): UsageChip[] => {
  const index = rows.value.findIndex((row) => row.name === name);
  if (index === -1) return [];
  const role = rows.value[index].role;
  return [
    {
      label: role === "index" ? "index" : "unpivot",
      title: `Show ${role === "index" ? "the index key" : "the column to unpivot"}`,
      isKey: role === "index",
      rows: [index],
    },
  ];
};

// ----- feedback -----

const flashedRows = ref(new Set<number>());
let flashTimer: ReturnType<typeof setTimeout> | null = null;

const flashRows = (indices: number[]) => {
  flashedRows.value = new Set(indices);
  if (flashTimer) clearTimeout(flashTimer);
  flashTimer = setTimeout(() => {
    flashedRows.value = new Set();
  }, 1200);
};

// ----- edits -----

const assign = (names: string[], role: string) => {
  if (!input.value || names.length === 0) return;
  if (role === "value" && !byColumns.value) return;
  const result = assignRole(rows.value, names, role, UNPIVOT_ROLES);
  writeUnpivotRows(input.value, result.rows);
  // Rows regroup by area on write, so the picker's positions are stale from here.
  void picker.value?.reveal(indicesOf(names));
  picker.value?.clearSelection();
  picker.value?.clearRowSelection();
};

const indicesOf = (names: string[]) =>
  rows.value.flatMap((row, index) => (names.includes(row.name) ? [index] : []));

const onColumnAction = (action: string, names: string[]) => assign(names, action);

const removeRows = (indices: number[]) => {
  if (input.value) writeUnpivotRows(input.value, withoutRows(rows.value, indices));
  flashedRows.value = new Set();
};

const removeRow = (index: number) => picker.value?.removeRows([index]);

/** Switching to data types drops the chosen columns, as the save would anyway. */
const setMode = (mode: DataSelectorMode) => {
  const settings = input.value;
  if (!settings || settings.data_type_selector_mode === mode) return;
  settings.data_type_selector_mode = mode;
  if (mode === "data_type") {
    settings.value_columns = [];
    settings.data_type_selector ??= "all";
  } else {
    settings.data_type_selector = null;
  }
};

// ----- menus, drops -----

const columnMenuOptions = (): ContextMenuOption[] => [
  { label: "Keep as index key", action: "index" },
  { label: "Unpivot into rows", action: "value", disabled: !byColumns.value },
];

const dropZones = computed<DropZoneSpec[]>(() =>
  byColumns.value
    ? [
        { value: "index", label: "Index" },
        { value: "value", label: "Unpivot" },
      ]
    : [{ value: "index", label: "Index" }],
);

// ----- lifecycle -----

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  const settings = (nodeData.value?.setting_input as NodeUnpivot | undefined) ?? null;
  if (settings) {
    settings.unpivot_input ??= {
      index_columns: [],
      value_columns: [],
      data_type_selector: null,
      data_type_selector_mode: "column",
    };
    settings.unpivot_input.index_columns ??= [];
    settings.unpivot_input.value_columns ??= [];
    settings.unpivot_input.data_type_selector_mode ??= "column";
    if (settings.unpivot_input.data_type_selector_mode === "data_type") {
      settings.unpivot_input.data_type_selector ??= "all";
    }
  }
  nodeUnpivot.value = settings;
  dataLoaded.value = true;
  validateConfig();
};

const validateConfig = () => {
  if (!nodeUnpivot.value) return;
  const nodeId = nodeUnpivot.value.node_id;
  if (missing.value.length > 0) {
    nodeStore.setNodeValidation(nodeId, {
      isValid: false,
      error: `Unpivot still needs: ${missing.value.join(", ")}.`,
    });
  } else {
    nodeStore.setNodeValidation(nodeId, { isValid: true, error: "" });
  }
};

onUnmounted(() => {
  if (flashTimer) clearTimeout(flashTimer);
});

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});
</script>

<style scoped>
/* Two areas side by side, each scrolling on its own; they stack when the pane is narrow.
   An element cannot query its own width, so the container sits one level up. */
.unpivot-layout {
  height: 100%;
  container-type: inline-size;
}

.unpivot-groups {
  display: flex;
  height: 100%;
}

.unpivot-group {
  display: flex;
  flex-direction: column;
  flex: 1 1 0;
  min-width: 0;
  min-height: 0;
  padding: var(--spacing-1) var(--spacing-2) var(--spacing-2);
}

.unpivot-group + .unpivot-group {
  border-left: 1px solid var(--color-border-light);
}

@container (max-width: 539px) {
  .unpivot-groups {
    flex-direction: column;
  }

  .unpivot-group {
    flex: 0 1 auto;
    max-height: 60%;
  }

  .unpivot-group:last-child {
    flex: 1 1 auto;
    max-height: none;
  }

  .unpivot-group + .unpivot-group {
    border-left: none;
    border-top: 1px solid var(--color-border-light);
  }
}

.unpivot-group-header {
  display: flex;
  align-items: center;
  flex-shrink: 0;
  gap: var(--spacing-2);
  height: 26px;
  font-size: var(--font-size-xs);
  user-select: none;
}

.unpivot-group-title {
  flex-shrink: 0;
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.unpivot-group-hint {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: var(--color-text-muted);
}

/* The list sits in a box, so an empty area still shows where a drop lands. */
.unpivot-well {
  flex: 1 1 auto;
  min-height: 56px;
  overflow: auto;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  background-color: var(--color-background-primary);
  transition:
    border-color var(--transition-fast) var(--transition-timing),
    background-color var(--transition-fast) var(--transition-timing),
    box-shadow var(--transition-fast) var(--transition-timing);
}

.unpivot-well::-webkit-scrollbar {
  width: 6px;
}

.unpivot-well::-webkit-scrollbar-track {
  background: transparent;
}

.unpivot-well::-webkit-scrollbar-thumb {
  background-color: var(--color-gray-300);
  border-radius: var(--border-radius-full);
}

.unpivot-group.is-active-zone .unpivot-well {
  border-color: var(--color-accent);
  background-color: var(--color-accent-subtle);
  box-shadow: inset 0 0 0 1px var(--color-accent);
}

/* One name per row: the cell divider would only fence off the hover buttons. */
.unpivot-rows td:not(:last-child) {
  border-right: none;
}

.unpivot-well-empty {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  min-height: 54px;
  padding: var(--spacing-2);
  color: var(--color-text-tertiary);
  font-size: var(--font-size-xs);
  text-align: center;
}

.unpivot-by-type {
  display: flex;
  flex-wrap: wrap;
  align-content: flex-start;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2);
  font-size: var(--font-size-xs);
}

.unpivot-type-select {
  flex: 0 1 220px;
  min-width: 140px;
}

/* Two-way switch in the area header: the on side is tinted, never a dark pill. */
.seg-control {
  display: inline-flex;
  flex-shrink: 0;
  margin-left: auto;
  padding: 1px;
  border: 1px solid var(--color-border-secondary);
  border-radius: var(--border-radius-full);
  background-color: var(--color-background-primary);
}

.seg-option {
  height: 18px;
  padding: 0 var(--spacing-2);
  border: none;
  border-radius: var(--border-radius-full);
  background: transparent;
  color: var(--color-text-secondary);
  font-family: inherit;
  font-size: var(--font-size-2xs);
  line-height: 18px;
  cursor: pointer;
  transition:
    background-color var(--transition-fast) var(--transition-timing),
    color var(--transition-fast) var(--transition-timing);
}

.seg-option:hover {
  color: var(--color-text-primary);
}

.seg-option.is-on {
  background-color: var(--color-accent-subtle);
  color: var(--color-accent-hover);
  font-weight: var(--font-weight-medium);
}
</style>
