<template>
  <div v-if="dataLoaded && nodeGroupBy" class="column-picker-host">
    <generic-node-settings
      v-model="nodeGroupBy"
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
        settings-label="Group by settings"
        @drop="onDrop"
        @column-action="onColumnAction"
        @remove-rows="removeRows"
        @reveal="flashRows"
        @drag-change="draggingNames = $event"
      >
        <template #selection-actions="{ selectedNames, openMenu }">
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
            @click="openMenu(selectedNames, $event, 'aggregate')"
          >
            Aggregate ▾
          </button>
        </template>

        <template #row-actions="{ column, openMenu }">
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
            @click.stop="openMenu([column.name], $event, 'aggregate')"
          >
            <span class="material-icons" aria-hidden="true">functions</span>
          </button>
        </template>

        <template #strip>
          <button
            v-if="duplicateNames.size > 0"
            type="button"
            class="picker-flag"
            :title="`Output names must be unique: ${[...duplicateNames].join(', ')}`"
            @mousedown.prevent
            @click="revealDuplicates"
          >
            {{ pluralize(duplicateNames.size, "duplicate name") }}
          </button>
        </template>

        <template #empty>
          Drag columns here, or hover a column and use
          <span class="material-icons" aria-label="add">add</span> /
          <span class="material-icons" aria-label="aggregate">functions</span>
        </template>

        <template
          #settings="{ isRowSelected, activeZone, onRowClick, onRowContextMenu, onRowMouseDown }"
        >
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
                <th class="picker-control-header">Action</th>
                <th class="picker-control-header">Output name</th>
                <th aria-label="Remove" />
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="(item, index) in rows"
                :key="index"
                :class="{
                  'is-new': flashedRows.has(index),
                  'is-selected': isRowSelected(index),
                  'is-stale': !columnNames.has(item.old_name),
                  'is-drop-after': activeZone !== null && index === rows.length - 1,
                }"
                @mousedown="onRowMouseDown"
                @click="onRowClick(index, $event)"
                @contextmenu="onRowContextMenu($event, index)"
              >
                <td
                  class="picker-field-cell agg-field-cell"
                  :title="
                    columnNames.has(item.old_name)
                      ? item.old_name
                      : `${item.old_name} is no longer in the input`
                  "
                >
                  {{ item.old_name }}
                </td>
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
                        : item.new_name || undefined
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
        </template>
      </column-picker-card>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref, computed, onUnmounted } from "vue";
import { CodeLoader } from "vue-content-loader";
import type { AggColl, FileColumn, NodeGroupBy } from "../../../baseNode/nodeInput";
import type { NodeData } from "../../../baseNode/nodeInterfaces";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { NO_AUTOFILL } from "../../../../../utils/noAutofill";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import ColumnPickerCard from "../../../baseNode/selectComponents/ColumnPickerCard.vue";
import type { ContextMenuOption } from "../../../../common";
import {
  capUsageChips,
  pluralize,
  withoutRows,
  type DropZoneSpec,
  type UsageChip,
} from "../../../baseNode/selectComponents/columnPicker";
import {
  AGG_OPTIONS,
  AGGREGATE_OPTIONS,
  addAggRows,
  aggLabel,
  defaultAggFor,
  duplicateOutputNames,
  effectiveOutputName,
  outputNameFor,
  renamedForAgg,
  usageByColumn,
  usesByAgg,
  type AggKind,
} from "./groupByLogic";

const nodeStore = useNodeStore();
const nodeGroupBy = ref<null | NodeGroupBy>(null);
const nodeData = ref<null | NodeData>(null);
const dataLoaded = ref(false);
const picker = ref<InstanceType<typeof ColumnPickerCard> | null>(null);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeGroupBy,
  onBeforeSave: () => {
    // The backend only defaults a missing name, so a blank field gets its placeholder here.
    rows.value.forEach((row) => {
      row.new_name = effectiveOutputName(row);
    });
    return true;
  },
  onAfterSave: async () => {
    validateConfig();
  },
});

const columns = computed<FileColumn[]>(() => nodeData.value?.main_input?.table_schema ?? []);
const rows = computed<AggColl[]>(() => nodeGroupBy.value?.groupby_input?.agg_cols ?? []);
const usage = computed(() => usageByColumn(rows.value));
const columnNames = computed(() => new Set(columns.value.map((column) => column.name)));
/** Rows for columns the input no longer has do not count as "in use". */
const usedCount = computed(
  () => [...usage.value.keys()].filter((name) => columnNames.value.has(name)).length,
);
const duplicateNames = computed(() => duplicateOutputNames(rows.value));
const isDuplicateName = (item: AggColl) => duplicateNames.value.has(effectiveOutputName(item));

const settingsCountLabel = computed(() => {
  if (rows.value.length === 0) return "";
  const keys = rows.value.filter((row) => row.agg === "groupby").length;
  return `${pluralize(keys, "key")} · ${pluralize(rows.value.length - keys, "aggregation")}`;
});

const usageChips = (name: string): UsageChip[] =>
  capUsageChips(
    usesByAgg(usage.value.get(name) ?? []).map((use) => ({
      label: use.agg === "groupby" ? "key" : use.agg,
      title: `Show the ${aggLabel(use.agg)} ${use.rows.length === 1 ? "row" : "rows"}`,
      isKey: use.agg === "groupby",
      rows: use.rows,
    })),
  );

// ----- feedback -----

const flashedRows = ref(new Set<number>());
let flashTimer: ReturnType<typeof setTimeout> | null = null;

/** Highlights rows briefly — the feedback for "where did it go?". */
const flashRows = (indices: number[]) => {
  flashedRows.value = new Set(indices);
  if (flashTimer) clearTimeout(flashTimer);
  flashTimer = setTimeout(() => {
    flashedRows.value = new Set();
  }, 1200);
};

const revealRows = (indices: number[]) => {
  if (indices.length > 0) void picker.value?.reveal(indices);
};

const revealDuplicates = () => {
  revealRows(rows.value.flatMap((row, index) => (isDuplicateName(row) ? [index] : [])));
};

// ----- edits -----

const addRows = (names: string[], agg: AggKind) => {
  const input = nodeGroupBy.value?.groupby_input;
  if (!input || names.length === 0) return;
  const result = addAggRows(input.agg_cols, names, agg);
  input.agg_cols = result.rows;
  revealRows([...result.added, ...result.existing]);
  picker.value?.clearSelection();
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
  revealRows(touched);
  picker.value?.clearSelection();
};

const setAgg = (item: AggColl, next: string) => {
  item.new_name = renamedForAgg(item, next);
  item.agg = next;
};

const removeRows = (indices: number[]) => {
  const input = nodeGroupBy.value?.groupby_input;
  if (input) input.agg_cols = withoutRows(input.agg_cols, indices);
  flashedRows.value = new Set();
};

const removeRow = (index: number) => picker.value?.removeRows([index]);

// ----- menus, drops -----

const aggregateMenuOptions: ContextMenuOption[] = AGGREGATE_OPTIONS.map((option) => ({
  label: option.label,
  action: option.value,
}));

const columnMenuOptions = (_names: string[], variant: string): ContextMenuOption[] =>
  variant === "aggregate"
    ? aggregateMenuOptions
    : [{ label: "Group by", action: "groupby" }, ...aggregateMenuOptions];

const onColumnAction = (action: string, names: string[]) => addRows(names, action as AggKind);

const draggingNames = ref<string[]>([]);

const dragAggHint = computed(() => {
  const aggs = new Set(
    draggingNames.value.map((name) =>
      defaultAggFor(columns.value.find((col) => col.name === name)?.data_type),
    ),
  );
  return [...aggs].join(" / ");
});

const dropZones = computed<DropZoneSpec[]>(() => [
  { value: "groupby", label: "Group by" },
  { value: "aggregate", label: `Aggregate · ${dragAggHint.value}` },
]);

const onDrop = (names: string[], zone: string) => {
  if (zone === "groupby") addRows(names, "groupby");
  else addDefaultAggregations(names);
};

// ----- lifecycle -----

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  const settings = nodeData.value?.setting_input ?? null;
  if (settings) {
    if (!settings.groupby_input) settings.groupby_input = { agg_cols: [] };
    if (!settings.groupby_input.agg_cols) settings.groupby_input.agg_cols = [];
  }
  nodeGroupBy.value = settings;
  dataLoaded.value = true;
  validateConfig();
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
.group-by-agg-table .inline-input.is-duplicate {
  border-color: var(--color-danger);
  box-shadow: 0 0 0 2px var(--color-focus-ring-error);
}
</style>
