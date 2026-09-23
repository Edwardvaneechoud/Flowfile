<template>
  <div v-if="dataLoaded && nodePivot && input" class="column-picker-host">
    <generic-node-settings
      v-model="nodePivot"
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
        settings-label="Pivot settings"
        @drop="assign"
        @column-action="onColumnAction"
        @remove-rows="removeRows"
        @reveal="flashRows"
        @drag-change="draggingNames = $event"
      >
        <template #selection-actions="{ selectedNames, openMenu }">
          <button
            class="btn btn-sm btn-ghost"
            type="button"
            title="Add the selected columns as index keys"
            @mousedown.prevent
            @click="assign(selectedNames, 'index')"
          >
            Index
          </button>
          <button
            class="btn btn-sm btn-ghost"
            type="button"
            title="Use the selected column as the pivot or value column"
            @mousedown.prevent
            @click="openMenu(selectedNames, $event, 'roles')"
          >
            Set as ▾
          </button>
        </template>

        <template #row-actions="{ column, openMenu }">
          <button
            type="button"
            class="btn btn-sm btn-ghost btn-icon"
            :title="`Add ${column.name} as an index key`"
            :aria-label="`Index by ${column.name}`"
            @mousedown.prevent
            @click.stop="assign([column.name], 'index')"
          >
            <span class="material-icons" aria-hidden="true">add</span>
          </button>
          <button
            type="button"
            class="btn btn-sm btn-ghost btn-icon"
            :title="`Use ${column.name} as the pivot or value column…`"
            :aria-label="`Set ${column.name} as`"
            @mousedown.prevent
            @click.stop="openMenu([column.name], $event, 'roles')"
          >
            <span class="material-icons" aria-hidden="true">pivot_table_chart</span>
          </button>
        </template>

        <template #strip>
          <span
            v-if="missing.length > 0"
            class="picker-flag"
            title="Required before this node can run"
          >
            Missing: {{ missing.join(", ") }}
          </span>
        </template>

        <template #empty>
          Drag columns here, or hover a column and use
          <span class="material-icons" aria-label="add">add</span> for index keys and
          <span class="material-icons" aria-label="set as">pivot_table_chart</span> for the pivot
          and value columns.
        </template>

        <template #settings="{ isRowSelected, onRowClick, onRowContextMenu, onRowMouseDown }">
          <table class="styled-table column-list pivot-role-table">
            <colgroup>
              <col />
              <col style="width: 132px" />
              <col style="width: 128px" />
              <col style="width: 34px" />
            </colgroup>
            <thead>
              <tr>
                <th>Field</th>
                <th class="picker-control-header">Role</th>
                <th>Becomes</th>
                <th aria-label="Remove" />
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="(row, index) in rows"
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
                    columnNames.has(row.name) ? row.name : `${row.name} is no longer in the input`
                  "
                >
                  {{ row.name }}
                </td>
                <td>
                  <el-select
                    :model-value="row.role"
                    size="small"
                    :aria-label="`Role of ${row.name}`"
                    @update:model-value="assign([row.name], $event)"
                  >
                    <el-option
                      v-for="role in PIVOT_ROLES"
                      :key="role.value"
                      :label="role.label"
                      :value="role.value"
                    />
                  </el-select>
                </td>
                <td class="picker-note-cell">{{ pivotBecomes(row.role) }}</td>
                <td class="row-tools-cell">
                  <span class="row-tools">
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
        </template>

        <template #footer>
          <span class="picker-footer-label">Aggregations</span>
          <el-select
            v-model="input.aggregations"
            multiple
            collapse-tags
            collapse-tags-tooltip
            :max-collapse-tags="3"
            size="small"
            class="pivot-agg-select"
            placeholder="Choose at least one"
            aria-label="Aggregations"
          >
            <el-option
              v-for="agg in PIVOT_AGGREGATIONS"
              :key="agg"
              :label="aggLabel(agg)"
              :value="agg"
            />
          </el-select>
        </template>
      </column-picker-card>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref, computed, onUnmounted } from "vue";
import { CodeLoader } from "vue-content-loader";
import { ElMessage } from "element-plus";
import type { FileColumn, NodePivot } from "../../../baseNode/nodeInput";
import type { NodeData } from "../../../baseNode/nodeInterfaces";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import ColumnPickerCard from "../../../baseNode/selectComponents/ColumnPickerCard.vue";
import type { ContextMenuOption } from "../../../../common";
import { aggLabel } from "../../../baseNode/aggregations";
import {
  pluralize,
  withoutRows,
  type DropZoneSpec,
  type UsageChip,
} from "../../../baseNode/selectComponents/columnPicker";
import { assignRole, type RoleRow } from "../../../baseNode/selectComponents/columnRoles";
import {
  PIVOT_AGGREGATIONS,
  PIVOT_ROLES,
  missingPivotParts,
  pivotBecomes,
  pivotRoleLabel,
  rowsFromPivot,
  writePivotRows,
} from "./pivotLogic";

const nodeStore = useNodeStore();
const nodePivot = ref<NodePivot | null>(null);
const nodeData = ref<null | NodeData>(null);
const dataLoaded = ref(false);
const picker = ref<InstanceType<typeof ColumnPickerCard> | null>(null);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodePivot,
  /** The backend rejects a pivot without all its parts, so the drawer says what is missing instead. */
  onBeforeSave: () => {
    if (missing.value.length === 0) return true;
    validateConfig();
    ElMessage.warning({
      message: `Pivot still needs: ${missing.value.join(", ")}.`,
      showClose: true,
    });
    return false;
  },
  onAfterSave: async () => {
    validateConfig();
  },
});

const columns = computed<FileColumn[]>(() => nodeData.value?.main_input?.table_schema ?? []);
const input = computed(() => nodePivot.value?.pivot_input ?? null);
const rows = computed<RoleRow[]>(() => (input.value ? rowsFromPivot(input.value) : []));
const missing = computed(() => (input.value ? missingPivotParts(input.value) : []));
const columnNames = computed(() => new Set(columns.value.map((column) => column.name)));
const usedCount = computed(
  () => rows.value.filter((row) => columnNames.value.has(row.name)).length,
);

const settingsCountLabel = computed(() => {
  if (!input.value || rows.value.length === 0) return "";
  const keys = pluralize(input.value.index_columns.length, "index key");
  return `${keys} · pivot ${input.value.pivot_column ?? "—"} · value ${input.value.value_col ?? "—"}`;
});

const usageChips = (name: string): UsageChip[] => {
  const index = rows.value.findIndex((row) => row.name === name);
  if (index === -1) return [];
  const role = rows.value[index].role;
  return [
    {
      label: role,
      title: `Show the ${pivotRoleLabel(role).toLowerCase()} row`,
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
  const result = assignRole(rows.value, names, role, PIVOT_ROLES);
  writePivotRows(input.value, result.rows);
  // Rows regroup by role on write, so the picker's positions are stale from here.
  void picker.value?.reveal(indicesOf(names));
  picker.value?.clearSelection();
  picker.value?.clearRowSelection();
};

const indicesOf = (names: string[]) =>
  rows.value.flatMap((row, index) => (names.includes(row.name) ? [index] : []));

const onColumnAction = (action: string, names: string[]) => assign(names, action);

const removeRows = (indices: number[]) => {
  if (input.value) writePivotRows(input.value, withoutRows(rows.value, indices));
  flashedRows.value = new Set();
};

const removeRow = (index: number) => picker.value?.removeRows([index]);

// ----- menus, drops -----

const columnMenuOptions = (names: string[], variant: string): ContextMenuOption[] => {
  const many = names.length > 1;
  const single = [
    { label: "Set as pivot column", action: "pivot", disabled: many },
    { label: "Set as value column", action: "value", disabled: many },
  ];
  return variant === "roles" ? single : [{ label: "Add as index key", action: "index" }, ...single];
};

const draggingNames = ref<string[]>([]);

/** The single-holder zones only take one column, so a multi-drag greys them out. */
const dropZones = computed<DropZoneSpec[]>(() =>
  PIVOT_ROLES.map((role) => ({
    value: role.value,
    label: role.label.replace(/ (key|column)$/, ""),
    disabled: Boolean(role.single) && draggingNames.value.length > 1,
  })),
);

// ----- lifecycle -----

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  const settings = (nodeData.value?.setting_input as NodePivot | undefined) ?? null;
  if (settings) {
    settings.pivot_input ??= {
      index_columns: [],
      pivot_column: null,
      value_col: null,
      aggregations: [],
    };
    settings.pivot_input.index_columns ??= [];
    settings.pivot_input.aggregations ??= [];
  }
  nodePivot.value = settings;
  dataLoaded.value = true;
  validateConfig();
};

const validateConfig = () => {
  if (!nodePivot.value) return;
  const nodeId = nodePivot.value.node_id;
  if (missing.value.length > 0) {
    nodeStore.setNodeValidation(nodeId, {
      isValid: false,
      error: `Pivot still needs: ${missing.value.join(", ")}.`,
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
.pivot-agg-select {
  flex: 1 1 auto;
  min-width: 0;
}
</style>
