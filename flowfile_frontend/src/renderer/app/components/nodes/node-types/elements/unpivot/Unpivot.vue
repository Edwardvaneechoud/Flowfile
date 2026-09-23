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
        :used-count="rows.length"
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

        <template #empty>
          <template v-if="byColumns">
            Drag columns here, or hover a column and use
            <span class="material-icons" aria-label="add">add</span> for index keys and
            <span class="material-icons" aria-label="unpivot">unfold_more</span> for the columns to
            unpivot.
          </template>
          <template v-else>
            Every column that is not an index key is unpivoted. Drag columns here, or hover one and
            use <span class="material-icons" aria-label="add">add</span>, to keep it on every row
            instead.
          </template>
        </template>

        <template
          #settings="{ isRowSelected, isDragging, onRowClick, onRowContextMenu, onRowMouseDown }"
        >
          <table class="styled-table column-list unpivot-role-table">
            <colgroup>
              <col />
              <col style="width: 132px" />
              <col style="width: 140px" />
              <col style="width: 34px" />
            </colgroup>
            <thead>
              <tr>
                <th>Field</th>
                <th>Role</th>
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
                  'is-drop-after': isDragging && index === rows.length - 1,
                }"
                @mousedown="onRowMouseDown"
                @click="onRowClick(index, $event)"
                @contextmenu="onRowContextMenu($event, index)"
              >
                <td class="picker-field-cell" :title="row.name">{{ row.name }}</td>
                <td>
                  <el-select
                    :model-value="row.role"
                    size="small"
                    :aria-label="`Role of ${row.name}`"
                    @update:model-value="assign([row.name], $event)"
                  >
                    <el-option
                      v-for="role in UNPIVOT_ROLES"
                      :key="role.value"
                      :label="role.label"
                      :value="role.value"
                      :disabled="role.value === 'value' && !byColumns"
                    />
                  </el-select>
                </td>
                <td class="picker-note-cell">{{ unpivotBecomes(row.role) }}</td>
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
          <span class="picker-footer-label">Values</span>
          <el-switch
            :model-value="input.data_type_selector_mode"
            active-value="column"
            inactive-value="data_type"
            active-text="Chosen columns"
            inactive-text="By data type"
            inline-prompt
            size="small"
            aria-label="How the value columns are chosen"
            @update:model-value="setMode"
          />
          <el-select
            v-if="!byColumns"
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
  unpivotBecomes,
  unpivotRoleLabel,
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
const rows = computed<RoleRow[]>(() => (input.value ? rowsFromUnpivot(input.value) : []));
const missing = computed(() => (input.value ? missingUnpivotParts(input.value) : []));

const settingsCountLabel = computed(() => {
  if (!input.value || rows.value.length === 0) return "";
  const keys = pluralize(input.value.index_columns.length, "index key");
  return byColumns.value
    ? `${keys} · ${pluralize(input.value.value_columns.length, "value column")}`
    : `${keys} · values: ${dataTypeLabel(input.value.data_type_selector).toLowerCase()}`;
});

const usageChips = (name: string): UsageChip[] => {
  const index = rows.value.findIndex((row) => row.name === name);
  if (index === -1) return [];
  const role = rows.value[index].role;
  return [
    {
      label: role,
      title: `Show the ${unpivotRoleLabel(role).toLowerCase()} row`,
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
  if (result.touched.length > 0) void picker.value?.reveal(result.touched);
  picker.value?.clearSelection();
};

const onColumnAction = (action: string, names: string[]) => assign(names, action);

const removeRows = (indices: number[]) => {
  if (input.value) writeUnpivotRows(input.value, withoutRows(rows.value, indices));
};

const removeRow = (index: number) => picker.value?.removeRows([index]);

/** Switching to data types drops the chosen columns, as the save would anyway. */
const setMode = (value: string | number | boolean) => {
  const mode = value as DataSelectorMode;
  const settings = input.value;
  if (!settings) return;
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
  }
  nodeUnpivot.value = settings;
  dataLoaded.value = true;
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
.unpivot-type-select {
  flex: 1 1 auto;
  min-width: 0;
}
</style>
