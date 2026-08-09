<template>
  <el-dialog
    :model-value="modelValue"
    fullscreen
    class="table-editor-dialog"
    :show-close="false"
    :close-on-click-modal="false"
    :close-on-press-escape="false"
    @update:model-value="requestClose"
  >
    <template #header>
      <div class="editor-header">
        <div class="editor-title">
          <i class="fa-solid fa-pen-to-square"></i>
          <span>Edit table — {{ table.name }}</span>
          <span v-if="expectedVersion !== null" class="version-chip">
            version {{ expectedVersion }}
          </span>
        </div>
        <div class="editor-actions">
          <template v-if="step === 'grid'">
            <span class="pending-summary">{{ pendingSummary }}</span>
            <el-button size="small" :disabled="!canLabel" @click="openLabeling">
              <i class="fa-solid fa-tags"></i>&nbsp;Label rows
            </el-button>
            <el-button size="small" @click="showAddColumn = true">
              <i class="fa-solid fa-plus"></i>&nbsp;Add column
            </el-button>
            <el-button size="small" @click="addRow">
              <i class="fa-solid fa-plus"></i>&nbsp;Add row
            </el-button>
            <el-button size="small" :disabled="!hasSelection" @click="toggleDeleteSelected">
              <i class="fa-solid fa-trash-can"></i>&nbsp;Delete / restore selected
            </el-button>
            <el-button
              size="small"
              type="primary"
              :loading="saving"
              :disabled="!hasChanges"
              @click="save"
            >
              Save changes
            </el-button>
          </template>
          <el-button size="small" @click="requestClose">Close</el-button>
        </div>
      </div>
    </template>

    <div v-if="step === 'loading'" class="editor-state">Loading table data…</div>
    <div v-else-if="step === 'error'" class="editor-state editor-error">
      <i class="fa-solid fa-triangle-exclamation"></i>
      <p>{{ loadError }}</p>
      <el-button size="small" @click="loadSession">Retry</el-button>
    </div>

    <!-- Step 1: pick key columns -->
    <div v-else-if="step === 'setup'" class="editor-setup">
      <h3>How should edits attach to rows?</h3>
      <p class="setup-explainer">
        Edits are saved as keyed merges: the key column(s) must uniquely identify every row. Row
        order in the preview is not stable, so a real key is required.
      </p>
      <el-select
        v-model="selectedKeys"
        multiple
        filterable
        :multiple-limit="8"
        placeholder="Pick key column(s)"
        class="key-select"
      >
        <el-option
          v-for="col in keyCandidates"
          :key="col.name"
          :label="`${col.name} (${col.dtype})`"
          :value="col.name"
        />
      </el-select>
      <p v-if="suggestedKeys.length" class="setup-hint">
        Suggested: <code>{{ suggestedKeys.join(", ") }}</code>
      </p>
      <div class="setup-row">
        <span>Rows to load:</span>
        <el-select v-model="rowLimit" class="limit-select" @change="loadSession">
          <el-option v-for="n in [100, 500, 1000, 5000, 10000]" :key="n" :label="n" :value="n" />
        </el-select>
        <span v-if="totalRows > rowLimit" class="setup-warning">
          This table has {{ totalRows.toLocaleString() }} rows — only the first
          {{ rowLimit.toLocaleString() }} are loaded for editing. Rows beyond that are not shown and
          will not be modified.
        </span>
      </div>
      <div class="setup-actions">
        <el-button type="primary" :disabled="selectedKeys.length === 0" @click="startEditing">
          Start editing
        </el-button>
        <el-tooltip
          content="Rewrites the table with a sequential record_id column (a new schema column, visible to flows reading this table)."
          placement="top"
        >
          <el-button :loading="addingKeyColumn" @click="mintKeyColumn">
            No unique key? Add a record_id column
          </el-button>
        </el-tooltip>
      </div>
    </div>

    <!-- Step 2: the grid -->
    <div v-else-if="step === 'grid'" class="editor-grid-wrap">
      <div v-if="totalRows > session.rows.length" class="slice-banner">
        <i class="fa-solid fa-circle-info"></i>
        Editing the first {{ session.rows.length.toLocaleString() }} of
        {{ totalRows.toLocaleString() }} rows. Other rows are untouched by saves.
      </div>
      <ag-grid-vue
        class="ag-theme-balham editor-grid"
        :column-defs="columnDefs"
        :row-data="rowData"
        :default-col-def="defaultColDef"
        :get-row-id="getRowId"
        :row-class-rules="rowClassRules"
        row-selection="multiple"
        :suppress-row-click-selection="true"
        :stop-editing-when-cells-lose-focus="true"
        @grid-ready="onGridReady"
        @cell-value-changed="onCellValueChanged"
        @selection-changed="onSelectionChanged"
      />
    </div>

    <!-- Add column dialog -->
    <el-dialog v-model="showAddColumn" title="Add column" width="420px" append-to-body>
      <el-form label-position="top" @submit.prevent>
        <el-form-item label="Column name">
          <el-input v-model="newColumnName" placeholder="label" />
        </el-form-item>
        <el-form-item label="Type">
          <el-select v-model="newColumnDtype">
            <el-option
              v-for="dtype in NEW_COLUMN_DTYPES"
              :key="dtype"
              :label="dtype"
              :value="dtype"
            />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showAddColumn = false">Cancel</el-button>
        <el-button type="primary" :disabled="!newColumnNameValid" @click="confirmAddColumn">
          Add column
        </el-button>
      </template>
    </el-dialog>

    <TableLabelingMode
      v-if="showLabeling"
      :session="session"
      :table="table"
      :row-limit="rowLimit"
      @close="closeLabeling"
      @add-column="addColumnFromLabeling"
    />
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, reactive, ref, watch } from "vue";
import { onBeforeRouteLeave } from "vue-router";
import { ElMessage, ElMessageBox } from "element-plus";
import { AgGridVue } from "@ag-grid-community/vue3";
import type {
  CellValueChangedEvent,
  ColDef,
  GridApi,
  GridReadyEvent,
} from "@ag-grid-community/core";
import { ModuleRegistry } from "@ag-grid-community/core";
import { ClientSideRowModelModule } from "@ag-grid-community/client-side-row-model";
import "@ag-grid-community/styles/ag-grid.css";
import "@ag-grid-community/styles/ag-theme-balham.css";
import type { CatalogTable, NewColumnDtype, NewColumnSpec } from "../../types";
import { CatalogApi } from "../../api/catalog.api";
import { catalogSaveErrorMessage } from "../../composables/saveError";
import TableLabelingMode from "./TableLabelingMode.vue";
import {
  applyCellEdit,
  buildEditsPayload,
  hasPendingChanges,
  isEditableDtype,
  sessionCounts,
  suggestKeyColumns,
  validateKeyValues,
  type EditRow,
  type EditSession,
} from "./tableEditing";

ModuleRegistry.registerModules([ClientSideRowModelModule]);

const NEW_COLUMN_DTYPES: NewColumnDtype[] = [
  "String",
  "Int64",
  "Float64",
  "Boolean",
  "Date",
  "Datetime",
];

const props = defineProps<{
  modelValue: boolean;
  table: CatalogTable;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: boolean): void;
  (e: "saved", table: CatalogTable): void;
}>();

const step = ref<"loading" | "setup" | "grid" | "error">("loading");
const loadError = ref("");
const saving = ref(false);
const addingKeyColumn = ref(false);
const rowLimit = ref(1000);
const totalRows = ref(0);
const expectedVersion = ref<number | null>(null);
const selectedKeys = ref<string[]>([]);
const suggestedKeys = ref<string[]>([]);
const showAddColumn = ref(false);
const newColumnName = ref("");
const newColumnDtype = ref<NewColumnDtype>("String");
const showLabeling = ref(false);
const hasSelection = ref(false);

const session = reactive<EditSession>({
  columns: [],
  rows: [],
  keyColumns: [],
  expectedVersion: null,
  newColumns: [],
});

let gridApi: GridApi | null = null;
let nextUid = 1;
let loadedColumns: string[] = [];
let loadedRows: unknown[][] = [];

const hasChanges = computed(() => hasPendingChanges(session));
const canLabel = computed(() => session.rows.some((r) => !r.deleted));
const pendingSummary = computed(() => {
  const counts = sessionCounts(session);
  const parts: string[] = [];
  if (counts.edited) parts.push(`${counts.edited} edited`);
  if (counts.added) parts.push(`${counts.added} added`);
  if (counts.deleted) parts.push(`${counts.deleted} to delete`);
  if (session.newColumns.length) parts.push(`${session.newColumns.length} new column(s)`);
  return parts.join(" · ");
});

const keyCandidates = computed(() =>
  session.columns.filter((c) => !c.isNew && isEditableDtype(c.dtype)),
);

const newColumnNameValid = computed(() => {
  const name = newColumnName.value.trim();
  if (!name || /[\s,;{}()=]/.test(name)) return false;
  return !session.columns.some((c) => c.name === name);
});

const rowClassRules = {
  "editor-row-deleted": (params: { data?: Record<string, unknown> }) => {
    const row = findRow(params.data);
    return !!row?.deleted;
  },
  "editor-row-added": (params: { data?: Record<string, unknown> }) => {
    const row = findRow(params.data);
    return !!row && row.original === null;
  },
};

const defaultColDef: ColDef = { sortable: true, resizable: true };

const columnDefs = computed<ColDef[]>(() => {
  const defs: ColDef[] = session.columns.map((col, index) => ({
    field: col.name,
    headerName: col.name,
    headerTooltip: col.dtype,
    checkboxSelection: index === 0,
    headerCheckboxSelection: index === 0,
    cellClass: session.keyColumns.includes(col.name) ? "editor-key-cell" : undefined,
    editable: (params) => {
      const row = findRow(params.data as Record<string, unknown>);
      if (!row || row.deleted) return false;
      if (!col.editable) return false;
      // Key cells stay locked on existing rows: changing a key silently turns
      // an update into an insert. New rows must fill their keys in.
      if (session.keyColumns.includes(col.name)) return row.original === null;
      return true;
    },
  }));
  return defs;
});

const rowData = computed(() => session.rows.map((row) => row.cells));

function getRowId(params: { data: Record<string, unknown> }): string {
  return String(params.data.__uid);
}

function findRow(data: Record<string, unknown> | undefined): EditRow | undefined {
  if (!data) return undefined;
  return session.rows.find((r) => r.uid === data.__uid);
}

function onGridReady(event: GridReadyEvent) {
  gridApi = event.api;
}

function onCellValueChanged(event: CellValueChangedEvent) {
  const row = findRow(event.data as Record<string, unknown>);
  if (!row || !event.colDef.field) return;
  applyCellEdit(row, event.colDef.field, event.newValue);
}

function onSelectionChanged() {
  hasSelection.value = (gridApi?.getSelectedNodes().length ?? 0) > 0;
}

async function loadSession() {
  step.value = "loading";
  loadError.value = "";
  try {
    // History first: if a write lands between the two reads, the save 409s (safe).
    const history = await CatalogApi.getTableHistory(props.table.id);
    const preview = await CatalogApi.getTablePreview(props.table.id, rowLimit.value);
    expectedVersion.value = history.current_version;
    totalRows.value = preview.total_rows;
    loadedColumns = preview.columns;
    loadedRows = preview.rows;

    session.columns = preview.columns.map((name, i) => ({
      name,
      dtype: preview.dtypes[i] ?? "",
      isNew: false,
      editable: isEditableDtype(preview.dtypes[i] ?? ""),
    }));
    // Clear before rebuilding rows: makeRow seeds new-column cells, and a column
    // from the previous session may now exist in the table with real values.
    session.newColumns = [];
    session.rows = preview.rows.map((values) => makeRow(preview.columns, values));
    session.expectedVersion = history.current_version;
    suggestedKeys.value = suggestKeyColumns(loadedColumns, loadedRows);
    if (selectedKeys.value.length === 0) selectedKeys.value = [...suggestedKeys.value];
    step.value = "setup";
  } catch (error) {
    loadError.value = catalogSaveErrorMessage(error, "Failed to load table data.");
    step.value = "error";
  }
}

function makeRow(columns: string[], values: unknown[] | null): EditRow {
  const cells: Record<string, unknown> = { __uid: nextUid };
  const original: Record<string, unknown> | null = values ? {} : null;
  columns.forEach((name, i) => {
    cells[name] = values ? values[i] : null;
    if (original) original[name] = values ? values[i] : null;
  });
  for (const spec of session.newColumns) {
    if (!(spec.name in cells)) cells[spec.name] = null;
  }
  const row: EditRow = {
    uid: nextUid,
    cells,
    original,
    deleted: false,
    dirty: new Set<string>(),
  };
  nextUid += 1;
  return row;
}

function startEditing() {
  session.keyColumns = [...selectedKeys.value];
  step.value = "grid";
}

async function mintKeyColumn() {
  try {
    await ElMessageBox.confirm(
      "This rewrites the whole table with a new record_id column appended — a schema change " +
        "every flow reading this table will see. Continue?",
      "Add record_id column",
      { confirmButtonText: "Add column", cancelButtonText: "Cancel", type: "warning" },
    );
  } catch {
    return;
  }
  addingKeyColumn.value = true;
  try {
    const response = await CatalogApi.addTableKeyColumn(props.table.id, {
      column_name: "record_id",
      expected_version: expectedVersion.value,
    });
    emit("saved", response.table);
    selectedKeys.value = [response.column_name];
    await loadSession();
    ElMessage.success(
      `Added ${response.column_name} — table rewritten as version ${response.new_version}.`,
    );
  } catch (error) {
    ElMessage.error(catalogSaveErrorMessage(error, "Failed to add the record id column."));
  } finally {
    addingKeyColumn.value = false;
  }
}

function addRow() {
  const row = makeRow(loadedColumns, null);
  session.rows.push(row);
  // Reactive rowData + getRowId handles the grid delta; scroll by node id so the
  // right row is shown even when the grid is sorted.
  requestAnimationFrame(() => {
    const node = gridApi?.getRowNode(String(row.uid));
    if (node) gridApi?.ensureNodeVisible(node);
  });
}

function toggleDeleteSelected() {
  const selected = gridApi?.getSelectedNodes() ?? [];
  const dropUids = new Set<number>();
  for (const node of selected) {
    const row = findRow(node.data as Record<string, unknown>);
    if (!row) continue;
    if (!row.deleted && row.original === null) {
      // An added row marked for deletion simply leaves the session.
      dropUids.add(row.uid);
      continue;
    }
    row.deleted = !row.deleted;
  }
  if (dropUids.size) session.rows = session.rows.filter((r) => !dropUids.has(r.uid));
  gridApi?.deselectAll();
  gridApi?.redrawRows();
}

function confirmAddColumn() {
  const spec: NewColumnSpec = { name: newColumnName.value.trim(), dtype: newColumnDtype.value };
  addNewColumn(spec);
  showAddColumn.value = false;
  newColumnName.value = "";
  newColumnDtype.value = "String";
}

function addNewColumn(spec: NewColumnSpec) {
  session.newColumns.push(spec);
  session.columns.push({ name: spec.name, dtype: spec.dtype, isNew: true, editable: true });
  for (const row of session.rows) {
    if (!(spec.name in row.cells)) row.cells[spec.name] = null;
  }
}

function addColumnFromLabeling(spec: NewColumnSpec) {
  addNewColumn(spec);
}

function openLabeling() {
  showLabeling.value = true;
}

function closeLabeling() {
  showLabeling.value = false;
  gridApi?.refreshCells({ force: true });
  gridApi?.redrawRows();
}

async function save() {
  const keyError = validateKeyValues(session);
  if (keyError) {
    ElMessage.warning(keyError);
    return;
  }
  saving.value = true;
  try {
    const response = await CatalogApi.applyTableEdits(props.table.id, buildEditsPayload(session));
    ElMessage.success(
      `Saved: ${response.rows_upserted} row(s) written, ${response.rows_deleted} deleted ` +
        `(now version ${response.new_version}).`,
    );
    emit("saved", response.table);
    await reloadAfterSave();
  } catch (error: unknown) {
    const detail = (error as any)?.response?.data?.detail;
    if ((error as any)?.response?.status === 409 && detail?.error === "stale_write") {
      try {
        await ElMessageBox.confirm(
          `This table changed while you were editing (version ${detail.expected} → ` +
            `${detail.current}). Reload the grid? Your unsaved edits will be lost.`,
          "Table changed",
          { confirmButtonText: "Reload", cancelButtonText: "Keep editing", type: "warning" },
        );
        await reloadAfterSave();
      } catch {
        // Keep editing: the user can copy values out before reloading.
      }
    } else {
      ElMessage.error(catalogSaveErrorMessage(error, "Failed to save table edits."));
    }
  } finally {
    saving.value = false;
  }
}

async function reloadAfterSave() {
  const keys = [...session.keyColumns];
  await loadSession();
  const stillValid = keys.every((k) => session.columns.some((c) => c.name === k));
  if (stillValid && keys.length) {
    selectedKeys.value = keys;
    session.keyColumns = keys;
    step.value = "grid";
  }
}

async function requestClose() {
  if (hasChanges.value) {
    try {
      await ElMessageBox.confirm(
        "You have unsaved edits. Close and discard them?",
        "Discard changes",
        { confirmButtonText: "Discard", cancelButtonText: "Keep editing", type: "warning" },
      );
    } catch {
      return;
    }
  }
  emit("update:modelValue", false);
}

// Edits live only in the in-memory session until Save; the labelling workspace mutates the same one.
const dirty = computed(() => props.modelValue && hasChanges.value);

function warnOnUnload(event: BeforeUnloadEvent) {
  event.preventDefault();
  event.returnValue = "";
}

watch(dirty, (isDirty) => {
  if (isDirty) window.addEventListener("beforeunload", warnOnUnload);
  else window.removeEventListener("beforeunload", warnOnUnload);
});

onBeforeUnmount(() => window.removeEventListener("beforeunload", warnOnUnload));

onBeforeRouteLeave(async () => {
  if (!dirty.value) return true;
  try {
    await ElMessageBox.confirm(
      "You have unsaved table edits. Leave and discard them?",
      "Discard changes",
      { confirmButtonText: "Discard", cancelButtonText: "Keep editing", type: "warning" },
    );
    return true;
  } catch {
    return false;
  }
});

watch(
  () => props.modelValue,
  (open) => {
    if (open) {
      selectedKeys.value = [];
      showLabeling.value = false;
      loadSession();
    }
  },
  { immediate: true },
);
</script>

<style scoped>
.editor-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-3, 12px);
  width: 100%;
}

.editor-title {
  display: flex;
  align-items: center;
  gap: var(--spacing-2, 8px);
  font-size: var(--font-size-lg, 16px);
  font-weight: 600;
  color: var(--color-text-primary);
}

.version-chip {
  font-size: var(--font-size-xs, 11px);
  font-weight: 400;
  padding: 2px 8px;
  border-radius: 10px;
  background: var(--color-background-soft, #f0f2f5);
  color: var(--color-text-secondary);
}

.editor-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-2, 8px);
}

.pending-summary {
  font-size: var(--font-size-xs, 11px);
  color: var(--color-text-secondary);
  margin-right: var(--spacing-2, 8px);
}

.editor-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: var(--spacing-3, 12px);
  padding: var(--spacing-8, 48px);
  color: var(--color-text-secondary);
}

.editor-error i {
  font-size: 24px;
  color: var(--color-warning, #e6a23c);
}

.editor-setup {
  max-width: 640px;
  margin: 0 auto;
  padding: var(--spacing-6, 32px) var(--spacing-4, 16px);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3, 12px);
}

.setup-explainer,
.setup-hint {
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm, 13px);
  margin: 0;
}

.key-select {
  width: 100%;
}

.setup-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2, 8px);
  font-size: var(--font-size-sm, 13px);
  flex-wrap: wrap;
}

.limit-select {
  width: 110px;
}

.setup-warning {
  color: var(--color-warning, #e6a23c);
  font-size: var(--font-size-xs, 11px);
}

.setup-actions {
  display: flex;
  gap: var(--spacing-2, 8px);
  margin-top: var(--spacing-2, 8px);
}

.editor-grid-wrap {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2, 8px);
  height: calc(100vh - 120px);
}

.slice-banner {
  display: flex;
  align-items: center;
  gap: var(--spacing-2, 8px);
  padding: var(--spacing-2, 8px) var(--spacing-3, 12px);
  border-radius: 6px;
  background: var(--color-background-soft, #f0f7ff);
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm, 13px);
}

.editor-grid {
  flex: 1;
  width: 100%;
}

:deep(.editor-row-deleted) {
  text-decoration: line-through;
  opacity: 0.5;
}

:deep(.editor-row-added) {
  background-color: rgba(103, 194, 58, 0.08);
}

:deep(.editor-key-cell) {
  background-color: rgba(64, 158, 255, 0.06);
}
</style>
