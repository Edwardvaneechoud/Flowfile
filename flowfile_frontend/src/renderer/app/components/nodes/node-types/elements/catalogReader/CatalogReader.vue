<template>
  <div v-if="dataLoaded && nodeData" class="listbox-wrapper">
    <div class="main-part">
      <!-- Mode toggle -->
      <div class="mode-toggle">
        <button class="mode-btn" :class="{ active: mode === 'table' }" @click="switchMode('table')">
          <i class="fa-solid fa-table"></i> Table
        </button>
        <button class="mode-btn" :class="{ active: mode === 'sql' }" @click="switchMode('sql')">
          <i class="fa-solid fa-code"></i> SQL
        </button>
      </div>

      <!-- Table mode -->
      <template v-if="mode === 'table'">
        <div class="catalog-field">
          <label class="catalog-label">Catalog / Schema</label>
          <el-select
            v-model="nodeData.catalog_namespace_id"
            size="small"
            placeholder="Select namespace"
            clearable
            @change="handleNamespaceChange"
          >
            <el-option
              v-for="ns in catalogNamespaces"
              :key="ns.id"
              :label="ns.label"
              :value="ns.id"
            />
          </el-select>
        </div>

        <div class="catalog-field">
          <label class="catalog-label">Table</label>
          <el-select
            v-model="nodeData.catalog_table_id"
            size="small"
            placeholder="Select table"
            filterable
            @change="handleTableChange"
          >
            <el-option
              v-for="table in filteredTables"
              :key="table.id"
              :label="tableOptionLabel(table)"
              :value="table.id"
            >
              <i
                :class="
                  table.table_type === 'virtual'
                    ? 'fa-solid fa-bolt catalog-option-icon virtual-option-icon'
                    : 'fa-solid fa-table catalog-option-icon'
                "
              ></i>
              {{ tableOptionLabel(table) }}
            </el-option>
          </el-select>
        </div>

        <div class="catalog-field">
          <label class="catalog-label">Read</label>
          <el-tooltip
            :disabled="readDisabledReason === null"
            :content="readDisabledReason ?? ''"
            placement="top"
          >
            <el-select v-model="cdcMode" size="small" :disabled="readDisabledReason !== null">
              <el-option label="Full table" value="off" />
              <el-option label="Changes since last run" value="since_last_run" />
              <el-option label="Changes since version" value="since_version" />
              <el-option label="Changes since time" value="since_timestamp" />
            </el-select>
          </el-tooltip>
        </div>

        <div v-if="cdcMode !== 'off' && !trackingEnabled" class="cdc-warning">
          <i class="fa-solid fa-triangle-exclamation"></i>
          <div class="cdc-warning-body">
            <span>
              Change tracking is not enabled on this table. Only commits made after it is turned on
              are tracked.
            </span>
            <el-button
              v-if="canEnableTracking"
              size="small"
              type="primary"
              :loading="enablingCdc"
              @click="enableTracking"
            >
              Enable change tracking
            </el-button>
          </div>
        </div>

        <div v-if="cdcMode === 'since_version'" class="catalog-field">
          <label class="catalog-label">Since version</label>
          <el-select
            v-if="!cdcVersionParam"
            v-model="nodeData.cdc_from_version"
            size="small"
            filterable
            placeholder="Pick a version"
          >
            <el-option
              v-for="v in versionOptions"
              :key="v.version"
              :label="v.label"
              :value="v.version"
            />
          </el-select>
          <el-select
            v-if="versionParamOptions.length > 0"
            v-model="cdcVersionParam"
            size="small"
            clearable
            placeholder="Or use a flow parameter"
          >
            <el-option
              v-for="p in versionParamOptions"
              :key="p.name"
              :label="'${' + p.name + '}'"
              :value="p.name"
            />
          </el-select>
          <p class="section-hint">Reads every change committed after this version.</p>
          <p v-if="cdcVersionError" class="field-error">{{ cdcVersionError }}</p>
        </div>

        <div v-if="cdcMode === 'since_timestamp'" class="catalog-field">
          <label class="catalog-label">Since</label>
          <DateTimePicker
            v-if="!cdcTimestampParam"
            v-model="nodeData.cdc_from_timestamp"
            placeholder="Pick a date and time"
            show-seconds
          />
          <el-select
            v-if="timestampParamOptions.length > 0"
            v-model="cdcTimestampParam"
            size="small"
            clearable
            placeholder="Or use a flow parameter"
          >
            <el-option
              v-for="p in timestampParamOptions"
              :key="p.name"
              :label="'${' + p.name + '}'"
              :value="p.name"
            />
          </el-select>
          <p v-if="cdcTimestampError" class="field-error">{{ cdcTimestampError }}</p>
        </div>

        <template v-if="cdcMode === 'since_last_run'">
          <div class="cdc-status">
            <i class="fa-solid fa-bookmark"></i>
            <span class="cdc-status-text">{{ cursorStatus }}</span>
            <el-button
              v-if="cdcCursor"
              text
              size="small"
              :loading="resettingCursor"
              @click="handleResetCursor"
            >
              Reset cursor
            </el-button>
          </div>
          <div class="catalog-field">
            <label class="catalog-label">Start from</label>
            <el-select v-model="nodeData.cdc_start" size="small">
              <el-option label="Now — skip existing history" value="now" />
              <el-option label="Beginning — replay everything tracked" value="beginning" />
            </el-select>
          </div>
          <CollapsibleSection
            title="Cursor name (optional)"
            :default-open="false"
            nested
            persist-key="catalogReader.cdcConsumer"
          >
            <div class="catalog-field">
              <el-input
                v-model="cdcConsumerName"
                size="small"
                placeholder="Defaults to this node in this flow"
              />
              <p v-if="cdcConsumerNameError" class="field-error">{{ cdcConsumerNameError }}</p>
              <p class="section-hint">
                A named cursor is shared by every flow and notebook reading this table under the
                same name.
              </p>
            </div>
          </CollapsibleSection>
          <p class="section-hint cdc-hint">
            <i class="fa-solid fa-circle-info"></i>
            Delivery is at-least-once — a failed run replays its window, so downstream writers
            should upsert, not append.
          </p>
        </template>

        <div v-if="cdcMode !== 'off'" class="catalog-field">
          <el-checkbox v-model="nodeData.cdc_include_preimage" size="small">
            Include row values from before each update
          </el-checkbox>
        </div>

        <div v-if="versionOptions.length > 0 && !selectedScd2" class="catalog-field">
          <label class="catalog-label">Version</label>
          <el-select v-model="nodeData.delta_version" size="small" placeholder="Latest" clearable>
            <el-option
              v-for="v in versionOptions"
              :key="v.version"
              :label="v.label"
              :value="v.version"
            />
          </el-select>
        </div>

        <div v-if="selectedScd2" class="catalog-field">
          <label class="catalog-label">History</label>
          <el-select v-model="scd2View" size="small">
            <el-option label="Active records" value="active" />
            <el-option label="All records (default)" value="all" />
            <el-option label="Active at a point in time" value="active_at" />
          </el-select>
          <p class="section-hint">
            SCD2 table — versioned by {{ selectedScd2.business_keys.join(", ") }}, using
            {{ selectedScd2.valid_from_column }} / {{ selectedScd2.valid_to_column }}.
          </p>
        </div>
        <div v-if="selectedScd2 && scd2View === 'active_at'" class="catalog-field">
          <label class="catalog-label">Active at</label>
          <DateTimePicker
            v-model="nodeData.scd2_as_of"
            placeholder="Pick a date and time"
            show-seconds
          />
          <p v-if="scd2AsOfError" class="field-error">{{ scd2AsOfError }}</p>
        </div>
        <!-- On SCD2 tables, Delta time travel is an escape hatch, not the history mechanism -->
        <CollapsibleSection
          v-if="selectedScd2 && versionOptions.length > 0"
          title="Table version (time travel)"
          :default-open="false"
          nested
          persist-key="catalogReader.deltaVersion"
        >
          <div class="catalog-field">
            <el-select v-model="nodeData.delta_version" size="small" placeholder="Latest" clearable>
              <el-option
                v-for="v in versionOptions"
                :key="v.version"
                :label="v.label"
                :value="v.version"
              />
            </el-select>
            <p class="section-hint">
              Snapshots the whole table at a past Delta commit — unrelated to the SCD2 row history
              above.
            </p>
          </div>
        </CollapsibleSection>

        <div v-if="selectedTableMeta" class="table-meta">
          Latest stats:
          <div class="meta-row">
            <span class="meta-label">Rows</span>
            <span class="meta-value">{{ formatNumber(selectedTableMeta.row_count) }}</span>
          </div>
          <div class="meta-row">
            <span class="meta-label">Columns</span>
            <span class="meta-value">{{ selectedTableMeta.column_count }}</span>
          </div>
          <div v-if="schemaPreviewColumns.length > 0" class="schema-preview">
            <label class="catalog-label">Schema</label>
            <div class="schema-list">
              <div v-for="col in schemaPreviewColumns" :key="col.name" class="schema-col">
                <span class="col-name">
                  {{ col.name }}
                  <span v-if="col.generated" class="col-generated-tag">changes</span>
                </span>
                <span class="col-type">{{ col.dtype }}</span>
              </div>
            </div>
          </div>
        </div>
      </template>

      <!-- SQL mode -->
      <template v-else>
        <div class="sql-editor-wrapper">
          <p class="section-hint">
            Write a SQL query against catalog tables. Tables are available by name.
          </p>
          <div class="editor-container">
            <codemirror
              v-model="sqlCode"
              placeholder="SELECT * FROM my_table LIMIT 100"
              :style="{ height: '200px' }"
              :autofocus="true"
              :indent-with-tab="false"
              :tab-size="2"
              :extensions="extensions"
            />
          </div>
        </div>

        <!-- Available tables reference -->
        <div v-if="allTables.length > 0" class="available-tables">
          <label class="catalog-label">Available tables</label>
          <div class="table-chips">
            <span
              v-for="t in allTables"
              :key="t.id"
              class="table-chip"
              :title="t.full_table_name ?? t.name"
              @click="insertTableName(t.full_table_name ?? t.name)"
            >
              {{ t.full_table_name ?? t.name }}
            </span>
          </div>
        </div>
      </template>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed, onMounted, watch } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import { sql } from "@codemirror/lang-sql";
import { oneDark } from "@codemirror/theme-one-dark";
import { Codemirror } from "vue-codemirror";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { useResourceSharing } from "../../../../../composables/useResourceSharing";
import { CatalogApi } from "../../../../../api/catalog.api";
import { CollapsibleSection, DateTimePicker } from "../../../../common";
import { useFlowStore } from "@/stores/flow-store";
import type { FlowParameter } from "@/types/flow.types";
import {
  CDF_COLUMNS,
  cursorStatusLine,
  findCursor,
  readModeDisabledReason,
} from "../../../../../utils/catalogCdc";
import type {
  CatalogTable,
  NamespaceTree,
  DeltaVersionCommit,
  TableCdcStatus,
} from "../../../../../types";
import {
  DEFAULT_CDC_SETTINGS,
  type CdcMode,
  type CdcReaderSettings,
  type NodeCatalogReader,
} from "../../../../../types/node.types";

const nodeStore = useNodeStore();
const nodeData = ref<NodeCatalogReader | null>(null);
const dataLoaded = ref(false);
const mode = ref<"table" | "sql">("table");
const sqlCode = ref("");

// Cached state for mode switching
const cachedTableState = ref<{
  catalog_table_id: number | null;
  catalog_table_name: string | null;
  catalog_full_table_name: string | null;
  catalog_namespace_id: number | null;
  delta_version: number | null;
  scd2_view: "active" | "all" | "active_at" | null;
  scd2_as_of: string | null;
  cdc: CdcReaderSettings;
  selectedTableMeta: CatalogTable | null;
  deltaVersions: DeltaVersionCommit[];
} | null>(null);
const cachedSqlCode = ref<string | null>(null);

const { saveSettings, pushNodeData } = useNodeSettings({
  nodeRef: nodeData,
  onBeforeSave: () => {
    if (scd2AsOfError.value) {
      ElMessage.error(scd2AsOfError.value);
      return false;
    }
    const cdcError = cdcConsumerNameError.value ?? cdcVersionError.value ?? cdcTimestampError.value;
    if (cdcError) {
      ElMessage.error(cdcError);
      return false;
    }
    if (nodeData.value?.cdc_consumer_name) {
      nodeData.value.cdc_consumer_name = nodeData.value.cdc_consumer_name.trim() || null;
    }
  },
});

const catalogNamespaces = ref<{ id: number; label: string }[]>([]);
const allTables = ref<CatalogTable[]>([]);
const selectedTableMeta = ref<CatalogTable | null>(null);
const deltaVersions = ref<DeltaVersionCommit[]>([]);

const tableSchema = computed(() => {
  const schema: Record<string, string[]> = {};
  for (const t of allTables.value) {
    schema[t.full_table_name ?? t.name] = (t.schema_columns ?? []).map((c) => c.name);
  }
  return schema;
});

const extensions = computed(() => [
  sql({ schema: tableSchema.value, upperCaseKeywords: true }),
  oneDark,
]);

const filteredTables = computed(() => {
  if (nodeData.value?.catalog_namespace_id == null) return allTables.value;
  return allTables.value.filter((t) => t.namespace_id === nodeData.value?.catalog_namespace_id);
});

// Whether the candidate list has any name collisions — used to force qualified
// labels even when the user is browsing within a single namespace.
const hasNameCollisions = computed(() => {
  const counts = new Map<string, number>();
  for (const t of filteredTables.value) {
    counts.set(t.name, (counts.get(t.name) ?? 0) + 1);
  }
  for (const n of counts.values()) if (n > 1) return true;
  return false;
});

function tableOptionLabel(table: CatalogTable): string {
  if (nodeData.value?.catalog_namespace_id == null || hasNameCollisions.value) {
    return table.full_table_name ?? table.name;
  }
  return table.name;
}

const versionOptions = computed(() => {
  return deltaVersions.value.map((v) => ({
    version: v.version,
    label: `v${v.version}${v.operation ? ` (${v.operation})` : ""}${v.timestamp ? ` - ${v.timestamp}` : ""}`,
  }));
});

const selectedScd2 = computed(() => selectedTableMeta.value?.scd2 ?? null);
// null on the wire means "all records" (no filter) — surface it explicitly so the
// control is never blank. Choosing "All" writes null back (not the string "all"),
// keeping the wire value the same as an unconfigured reader.
const scd2View = computed({
  get: () => nodeData.value?.scd2_view ?? "all",
  set: (v: "active" | "all" | "active_at") => {
    if (!nodeData.value) return;
    nodeData.value.scd2_view = v === "all" ? null : v;
  },
});

// The picker is clearable and selecting "active at" seeds nothing, so the empty state is easy to
// reach and the backend only rejects it with a message that never names the field.
const scd2AsOfError = computed(() =>
  selectedScd2.value && scd2View.value === "active_at" && !nodeData.value?.scd2_as_of
    ? "Pick the date and time to read the table as of"
    : null,
);

const { canManage } = useResourceSharing();
const cdcStatus = ref<TableCdcStatus | null>(null);
const enablingCdc = ref(false);
const resettingCursor = ref(false);

const readDisabledReason = computed(() =>
  readModeDisabledReason(selectedTableMeta.value, nodeData.value?.delta_version ?? null),
);

const cdcMode = computed<CdcMode>({
  get: () => nodeData.value?.cdc_mode ?? "off",
  set: (mode: CdcMode) => {
    if (!nodeData.value) return;
    nodeData.value.cdc_mode = mode;
    if (mode !== "since_version") nodeData.value.cdc_from_version = null;
    if (mode !== "since_timestamp") nodeData.value.cdc_from_timestamp = null;
    if (mode === "off") {
      nodeData.value.cdc_consumer_name = null;
      nodeData.value.cdc_include_preimage = false;
      cdcStatus.value = null;
    } else if (mode === "since_last_run") {
      void loadCdcStatus();
    }
  },
});

const cdcConsumerName = computed({
  get: () => nodeData.value?.cdc_consumer_name ?? "",
  set: (value: string) => {
    if (nodeData.value) nodeData.value.cdc_consumer_name = value.trim() ? value : null;
  },
});

const cdcConsumerNameError = computed(() => {
  const name = nodeData.value?.cdc_consumer_name?.trim();
  if (!name) return null;
  return /^[A-Za-z0-9_.:-]+$/.test(name)
    ? null
    : "Cursor name: use letters, digits and _ . : - only";
});

const flowStore = useFlowStore();
const PARAM_REF = /^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$/;
const paramRefName = (value: unknown): string =>
  typeof value === "string" ? (PARAM_REF.exec(value)?.[1] ?? "") : "";
const versionParamOptions = computed<FlowParameter[]>(() =>
  flowStore.parameters.filter((p) => p.type === "integer"),
);
const timestampParamOptions = computed<FlowParameter[]>(() =>
  flowStore.parameters.filter((p) => !p.type || p.type === "string"),
);
const cdcVersionParam = computed({
  get: () => paramRefName(nodeData.value?.cdc_from_version),
  set: (name: string) => {
    if (nodeData.value) nodeData.value.cdc_from_version = name ? "${" + name + "}" : null;
  },
});
const cdcTimestampParam = computed({
  get: () => paramRefName(nodeData.value?.cdc_from_timestamp),
  set: (name: string) => {
    if (nodeData.value) nodeData.value.cdc_from_timestamp = name ? "${" + name + "}" : null;
  },
});
onMounted(() => {
  if (flowStore.flowId > 0) void flowStore.loadParameters(flowStore.flowId);
});

const cdcVersionError = computed(() =>
  cdcMode.value === "since_version" && nodeData.value?.cdc_from_version == null
    ? "Pick the version to read changes after"
    : null,
);

const cdcTimestampError = computed(() =>
  cdcMode.value === "since_timestamp" && !nodeData.value?.cdc_from_timestamp
    ? "Pick the date and time to read changes from"
    : null,
);

const cdcCursor = computed(() =>
  cdcStatus.value
    ? findCursor(
        cdcStatus.value.cursors,
        nodeData.value?.cdc_consumer_name ?? null,
        nodeData.value?.node_id ?? -1,
      )
    : null,
);

const cursorStatus = computed(() =>
  cursorStatusLine(cdcCursor.value, nodeData.value?.cdc_start ?? "now"),
);

// The tree's table row already carries the flag, so the /cdc round trip is only for cursors.
const trackingEnabled = computed(
  () => cdcStatus.value?.cdc_enabled ?? selectedTableMeta.value?.cdc_enabled ?? false,
);

const canEnableTracking = computed(
  () => !!selectedTableMeta.value && canManage(selectedTableMeta.value),
);

const schemaPreviewColumns = computed(() => {
  const columns = (selectedTableMeta.value?.schema_columns ?? []).map((c) => ({
    name: c.name,
    dtype: c.dtype,
    generated: false,
  }));
  if (cdcMode.value === "off") return columns;
  return [...columns, ...CDF_COLUMNS.map((c) => ({ ...c, generated: true }))];
});

function currentCdcSettings(): CdcReaderSettings {
  const d = nodeData.value;
  if (!d) return { ...DEFAULT_CDC_SETTINGS };
  return {
    cdc_mode: d.cdc_mode,
    cdc_from_version: d.cdc_from_version,
    cdc_from_timestamp: d.cdc_from_timestamp,
    cdc_consumer_name: d.cdc_consumer_name,
    cdc_start: d.cdc_start,
    cdc_include_preimage: d.cdc_include_preimage,
  };
}

function applyCdcSettings(settings: CdcReaderSettings) {
  if (!nodeData.value) return;
  Object.assign(nodeData.value, settings);
  cdcStatus.value = null;
}

function resetCdcSettings() {
  applyCdcSettings({ ...DEFAULT_CDC_SETTINGS });
}

async function loadCdcStatus() {
  const tableId = nodeData.value?.catalog_table_id;
  if (tableId == null) {
    cdcStatus.value = null;
    return;
  }
  try {
    cdcStatus.value = await CatalogApi.getTableCdc(tableId);
  } catch {
    cdcStatus.value = null;
  }
}

async function enableTracking() {
  const table = selectedTableMeta.value;
  if (!table) return;
  enablingCdc.value = true;
  try {
    const status = await CatalogApi.enableTableCdc(table.id);
    cdcStatus.value = status;
    table.cdc_enabled = status.cdc_enabled;
    table.cdc_enabled_version = status.cdc_enabled_version;
    ElMessage.success("Change tracking enabled");
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? e?.message ?? "Could not enable change tracking");
  } finally {
    enablingCdc.value = false;
  }
}

async function handleResetCursor() {
  const tableId = nodeData.value?.catalog_table_id;
  const cursor = cdcCursor.value;
  if (tableId == null || !cursor) return;
  const target = nodeData.value?.cdc_start ?? "now";
  const targetLabel = target === "beginning" ? "the beginning of tracked history" : "now";
  try {
    await ElMessageBox.confirm(
      `Reset this cursor to ${targetLabel}? The next run reads from there instead of after v${cursor.last_version}.`,
      "Reset cursor",
      { confirmButtonText: "Reset", cancelButtonText: "Cancel", type: "warning" },
    );
  } catch {
    return;
  }
  resettingCursor.value = true;
  try {
    await CatalogApi.resetCdcCursor(tableId, cursor.consumer_key, target);
    await loadCdcStatus();
    ElMessage.success("Cursor reset");
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? e?.message ?? "Could not reset the cursor");
  } finally {
    resettingCursor.value = false;
  }
}

// Pinning a version and reading changes are mutually exclusive (the backend rejects both).
watch(
  () => nodeData.value?.delta_version,
  (version) => {
    if (version != null && nodeData.value && nodeData.value.cdc_mode !== "off") {
      resetCdcSettings();
    }
  },
);

watch(sqlCode, (newCode) => {
  if (nodeData.value) {
    nodeData.value.sql_query = newCode || null;
  }
});

function switchMode(newMode: "table" | "sql") {
  if (mode.value === newMode) return;
  if (!nodeData.value) return;

  if (newMode === "sql") {
    cachedTableState.value = {
      catalog_table_id: nodeData.value.catalog_table_id,
      catalog_table_name: nodeData.value.catalog_table_name,
      catalog_full_table_name: nodeData.value.catalog_full_table_name,
      catalog_namespace_id: nodeData.value.catalog_namespace_id,
      delta_version: nodeData.value.delta_version,
      scd2_view: nodeData.value.scd2_view,
      scd2_as_of: nodeData.value.scd2_as_of,
      cdc: currentCdcSettings(),
      selectedTableMeta: selectedTableMeta.value,
      deltaVersions: [...deltaVersions.value],
    };
    nodeData.value.catalog_table_id = null;
    nodeData.value.catalog_table_name = null;
    nodeData.value.catalog_full_table_name = null;
    nodeData.value.delta_version = null;
    nodeData.value.scd2_view = null;
    nodeData.value.scd2_as_of = null;
    resetCdcSettings();
    selectedTableMeta.value = null;
    deltaVersions.value = [];

    if (cachedSqlCode.value != null) {
      sqlCode.value = cachedSqlCode.value;
      nodeData.value.sql_query = cachedSqlCode.value || null;
    }
  } else {
    cachedSqlCode.value = sqlCode.value;
    nodeData.value.sql_query = null;
    sqlCode.value = "";

    if (cachedTableState.value) {
      nodeData.value.catalog_table_id = cachedTableState.value.catalog_table_id;
      nodeData.value.catalog_table_name = cachedTableState.value.catalog_table_name;
      nodeData.value.catalog_full_table_name = cachedTableState.value.catalog_full_table_name;
      nodeData.value.catalog_namespace_id = cachedTableState.value.catalog_namespace_id;
      nodeData.value.delta_version = cachedTableState.value.delta_version;
      nodeData.value.scd2_view = cachedTableState.value.scd2_view;
      nodeData.value.scd2_as_of = cachedTableState.value.scd2_as_of;
      applyCdcSettings(cachedTableState.value.cdc);
      selectedTableMeta.value = cachedTableState.value.selectedTableMeta;
      deltaVersions.value = cachedTableState.value.deltaVersions;
      if (nodeData.value.cdc_mode === "since_last_run") void loadCdcStatus();
    }
  }

  mode.value = newMode;
  saveSettings();
}

function insertTableName(name: string) {
  const quoted = /^[a-zA-Z_]\w*$/.test(name) ? name : `"${name}"`;
  sqlCode.value += quoted;
}

function formatNumber(n: number | null): string {
  if (n === null) return "\u2014";
  return n.toLocaleString();
}

function handleNamespaceChange() {
  if (nodeData.value) {
    nodeData.value.catalog_table_id = null;
    nodeData.value.catalog_table_name = null;
    nodeData.value.catalog_full_table_name = null;
    nodeData.value.delta_version = null;
    nodeData.value.scd2_view = null;
    nodeData.value.scd2_as_of = null;
    resetCdcSettings();
  }
  selectedTableMeta.value = null;
  deltaVersions.value = [];
}

async function handleTableChange(tableId: number | null) {
  if (!nodeData.value) return;
  nodeData.value.delta_version = null;
  deltaVersions.value = [];
  // Cursors are per table, so a retarget always starts over.
  resetCdcSettings();

  const table = allTables.value.find((t) => t.id === tableId);
  if (table) {
    nodeData.value.catalog_table_name = table.name;
    nodeData.value.catalog_namespace_id = table.namespace_id;
    nodeData.value.catalog_full_table_name = table.full_table_name ?? null;
    selectedTableMeta.value = table;
    // The newly selected table may not be SCD2-tracked — drop a stale view/timestamp.
    if (!table.scd2) {
      nodeData.value.scd2_view = null;
      nodeData.value.scd2_as_of = null;
    }
    await loadTableHistory(table.id);
  } else {
    nodeData.value.catalog_table_name = null;
    nodeData.value.catalog_full_table_name = null;
    nodeData.value.scd2_view = null;
    nodeData.value.scd2_as_of = null;
    selectedTableMeta.value = null;
  }
}

async function loadTableHistory(tableId: number) {
  try {
    const history = await CatalogApi.getTableHistory(tableId);
    deltaVersions.value = history.history;
  } catch {
    deltaVersions.value = [];
  }
}

function collectTablesFromTree(nodes: NamespaceTree[]): CatalogTable[] {
  const result: CatalogTable[] = [];
  for (const node of nodes) {
    for (const t of node.tables ?? []) {
      result.push(t);
    }
    result.push(...collectTablesFromTree(node.children ?? []));
  }
  return result;
}

let catalogLoadPromise: Promise<void> | null = null;

async function loadCatalogData() {
  try {
    const tree = await CatalogApi.getNamespaceTree();
    for (const catalog of tree) {
      for (const schema of catalog.children ?? []) {
        catalogNamespaces.value.push({
          id: schema.id,
          label: `${catalog.name} / ${schema.name}`,
        });
      }
    }
    allTables.value = collectTablesFromTree(tree).filter(
      (t) => t.file_exists || t.table_type === "virtual",
    );
  } catch {
    // Catalog not available
  }
}

onMounted(() => {
  catalogLoadPromise = loadCatalogData();
});

async function loadNodeData(nodeId: number) {
  const nodeResult = await nodeStore.getNodeData(nodeId, false);
  if (nodeResult?.setting_input && nodeResult.setting_input.is_setup) {
    nodeData.value = nodeResult.setting_input;
    // Backward compatibility
    if (nodeData.value!.delta_version === undefined) {
      nodeData.value!.delta_version = null;
    }
    if (nodeData.value!.sql_query === undefined) {
      nodeData.value!.sql_query = null;
    }
    if (nodeData.value!.catalog_full_table_name === undefined) {
      nodeData.value!.catalog_full_table_name = null;
    }
    if (nodeData.value!.scd2_view === undefined) {
      nodeData.value!.scd2_view = null;
    }
    if (nodeData.value!.scd2_as_of === undefined) {
      nodeData.value!.scd2_as_of = null;
    }
    if (nodeData.value!.cdc_mode === undefined) {
      Object.assign(nodeData.value!, DEFAULT_CDC_SETTINGS);
    }
  } else {
    nodeData.value = {
      catalog_table_id: null,
      catalog_full_table_name: null,
      catalog_table_name: null,
      catalog_namespace_id: null,
      delta_version: null,
      scd2_view: null,
      scd2_as_of: null,
      sql_query: null,
      ...DEFAULT_CDC_SETTINGS,
      flow_id: nodeStore.flow_id,
      node_id: nodeId,
      cache_results: false,
      pos_x: 0,
      pos_y: 0,
      is_setup: false,
      description: "",
    };
  }

  if (nodeData.value?.sql_query) {
    mode.value = "sql";
    sqlCode.value = nodeData.value.sql_query;
  } else {
    mode.value = "table";
  }

  dataLoaded.value = true;

  if (catalogLoadPromise) await catalogLoadPromise;

  if (mode.value === "table" && nodeData.value?.catalog_table_id) {
    const table = allTables.value.find((t) => t.id === nodeData.value?.catalog_table_id);
    if (table) {
      selectedTableMeta.value = table;
      await loadTableHistory(table.id);
      if (nodeData.value.cdc_mode === "since_last_run") await loadCdcStatus();
    }
  }
}

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});
</script>

<style scoped>
.catalog-option-icon {
  margin-right: 6px;
  font-size: 12px;
  color: var(--color-success);
}

.virtual-option-icon {
  color: var(--el-color-primary, var(--color-primary));
}

.main-part {
  display: flex;
  flex-direction: column;
  padding: 20px;
  border: 1px solid var(--color-border-primary);
  border-radius: 8px;
  background-color: var(--color-background-primary);
  margin-top: 20px;
  gap: 12px;
}

.mode-toggle {
  display: flex;
  gap: 4px;
  padding: 2px;
  background: var(--color-background-secondary, #f5f7fa);
  border-radius: 6px;
  border: 1px solid var(--color-border-primary);
}

.mode-btn {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  padding: 6px 12px;
  border: none;
  border-radius: 4px;
  background: transparent;
  font-size: 12px;
  font-weight: 500;
  color: var(--color-text-secondary);
  cursor: pointer;
  transition: all 0.15s ease;
}

.mode-btn:hover {
  color: var(--color-text-primary);
  background: var(--color-background-hover, rgba(0, 0, 0, 0.04));
}

.mode-btn.active {
  color: var(--color-primary, #409eff);
  background: var(--color-background-primary, #fff);
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.08);
}

.catalog-field {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.catalog-label {
  font-size: 12px;
  font-weight: 500;
  color: var(--color-text-secondary);
}

.table-meta {
  border-top: 1px solid var(--color-border-primary);
  padding-top: 12px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.meta-row {
  display: flex;
  justify-content: space-between;
  font-size: 13px;
}

.meta-label {
  color: var(--color-text-secondary);
}

.meta-value {
  color: var(--color-text-primary);
  font-weight: 500;
}

.schema-preview {
  margin-top: 4px;
}

.schema-list {
  max-height: 200px;
  overflow-y: auto;
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  margin-top: 4px;
}

.schema-col {
  display: flex;
  justify-content: space-between;
  padding: 4px 8px;
  font-size: 12px;
  border-bottom: 1px solid var(--color-border-light, var(--color-border-primary));
}

.schema-col:last-child {
  border-bottom: none;
}

.col-name {
  color: var(--color-text-primary);
  font-family: monospace;
}

.col-type {
  color: var(--color-text-muted);
  font-family: monospace;
  font-size: 11px;
}

.col-generated-tag {
  margin-left: 6px;
  padding: 0 4px;
  font-family: inherit;
  font-size: 10px;
  color: var(--el-color-primary, var(--color-primary));
  background: var(--el-color-primary-light-9, rgba(64, 158, 255, 0.1));
  border-radius: 3px;
}

.cdc-status {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 6px 8px;
  font-size: 12px;
  color: var(--color-text-secondary);
  background: var(--color-background-secondary, #f5f7fa);
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
}

.cdc-status-text {
  flex: 1;
}

.cdc-warning {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  padding: 8px 10px;
  background: rgba(245, 158, 11, 0.08);
  border: 1px solid rgba(245, 158, 11, 0.3);
  border-radius: 4px;
  font-size: 12px;
  color: var(--color-text-secondary);
  line-height: 1.4;
}

.cdc-warning i {
  color: var(--color-warning, #f59e0b);
  margin-top: 2px;
}

.cdc-warning-body {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 6px;
}

.cdc-hint {
  display: flex;
  align-items: flex-start;
  gap: 6px;
  line-height: 1.4;
}

.sql-editor-wrapper {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.section-hint {
  margin: 0;
  font-size: 12px;
  color: var(--color-text-muted);
}

.field-error {
  margin: 0;
  font-size: 11px;
  color: var(--el-color-danger);
}

.editor-container {
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  overflow: hidden;
  font-size: 13px;
}

.available-tables {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.table-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
}

.table-chip {
  padding: 2px 8px;
  font-size: 11px;
  font-family: monospace;
  background: var(--color-background-secondary, #f5f7fa);
  border: 1px solid var(--color-border-primary);
  border-radius: 3px;
  cursor: pointer;
  color: var(--color-text-primary);
  transition: background 0.15s ease;
}

.table-chip:hover {
  background: var(--color-background-hover, #e8eaed);
}
</style>
