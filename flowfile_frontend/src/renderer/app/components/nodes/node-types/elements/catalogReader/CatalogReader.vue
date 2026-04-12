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
              :label="table.name"
              :value="table.id"
            />
          </el-select>
        </div>

        <div v-if="versionOptions.length > 0" class="catalog-field">
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
          <div v-if="selectedTableMeta.schema_columns.length > 0" class="schema-preview">
            <label class="catalog-label">Schema</label>
            <div class="schema-list">
              <div
                v-for="col in selectedTableMeta.schema_columns"
                :key="col.name"
                class="schema-col"
              >
                <span class="col-name">{{ col.name }}</span>
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
              @click="insertTableName(t.name)"
            >
              {{ t.name }}
            </span>
          </div>
        </div>
      </template>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed, onMounted, watch } from "vue";
import { sql } from "@codemirror/lang-sql";
import { oneDark } from "@codemirror/theme-one-dark";
import { Codemirror } from "vue-codemirror";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { CatalogApi } from "../../../../../api/catalog.api";
import type { CatalogTable, NamespaceTree, DeltaVersionCommit } from "../../../../../types";
import type { NodeCatalogReader } from "../../../../../types/node.types";

const nodeStore = useNodeStore();
const nodeData = ref<NodeCatalogReader | null>(null);
const dataLoaded = ref(false);
const mode = ref<"table" | "sql">("table");
const sqlCode = ref("");

// Cached state for mode switching
const cachedTableState = ref<{
  catalog_table_id: number | null;
  catalog_table_name: string | null;
  catalog_namespace_id: number | null;
  delta_version: number | null;
  selectedTableMeta: CatalogTable | null;
  deltaVersions: DeltaVersionCommit[];
} | null>(null);
const cachedSqlCode = ref<string | null>(null);

const { saveSettings, pushNodeData } = useNodeSettings({
  nodeRef: nodeData,
});

const catalogNamespaces = ref<{ id: number; label: string }[]>([]);
const allTables = ref<CatalogTable[]>([]);
const selectedTableMeta = ref<CatalogTable | null>(null);
const deltaVersions = ref<DeltaVersionCommit[]>([]);

const tableSchema = computed(() => {
  const schema: Record<string, string[]> = {};
  for (const t of allTables.value) {
    schema[t.name] = (t.schema_columns ?? []).map((c) => c.name);
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

const versionOptions = computed(() => {
  return deltaVersions.value.map((v) => ({
    version: v.version,
    label: `v${v.version}${v.operation ? ` (${v.operation})` : ""}${v.timestamp ? ` - ${v.timestamp}` : ""}`,
  }));
});

// Sync sqlCode with nodeData.sql_query
watch(sqlCode, (newCode) => {
  if (nodeData.value) {
    nodeData.value.sql_query = newCode || null;
  }
});

function switchMode(newMode: "table" | "sql") {
  if (mode.value === newMode) return;
  if (!nodeData.value) return;

  if (newMode === "sql") {
    // Cache table state before clearing
    cachedTableState.value = {
      catalog_table_id: nodeData.value.catalog_table_id,
      catalog_table_name: nodeData.value.catalog_table_name,
      catalog_namespace_id: nodeData.value.catalog_namespace_id,
      delta_version: nodeData.value.delta_version,
      selectedTableMeta: selectedTableMeta.value,
      deltaVersions: [...deltaVersions.value],
    };
    nodeData.value.catalog_table_id = null;
    nodeData.value.catalog_table_name = null;
    nodeData.value.delta_version = null;
    selectedTableMeta.value = null;
    deltaVersions.value = [];

    // Restore cached SQL if available
    if (cachedSqlCode.value != null) {
      sqlCode.value = cachedSqlCode.value;
      nodeData.value.sql_query = cachedSqlCode.value || null;
    }
  } else {
    // Cache SQL state before clearing
    cachedSqlCode.value = sqlCode.value;
    nodeData.value.sql_query = null;
    sqlCode.value = "";

    // Restore cached table state if available
    if (cachedTableState.value) {
      nodeData.value.catalog_table_id = cachedTableState.value.catalog_table_id;
      nodeData.value.catalog_table_name = cachedTableState.value.catalog_table_name;
      nodeData.value.catalog_namespace_id = cachedTableState.value.catalog_namespace_id;
      nodeData.value.delta_version = cachedTableState.value.delta_version;
      selectedTableMeta.value = cachedTableState.value.selectedTableMeta;
      deltaVersions.value = cachedTableState.value.deltaVersions;
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
    nodeData.value.delta_version = null;
  }
  selectedTableMeta.value = null;
  deltaVersions.value = [];
}

async function handleTableChange(tableId: number | null) {
  if (!nodeData.value) return;
  nodeData.value.delta_version = null;
  deltaVersions.value = [];

  const table = allTables.value.find((t) => t.id === tableId);
  if (table) {
    nodeData.value.catalog_table_name = table.name;
    nodeData.value.catalog_namespace_id = table.namespace_id;
    selectedTableMeta.value = table;
    await loadTableHistory(table.id);
  } else {
    nodeData.value.catalog_table_name = null;
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
    allTables.value = collectTablesFromTree(tree).filter((t) => t.file_exists);
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
  } else {
    nodeData.value = {
      catalog_table_id: null,
      catalog_table_name: null,
      catalog_namespace_id: null,
      delta_version: null,
      sql_query: null,
      flow_id: nodeStore.flow_id,
      node_id: nodeId,
      cache_results: false,
      pos_x: 0,
      pos_y: 0,
      is_setup: false,
      description: "",
    };
  }

  // Detect mode from saved settings
  if (nodeData.value?.sql_query) {
    mode.value = "sql";
    sqlCode.value = nodeData.value.sql_query;
  } else {
    mode.value = "table";
  }

  dataLoaded.value = true;

  // Ensure catalog data is loaded before looking up the table
  if (catalogLoadPromise) await catalogLoadPromise;

  if (mode.value === "table" && nodeData.value?.catalog_table_id) {
    const table = allTables.value.find((t) => t.id === nodeData.value?.catalog_table_id);
    if (table) {
      selectedTableMeta.value = table;
      await loadTableHistory(table.id);
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
