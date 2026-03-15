<template>
  <div v-if="dataLoaded && nodeData" class="listbox-wrapper">
    <div class="main-part">
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

      <div v-if="selectedTableMeta" class="table-meta">
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
            <div v-for="col in selectedTableMeta.schema_columns" :key="col.name" class="schema-col">
              <span class="col-name">{{ col.name }}</span>
              <span class="col-type">{{ col.dtype }}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed, onMounted } from "vue";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { CatalogApi } from "../../../../../api/catalog.api";
import type { CatalogTable, NamespaceTree } from "../../../../../types";
import type { NodeCatalogReader } from "../../../../../types/node.types";

const nodeStore = useNodeStore();
const nodeData = ref<NodeCatalogReader | null>(null);
const dataLoaded = ref(false);

const { saveSettings, pushNodeData } = useNodeSettings({
  nodeRef: nodeData,
});

const catalogNamespaces = ref<{ id: number; label: string }[]>([]);
const allTables = ref<CatalogTable[]>([]);
const selectedTableMeta = ref<CatalogTable | null>(null);

const filteredTables = computed(() => {
  if (nodeData.value?.catalog_namespace_id == null) return allTables.value;
  return allTables.value.filter((t) => t.namespace_id === nodeData.value?.catalog_namespace_id);
});

function formatNumber(n: number | null): string {
  if (n === null) return "—";
  return n.toLocaleString();
}

function handleNamespaceChange() {
  // Reset table selection when namespace changes
  if (nodeData.value) {
    nodeData.value.catalog_table_id = null;
    nodeData.value.catalog_table_name = null;
  }
  selectedTableMeta.value = null;
}

function handleTableChange(tableId: number | null) {
  if (!nodeData.value) return;
  const table = allTables.value.find((t) => t.id === tableId);
  if (table) {
    nodeData.value.catalog_table_name = table.name;
    nodeData.value.catalog_namespace_id = table.namespace_id;
    selectedTableMeta.value = table;
  } else {
    nodeData.value.catalog_table_name = null;
    selectedTableMeta.value = null;
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
    allTables.value = collectTablesFromTree(tree);
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
  } else {
    nodeData.value = {
      catalog_table_id: null,
      catalog_table_name: null,
      catalog_namespace_id: null,
      flow_id: nodeStore.flow_id,
      node_id: nodeId,
      cache_results: false,
      pos_x: 0,
      pos_y: 0,
      is_setup: false,
      description: "",
    };
  }
  dataLoaded.value = true;

  // Ensure catalog data is loaded before looking up the table
  if (catalogLoadPromise) await catalogLoadPromise;

  if (nodeData.value?.catalog_table_id) {
    const table = allTables.value.find((t) => t.id === nodeData.value?.catalog_table_id);
    if (table) {
      selectedTableMeta.value = table;
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
</style>
