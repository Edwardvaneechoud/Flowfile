<template>
  <div v-if="dataLoaded && nodeData" class="listbox-wrapper">
    <div class="main-part">
      <!-- Shared fields: table name, namespace, description -->
      <div class="catalog-field">
        <label class="catalog-label">Table name</label>
        <el-input
          v-model="nodeData.catalog_write_settings.table_name"
          size="small"
          placeholder="Enter table name"
        />
      </div>

      <div class="catalog-field">
        <label class="catalog-label">Catalog / Schema</label>
        <el-select
          v-model="nodeData.catalog_write_settings.namespace_id"
          size="small"
          placeholder="Select namespace"
          clearable
        >
          <el-option
            v-for="ns in catalogNamespaces"
            :key="ns.id"
            :label="ns.label"
            :value="ns.id"
          />
        </el-select>
      </div>

      <!-- Tabs: Physical Write vs Virtual Table -->
      <el-tabs v-model="activeTab" class="writer-tabs" @tab-change="handleTabChange">
        <el-tab-pane label="Write to Catalog" name="physical">
          <div class="tab-content">
            <div class="catalog-field">
              <label class="catalog-label">Write mode</label>
              <el-select v-model="physicalWriteMode" size="small">
                <el-option label="Overwrite" value="overwrite" />
                <el-option label="Error if exists" value="error" />
                <el-option label="Append" value="append" />
                <el-option label="Upsert" value="upsert" />
                <el-option label="Update" value="update" />
                <el-option label="Delete" value="delete" />
              </el-select>
            </div>

            <div v-if="needsMergeKeys" class="catalog-field">
              <label class="catalog-label">Key columns</label>
              <el-select
                v-model="nodeData.catalog_write_settings.merge_keys"
                size="small"
                multiple
                filterable
                placeholder="Select key columns"
              >
                <el-option v-for="col in availableColumns" :key="col" :label="col" :value="col" />
              </el-select>
            </div>

            <div v-if="physicalModeDescription" class="mode-description">
              {{ physicalModeDescription }}
            </div>
          </div>
        </el-tab-pane>

        <el-tab-pane label="Virtual Table" name="virtual">
          <div class="tab-content">
            <div class="virtual-info">
              <i class="fa-solid fa-bolt"></i>
              <span
                >No data is written to disk. When this table is queried, the flow will be
                re-executed on demand to produce results.</span
              >
            </div>

            <!-- Laziness check results -->
            <div v-if="lazinessLoading" class="laziness-loading">
              <i class="fa-solid fa-spinner fa-spin"></i>
              <span>Checking flow optimization...</span>
            </div>
            <div v-else-if="lazinessCheck" class="laziness-result">
              <div v-if="lazinessCheck.is_optimizable" class="laziness-ok">
                <i class="fa-solid fa-circle-check"></i>
                <span
                  >This flow is fully lazy — the virtual table will be
                  <strong>optimized</strong> with predicate and projection pushdown.</span
                >
              </div>
              <div v-else class="laziness-warn">
                <div class="laziness-warn-header">
                  <i class="fa-solid fa-triangle-exclamation"></i>
                  <span
                    >This flow has nodes that prevent full lazy execution. The virtual table will
                    use <strong>standard</strong> (non-optimized) resolution.</span
                  >
                </div>
                <ul class="blocker-list">
                  <li v-for="(reason, i) in lazinessCheck.blockers" :key="i">{{ reason }}</li>
                </ul>
              </div>
            </div>
          </div>
        </el-tab-pane>
      </el-tabs>

      <div class="catalog-field">
        <label class="catalog-label">Description (optional)</label>
        <el-input
          v-model="nodeData.catalog_write_settings.description"
          size="small"
          type="textarea"
          :rows="2"
          placeholder="Table description"
        />
      </div>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed, onMounted, watch } from "vue";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { CatalogApi } from "../../../../../api/catalog.api";
import axios from "../../../../../services/axios.config";
import type {
  CatalogWriteMode,
  NodeCatalogWriter,
  NodeData,
} from "../../../../../types/node.types";

const nodeStore = useNodeStore();
const nodeData = ref<NodeCatalogWriter | null>(null);
const fullNodeData = ref<NodeData | null>(null);
const dataLoaded = ref(false);

const { saveSettings, pushNodeData } = useNodeSettings({
  nodeRef: nodeData,
});

const catalogNamespaces = ref<{ id: number; label: string }[]>([]);

// Tab state — determines whether we're in physical write or virtual mode
const activeTab = ref<"physical" | "virtual">("physical");
// Track the last-used physical write mode so we can restore it when switching back
const physicalWriteMode = ref<CatalogWriteMode>("overwrite");

// Laziness check state
const lazinessCheck = ref<{ is_optimizable: boolean; blockers: string[] } | null>(null);
const lazinessLoading = ref(false);

const needsMergeKeys = computed(() => {
  const mode = physicalWriteMode.value;
  return mode === "upsert" || mode === "update" || mode === "delete";
});

const availableColumns = computed(() => {
  return fullNodeData.value?.main_input?.columns ?? [];
});

const physicalModeDescription = computed(() => {
  const descriptions: Record<string, string> = {
    overwrite: "Replace all existing data in the table.",
    error: "Fail if the table already exists.",
    append: "Add rows to the existing table without modifying existing data.",
    upsert: "Update rows that match the key columns, insert rows that don't match.",
    update: "Update only rows that match the key columns. No new rows are inserted.",
    delete: "Remove rows from the target table that match the key columns in the source data.",
  };
  return descriptions[physicalWriteMode.value] ?? null;
});

// Sync physicalWriteMode → nodeData.write_mode when in physical tab
watch(physicalWriteMode, (newMode) => {
  if (nodeData.value && activeTab.value === "physical") {
    nodeData.value.catalog_write_settings.write_mode = newMode;
    if (!["upsert", "update", "delete"].includes(newMode)) {
      nodeData.value.catalog_write_settings.merge_keys = [];
    }
  }
});

function handleTabChange(tab: string) {
  if (!nodeData.value) return;
  if (tab === "virtual") {
    nodeData.value.catalog_write_settings.write_mode = "virtual";
    nodeData.value.catalog_write_settings.merge_keys = [];
    fetchLazinessCheck();
  } else {
    nodeData.value.catalog_write_settings.write_mode = physicalWriteMode.value;
  }
}

async function fetchLazinessCheck() {
  if (!nodeData.value) return;
  lazinessLoading.value = true;
  lazinessCheck.value = null;
  try {
    const response = await axios.get<{ is_optimizable: boolean; blockers: string[] }>(
      "/editor/laziness_check",
      {
        params: {
          flow_id: nodeData.value.flow_id,
          node_id: nodeData.value.node_id,
        },
      },
    );
    lazinessCheck.value = response.data;
  } catch {
    // Non-critical
  } finally {
    lazinessLoading.value = false;
  }
}

onMounted(async () => {
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
  } catch {
    // Catalog not available
  }
});

async function loadNodeData(nodeId: number) {
  const nodeResult = await nodeStore.getNodeData(nodeId, false);
  fullNodeData.value = nodeResult;
  if (nodeResult?.setting_input && nodeResult.setting_input.is_setup) {
    nodeData.value = nodeResult.setting_input;
    // Ensure merge_keys exists for backward compatibility
    if (!nodeData.value!.catalog_write_settings.merge_keys) {
      nodeData.value!.catalog_write_settings.merge_keys = [];
    }
  } else {
    nodeData.value = {
      catalog_write_settings: {
        table_name: "",
        namespace_id: null,
        description: null,
        write_mode: "overwrite",
        merge_keys: [],
      },
      flow_id: nodeStore.flow_id,
      node_id: nodeId,
      cache_results: false,
      pos_x: 0,
      pos_y: 0,
      is_setup: false,
      description: "",
    };
  }

  // Initialize tab and physical mode from persisted write_mode
  const mode = nodeData.value!.catalog_write_settings.write_mode;
  if (mode === "virtual") {
    activeTab.value = "virtual";
    physicalWriteMode.value = "overwrite";
    fetchLazinessCheck();
  } else {
    activeTab.value = "physical";
    physicalWriteMode.value = mode;
  }

  dataLoaded.value = true;
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

.writer-tabs {
  margin-top: 4px;
}

.tab-content {
  display: flex;
  flex-direction: column;
  gap: 12px;
  padding-top: 4px;
}

.mode-description {
  font-size: 11px;
  color: var(--color-text-tertiary);
  padding: 6px 8px;
  background-color: var(--color-background-secondary);
  border-radius: 4px;
}

.virtual-info {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  padding: 8px 10px;
  background: rgba(59, 130, 246, 0.08);
  border: 1px solid rgba(59, 130, 246, 0.25);
  border-radius: 4px;
  font-size: 11px;
  color: var(--color-primary);
  line-height: 1.4;
}

.laziness-loading {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 11px;
  color: var(--color-text-muted);
  padding: 6px 0;
}

.laziness-result {
  font-size: 11px;
  line-height: 1.5;
}

.laziness-ok {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  padding: 8px 10px;
  background: rgba(34, 197, 94, 0.08);
  border: 1px solid rgba(34, 197, 94, 0.25);
  border-radius: 4px;
  color: var(--color-success, #22c55e);
}

.laziness-warn {
  padding: 8px 10px;
  background: rgba(245, 158, 11, 0.08);
  border: 1px solid rgba(245, 158, 11, 0.3);
  border-radius: 4px;
  color: var(--color-text-secondary);
}

.laziness-warn-header {
  display: flex;
  align-items: flex-start;
  gap: 6px;
  color: var(--color-warning, #f59e0b);
  margin-bottom: 6px;
}

.blocker-list {
  margin: 0;
  padding-left: 20px;
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.blocker-list li {
  font-family: var(--font-family-mono, monospace);
  font-size: 11px;
}
</style>
