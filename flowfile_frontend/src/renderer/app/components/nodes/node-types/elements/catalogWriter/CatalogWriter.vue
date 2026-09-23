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
        <p v-if="tableNameError" class="field-error">{{ tableNameError }}</p>
      </div>

      <div class="catalog-field">
        <label class="catalog-label">Catalog / Schema</label>
        <el-select
          v-model="nodeData.catalog_write_settings.namespace_id"
          size="small"
          placeholder="Select namespace"
          clearable
          @change="onNamespaceChange"
        >
          <el-option
            v-for="ns in catalogNamespaces"
            :key="ns.id"
            :label="ns.label"
            :value="ns.id"
          />
        </el-select>
      </div>

      <!-- Target table status: exists vs new -->
      <DeltaTableStatusLine
        v-if="nodeData.catalog_write_settings.table_name && tableLookupDone"
        :exists="!!existingTable"
        :row-count="existingTable?.row_count ?? 0"
        :verb="existingModeVerb"
        :partition-columns="existingPartitionColumns"
      >
        <template #before>
          <div v-if="scd2TargetWarning" class="status-line status-warn">
            <i class="fa-solid fa-triangle-exclamation"></i>
            <span>{{ scd2TargetWarning }}</span>
          </div>
        </template>
        <div
          v-if="existingTable?.scd2 && physicalWriteMode === 'scd2'"
          class="status-line status-exists"
        >
          <i class="fa-solid fa-clock-rotate-left"></i>
          <span>SCD2 table - versioned by {{ existingTable.scd2.business_keys.join(", ") }}.</span>
        </div>
      </DeltaTableStatusLine>

      <!-- Tabs: Physical Write vs Virtual Table -->
      <el-tabs v-model="activeTab" class="writer-tabs" @tab-change="handleTabChange">
        <el-tab-pane label="Write to Catalog" name="physical">
          <div class="tab-content">
            <div class="catalog-field">
              <label class="catalog-label">Write mode</label>
              <el-select v-model="physicalWriteMode" size="small" @change="handleWriteModeChange">
                <el-option label="Overwrite" value="overwrite" />
                <el-option label="Error if exists" value="error" />
                <el-option label="Append" value="append" />
                <el-option label="Upsert" value="upsert" />
                <el-option label="Update" value="update" />
                <el-option label="Delete" value="delete" />
                <el-option label="SCD2 (slowly changing dimension)" value="scd2" />
              </el-select>
            </div>

            <div v-if="needsMergeKeys" class="catalog-field">
              <label class="catalog-label">{{ keyColumnsLabel }}</label>
              <MergeKeysSelect
                v-model="nodeData.catalog_write_settings.merge_keys"
                :columns="keyColumnOptions"
                :error="scd2KeyClashError"
              />
            </div>

            <div v-if="canPartition" class="catalog-field">
              <label class="catalog-label">Partition by (optional)</label>
              <el-select
                v-model="nodeData.catalog_write_settings.partition_by"
                size="small"
                multiple
                filterable
                placeholder="Select partition columns"
              >
                <el-option
                  v-for="col in partitionColumnOptions"
                  :key="col"
                  :label="col"
                  :value="col"
                />
              </el-select>
              <p v-if="existingPartitionColumns.length" class="partition-hint">
                This table is partitioned by {{ existingPartitionColumns.join(", ") }} — leave empty
                to inherit, or select the same columns.
              </p>
              <p v-else class="partition-hint">
                Set at table creation; appends must match the existing partitioning. Use
                low-cardinality columns.
              </p>
            </div>

            <template v-if="physicalWriteMode === 'scd2' && nodeData.catalog_write_settings.scd2">
              <div class="catalog-field catalog-field-inline">
                <el-switch
                  v-model="nodeData.catalog_write_settings.scd2.partition_on_current"
                  size="small"
                />
                <label class="catalog-label">
                  Partition on {{ nodeData.catalog_write_settings.scd2.is_current_column }}
                </label>
                <p class="partition-hint">
                  Speeds up "Active records" reads. Applied at table creation.
                </p>
              </div>
              <div class="catalog-field">
                <label class="catalog-label">Compare columns</label>
                <el-select
                  v-model="nodeData.catalog_write_settings.scd2.compare_columns"
                  size="small"
                  multiple
                  filterable
                  clearable
                  placeholder="All non-key columns"
                >
                  <el-option
                    v-for="col in comparableColumns"
                    :key="col"
                    :label="col"
                    :value="col"
                  />
                </el-select>
                <p class="partition-hint">
                  Leave empty to detect a change on every non-key column.
                </p>
              </div>
              <div class="catalog-field catalog-field-inline">
                <el-switch
                  v-model="nodeData.catalog_write_settings.scd2.full_snapshot"
                  size="small"
                />
                <label class="catalog-label">Input is a full snapshot</label>
                <p class="partition-hint">
                  End-date current rows whose business key is absent from this run's input.
                </p>
              </div>
              <div class="catalog-field">
                <label class="catalog-label">Output</label>
                <el-select
                  v-model="nodeData.catalog_write_settings.scd2.output_mode"
                  size="small"
                  placeholder="All records that are inputted"
                >
                  <el-option label="All records that are inputted" value="input" />
                  <el-option label="All changed records" value="changed" />
                  <el-option label="All active records" value="current" />
                </el-select>
                <p class="partition-hint">{{ scd2OutputModeHint }}</p>
              </div>
              <CollapsibleSection
                title="Generated columns"
                :default-open="false"
                nested
                persist-key="catalogWriter.scd2.columns"
              >
                <div v-for="f in scd2ColumnFields" :key="f.key" class="catalog-field">
                  <label class="catalog-label">{{ f.label }}</label>
                  <el-input
                    v-model="nodeData.catalog_write_settings.scd2[f.key]"
                    size="small"
                    :placeholder="f.def"
                  />
                  <p v-if="scd2ColumnErrors[f.key]" class="field-error">
                    {{ scd2ColumnErrors[f.key] }}
                  </p>
                </div>
              </CollapsibleSection>
            </template>

            <div class="catalog-field">
              <el-tooltip
                :disabled="trackChangesDisabledReason === null"
                :content="trackChangesDisabledReason ?? ''"
                placement="top"
              >
                <span>
                  <el-checkbox
                    v-model="nodeData.catalog_write_settings.track_changes"
                    size="small"
                    :disabled="trackChangesDisabledReason !== null"
                  >
                    Track changes
                  </el-checkbox>
                </span>
              </el-tooltip>
              <p class="partition-hint">
                Records every insert, update and delete so a catalog reader can read only what
                changed. Turning it on never turns it off again.
              </p>
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
import { ref, computed, nextTick, onMounted, onUnmounted, watch } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import { useNodeStore } from "../../../../../stores/node-store";
import { useFlowStore } from "../../../../../stores/flow-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { useWritableNamespaces } from "../../../../../composables/useWritableNamespaces";
import { validateCatalogName } from "../../../../../composables/catalogNameValidation";
import { suppressedEdgeRemovals } from "../../../../../composables/useDragAndDrop";
import { CatalogApi } from "../../../../../api/catalog.api";
import { FlowApi } from "../../../../../api";
import { SYSTEM_NAMESPACE_NAMES } from "../../../../../types";
import axios from "../../../../../services/axios.config";
import { buildOutputHandles } from "../../../../../utils/nodeHandles";
import * as writeModes from "../../../../../utils/deltaWriteModes";
import { CollapsibleSection, DeltaTableStatusLine, MergeKeysSelect } from "../../../../common";
import type { CatalogTable } from "../../../../../types/catalog.types";
import type { NodeConnection } from "../../../../../types/canvas.types";
import {
  DEFAULT_SCD2_SETTINGS,
  type CatalogWriteMode,
  type NodeCatalogWriter,
  type NodeData,
  type Scd2OutputMode,
  type Scd2Settings,
} from "../../../../../types/node.types";

const nodeStore = useNodeStore();
const flowStore = useFlowStore();
const nodeData = ref<NodeCatalogWriter | null>(null);
const fullNodeData = ref<NodeData | null>(null);
const dataLoaded = ref(false);

const tableNameError = computed(() =>
  validateCatalogName(nodeData.value?.catalog_write_settings.table_name ?? "", "Table"),
);

const { saveSettings, pushNodeData } = useNodeSettings({
  nodeRef: nodeData,
  onBeforeSave: () => {
    if (tableNameError.value) {
      ElMessage.error(tableNameError.value);
      return false;
    }
    if (physicalWriteMode.value === "scd2") {
      const firstError = Object.values(scd2ColumnErrors.value).find((e) => e != null);
      if (firstError) {
        ElMessage.error(`Generated columns: ${firstError}`);
        return false;
      }
      if (scd2KeyClashError.value) {
        ElMessage.error(`${keyColumnsLabel.value}: ${scd2KeyClashError.value}`);
        return false;
      }
    }
  },
});

const catalogNamespaces = ref<{ id: number; label: string; full: string }[]>([]);
// System-stripped but unpruned — for restoring a configured target the pruned list hides.
const allCatalogNamespaces = ref<{ id: number; label: string; full: string }[]>([]);
const { pruneNonWritable } = useWritableNamespaces();

// Keep the portable name in sync with the numeric selection. ``namespace_full_name`` ("catalog.schema")
// is what survives recreation on another machine; the id is install-local. Mirrors the catalog reader.
function onNamespaceChange(id: number | null) {
  const settings = nodeData.value?.catalog_write_settings;
  if (!settings) return;
  const match = catalogNamespaces.value.find((ns) => ns.id === id);
  settings.namespace_full_name = match ? match.full : null;
}

// On load, an imported flow may carry only ``namespace_full_name`` (id nulled by projection). Restore
// the numeric selection from the name so the dropdown shows it; runs once both options + data exist.
function reconcileNamespaceSelection() {
  const settings = nodeData.value?.catalog_write_settings;
  if (!settings || settings.namespace_id != null || !settings.namespace_full_name) return;
  const match = allCatalogNamespaces.value.find((ns) => ns.full === settings.namespace_full_name);
  if (match) settings.namespace_id = match.id;
}

// A pre-configured target outside the writable list (manage-granted table in a
// read-only namespace, or access revoked after configuration) must keep rendering
// with its label and stay re-selectable; the backend gates any actual retarget.
function ensureConfiguredOption() {
  const settings = nodeData.value?.catalog_write_settings;
  if (!settings) return;
  const current = allCatalogNamespaces.value.find(
    (ns) =>
      ns.id === settings.namespace_id ||
      (settings.namespace_full_name != null && ns.full === settings.namespace_full_name),
  );
  if (current && !catalogNamespaces.value.some((ns) => ns.id === current.id)) {
    catalogNamespaces.value.push(current);
  }
}

// Tab state — determines whether we're in physical write or virtual mode
const activeTab = ref<"physical" | "virtual">("physical");
// Track the last-used physical write mode so we can restore it when switching back
const physicalWriteMode = ref<CatalogWriteMode>("overwrite");

// Same derivation the canvas uses on flow open (NodeCatalogWriter.output_names), so switching
// modes live and reloading the flow agree on the handle. SCD2 is the only mode with an output.
const updateNodeOutputHandles = (mode: CatalogWriteMode) => {
  const vfInstance = flowStore.vueFlowInstance;
  const nodeId = nodeData.value?.node_id;
  if (!vfInstance || nodeId == null) return;
  const vfNode = vfInstance.findNode(String(nodeId));
  if (!vfNode) return;
  vfNode.data.outputs = buildOutputHandles(mode === "scd2" ? 1 : 0);
};

// Fired only on user changes (not on load), so drawer opens never prompt.
const handleWriteModeChange = async (mode: CatalogWriteMode) => {
  updateNodeOutputHandles(mode);
  if (mode !== "scd2") await removeStaleOutputEdges();
};

const removeStaleOutputEdges = async () => {
  const vfInstance = flowStore.vueFlowInstance;
  const nodeId = nodeData.value?.node_id;
  if (!vfInstance || nodeId == null) return;
  const staleEdges = vfInstance.getEdges.value.filter(
    (edge: { source: string; sourceHandle?: string | null }) =>
      edge.source === String(nodeId) && edge.sourceHandle === "output-0",
  );
  if (staleEdges.length === 0) return;
  const plural = staleEdges.length === 1 ? "connection" : "connections";
  try {
    await ElMessageBox.confirm(
      `This node still has ${staleEdges.length} outgoing ${plural} on the canvas. Only an SCD2 ` +
        "write passes data downstream; every other write mode is an endpoint, so those " +
        "connections carry nothing. Remove them?",
      "Write mode has no output",
      {
        confirmButtonText: staleEdges.length === 1 ? "Remove edge" : "Remove edges",
        cancelButtonText: "Keep",
        type: "warning",
      },
    );
  } catch {
    return;
  }
  await deleteOutputEdges(staleEdges);
};

// Same backend-first prune as Gate.vue's removeElseEdges.
const deleteOutputEdges = async (
  edges: {
    id: string;
    source: string;
    target: string;
    sourceHandle?: string | null;
    targetHandle?: string | null;
  }[],
) => {
  const vfInstance = flowStore.vueFlowInstance;
  if (!vfInstance || !nodeData.value) return;
  const removedIds: string[] = [];
  let failedCount = 0;
  for (const edge of edges) {
    const connection: NodeConnection = {
      input_connection: {
        node_id: Number(edge.target),
        connection_class: (edge.targetHandle ??
          "input-0") as NodeConnection["input_connection"]["connection_class"],
      },
      output_connection: {
        node_id: Number(edge.source),
        connection_class: (edge.sourceHandle ??
          "output-0") as NodeConnection["output_connection"]["connection_class"],
      },
    };
    try {
      await FlowApi.deleteConnection(Number(nodeData.value.flow_id), connection);
    } catch (error) {
      // 422 = already gone server-side; the edge is stale either way.
      const status = (error as { response?: { status?: number } })?.response?.status;
      if (status !== 422) {
        console.error("Failed to delete stale catalog writer connection:", error);
        failedCount += 1;
        continue;
      }
    }
    suppressedEdgeRemovals.add(edge.id);
    removedIds.push(edge.id);
  }
  // One removal per call: Canvas.handleEdgeChange ignores batched change events.
  for (const id of removedIds) {
    vfInstance.removeEdges([id]);
  }
  if (failedCount > 0) {
    ElMessage.error(
      `Could not remove ${failedCount} connection${failedCount === 1 ? "" : "s"}; ` +
        "they remain on the canvas.",
    );
  }
};

const lazinessCheck = ref<{ is_optimizable: boolean; blockers: string[] } | null>(null);
const lazinessLoading = ref(false);

const needsMergeKeys = computed(() => writeModes.needsMergeKeys(physicalWriteMode.value));

const keyColumnsLabel = computed(() =>
  physicalWriteMode.value === "scd2" ? "Business key columns" : "Key columns",
);

const trackChangesDisabledReason = computed(() =>
  writeModes.trackChangesDisabledReason(physicalWriteMode.value, {
    virtual: activeTab.value === "virtual",
  }),
);

const canPartition = computed(() => writeModes.canPartition(physicalWriteMode.value));

// The generated is-current column is offered by its own toggle, never here.
const partitionColumnOptions = computed(() => availableColumns.value);

const scd2ColumnFields: { key: keyof Scd2Settings; label: string; def: string }[] = [
  { key: "surrogate_key_column", label: "Surrogate key column", def: "sk" },
  { key: "valid_from_column", label: "Valid from column", def: "valid_from" },
  { key: "valid_to_column", label: "Valid to column", def: "valid_to" },
  { key: "is_current_column", label: "Is current column", def: "is_current" },
];

// The live names, not the defaults — renaming a generated column re-opens the one it vacated.
const scd2GeneratedNames = computed(() => {
  const scd2 = nodeData.value?.catalog_write_settings.scd2;
  if (physicalWriteMode.value !== "scd2" || !scd2) return new Set<string>();
  return new Set(scd2ColumnFields.map((f) => scd2[f.key] as string));
});

const keyColumnOptions = computed(() =>
  availableColumns.value.filter((col) => !scd2GeneratedNames.value.has(col)),
);

const comparableColumns = computed(() => {
  const s = nodeData.value?.catalog_write_settings;
  if (!s) return [];
  const excluded = new Set(s.merge_keys);
  return availableColumns.value.filter(
    (col) => !excluded.has(col) && !scd2GeneratedNames.value.has(col),
  );
});

// Filtering the picker can't catch a key that was legal when picked and became a generated column
// afterwards. The backend rejects that; say so at the field instead.
const scd2KeyClashError = computed(() => {
  const keys = nodeData.value?.catalog_write_settings.merge_keys ?? [];
  const clash = keys.filter((k) => scd2GeneratedNames.value.has(k));
  return clash.length ? `Cannot also be a generated column: ${clash.join(", ")}` : null;
});

const scd2ColumnErrors = computed<Record<string, string | null>>(() => {
  const scd2Settings = nodeData.value?.catalog_write_settings.scd2;
  const errors: Record<string, string | null> = {};
  if (!scd2Settings) return errors;
  const values = scd2ColumnFields.map((f) => scd2Settings[f.key] as string);
  for (const f of scd2ColumnFields) {
    const value = scd2Settings[f.key] as string;
    if (!value.trim()) {
      errors[f.key] = "Required";
      continue;
    }
    const nameError = validateCatalogName(value, "Column");
    if (nameError) {
      errors[f.key] = nameError;
      continue;
    }
    errors[f.key] = values.filter((v) => v === value).length > 1 ? "Must be unique" : null;
  }
  return errors;
});

const SCD2_OUTPUT_HINTS: Record<Scd2OutputMode, string> = {
  input: "Passes on the rows you fed in, each carrying its current surrogate key and validity.",
  changed: "Passes on only the row versions this run inserted or end-dated.",
  current: "Passes on every active row in the table, including keys this run did not touch.",
};

const scd2OutputModeHint = computed(
  () => SCD2_OUTPUT_HINTS[nodeData.value?.catalog_write_settings.scd2?.output_mode ?? "input"],
);

// Target-table existence lookup (resolved by name + namespace)
const existingTable = ref<CatalogTable | null>(null);
const tableLookupDone = ref(false);
let lookupTimer: ReturnType<typeof setTimeout> | null = null;

const existingPartitionColumns = computed(() => existingTable.value?.partition_columns ?? []);

const MODE_VERBS: Record<string, string> = {
  overwrite: "replaces all its data",
  error: "will fail (the table already exists)",
  append: "adds rows to it",
  upsert: "updates matched rows and inserts the rest",
  update: "updates matched rows",
  delete: "deletes matched rows",
  scd2: "closes changed rows and inserts new versions",
};

const existingModeVerb = computed(() => {
  if (activeTab.value === "virtual") return "re-registers it as a virtual table";
  return MODE_VERBS[physicalWriteMode.value] ?? "modifies it";
});

const scd2TargetWarning = computed(() => {
  const t = existingTable.value;
  if (!t || activeTab.value === "virtual") return null;
  const mode = physicalWriteMode.value;
  if (mode === "scd2" && !t.scd2) {
    return "This table exists and is not SCD2-tracked. The write will fail - pick a new table name, or delete the existing table first.";
  }
  if (mode !== "scd2" && t.scd2) {
    if (mode === "overwrite") {
      return "This is an SCD2 table. Overwrite rebuilds it as a normal table and clears SCD2 tracking.";
    }
    return `This is an SCD2 table - a ${mode} write will fail. Use scd2 mode, or overwrite to rebuild it as a normal table.`;
  }
  return null;
});

async function lookupExistingTable() {
  const name = nodeData.value?.catalog_write_settings.table_name?.trim();
  if (!name) {
    existingTable.value = null;
    tableLookupDone.value = false;
    return;
  }
  const namespaceId = nodeData.value?.catalog_write_settings.namespace_id ?? null;
  try {
    existingTable.value = await CatalogApi.resolveTableByName(name, namespaceId);
    tableLookupDone.value = true;
  } catch {
    // Lookup failed (network/5xx): show no status rather than a wrong "new table".
    existingTable.value = null;
    tableLookupDone.value = false;
  }
}

function scheduleLookup() {
  if (lookupTimer) clearTimeout(lookupTimer);
  tableLookupDone.value = false;
  lookupTimer = setTimeout(lookupExistingTable, 350);
}

onUnmounted(() => {
  if (lookupTimer) clearTimeout(lookupTimer);
});

watch(
  () => [
    nodeData.value?.catalog_write_settings.table_name,
    nodeData.value?.catalog_write_settings.namespace_id,
  ],
  () => scheduleLookup(),
);

const availableColumns = computed(() => {
  return fullNodeData.value?.main_input?.columns ?? [];
});

const physicalModeDescription = computed(() => writeModes.modeDescription(physicalWriteMode.value));

watch(physicalWriteMode, (newMode) => {
  if (nodeData.value && activeTab.value === "physical") {
    const s = nodeData.value.catalog_write_settings;
    s.write_mode = newMode;
    if (!writeModes.needsMergeKeys(newMode)) {
      s.merge_keys = [];
    }
    if (!writeModes.canPartition(newMode)) {
      s.partition_by = [];
    }
    // Dangling scd2 block on other physical modes is kept (not nulled) so toggling back restores it.
    if (newMode === "scd2" && !s.scd2) {
      s.scd2 = { ...DEFAULT_SCD2_SETTINGS };
    }
    if (newMode === "overwrite" || newMode === "scd2") {
      s.track_changes = false;
    }
  }
});

async function handleTabChange(tab: string) {
  if (!nodeData.value) return;
  if (tab === "virtual") {
    nodeData.value.catalog_write_settings.write_mode = "virtual";
    nodeData.value.catalog_write_settings.merge_keys = [];
    nodeData.value.catalog_write_settings.partition_by = [];
    // The backend rejects a dangling scd2 block on virtual (Scd2Settings is physical-mode-only).
    nodeData.value.catalog_write_settings.scd2 = null;
    nodeData.value.catalog_write_settings.track_changes = false;
    fetchLazinessCheck();
  } else {
    nodeData.value.catalog_write_settings.write_mode = physicalWriteMode.value;
  }
  await handleWriteModeChange(nodeData.value.catalog_write_settings.write_mode);
}

async function fetchLazinessCheck() {
  if (!nodeData.value) return;
  lazinessLoading.value = true;
  lazinessCheck.value = null;
  try {
    const response = await axios.get<{ is_optimizable: boolean; blockers: string[] }>(
      "/editor/laziness_check",
      { params: { flow_id: nodeData.value.flow_id } },
    );
    lazinessCheck.value = response.data;
  } catch {
    // Non-critical
  } finally {
    lazinessLoading.value = false;
  }
}

function collectSchemaOptions(tree: { name: string; children?: { id: number; name: string }[] }[]) {
  const options: { id: number; label: string; full: string }[] = [];
  for (const catalog of tree) {
    for (const schema of catalog.children ?? []) {
      // Hide system-managed schemas so only "default" + user-created
      // namespaces are offered as a write target.
      if (catalog.name === "General" && SYSTEM_NAMESPACE_NAMES.has(schema.name)) continue;
      options.push({
        id: schema.id,
        label: `${catalog.name} / ${schema.name}`,
        full: `${catalog.name}.${schema.name}`,
      });
    }
  }
  return options;
}

onMounted(async () => {
  try {
    const fetched = await CatalogApi.getNamespaceTree();
    allCatalogNamespaces.value = collectSchemaOptions(fetched);
    // Read-only ("use" grant) schemas are pruned — the backend refuses writes there.
    catalogNamespaces.value = collectSchemaOptions(pruneNonWritable(fetched));
  } catch {
    // Catalog not available
  }
  reconcileNamespaceSelection();
  ensureConfiguredOption();
});

async function loadNodeData(nodeId: number) {
  const nodeResult = await nodeStore.getNodeData(nodeId, false);
  fullNodeData.value = nodeResult;
  if (nodeResult?.setting_input && nodeResult.setting_input.is_setup) {
    nodeData.value = nodeResult.setting_input;
    // Ensure merge_keys / partition_by exist for backward compatibility
    if (!nodeData.value!.catalog_write_settings.merge_keys) {
      nodeData.value!.catalog_write_settings.merge_keys = [];
    }
    if (!nodeData.value!.catalog_write_settings.partition_by) {
      nodeData.value!.catalog_write_settings.partition_by = [];
    }
    if (nodeData.value!.catalog_write_settings.scd2 === undefined) {
      nodeData.value!.catalog_write_settings.scd2 = null;
    }
    if (nodeData.value!.catalog_write_settings.track_changes === undefined) {
      nodeData.value!.catalog_write_settings.track_changes = false;
    }
    if (
      nodeData.value!.catalog_write_settings.scd2 &&
      nodeData.value!.catalog_write_settings.scd2.partition_on_current === undefined
    ) {
      nodeData.value!.catalog_write_settings.scd2.partition_on_current = true;
    }
    if (
      nodeData.value!.catalog_write_settings.scd2 &&
      nodeData.value!.catalog_write_settings.scd2.output_mode === undefined
    ) {
      nodeData.value!.catalog_write_settings.scd2.output_mode = "input";
    }
  } else {
    nodeData.value = {
      catalog_write_settings: {
        table_name: "",
        namespace_id: null,
        namespace_full_name: null,
        description: null,
        write_mode: "overwrite",
        merge_keys: [],
        partition_by: [],
        scd2: null,
        track_changes: false,
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

  reconcileNamespaceSelection();
  ensureConfiguredOption();
  dataLoaded.value = true;
  await nextTick();
  updateNodeOutputHandles(mode);
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

.catalog-field-inline {
  flex-direction: row;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
}

.catalog-field-inline .partition-hint {
  flex-basis: 100%;
}

.catalog-label {
  font-size: 12px;
  font-weight: 500;
  color: var(--color-text-secondary);
}

.field-error {
  margin: 0;
  font-size: 11px;
  color: var(--el-color-danger);
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

.partition-hint {
  margin: 0;
  font-size: 11px;
  color: var(--color-text-tertiary);
}

.table-status {
  background-color: var(--color-background-secondary);
}

.status-warn {
  color: var(--color-text-secondary);
  padding: 6px 8px;
  background: rgba(245, 158, 11, 0.08);
  border: 1px solid rgba(245, 158, 11, 0.3);
  border-radius: 4px;
}

.status-warn i {
  color: var(--color-warning, #f59e0b);
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
