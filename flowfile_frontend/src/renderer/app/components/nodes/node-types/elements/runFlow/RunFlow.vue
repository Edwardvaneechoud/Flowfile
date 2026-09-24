<template>
  <div v-if="dataLoaded && nodeRunFlow" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeRunFlow"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="main-part">
        <!-- Flow picker: namespace scopes the flow options -->
        <div class="run-flow-field">
          <label class="run-flow-label">Flow</label>
          <div class="flow-picker-row">
            <el-select
              v-model="selectedNamespaceId"
              size="small"
              placeholder="All namespaces"
              clearable
              class="namespace-select"
            >
              <el-option v-for="ns in namespaces" :key="ns.id" :label="ns.label" :value="ns.id" />
            </el-select>
            <el-select
              v-model="selectedRegistrationId"
              size="small"
              placeholder="Select a flow"
              filterable
              class="flow-picker-select"
              @change="handleFlowChange"
            >
              <el-option
                v-for="flow in filteredFlows"
                :key="flow.id"
                :label="flow.name"
                :value="flow.id"
              />
            </el-select>
            <el-button
              size="small"
              :loading="interfaceLoading"
              :disabled="!selectedRegistrationId"
              title="Re-read the flow's inputs, outputs and parameters"
              @click="refreshInterface"
            >
              Refresh
            </el-button>
          </div>
        </div>

        <!-- Interface summary -->
        <div class="interface-summary">
          <div class="chip-row">
            <span class="chip-row-label">Inputs:</span>
            <template v-if="nodeRunFlow.input_slots.length > 0">
              <el-tag v-for="name in nodeRunFlow.input_slots" :key="name" size="small">
                {{ name }}
              </el-tag>
            </template>
            <span v-else class="chip-empty">no data inputs</span>
          </div>
          <div class="chip-row">
            <span class="chip-row-label">Outputs:</span>
            <template v-if="nodeRunFlow.output_slots.length > 0">
              <el-tag
                v-for="name in nodeRunFlow.output_slots"
                :key="name"
                size="small"
                type="success"
              >
                {{ name }}
              </el-tag>
            </template>
            <span v-else class="chip-empty">no outputs</span>
          </div>
        </div>

        <!-- Parameters -->
        <div class="run-flow-field">
          <label class="run-flow-label">Parameters</label>
          <div v-if="bindingRows.length > 0" class="param-table">
            <div class="param-table-header">
              <span class="param-col param-col--name">Name</span>
              <span class="param-col param-col--source">Source</span>
              <span class="param-col param-col--value">Value</span>
            </div>
            <div v-for="row in bindingRows" :key="row.spec.name" class="param-table-row">
              <span class="param-col param-col--name">
                {{ row.spec.name }}
                <el-tag size="small" type="info" class="param-type-tag">
                  {{ paramType(row.spec) }}
                </el-tag>
              </span>
              <span class="param-col param-col--source">
                <el-select
                  v-model="row.binding.source"
                  size="small"
                  @change="handleSourceChange(row)"
                >
                  <el-option label="Default" value="default" />
                  <el-option label="Constant" value="constant" />
                  <el-option label="Column" value="column" />
                </el-select>
              </span>
              <span class="param-col param-col--value">
                <span v-if="row.binding.source === 'default'" class="param-default-value">
                  {{ row.spec.default_value || "—" }}
                </span>
                <template v-else-if="row.binding.source === 'constant'">
                  <el-checkbox
                    v-if="paramType(row.spec) === 'boolean'"
                    :model-value="row.binding.constant_value === 'true'"
                    @update:model-value="
                      (v: string | number | boolean) => setConstant(row, v ? 'true' : 'false')
                    "
                  />
                  <el-input-number
                    v-else-if="paramType(row.spec) === 'integer'"
                    :model-value="constantAsNumber(row)"
                    :step="1"
                    :precision="0"
                    size="small"
                    controls-position="right"
                    @update:model-value="
                      (v: number | undefined) => setConstant(row, v == null ? '' : String(v))
                    "
                  />
                  <el-input-number
                    v-else-if="paramType(row.spec) === 'float'"
                    :model-value="constantAsNumber(row)"
                    size="small"
                    controls-position="right"
                    @update:model-value="
                      (v: number | undefined) => setConstant(row, v == null ? '' : String(v))
                    "
                  />
                  <el-select
                    v-else-if="paramType(row.spec) === 'enum'"
                    :model-value="row.binding.constant_value ?? ''"
                    size="small"
                    placeholder="Pick a value"
                    @update:model-value="(v: string) => setConstant(row, v)"
                  >
                    <el-option
                      v-for="opt in row.spec.enum_values ?? []"
                      :key="opt"
                      :label="opt"
                      :value="opt"
                    />
                  </el-select>
                  <el-input
                    v-else
                    :model-value="row.binding.constant_value ?? ''"
                    size="small"
                    @update:model-value="(v: string) => setConstant(row, v)"
                  />
                </template>
                <el-select
                  v-else
                  :model-value="row.binding.column_name ?? undefined"
                  size="small"
                  :disabled="availableColumns.length === 0"
                  :placeholder="columnPlaceholder(row)"
                  @update:model-value="(v: string) => (row.binding.column_name = v)"
                >
                  <el-option
                    v-for="col in columnsForRow(row)"
                    :key="col"
                    :label="col"
                    :value="col"
                  />
                </el-select>
              </span>
            </div>
          </div>
          <div v-else class="param-empty">This flow has no parameters.</div>
        </div>

        <!-- Row handling mode (only relevant when a parameter is fed from a column) -->
        <div v-if="showParameterRows" class="run-flow-field">
          <label class="run-flow-label">Parameter rows</label>
          <el-radio-group v-model="nodeRunFlow.iteration_mode" size="small">
            <el-radio label="first_value">Use first row only</el-radio>
            <el-radio label="iterate">Run once per row</el-radio>
          </el-radio-group>
          <div v-if="nodeRunFlow.iteration_mode === 'iterate'" class="iterate-option">
            <el-checkbox
              v-model="nodeRunFlow.append_run_metadata"
              label="Append run info columns"
              size="small"
            />
          </div>
        </div>
      </div>
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed, onMounted, watch } from "vue";
import { ElMessage } from "element-plus";
import { useNodeStore } from "../../../../../stores/node-store";
import { useFlowStore } from "../../../../../stores/flow-store";
import { useCatalogStore } from "../../../../../stores/catalog-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { CatalogApi } from "../../../../../api/catalog.api";
import {
  buildDynamicInputHandles,
  buildDynamicOutputHandles,
} from "../../../../../utils/nodeHandles";
import { deleteConnectionOperations } from "../../../../../utils/graphOperations";
import { plural } from "../../../../../utils/text";
import type { NodeData, NodeRunFlow, FileColumn } from "../../../../../types/node.types";
import type { FlowParameter, FlowParamType, NodeHandle } from "../../../../../types/flow.types";
import type { SubflowInterface } from "../../../../../types/catalog.types";
import {
  MAX_RUN_FLOW_INPUTS,
  MAX_RUN_FLOW_OUTPUTS,
  mergeBindingRows,
  reconcileBindings,
  findDanglingEdges,
  danglingEdgesToDelete,
  hasColumnBinding,
  matchingColumnNames,
  subflowInterfaceChanged,
  type BindingRow,
} from "./runFlowLogic";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const flowStore = useFlowStore();
const catalogStore = useCatalogStore();

const dataLoaded = ref(false);
const nodeData = ref<null | NodeData>(null);
const nodeRunFlow = ref<NodeRunFlow | null>(null);
const selectedNamespaceId = ref<number | null>(null);
const selectedRegistrationId = ref<number | null>(null);
const interfaceLoading = ref(false);
const namespaces = ref<{ id: number; label: string }[]>([]);
// Selected namespace id -> ids of itself + its child schemas (catalog selection includes schemas).
const namespaceDescendants = ref<Map<number, Set<number>>>(new Map());

const {
  saveSettings: saveNodeSettings,
  saveSettingsWithOperations,
  handleGenericSettingsUpdate,
} = useNodeSettings({ nodeRef: nodeRunFlow });

const filteredFlows = computed(() => {
  let flows = catalogStore.allFlows;
  if (selectedNamespaceId.value != null) {
    const ids =
      namespaceDescendants.value.get(selectedNamespaceId.value) ??
      new Set([selectedNamespaceId.value]);
    flows = flows.filter((f) => f.namespace_id != null && ids.has(f.namespace_id));
  }
  // Keep the currently selected flow visible even when browsing other
  // namespaces, so the select never shows a bare id.
  const selected = catalogStore.allFlows.find((f) => f.id === selectedRegistrationId.value);
  if (selected && !flows.some((f) => f.id === selected.id)) {
    return [selected, ...flows];
  }
  return flows;
});

const availableColumns = computed((): string[] => {
  return (nodeData.value?.main_input?.table_schema ?? []).map((col) => col.name);
});

const availableColumnSchema = computed(
  (): FileColumn[] => nodeData.value?.main_input?.table_schema ?? [],
);

const bindingRows = computed((): BindingRow[] => {
  if (!nodeRunFlow.value) return [];
  return mergeBindingRows(nodeRunFlow.value.parameter_specs, nodeRunFlow.value.parameter_bindings);
});

// Iterating over input rows only makes sense when a parameter is fed from a
// column; hide the section (and neutralize the mode) otherwise.
const showParameterRows = computed((): boolean => hasColumnBinding(bindingRows.value));

watch(showParameterRows, (visible) => {
  if (!visible && nodeRunFlow.value) {
    nodeRunFlow.value.iteration_mode = "first_value";
  }
});

const paramType = (spec: FlowParameter): FlowParamType => spec.type ?? "string";

// Columns offered for a "column" binding, filtered to types that match the
// parameter. Keep the current selection visible even if its type no longer
// matches, so an existing binding is never silently hidden.
const columnsForRow = (row: BindingRow): string[] => {
  const matching = matchingColumnNames(availableColumnSchema.value, paramType(row.spec));
  const current = row.binding.column_name;
  if (current && !matching.includes(current)) {
    return [current, ...matching];
  }
  return matching;
};

const columnPlaceholder = (row: BindingRow): string => {
  if (availableColumns.value.length === 0) return "Connect data to the params input";
  if (columnsForRow(row).length === 0) return `No matching ${paramType(row.spec)} column`;
  return "Select column";
};

const setConstant = (row: BindingRow, value: string | null) => {
  row.binding.constant_value = value;
};

const constantAsNumber = (row: BindingRow): number | undefined => {
  const raw = row.binding.constant_value;
  if (raw == null || raw === "") return undefined;
  const num = Number(raw);
  return Number.isFinite(num) ? num : undefined;
};

const handleSourceChange = (row: BindingRow) => {
  if (row.binding.source === "constant") {
    row.binding.column_name = null;
    if (row.binding.constant_value == null) {
      row.binding.constant_value =
        paramType(row.spec) === "boolean" && row.spec.default_value !== "true"
          ? "false"
          : row.spec.default_value;
    }
  } else if (row.binding.source === "column") {
    row.binding.constant_value = null;
  } else {
    row.binding.constant_value = null;
    row.binding.column_name = null;
  }
};

const handleFlowChange = async (registrationId: number | null) => {
  if (!nodeRunFlow.value) return;
  if (registrationId == null) {
    nodeRunFlow.value.flow_reference = { registration_id: 0, flow_uuid: null, flow_path: null };
    nodeRunFlow.value.input_slots = [];
    nodeRunFlow.value.output_slots = [];
    nodeRunFlow.value.parameter_specs = [];
    nodeRunFlow.value.parameter_bindings = [];
    await saveSettings();
    return;
  }
  const registration = catalogStore.allFlows.find((f) => f.id === registrationId);
  nodeRunFlow.value.flow_reference = {
    registration_id: registrationId,
    flow_uuid: null,
    flow_path: registration?.flow_path ?? null,
  };
  await refreshInterface();
};

// Adopt a freshly-read interface in the local draft only; it is saved with the node.
const applyInterfaceDraft = (iface: SubflowInterface) => {
  if (!nodeRunFlow.value) return;
  nodeRunFlow.value.parameter_bindings = reconcileBindings(
    nodeRunFlow.value.parameter_bindings,
    iface.parameters,
    availableColumns.value,
  );
  nodeRunFlow.value.input_slots = iface.inputs.map((port) => port.name);
  nodeRunFlow.value.output_slots = iface.outputs.map((port) => port.name);
  nodeRunFlow.value.parameter_specs = iface.parameters;
};

const handleSignature = (handles: NodeHandle[] | undefined) =>
  JSON.stringify((handles ?? []).map((handle) => [handle.id, handle.label ?? ""]));

/**
 * Save the node and, in the same step, drop the canvas edges its interface no longer
 * has handles for. When the handles change the canvas reloads: core remaps keyed data
 * inputs by slot name, which only a reload shows faithfully.
 */
const saveSettings = async (): Promise<boolean> => {
  const vfInstance = flowStore.vueFlowInstance;
  const node = nodeRunFlow.value;
  const vfNode = node ? vfInstance?.findNode(String(node.node_id)) : undefined;
  if (!node || !vfInstance || !vfNode) return saveNodeSettings();

  const paramLabel = node.parameter_specs.length > 0 ? "Parameters" : "";
  const inputs = buildDynamicInputHandles([paramLabel, ...node.input_slots]);
  const outputs = buildDynamicOutputHandles(node.output_slots);
  if (
    handleSignature(vfNode.data.inputs) === handleSignature(inputs) &&
    handleSignature(vfNode.data.outputs) === handleSignature(outputs)
  ) {
    return saveNodeSettings();
  }

  const nodeId = String(node.node_id);
  const dangling = findDanglingEdges(
    vfInstance.getEdges.value,
    nodeId,
    inputs.map((handle) => handle.id),
    outputs.map((handle) => handle.id),
  );
  const saved = await saveSettingsWithOperations(
    "Update run_flow settings",
    deleteConnectionOperations(danglingEdgesToDelete(dangling, nodeId)),
  );
  if (!saved) return false;
  flowStore.requestReload();
  if (dangling.length > 0) {
    ElMessage.warning(
      `Removed ${plural(dangling.length, "connection")} that no longer match ` +
        "the flow's inputs/outputs.",
    );
  }
  return true;
};

const pushNodeData = (): Promise<boolean> => saveSettings();

const refreshInterface = async () => {
  const registrationId = nodeRunFlow.value?.flow_reference?.registration_id;
  if (!nodeRunFlow.value || !registrationId || registrationId <= 0) return;

  interfaceLoading.value = true;
  try {
    const iface = await CatalogApi.getFlowInterface(registrationId);
    if (iface.inputs.length > MAX_RUN_FLOW_INPUTS || iface.outputs.length > MAX_RUN_FLOW_OUTPUTS) {
      ElMessage.error(
        `This flow exposes too many ports (max ${MAX_RUN_FLOW_INPUTS} inputs / ` +
          `${MAX_RUN_FLOW_OUTPUTS} outputs).`,
      );
      return;
    }
    if (!iface.file_exists) {
      // Applying the degraded (empty) interface would wipe the saved slots and
      // delete the node's connections; keep the last-known interface instead.
      ElMessage.warning(
        "The flow file for this registration is missing; keeping the saved interface.",
      );
      return;
    }
    applyInterfaceDraft(iface);
    if (!(await saveSettings())) {
      ElMessage.error("Could not save the updated interface; connections were left untouched.");
    }
  } catch (error) {
    console.error("Failed to load flow interface:", error);
    ElMessage.error("Failed to load the flow's interface");
  } finally {
    interfaceLoading.value = false;
  }
};

/**
 * On open, reflect the referenced subflow's current inputs/outputs/parameters in the
 * draft (never saved on open: it rides along with the next save). Only touches the
 * draft when the interface actually drifted, and never on a missing file or a
 * too-large interface.
 */
const syncInterfaceOnOpen = async (nodeId: number) => {
  const registrationId = nodeRunFlow.value?.flow_reference?.registration_id;
  if (!nodeRunFlow.value || !registrationId || registrationId <= 0) return;
  try {
    const iface = await CatalogApi.getFlowInterface(registrationId);
    // The fetch is a network round-trip; if the drawer moved to another node or
    // closed meanwhile, don't touch the node the user navigated away from.
    if (!nodeRunFlow.value || nodeStore.node_id !== nodeId) return;
    if (
      !iface.file_exists ||
      iface.inputs.length > MAX_RUN_FLOW_INPUTS ||
      iface.outputs.length > MAX_RUN_FLOW_OUTPUTS
    ) {
      return;
    }
    if (
      !subflowInterfaceChanged(
        iface,
        nodeRunFlow.value.input_slots,
        nodeRunFlow.value.output_slots,
        nodeRunFlow.value.parameter_specs,
      )
    ) {
      return;
    }
    applyInterfaceDraft(iface);
    ElMessage.info("The subflow's interface changed. Apply to update this node.");
  } catch (error) {
    console.error("Could not auto-sync the run_flow interface on open:", error);
  }
};

let catalogLoadPromise: Promise<void> | null = null;

const loadCatalogData = async () => {
  try {
    const [tree] = await Promise.all([CatalogApi.getNamespaceTree(), catalogStore.loadAllFlows()]);
    // Flows can be registered on a catalog (level 0) or a schema (level 1):
    // offer both, and let picking a catalog include its schemas' flows.
    const options: { id: number; label: string }[] = [];
    const descendants = new Map<number, Set<number>>();
    for (const catalog of tree) {
      const catalogIds = new Set<number>([catalog.id]);
      options.push({ id: catalog.id, label: catalog.name });
      for (const schema of catalog.children ?? []) {
        catalogIds.add(schema.id);
        options.push({ id: schema.id, label: `${catalog.name} / ${schema.name}` });
        descendants.set(schema.id, new Set([schema.id]));
      }
      descendants.set(catalog.id, catalogIds);
    }
    namespaces.value = options;
    namespaceDescendants.value = descendants;
  } catch {
    // Catalog not available
  }
};

onMounted(() => {
  catalogLoadPromise = loadCatalogData();
});

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  const settings = nodeData.value?.setting_input as NodeRunFlow | undefined;
  if (settings && settings.flow_reference) {
    nodeRunFlow.value = settings;
  } else {
    nodeRunFlow.value = {
      flow_id: nodeStore.flow_id,
      node_id: nodeId,
      cache_results: false,
      pos_x: 0,
      pos_y: 0,
      is_setup: false,
      description: "",
      flow_reference: { registration_id: 0, flow_uuid: null, flow_path: null },
      input_slots: [],
      output_slots: [],
      parameter_specs: [],
      parameter_bindings: [],
      iteration_mode: "first_value",
      append_run_metadata: false,
    };
  }
  // Guarantee one binding per spec so the table edits the saved objects.
  nodeRunFlow.value.parameter_bindings = mergeBindingRows(
    nodeRunFlow.value.parameter_specs,
    nodeRunFlow.value.parameter_bindings,
  ).map((row) => row.binding);

  const registrationId = nodeRunFlow.value.flow_reference.registration_id;
  selectedRegistrationId.value = registrationId > 0 ? registrationId : null;

  dataLoaded.value = true;

  if (catalogLoadPromise) await catalogLoadPromise;
  const registration = catalogStore.allFlows.find((f) => f.id === selectedRegistrationId.value);
  // Preselect only when the namespace is a known option — otherwise the select
  // would display the raw id.
  if (
    registration?.namespace_id != null &&
    namespaces.value.some((ns) => ns.id === registration.namespace_id)
  ) {
    selectedNamespaceId.value = registration.namespace_id;
  }

  // Reflect any change to the referenced subflow's interface since this node was
  // last saved (the reason a stale run_flow node keeps old handles on the canvas).
  await syncInterfaceOnOpen(nodeId);
};

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
  gap: 14px;
}

.run-flow-field {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.run-flow-label {
  font-size: 12px;
  font-weight: 500;
  color: var(--color-text-secondary);
}

.flow-picker-row {
  display: flex;
  gap: 6px;
  align-items: center;
}

.namespace-select {
  flex: 0 0 38%;
  min-width: 120px;
}

.flow-picker-select {
  flex: 1;
}

.interface-summary {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 8px 10px;
  border: 1px solid var(--color-border-primary);
  border-radius: 6px;
  background: var(--color-background-secondary, #f5f7fa);
}

.chip-row {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  align-items: center;
  font-size: 12px;
}

.chip-row-label {
  font-weight: 500;
  color: var(--color-text-secondary);
  margin-right: 2px;
}

.chip-empty {
  color: var(--color-text-muted);
  font-style: italic;
}

.param-table {
  display: flex;
  flex-direction: column;
  border: 1px solid var(--color-border-primary);
  border-radius: 6px;
  overflow: hidden;
}

.param-table-header,
.param-table-row {
  display: flex;
  gap: 8px;
  align-items: center;
  padding: 6px 8px;
}

.param-table-header {
  background: var(--color-background-secondary, #f5f7fa);
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: var(--color-text-secondary);
}

.param-table-row {
  border-top: 1px solid var(--color-border-light, var(--color-border-primary));
  font-size: 12px;
}

.param-col--name {
  flex: 2;
  display: flex;
  align-items: center;
  gap: 6px;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
}

.param-col--source {
  flex: 1.4;
  min-width: 96px;
}

.param-col--value {
  flex: 2;
  min-width: 120px;
}

.param-col--value > * {
  width: 100%;
}

.param-type-tag {
  flex-shrink: 0;
}

.param-default-value {
  color: var(--color-text-muted);
  font-family: monospace;
}

.param-empty {
  font-size: 12px;
  color: var(--color-text-muted);
  font-style: italic;
}

.iterate-option {
  margin-top: 4px;
  padding-left: 18px;
}
</style>
