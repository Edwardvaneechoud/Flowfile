<template>
  <div v-if="dataLoaded && nodeGate && gateInput" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeGate"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Condition source</div>
        <el-radio-group
          v-model="gateInput.condition_source"
          class="source-group"
          size="small"
          @change="handleConditionSourceChange"
        >
          <el-radio-button value="parameter">Flow parameter</el-radio-button>
          <el-radio-button value="formula">Formula</el-radio-button>
        </el-radio-group>

        <template v-if="gateInput.condition_source === 'parameter'">
          <div class="listbox-subtitle section-gap">Condition</div>
          <div v-if="parameters.length === 0" class="empty-state">
            <p class="empty-state-text">
              This flow has no parameters yet. The gate compares a flow parameter (e.g.
              <code>env</code>) against a value, so define one first.
            </p>
            <el-button size="small" type="primary" @click="openFlowSettings">
              Add a parameter in Flow settings
            </el-button>
          </div>
          <template v-else>
            <div class="condition-row">
              <div class="condition-field">
                <label class="field-label">Parameter</label>
                <el-select
                  v-model="gateInput.parameter"
                  size="small"
                  filterable
                  placeholder="Select a parameter"
                >
                  <el-option
                    v-for="parameter in parameters"
                    :key="parameter.name"
                    :label="parameter.name"
                    :value="parameter.name"
                  />
                </el-select>
              </div>
              <div class="condition-field">
                <label class="field-label">Operator</label>
                <el-select v-model="gateInput.operator" size="small">
                  <el-option
                    v-for="option in operatorOptions"
                    :key="option.value"
                    :label="option.label"
                    :value="option.value"
                  />
                </el-select>
              </div>
              <div v-if="showValueInput" class="condition-field">
                <label class="field-label">Value</label>
                <el-input v-model="gateInput.value" size="small" :placeholder="valuePlaceholder" />
              </div>
            </div>
            <div v-if="!gateInput.parameter" class="help-text">
              Pick the flow parameter the condition reads.
            </div>
            <div v-else-if="parameterHint" class="help-text">{{ parameterHint }}</div>
            <div v-if="showListHint" class="help-text">Enter comma-separated values.</div>
          </template>
          <div v-if="hasControlInput" class="help-text">
            The control input is only read by Formula conditions.
          </div>
        </template>

        <template v-else>
          <div class="listbox-subtitle section-gap">Formula</div>
          <div v-if="hasControlInput" class="source-banner">
            The gate opens when <strong>at least one row</strong> of the
            <strong>control input (the bottom handle)</strong> matches this formula, and closes when
            none do. Use the control's columns here — the data input only passes through.
          </div>
          <div v-else class="source-banner">
            The gate opens when <strong>at least one row</strong> of the
            <strong>data input</strong> matches this formula, and closes when none do. Connect a
            node to the bottom control handle to check that frame instead.
          </div>
          <main-editor-ref
            :key="String(nodeGate.node_id)"
            ref="editorChild"
            :editor-string="editorString"
            :input-source="
              gateInput.condition_source === 'formula' && hasControlInput ? 'right' : 'main'
            "
          />
          <div class="help-text">
            Write it like a Filter condition — e.g. <code>[status] = 'error'</code> opens on a
            single error row anywhere in the data, not just the first value. An empty input closes
            the gate.
          </div>
        </template>

        <div class="else-output-row">
          <el-switch
            v-model="elseOutputEnabled"
            size="small"
            active-text="Add an else output"
            inactive-text="Single output"
            @change="handleElseToggle"
          />
          <el-popover placement="top-start" :width="340" trigger="click">
            <template #reference>
              <span class="help-icon" role="button" aria-label="How the gate routes">?</span>
            </template>
            <div class="gate-help">
              <p>
                <strong>How the gate routes.</strong> The data flows through unchanged while the
                condition holds. When it does not, the gate still succeeds and everything downstream
                is <strong>skipped</strong>, not failed — the run stays green.
              </p>
              <p>
                <strong>If/else.</strong> With the else output on, the data leaves
                <strong>T</strong> (then) when the condition holds and <strong>E</strong> (else)
                when it does not — exactly one side runs. Put each branch behind one exit and merge
                them with a Union: whichever branch ran is what comes out.
              </p>
            </div>
          </el-popover>
        </div>
        <div v-if="elseOutputEnabled" class="else-output-hint">
          T (then) runs while the condition holds; E (else) runs when it does not.
        </div>
      </div>
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import { computed, nextTick, ref, watch } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import {
  GATE_OPERATOR_LABELS,
  GATE_OPERATORS_WITH_VALUE,
  type GateInput,
  type GateOperator,
  type NodeGate,
} from "@/types/node.types";
import { buildOutputHandles } from "@/utils/nodeHandles";
import type { FlowParameter } from "@/types/flow.types";
import type { NodeConnection } from "@/types/canvas.types";
import type { NodeData } from "@/components/nodes/baseNode/nodeInterfaces";
import { FlowApi } from "@/api";
import { suppressedEdgeRemovals } from "@/composables/useDragAndDrop";
import { useNodeStore } from "@/stores/node-store";
import { useFlowStore } from "@/stores/flow-store";
import { useEditorStore } from "@/stores/editor-store";
import { useNodeSettings } from "@/composables/useNodeSettings";
import GenericNodeSettings from "@/components/nodes/baseNode/genericNodeSettings.vue";
import MainEditorRef from "@/features/designer/editor/fullEditor.vue";

const nodeStore = useNodeStore();
const flowStore = useFlowStore();
const editorStore = useEditorStore();
const dataLoaded = ref(false);
const nodeGate = ref<NodeGate | null>(null);
const nodeData = ref<NodeData | null>(null);
const gateInput = ref<GateInput | null>(null);
const editorString = ref("");
const editorChild = ref(null);
const elseOutputEnabled = ref(false);

const updateNodeOutputHandles = () => {
  const vfInstance = flowStore.vueFlowInstance;
  if (!vfInstance || !nodeGate.value) return;
  const vfNode = vfInstance.findNode(String(nodeGate.value.node_id));
  if (!vfNode) return;
  // Same derivation the canvas uses on flow open (NodeGate.output_names), so
  // toggling the else output live and reloading the flow agree on the handles.
  vfNode.data.outputs = elseOutputEnabled.value
    ? buildOutputHandles(2, ["then", "else"])
    : buildOutputHandles(1);
};

watch(elseOutputEnabled, () => {
  updateNodeOutputHandles();
});

// Fired only on user toggles (not on load), so drawer opens never prompt.
const handleElseToggle = async (enabled: boolean | string | number) => {
  if (enabled) return;
  const vfInstance = flowStore.vueFlowInstance;
  const nodeId = nodeGate.value?.node_id;
  if (!vfInstance || nodeId == null) return;
  const staleEdges = vfInstance.getEdges.value.filter(
    (edge: { source: string; sourceHandle?: string | null }) =>
      edge.source === String(nodeId) && edge.sourceHandle === "output-1",
  );
  if (staleEdges.length === 0) return;
  const plural = staleEdges.length === 1 ? "connection" : "connections";
  try {
    await ElMessageBox.confirm(
      `The else output still has ${staleEdges.length} ${plural} on the canvas. Without an else ` +
        "output, an open gate serves those edges the then-data instead of skipping them. " +
        "Remove them, or keep them and accept that behavior?",
      "Else output disabled",
      {
        confirmButtonText: staleEdges.length === 1 ? "Remove edge" : "Remove edges",
        cancelButtonText: "Keep",
        type: "warning",
      },
    );
  } catch {
    return;
  }
  await removeElseEdges(staleEdges);
};

// Same backend-first prune as RunFlow.vue's applyInterfaceToCanvas.
const removeElseEdges = async (
  edges: {
    id: string;
    source: string;
    target: string;
    sourceHandle?: string | null;
    targetHandle?: string | null;
  }[],
) => {
  const vfInstance = flowStore.vueFlowInstance;
  if (!vfInstance || !nodeGate.value) return;
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
      await FlowApi.deleteConnection(Number(nodeGate.value.flow_id), connection);
    } catch (error) {
      // 422 = already gone server-side; the edge is stale either way.
      const status = (error as { response?: { status?: number } })?.response?.status;
      if (status !== 422) {
        console.error("Failed to delete stale else connection:", error);
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
      `Could not remove ${failedCount} else connection${failedCount === 1 ? "" : "s"}; ` +
        "they remain on the canvas.",
    );
  }
};

const defaultGateInput = (): GateInput => ({
  condition_source: "parameter",
  parameter: "",
  operator: "equals",
  value: "",
  formula: "",
});

const operatorOptions = (Object.keys(GATE_OPERATOR_LABELS) as GateOperator[]).map((operator) => ({
  value: operator,
  label: GATE_OPERATOR_LABELS[operator],
}));

const parameters = computed<FlowParameter[]>(() => flowStore.parameters);

// Live canvas edges, not the one-time nodeData snapshot, so connecting or
// disconnecting the control pip while the drawer is open stays truthful.
const hasControlInput = computed(() => {
  const vfInstance = flowStore.vueFlowInstance;
  const nodeId = nodeGate.value?.node_id;
  if (vfInstance && nodeId != null) {
    return vfInstance.getEdges.value.some(
      (edge: { target: string; targetHandle?: string | null }) =>
        edge.target === String(nodeId) && edge.targetHandle === "input-1",
    );
  }
  return Boolean(nodeData.value?.right_input);
});

const openFlowSettings = () => editorStore.requestOpenFlowSettings();

// Leaving Formula unmounts the editor; capture its live buffer so flipping
// back remounts with the unsaved formula instead of the last-saved one.
const handleConditionSourceChange = (source: string | number | boolean | undefined) => {
  if (source !== "formula") {
    editorString.value = nodeStore.inputCode;
  }
};

const showValueInput = computed(() =>
  GATE_OPERATORS_WITH_VALUE.includes(gateInput.value?.operator ?? "equals"),
);

const showListHint = computed(
  () => gateInput.value?.operator === "in" || gateInput.value?.operator === "not_in",
);

const valuePlaceholder = computed(() => (showListHint.value ? "value1, value2" : "Enter value"));

const parameterHint = computed(() => {
  const selected = parameters.value.find((p) => p.name === gateInput.value?.parameter);
  if (!selected) return "";
  const type = selected.type ?? "string";
  return `${selected.name} is a ${type} parameter (default: ${selected.default_value || "empty"}).`;
});

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeGate,
  onBeforeSave: () => {
    if (gateInput.value?.condition_source === "formula") {
      gateInput.value.formula = nodeStore.inputCode;
    }
    if (nodeGate.value) {
      nodeGate.value.else_output = elseOutputEnabled.value;
    }
  },
});

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeGate.value = (nodeData.value?.setting_input as NodeGate) ?? null;
  if (nodeGate.value) {
    if (!nodeGate.value.gate_input) {
      nodeGate.value.gate_input = defaultGateInput();
    }
    gateInput.value = nodeGate.value.gate_input;
    editorString.value = gateInput.value.formula ?? "";
    elseOutputEnabled.value = Boolean(nodeGate.value.else_output);
    dataLoaded.value = true;
  }
  // The drawer can open before the header has fetched flow settings, so make
  // sure the parameter dropdown has something to offer.
  if (flowStore.flowId > 0) await flowStore.loadParameters(flowStore.flowId);
  await nextTick();
  updateNodeOutputHandles();
};

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>

<style scoped>
.source-group {
  margin-top: 4px;
}
.else-output-row {
  margin-top: 12px;
  display: flex;
  align-items: center;
  gap: 8px;
}
.else-output-hint {
  margin-top: 4px;
  font-size: 12px;
  color: var(--color-text-secondary);
}
.help-icon {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 16px;
  height: 16px;
  border-radius: 50%;
  background: var(--color-background-soft, #f0f0f0);
  color: var(--color-text-secondary, #777);
  font-size: 11px;
  font-weight: 600;
  cursor: help;
}
.gate-help p {
  margin: 0 0 8px;
  font-size: 12px;
  line-height: 1.5;
  color: var(--color-text-secondary);
}
.gate-help p:last-child {
  margin-bottom: 0;
}
.section-gap {
  margin-top: 12px;
}
.condition-row {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  align-items: flex-end;
}
.condition-field {
  flex: 1;
  min-width: 150px;
  max-width: 250px;
}
.field-label {
  display: block;
  font-size: 12px;
  font-weight: 500;
  color: var(--color-text-secondary);
  margin-bottom: 4px;
}
.help-text {
  margin-top: 6px;
  font-size: 11px;
  color: var(--color-text-tertiary);
}
.source-banner {
  margin-bottom: 6px;
  padding: 6px 10px;
  font-size: 12px;
  color: var(--color-text-secondary);
  background: var(--color-background-muted);
  border-left: 3px solid var(--color-primary, #0891b2);
  border-radius: 4px;
}
.empty-state {
  padding: 10px 12px;
  border: 1px dashed var(--color-border, #dcdfe6);
  border-radius: 6px;
}
.empty-state-text {
  margin: 0 0 8px;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  line-height: 1.4;
}
</style>
