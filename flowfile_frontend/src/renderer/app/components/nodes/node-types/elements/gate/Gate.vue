<template>
  <div v-if="dataLoaded && nodeGate && gateInput" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeGate"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Condition source</div>
        <el-radio-group v-model="gateInput.condition_source" class="source-group" size="small">
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
        </template>

        <template v-else>
          <div class="listbox-subtitle section-gap">Formula</div>
          <div v-if="hasControlInput" class="source-banner">
            The gate opens when <strong>at least one row</strong> of the
            <strong>control input (C)</strong> matches this formula, and closes when none do. Use
            the control's columns here — the data input (D) only passes through.
          </div>
          <div v-else class="source-banner">
            The gate opens when <strong>at least one row</strong> of the
            <strong>data input (D)</strong> matches this formula, and closes when none do. Connect
            a node to the C handle to check that frame instead.
          </div>
          <main-editor-ref
            :key="String(nodeGate.node_id)"
            ref="editorChild"
            :editor-string="editorString"
          />
          <div class="help-text">
            Write it like a Filter condition — e.g. <code>[status] = 'error'</code> opens on a
            single error row anywhere in the data, not just the first value. An empty input closes
            the gate.
          </div>
        </template>

        <p class="explainer">
          The data input flows through unchanged while the condition holds. When it does not, the
          gate still succeeds and every node downstream of it is <strong>skipped</strong>, not
          failed — the run stays green.
        </p>
        <p class="explainer">
          For an if/else branch, feed two gates with complementary conditions (e.g.
          <code>equals</code> vs <code>not equals</code>, or a negated formula) and merge the two
          branches with a Union: whichever branch survived is what comes out.
        </p>
      </div>
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import { computed, ref } from "vue";
import {
  GATE_OPERATOR_LABELS,
  GATE_OPERATORS_WITH_VALUE,
  type GateInput,
  type GateOperator,
  type NodeGate,
} from "@/types/node.types";
import type { FlowParameter } from "@/types/flow.types";
import type { NodeData } from "@/components/nodes/baseNode/nodeInterfaces";
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

const hasControlInput = computed(() => Boolean(nodeData.value?.right_input));

const openFlowSettings = () => editorStore.requestOpenFlowSettings();

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
    dataLoaded.value = true;
  }
  // The drawer can open before the header has fetched flow settings, so make
  // sure the parameter dropdown has something to offer.
  if (flowStore.flowId > 0) await flowStore.loadParameters(flowStore.flowId);
};

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>

<style scoped>
.source-group {
  margin-top: 4px;
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
  background: var(--color-background-mute, #f4f4f5);
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
.explainer {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  line-height: 1.4;
  margin: var(--spacing-2) 0;
}
</style>
