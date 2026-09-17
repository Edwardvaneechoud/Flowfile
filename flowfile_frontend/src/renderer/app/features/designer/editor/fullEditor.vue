<template>
  <div class="container">
    <editor-side-rail
      :table-schema="activeInput?.table_schema"
      :parameters="parameters"
      @value-selected="handleNodeSelected"
    />
    <div ref="editorWrapper" class="editor-wrapper">
      <function-editor
        ref="functionEditor"
        class="prism-editor-ref"
        :editor-string="code"
        :columns="activeInput?.columns"
        :column-types="columnTypes"
        :parameters="parameters"
        @update-editor-string="handleCodeChange"
      />
    </div>
  </div>
  <instant-func-results ref="instantFuncResultsRef" :node-id="nodeStore.node_id" />
</template>

<script lang="ts" setup>
import { ref, Ref, watch, onMounted, nextTick, computed, type PropType } from "vue";
import EditorSideRail from "./EditorSideRail.vue";
import FunctionEditor from "./FunctionEditor.vue";
import { useNodeStore } from "../../../stores/column-store";
import { useFlowStore } from "../../../stores/flow-store";
import type { FlowParameter } from "../../../types/flow.types";
import InstantFuncResults from "./instantFuncResults.vue";
import debounce from "lodash/debounce";

const nodeStore = useNodeStore();
const flowStore = useFlowStore();
const parameters = computed<FlowParameter[]>(() => flowStore.parameters);

const props = defineProps({
  editorString: { type: String, required: true },
  // Which connected input feeds columns/autocomplete/field list: the gate's
  // formula editor points this at the control input (right_input).
  inputSource: { type: String as PropType<"main" | "right">, default: "main" },
});

const activeInput = computed(() =>
  props.inputSource === "right" ? nodeStore.nodeData?.right_input : nodeStore.nodeData?.main_input,
);

const instantFuncResultsRef = ref<Ref<typeof InstantFuncResults> | null>(null);
const code = ref(props.editorString);
nodeStore.setInputCode(props.editorString);

const functionEditor = ref<typeof FunctionEditor | null>(null);
const showTools: Ref<boolean> = ref(true);
const showHideOptions = () => {
  showTools.value = !showTools.value;
};

const columnTypes = computed<Record<string, string>>(() => {
  const schema = activeInput.value?.table_schema ?? [];
  const map: Record<string, string> = {};
  for (const col of schema) {
    map[col.name] = col.data_type;
  }
  return map;
});

const handleCodeChange = (newCode: string) => {
  code.value = newCode;
  nodeStore.setInputCode(newCode);
};

watch(
  code,
  debounce((newCode: string) => {
    if (instantFuncResultsRef.value) {
      instantFuncResultsRef.value.getInstantFuncResults(newCode, nodeStore.flow_id);
    }
  }, 1500),
);

defineExpose({ showHideOptions, functionEditor, showTools });
const handleNodeSelected = (nodeLabel: string) => {
  functionEditor.value?.insertTextAtCursor(nodeLabel);
};

// Make sure the editor has the flow's parameters for highlight/autocomplete/hover.
// Fetch on open (and if the flow id changes) so we never depend on the header
// having populated the store first.
const ensureParameters = () => {
  if (flowStore.flowId > 0) flowStore.loadParameters(flowStore.flowId);
};
watch(() => flowStore.flowId, ensureParameters);

onMounted(async () => {
  await nextTick();
  ensureParameters();
  if (instantFuncResultsRef.value) {
    instantFuncResultsRef.value.getInstantFuncResults(props.editorString, nodeStore.flow_id);
  }
});
</script>

<style scoped>
.container {
  display: flex;
  border: 1px solid var(--color-border-primary);
  border-radius: 5px;
  overflow: hidden;
  height: 100%;
  cursor: auto;
  background-color: var(--color-background-primary);
}

.editor-wrapper {
  flex-grow: 1;
  flex-direction: column;
  overflow: hidden;
  background-color: var(--color-background-secondary);
}

.prism-editor-ref {
  flex: 1;
  padding: 1px;
  min-height: 0px;
}

.error-box-wrapper {
  overflow-y: auto;
  border-top: 1px solid var(--color-border-primary);
}
</style>
