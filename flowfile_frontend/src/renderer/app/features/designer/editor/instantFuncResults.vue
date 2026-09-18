<template>
  <div class="instant-function-results">
    <div class="result-content" :class="state" role="status">
      <span class="result-label">{{ label }}</span>
      <span class="result-value" :class="{ 'is-muted': muted }">{{ value }}</span>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { computed, ref, onMounted, type PropType } from "vue";
import axios from "axios";
import { InstantFuncResult } from "./types";
import { useNodeStore } from "../../../stores/column-store";

const nodeStore = useNodeStore();

const hasInput = ref<boolean>(false);

const props = defineProps({
  nodeId: { type: Number, required: true },
  // Names what is being previewed (the formula node's active output column).
  label: { type: String, default: "" },
  // Supplied by hosts that evaluate something other than a lone expression (the
  // formula node's chain). Without it the single-expression endpoint is used.
  fetcher: {
    type: Function as PropType<(funcString: string) => Promise<InstantFuncResult>>,
    default: null,
  },
});

const instantFuncResult = ref<InstantFuncResult>({
  result: "",
  success: false,
});

const state = computed(() => {
  if (!hasInput.value) return "idle";
  if (instantFuncResult.value.success === null) return "loading";
  return instantFuncResult.value.success ? "success" : "error";
});

const muted = computed(() => state.value === "idle" || state.value === "loading");

const label = computed(() => {
  const word = state.value === "error" ? "Error" : "Preview";
  return props.label ? `${word} · ${props.label}` : word;
});

const value = computed(() => {
  if (state.value === "idle") return "Type an expression to see an example result";
  if (state.value === "loading") return "Expression valid, run the flow to see results";
  return instantFuncResult.value.result;
});

const getInstantFuncResults = async (funcString: string, flowId: number) => {
  if (funcString !== "") {
    hasInput.value = true;
    if (props.fetcher) {
      instantFuncResult.value = await props.fetcher(funcString);
      return;
    }
    const response = await axios.get("/custom_functions/instant_result", {
      params: {
        node_id: props.nodeId,
        flow_id: flowId,
        func_string: funcString,
      },
    });
    instantFuncResult.value = response.data;
  } else {
    hasInput.value = false;
  }
};

onMounted(() => {
  // The global input-code buffer belongs to the single-expression hosts only.
  if (!props.fetcher && nodeStore.inputCode !== "") {
    hasInput.value = true;
    getInstantFuncResults(nodeStore.inputCode, nodeStore.flow_id);
  }
});

defineExpose({ getInstantFuncResults });
</script>

<style scoped>
.instant-function-results {
  width: 100%;
  margin: 12px 0 0;
}

.result-content {
  display: flex;
  align-items: baseline;
  gap: 12px;
  padding: 8px 12px;
  font-size: 12px;
  line-height: 1.5;
  border: 1px solid var(--color-border-primary);
  border-left: 3px solid var(--color-text-muted);
  border-radius: 0 4px 4px 0;
  background: var(--color-background-primary);
  transition: border-color var(--transition-normal, 200ms) var(--transition-timing, ease);
}

.result-content.loading {
  border-left-color: var(--color-accent);
}

.result-content.success {
  border-left-color: var(--color-success);
}

.result-content.error {
  border-left-color: var(--color-danger);
}

.result-label {
  flex: 0 0 auto;
  white-space: nowrap;
  color: var(--color-text-secondary);
}

.result-content.error .result-label {
  color: var(--color-danger);
}

.result-value {
  min-width: 0;
  font-family: var(--font-family-mono, monospace);
  color: var(--color-text-primary);
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}

.result-value.is-muted {
  font-family: inherit;
  color: var(--color-text-muted);
}

.result-content.error .result-value {
  font-family: inherit;
  color: var(--color-danger);
}
</style>
