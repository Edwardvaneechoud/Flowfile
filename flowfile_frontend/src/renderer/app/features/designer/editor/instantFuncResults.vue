<template>
  <div class="container">
    <div v-if="!hasInput" class="result-content loading">
      <div class="label">Waiting for input</div>
      <div class="content"></div>
    </div>
    <div v-else-if="instantFuncResult.success === null" class="result-content loading">
      <div class="label">Processing</div>
      <div class="content">Function valid, run process to see results</div>
    </div>
    <div v-else-if="instantFuncResult.success" class="result-content success">
      <div class="label">Example result</div>
      <div class="content">{{ instantFuncResult.result }}</div>
    </div>
    <div v-else class="result-content error">
      <div class="label">Validation error</div>
      <div class="content">{{ instantFuncResult.result }}</div>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, defineExpose, defineProps, onMounted } from "vue";
import axios from "axios";
import { InstantFuncResult } from "./types";
import { useNodeStore } from "../../../stores/column-store";

const nodeStore = useNodeStore();

const hasInput = ref<boolean>(false);

const props = defineProps({
  nodeId: { type: Number, required: true },
});

const instantFuncResult = ref<InstantFuncResult>({
  result: "",
  success: false,
});

const getInstantFuncResults = async (funcString: string, flowId: number) => {
  if (funcString !== "") {
    hasInput.value = true;
    const response = await axios.get("/custom_functions/instant_result", {
      params: {
        node_id: props.nodeId,
        flow_id: flowId,
        func_string: funcString,
      },
    });
    instantFuncResult.value = response.data;
    console.log(instantFuncResult.value.result);
  } else {
    hasInput.value = false;
  }
};

onMounted(() => {
  if (nodeStore.inputCode !== "") {
    hasInput.value = true;
    getInstantFuncResults(nodeStore.inputCode, nodeStore.flow_id);
  }
});

defineExpose({ getInstantFuncResults });
</script>

<style scoped>
.container {
  width: 100%;
  margin: 12px 0 0 0;
  font-family:
    -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen, Ubuntu, Cantarell, "Open Sans",
    "Helvetica Neue", sans-serif;
}

.result-content {
  display: flex;
  width: 100%;
  border-radius: 6px;
  overflow: hidden;
  height: auto;
  min-height: 48px;
}

.result-content.success .label {
  background: #1a202c;
}

.result-content.loading .label {
  background: #1a202c;
}

.result-content.error .label {
  background: #7f1d1d;
  color: rgba(255, 255, 255, 0.9);
}

.label {
  flex: 0 0 150px;
  padding: 14px 16px;
  display: flex;
  align-items: center;
  font-weight: 500;
  font-size: 14px;
  color: rgba(255, 255, 255, 0.84);
  position: relative;
}

.label::after {
  content: "";
  position: absolute;
  top: 50%;
  right: 0;
  width: 1px;
  height: 24px;
  transform: translateY(-50%);
  background-color: rgba(255, 255, 255, 0.1);
}

.content {
  flex: 1;
  padding: 14px 20px;
  color: rgb(0, 0, 0);
  font-size: 14px;
  font-weight: 400;
  line-height: 1.5;
  overflow-wrap: break-word;
  word-break: break-word;
  background-color: var(--color-background-primary);
}

.result-content.error .content {
  background-color: var(--color-background-primary);
  color: var(--color-text-error);
}

.result-content.loading .label {
  display: flex;
  align-items: center;
}

.result-content .label::before {
  content: "";
  display: inline-block;
  width: 10px;
  height: 10px;
  margin-right: 8px;
  border-radius: 50%;
}

.result-content.success .label::before {
  background-color: #10b981;
  box-shadow: 0 0 0 4px rgba(16, 185, 129, 0.2);
}

.result-content.loading .label::before {
  background-color: #8b5cf6;
  box-shadow: 0 0 0 4px rgba(139, 92, 246, 0.2);
  animation: pulse 1.5s infinite ease-in-out;
}

.result-content.error .label::before {
  background-color: #ef4444;
  box-shadow: 0 0 0 4px rgba(239, 68, 68, 0.2);
}

@keyframes pulse {
  0% {
    opacity: 0.4;
    transform: scale(0.8);
  }
  50% {
    opacity: 1;
    transform: scale(1);
  }
  100% {
    opacity: 0.4;
    transform: scale(0.8);
  }
}
</style>
