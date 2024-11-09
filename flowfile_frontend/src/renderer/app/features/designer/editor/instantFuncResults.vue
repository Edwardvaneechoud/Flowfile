<template>
  <div class="container">
    <div v-if="instantFuncResult.success" class="result-content">
      <div class="result-title">Example result:</div>
      <div>{{ instantFuncResult.result }}</div>
    </div>
    <div v-else-if="instantFuncResult.success === null" class="result-content">
      <div class="result-title">Loading...</div>
      Function valid, Run process to see results
    </div>
    <div v-else class="error-content">
      <div class="error-title">Validation error:</div>
      <div>{{ instantFuncResult.result }}</div>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, defineExpose, defineProps } from "vue";
import axios from "axios";
import { InstantFuncResult } from "./types";

const props = defineProps({
  nodeId: { type: Number, required: true },
});

const instantFuncResult = ref<InstantFuncResult>({
  result: "",
  success: false,
});

const getInstantFuncResults = async (funcString: string) => {
  const response = await axios.get("/custom_functions/instant_result", {
    params: {
      node_id: props.nodeId,
      flow_id: 1,
      func_string: funcString,
    },
  });
  instantFuncResult.value = response.data;
  console.log(instantFuncResult.value.result);
};
defineExpose({ getInstantFuncResults });
</script>

<style scoped>
.container {
  padding: 16px;
  background-color: #f9f9f9;
  border: 1px solid #ddd;
  border-radius: 8px;
  box-shadow: rgba(0, 0, 0, 0.1);
  margin: 20px;
  font-family: Arial, sans-serif;
}

.result-title,
.error-title {
  font-weight: bold;
  font-size: 18px;
  margin-bottom: 8px;
}

.result-content {
  font-size: 16px;
  color: #333;
  background-color: #fff;
  padding: 10px;
  border-radius: 4px;
  border: 1px solid #ddd;
  width: 100%;
  box-shadow: inset 0 1px 3px rgba(0, 0, 0, 0.1);
}

.error-content {
  font-size: 16px;
  color: #b00020;
  background-color: #ffebee;
  padding: 10px;
  border-radius: 4px;
  border: 1px solid #b00020;
  width: 100%;
  box-shadow: inset 0 1px 3px rgba(0, 0, 0, 0.1);
}
</style>
