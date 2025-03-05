<template>
    <div class="flow-selector">
      <el-select 
        v-model="selectedFlowId" 
        placeholder="Select Flow" 
        @change="handleFlowChange"
        size="small"
        class="flow-dropdown"
      >
        <el-option
          v-for="flow in flows"
          :key="flow.flow_id"
          :label="flow.name"
          :value="flow.flow_id"
        >
          <div class="flow-option">
            <span class="material-icons">account_tree</span>
            <span>{{ flow.name }}</span>
          </div>
        </el-option>
      </el-select>
    </div>
  </template>
  
  <script setup lang="ts">
  import { ref, watch, onMounted } from 'vue';
  import { useNodeStore } from "../../stores/column-store";
  import { getAllFlows } from "../../features/designer/components/Canvas/backendInterface";
  import { FlowSettings } from "../../features/designer/nodes/nodeLogic";
  
  const props = defineProps({
    onFlowChange: {
      type: Function,
      default: () => {}
    }
  });
  
  const emit = defineEmits(['flow-changed']);
  
  const flows = ref<FlowSettings[]>([]);
  const selectedFlowId = ref<number>(1); // Default to 1 as seen in your code
  const nodeStore = useNodeStore();
  
  const loadFlows = async () => {
    try {
      const flowsData = await getAllFlows();
      flows.value = flowsData;
      
      if (flowsData.length > 0 && !selectedFlowId.value) {
        selectedFlowId.value = flowsData[0].flow_id;
      }
      
      if (nodeStore.flow_id) {
        selectedFlowId.value = nodeStore.flow_id;
      }
    } catch (error) {
      console.error("Failed to load flows for selector:", error);
    }
  };
  
  const handleFlowChange = (flowId: number) => {
    nodeStore.setFlowId(flowId);
    emit('flow-changed', flowId);
    props.onFlowChange(flowId);
  };
  
  // Watch for changes to the flow ID in the store
  watch(() => nodeStore.flow_id, (newFlowId) => {
    if (newFlowId && newFlowId !== selectedFlowId.value) {
      selectedFlowId.value = newFlowId;
    }
  });
  
  onMounted(() => {
    loadFlows();
  });
  
  defineExpose({
    loadFlows,
    selectedFlowId
  });
  </script>
  
  <style scoped>
  .flow-selector {
    display: flex;
    align-items: center;
    margin: 0 10px;
  }
  
  .flow-dropdown {
    min-width: 150px;
  }
  
  .flow-option {
    display: flex;
    align-items: center;
    gap: 8px;
  }
  
  .flow-option .material-icons {
    font-size: 16px;
    color: #606266;
  }
  </style>