<template>
  <el-card class="run-card" shadow="hover">
    <div class="clearfix">
      <span>Flow ID: {{ runInformation?.flow_id }}</span>
      <span class="flow-summary">
        - {{ runInformation?.success ? "Succeeded" : "Failed" }}, Nodes:
        {{ runInformation?.nodes_completed }}/{{
          runInformation?.number_of_nodes
        }}
      </span>
    </div>
    <br />
    <div>
      <el-timeline>
        <el-timeline-item
          v-for="node in runInformation?.node_step_result"
          :key="node.node_id"
          :timestamp="formatTimestamp(node.start_timestamp)"
          :color="calculateColor(node.success)"
          @click="navigateToNode(`node-${node.node_id}`)"
        >
          <el-card class="node-card">
            <h4 v-if="nodeStore.nodeDescriptions[node.node_id]">
              {{ nodeStore.nodeDescriptions[node.node_id] }} ({{ node.node_name }})
            </h4>
            <h4 v-else>{{ `Node ${node.node_id}` }}: {{ node.node_name }}</h4>
            <div class="node-details">
              <p>Duration: {{ formatRunTime(node.run_time) }}</p>
              <p>
                Status:
                <span
                  :class="{ success: node.success, failure: !node.success }"
                  >{{ node.success ? "Success" : "Failure" }}</span
                >
              </p>
              <p v-if="!node.success" class="failure">
                Error: {{ node.error }}
              </p>
            </div>
          </el-card>
        </el-timeline-item>
      </el-timeline>
    </div>
  </el-card>
</template>

<script setup lang="ts">
import { ref, computed, defineProps } from "vue";
import { format } from "date-fns"; // Assuming date-fns is added for date formatting
import { useNodeStore } from "../../../stores/column-store";

const props = defineProps({
  tableViewer: {
    type: Object,
    required: false,
  },
  onClick: {
    type: Function,
    required: true,
  },
});

const nodeStore = useNodeStore();
const runInformation = computed(() => nodeStore.currentRunResult);
const selectedNode = ref<Element | null>(null);

const formatTimestamp = (timestamp: number) => {
  return format(new Date(timestamp * 1000), "yyyy-MM-dd HH:mm:ss");
};

const calculateColor = (success: boolean) => {
  return success ? "green" : "red";
};

const formatRunTime = (runTime: number) => {
  const ms = runTime * 1000;
  if (ms < 1000) {
    return `${Math.round(ms)} ms`;
  } else if (ms >= 1000 && runTime < 60) {
    return `${Math.round(runTime)} seconds`;
  } else {
    const minutes = Math.floor(runTime / 60);
    const seconds = Math.round(runTime % 60);
    return seconds > 0
      ? `${minutes} minutes, ${seconds} seconds`
      : `${minutes} minutes`;
  }
};

const navigateToNode = (nodeId: string) => {
  // Assuming you've assigned IDs to each node's root element in a way that incorporates the nodeId
  console.log(nodeId, "nodeId");
  if (selectedNode.value) {
    if (selectedNode.value?.classList.contains("selected")) {
      selectedNode.value.classList.remove("selected");
    }
  }

  const elementId = `#${nodeId}`;
  const nodeComponent = document.querySelector(elementId);
  console.log(nodeComponent, "nodeComponent");
  if (props.onClick) {
    props.onClick(nodeId.slice(5));
    //props.tableViewer.downloadData(nodeId.slice(5))
  }
  if (nodeComponent) {
    if (!nodeComponent.classList.contains("selected")) {
      nodeComponent.classList.add("selected");
    }

    // Assuming the button can be uniquely identified within the node component
    // Here we use `el-button` as a selector; adjust if necessary
    const button = nodeComponent.querySelector(".el-button");

    if (button) {
      let event = new MouseEvent("click", {
        bubbles: true,
        cancelable: true,
        view: window,
      });
      event.preventDefault();
      button.dispatchEvent(event);
      selectedNode.value = nodeComponent;
    }
  }
};
</script>

<style scoped>
.hide-results-button {
  margin-bottom: 10px;
}
.flow-summary {
  margin-left: 10px;
  font-weight: bold;
  color: #333;
}
.node-card {
  padding: 15px;
}
.node-details p {
  margin: 5px 0;
}
.success {
  color: green;
}
.failure {
  color: red;
}
</style>
