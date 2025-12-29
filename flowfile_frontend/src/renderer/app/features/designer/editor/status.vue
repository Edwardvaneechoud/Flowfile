<template>
  <div class="status-wrapper">
    <pop-over
      :content="tooltipContent"
      :title="isRunning ? 'Processing Flow' : 'Flow Idle'"
      placement="left"
    >
      <div class="flow-card">
        <svg viewBox="0 0 100 100" class="flow-animation" :class="{ 'is-flowing': isRunning }">
          <!-- Gradient Definitions -->
          <defs>
            <linearGradient id="lineGradient" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stop-color="#4834d4" />
              <stop offset="100%" stop-color="#686de0" />
            </linearGradient>
          </defs>

          <!-- Background -->
          <rect width="100" height="100" class="flow-container" />

          <!-- Thick Gradient Lines -->
          <g class="flow-lines">
            <path d="M-10 25 Q 25 45, 60 25 T 130 25" class="flow-line flow-line-1" />
            <path d="M-10 50 Q 25 70, 60 50 T 130 50" class="flow-line flow-line-2" />
            <path d="M-10 75 Q 25 95, 60 75 T 130 75" class="flow-line flow-line-3" />
          </g>

          <!-- Flow Particles (only visible when running) -->
          <g class="flow-elements">
            <circle cx="20" cy="50" r="4" class="flow-particle particle-1" />
            <circle cx="40" cy="50" r="4" class="flow-particle particle-2" />
            <circle cx="60" cy="50" r="4" class="flow-particle particle-3" />
          </g>
        </svg>
      </div>
    </pop-over>

    <pop-over content="Show or hide logs and status" placement="left">
      <button
        class="control-button"
        :class="{ 'is-active': showFlowResult }"
        :title="buttonText"
        @click="toggleResults"
      >
        <el-icon v-if="!showFlowResult"><View /></el-icon>
        <el-icon v-if="showFlowResult"><Minus /></el-icon>
      </button>
    </pop-over>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from "vue";
import { useNodeStore } from "../../../stores/column-store";
import { useEditorStore } from "../../../stores/editor-store";
import { useItemStore } from "../../../components/common/DraggableItem/stateStore";
import { View, Minus } from "@element-plus/icons-vue";
import PopOver from "../editor/PopOver.vue";

const nodeStore = useNodeStore();
const editorStore = useEditorStore();
const draggableItemStore = useItemStore();
const lastStatusChange = ref(new Date());

const isRunning = computed(() => nodeStore.isRunning);
const showFlowResult = computed(() => nodeStore.showFlowResult);

const formattedTime = computed(() => {
  return lastStatusChange.value.toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
});

const popoverTitle = computed(() => {
  return isRunning.value ? "Processing Flow" : "Flow Idle";
});

const tooltipContent = computed(() => {
  let content = `Last updated: ${formattedTime.value}<br>`;
  // Get current run result for this flow if available
  const currentRunResult = nodeStore.getRunResult(nodeStore.flow_id);

  if (isRunning.value) {
    content += "Currently processing data...";
  } else {
    content += "Flow is waiting to start.";

    // Check if there are any run results for this flow
    if (currentRunResult) {
      const thisNodeResult = currentRunResult.node_step_result.find(
        (result) => result.node_id === nodeStore.node_id,
      );

      if (thisNodeResult?.error) {
        content = `Last Error: ${thisNodeResult.error}<br>`; // existing error handling
      }
    }
  }
  return content;
});

const buttonText = computed(() => (showFlowResult.value ? "Hide Results" : "Show Results"));

const toggleResults = () => {
  editorStore.showFlowResult = !editorStore.showFlowResult;
  editorStore.isShowingLogViewer = editorStore.showFlowResult;
  if (editorStore.isShowingLogViewer) {
    draggableItemStore.bringToFront("logViewer");
    draggableItemStore.bringToFront("flowresults");
  }
};
</script>

<style scoped>
.status-wrapper {
  display: flex;
  align-items: center;
  gap: 12px;
}

.flow-card {
  position: relative;
  width: 40px;
  height: 40px;
  border-radius: 8px;
  transition: all 0.3s ease;
  cursor: help;
  overflow: hidden;
}

.flow-card:hover {
  transform: translateY(-0.5px);
  box-shadow: 0 4px 16px rgba(159, 129, 228, 0.25);
}

.flow-animation {
  width: 100%;
  height: 100%;
}

/* Flow Container */
.flow-container {
  fill: #8c8caa;
}

/* Extra Thick Flow Lines with Gradient */
.flow-line {
  fill: none;
  stroke: url(#lineGradient);
  stroke-width: 8; /* Extra thick lines */
  stroke-linecap: round;
  opacity: 0.7; /* Increased opacity */
  transition: all 0.3s ease;
}

/* Animation only when flowing */
.is-flowing .flow-line {
  animation: flowMove 3s infinite linear;
}

.is-flowing .flow-line-2 {
  animation-delay: -1s;
}

.is-flowing .flow-line-3 {
  animation-delay: -2s;
}

/* Flow Particles - only visible when running */
.flow-particle {
  fill: #5256c6;
  opacity: 0;
}

.is-flowing .flow-particle {
  animation: particleFlow 3s infinite ease-in-out;
}

.particle-1 {
  animation-delay: 0s;
}

.particle-2 {
  animation-delay: -1s;
}

.particle-3 {
  animation-delay: -2s;
}
/* Control Button */
.control-button {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  border-radius: 50%;
  border: none;
  background: #8c8caa;
  color: #4834d4;
  cursor: pointer;
  transition: all 0.3s ease;
  box-shadow: 0 2px 8px rgba(76, 0, 255, 0.2);
}

.control-button:hover {
  background: #4834d4;
  color: white;
  transform: translateY(-1px);
  box-shadow: 0 4px 16px rgba(76, 0, 255, 0.3);
}

.control-button.is-active {
  background: #4834d4;
  color: white;
}

/* Animations */
@keyframes flowMove {
  0% {
    transform: translateX(0);
  }
  100% {
    transform: translateX(-70px);
  }
}

@keyframes particleFlow {
  0% {
    opacity: 0;
    transform: translateX(-10px) scale(0.8);
  }
  20% {
    opacity: 1;
    transform: translateX(0) scale(1);
  }
  80% {
    opacity: 1;
    transform: translateX(80px) scale(1);
  }
  100% {
    opacity: 0;
    transform: translateX(90px) scale(0.8);
  }
}
</style>
