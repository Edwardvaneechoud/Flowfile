<template>
  <div class="status-wrapper">
    <pop-over
      :content="tooltipContent"
      :title="isRunning ? 'Processing Flow' : 'Flow Idle'"
      placement="left"
    >
      <div class="flow-card" :class="{ 'is-running': isRunning }">
        <svg viewBox="0 0 100 100" class="flow-animation" :class="{ 'is-flowing': isRunning }">
          <defs>
            <linearGradient id="lineGradient1" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stop-color="#818cf8" />
              <stop offset="50%" stop-color="#a78bfa" />
              <stop offset="100%" stop-color="#818cf8" />
            </linearGradient>

            <linearGradient id="lineGradient2" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stop-color="#6366f1" />
              <stop offset="50%" stop-color="#8b5cf6" />
              <stop offset="100%" stop-color="#6366f1" />
            </linearGradient>

            <linearGradient id="lineGradient3" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stop-color="#4f46e5" />
              <stop offset="50%" stop-color="#7c3aed" />
              <stop offset="100%" stop-color="#4f46e5" />
            </linearGradient>
          </defs>

          <!-- Background -->
          <rect width="100" height="100" class="flow-bg" />

          <!-- Flowing lines -->
          <g class="flow-lines">
            <path class="flow-line line-1" d="M-20 30 Q5 20, 30 30 T80 30 T130 30" />
            <path class="flow-line line-2" d="M-20 50 Q5 40, 30 50 T80 50 T130 50" />
            <path class="flow-line line-3" d="M-20 70 Q5 60, 30 70 T80 70 T130 70" />
          </g>

          <!-- Particles -->
          <g class="particles">
            <circle class="particle p1" cx="20" cy="50" r="3.5" />
            <circle class="particle p2" cx="50" cy="50" r="3.5" />
            <circle class="particle p3" cx="80" cy="50" r="3.5" />
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

const tooltipContent = computed(() => {
  let content = `Last updated: ${formattedTime.value}<br>`;
  const currentRunResult = nodeStore.getRunResult(nodeStore.flow_id);

  if (isRunning.value) {
    content += "Currently processing data...";
  } else {
    content += "Flow is waiting to start.";

    if (currentRunResult) {
      const thisNodeResult = currentRunResult.node_step_result.find(
        (result) => result.node_id === nodeStore.node_id,
      );

      if (thisNodeResult?.error) {
        content = `Last Error: ${thisNodeResult.error}<br>`;
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
  width: 44px;
  height: 44px;
  border-radius: 12px;
  cursor: help;
  overflow: hidden;
  background: linear-gradient(145deg, #1e1b4b 0%, #312e81 100%);
  box-shadow:
    0 4px 12px rgba(99, 102, 241, 0.15),
    inset 0 1px 0 rgba(255, 255, 255, 0.08);
  transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
}

.flow-card:hover {
  transform: translateY(-2px);
  box-shadow:
    0 8px 20px rgba(99, 102, 241, 0.25),
    inset 0 1px 0 rgba(255, 255, 255, 0.12);
}

.flow-card.is-running {
  box-shadow:
    0 6px 24px rgba(99, 102, 241, 0.3),
    inset 0 1px 0 rgba(255, 255, 255, 0.1);
}

.flow-animation {
  width: 100%;
  height: 100%;
}

.flow-bg {
  fill: transparent;
}

/* Line styles */
.flow-line {
  fill: none;
  stroke-linecap: round;
  stroke-width: 6;
}

.line-1 {
  stroke: url(#lineGradient1);
  opacity: 0.9;
  animation: smoothFlow 8s ease-in-out infinite;
}

.line-2 {
  stroke: url(#lineGradient2);
  opacity: 0.7;
  animation: smoothFlow 8s ease-in-out infinite -2.6s;
}

.line-3 {
  stroke: url(#lineGradient3);
  opacity: 0.5;
  animation: smoothFlow 8s ease-in-out infinite -5.3s;
}

/* When processing - continuous flowing */
.is-flowing .line-1 {
  animation: continuousFlow 3s linear infinite;
  opacity: 0.95;
}

.is-flowing .line-2 {
  animation: continuousFlow 3s linear infinite -1s;
  opacity: 0.75;
}

.is-flowing .line-3 {
  animation: continuousFlow 3s linear infinite -2s;
  opacity: 0.55;
}

/* Particles */
.particle {
  fill: rgba(255, 255, 255, 0.8);
  opacity: 0;
}

.is-flowing .particle {
  opacity: 1;
}

.is-flowing .p1 {
  animation: particleMove 3s linear infinite;
}

.is-flowing .p2 {
  animation: particleMove 3s linear infinite -1s;
}

.is-flowing .p3 {
  animation: particleMove 3s linear infinite -2s;
}

/* Control Button */
.control-button {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 36px;
  height: 36px;
  border-radius: 50%;
  border: none;
  background: linear-gradient(145deg, #1e1b4b 0%, #312e81 100%);
  color: #a5b4fc;
  cursor: pointer;
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow:
    0 4px 12px rgba(99, 102, 241, 0.15),
    inset 0 1px 0 rgba(255, 255, 255, 0.08);
}

.control-button:hover {
  background: linear-gradient(145deg, #312e81 0%, #4c1d95 100%);
  color: white;
  transform: translateY(-2px);
  box-shadow:
    0 8px 20px rgba(99, 102, 241, 0.25),
    inset 0 1px 0 rgba(255, 255, 255, 0.12);
}

.control-button.is-active {
  background: linear-gradient(145deg, #6366f1 0%, #8b5cf6 100%);
  color: white;
  box-shadow:
    0 4px 16px rgba(99, 102, 241, 0.35),
    inset 0 1px 0 rgba(255, 255, 255, 0.15);
}

/* Smooth idle animation - gentle wave motion */
@keyframes smoothFlow {
  0%, 100% {
    transform: translateX(0) translateY(0);
  }
  25% {
    transform: translateX(3px) translateY(2px);
  }
  50% {
    transform: translateX(0) translateY(4px);
  }
  75% {
    transform: translateX(-3px) translateY(2px);
  }
}

/* Continuous flow when processing */
@keyframes continuousFlow {
  0% {
    transform: translateX(0);
  }
  100% {
    transform: translateX(-50px);
  }
}

/* Particle movement along the flow */
@keyframes particleMove {
  0% {
    transform: translateX(-30px);
    opacity: 0;
  }
  10% {
    opacity: 1;
  }
  90% {
    opacity: 1;
  }
  100% {
    transform: translateX(70px);
    opacity: 0;
  }
}
</style>
