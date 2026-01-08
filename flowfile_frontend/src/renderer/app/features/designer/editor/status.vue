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
            <!-- Soft gradient for waves -->
            <linearGradient id="waveGradient" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stop-color="#818cf8" />
              <stop offset="50%" stop-color="#a78bfa" />
              <stop offset="100%" stop-color="#818cf8" />
            </linearGradient>

            <linearGradient id="waveGradient2" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stop-color="#6366f1" />
              <stop offset="50%" stop-color="#8b5cf6" />
              <stop offset="100%" stop-color="#6366f1" />
            </linearGradient>

            <linearGradient id="waveGradient3" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stop-color="#4f46e5" />
              <stop offset="50%" stop-color="#7c3aed" />
              <stop offset="100%" stop-color="#4f46e5" />
            </linearGradient>
          </defs>

          <!-- Gentle wave layers -->
          <g class="waves">
            <path class="wave wave-3" d="M0 70 Q25 60, 50 70 T100 70 L100 100 L0 100 Z" />
            <path class="wave wave-2" d="M0 60 Q25 50, 50 60 T100 60 L100 100 L0 100 Z" />
            <path class="wave wave-1" d="M0 52 Q25 42, 50 52 T100 52 L100 100 L0 100 Z" />
          </g>

          <!-- Friendly floating dots -->
          <g class="dots">
            <circle class="dot dot-1" cx="25" cy="35" r="4" />
            <circle class="dot dot-2" cx="50" cy="28" r="3" />
            <circle class="dot dot-3" cx="75" cy="38" r="3.5" />
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

/* Wave styles - filled shapes */
.wave {
  transition: all 0.6s cubic-bezier(0.4, 0, 0.2, 1);
}

.wave-1 {
  fill: url(#waveGradient);
  opacity: 0.95;
  animation: gentleBob1 5s ease-in-out infinite;
}

.wave-2 {
  fill: url(#waveGradient2);
  opacity: 0.7;
  animation: gentleBob2 6s ease-in-out infinite;
}

.wave-3 {
  fill: url(#waveGradient3);
  opacity: 0.45;
  animation: gentleBob3 7s ease-in-out infinite;
}

/* When flowing - slightly faster, more lively */
.is-flowing .wave-1 {
  animation: flowBob1 2.5s ease-in-out infinite;
}

.is-flowing .wave-2 {
  animation: flowBob2 3s ease-in-out infinite;
}

.is-flowing .wave-3 {
  animation: flowBob3 3.5s ease-in-out infinite;
}

/* Dots - friendly floating circles */
.dot {
  fill: rgba(255, 255, 255, 0.85);
  transition: all 0.4s ease;
}

.dot-1 {
  animation: floatDot1 6s ease-in-out infinite;
}

.dot-2 {
  animation: floatDot2 7s ease-in-out infinite;
}

.dot-3 {
  animation: floatDot3 5.5s ease-in-out infinite;
}

/* When flowing - dots become more active */
.is-flowing .dot-1 {
  animation: activeDot1 2s ease-in-out infinite;
}

.is-flowing .dot-2 {
  animation: activeDot2 2.2s ease-in-out infinite;
}

.is-flowing .dot-3 {
  animation: activeDot3 1.8s ease-in-out infinite;
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

/* Animations - Gentle idle breathing */
@keyframes gentleBob1 {
  0%, 100% {
    transform: translateY(0);
  }
  50% {
    transform: translateY(4px);
  }
}

@keyframes gentleBob2 {
  0%, 100% {
    transform: translateY(0);
  }
  50% {
    transform: translateY(5px);
  }
}

@keyframes gentleBob3 {
  0%, 100% {
    transform: translateY(0);
  }
  50% {
    transform: translateY(6px);
  }
}

/* Animations - Active flowing */
@keyframes flowBob1 {
  0%, 100% {
    transform: translateY(0);
  }
  25% {
    transform: translateY(6px);
  }
  75% {
    transform: translateY(-2px);
  }
}

@keyframes flowBob2 {
  0%, 100% {
    transform: translateY(0);
  }
  30% {
    transform: translateY(8px);
  }
  70% {
    transform: translateY(-3px);
  }
}

@keyframes flowBob3 {
  0%, 100% {
    transform: translateY(0);
  }
  35% {
    transform: translateY(10px);
  }
  65% {
    transform: translateY(-4px);
  }
}

/* Dot floating animations - idle */
@keyframes floatDot1 {
  0%, 100% {
    transform: translate(0, 0);
    opacity: 0.85;
  }
  50% {
    transform: translate(2px, 3px);
    opacity: 0.7;
  }
}

@keyframes floatDot2 {
  0%, 100% {
    transform: translate(0, 0);
    opacity: 0.75;
  }
  50% {
    transform: translate(-2px, 4px);
    opacity: 0.6;
  }
}

@keyframes floatDot3 {
  0%, 100% {
    transform: translate(0, 0);
    opacity: 0.8;
  }
  50% {
    transform: translate(3px, 2px);
    opacity: 0.65;
  }
}

/* Dot animations - active */
@keyframes activeDot1 {
  0%, 100% {
    transform: translate(0, 0) scale(1);
    opacity: 0.9;
  }
  50% {
    transform: translate(3px, -4px) scale(1.15);
    opacity: 1;
  }
}

@keyframes activeDot2 {
  0%, 100% {
    transform: translate(0, 0) scale(1);
    opacity: 0.85;
  }
  50% {
    transform: translate(-2px, -5px) scale(1.2);
    opacity: 1;
  }
}

@keyframes activeDot3 {
  0%, 100% {
    transform: translate(0, 0) scale(1);
    opacity: 0.88;
  }
  50% {
    transform: translate(2px, -3px) scale(1.1);
    opacity: 1;
  }
}
</style>
