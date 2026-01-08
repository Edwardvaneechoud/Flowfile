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
            <!-- Primary wave gradient -->
            <linearGradient id="waveGradient" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" stop-color="#6366f1" />
              <stop offset="50%" stop-color="#8b5cf6" />
              <stop offset="100%" stop-color="#a78bfa" />
            </linearGradient>

            <!-- Secondary wave gradient for depth -->
            <linearGradient id="waveGradient2" x1="0%" y1="100%" x2="100%" y2="0%">
              <stop offset="0%" stop-color="#4f46e5" />
              <stop offset="100%" stop-color="#7c3aed" />
            </linearGradient>

            <!-- Glow effect -->
            <filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
              <feGaussianBlur stdDeviation="2" result="coloredBlur" />
              <feMerge>
                <feMergeNode in="coloredBlur" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>

            <!-- Clip path for rounded container -->
            <clipPath id="roundedClip">
              <rect x="0" y="0" width="100" height="100" rx="16" ry="16" />
            </clipPath>
          </defs>

          <g clip-path="url(#roundedClip)">
            <!-- Background with subtle gradient -->
            <rect width="100" height="100" class="flow-bg" />

            <!-- Wave layers - back to front for depth -->
            <path class="wave wave-3" d="M-40 65 Q-20 55, 0 65 T40 65 T80 65 T120 65 T160 65" />
            <path class="wave wave-2" d="M-40 55 Q-20 45, 0 55 T40 55 T80 55 T120 55 T160 55" />
            <path class="wave wave-1" d="M-40 45 Q-20 35, 0 45 T40 45 T80 45 T120 45 T160 45" />

            <!-- Floating particles when processing -->
            <g class="particles" filter="url(#glow)">
              <circle class="particle p1" cx="25" cy="50" r="3" />
              <circle class="particle p2" cx="50" cy="45" r="2.5" />
              <circle class="particle p3" cx="75" cy="55" r="2" />
            </g>
          </g>
        </svg>

        <!-- Status indicator dot -->
        <div class="status-dot" :class="{ 'active': isRunning }"></div>
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
  background: linear-gradient(135deg, #1e1b4b 0%, #312e81 100%);
  box-shadow:
    0 4px 12px rgba(99, 102, 241, 0.15),
    inset 0 1px 0 rgba(255, 255, 255, 0.1);
  transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
}

.flow-card:hover {
  transform: translateY(-2px);
  box-shadow:
    0 8px 24px rgba(99, 102, 241, 0.25),
    inset 0 1px 0 rgba(255, 255, 255, 0.15);
}

.flow-card.is-running {
  box-shadow:
    0 8px 32px rgba(99, 102, 241, 0.35),
    0 0 0 2px rgba(99, 102, 241, 0.2),
    inset 0 1px 0 rgba(255, 255, 255, 0.1);
}

.flow-animation {
  width: 100%;
  height: 100%;
}

/* Background */
.flow-bg {
  fill: transparent;
}

/* Wave styles */
.wave {
  fill: none;
  stroke-linecap: round;
  stroke-width: 6;
}

.wave-1 {
  stroke: url(#waveGradient);
  opacity: 0.9;
  animation: breathe 4s ease-in-out infinite, waveFloat1 6s ease-in-out infinite;
}

.wave-2 {
  stroke: url(#waveGradient2);
  opacity: 0.6;
  animation: breathe 4s ease-in-out infinite 0.5s, waveFloat2 7s ease-in-out infinite;
}

.wave-3 {
  stroke: url(#waveGradient);
  opacity: 0.3;
  animation: breathe 4s ease-in-out infinite 1s, waveFloat3 8s ease-in-out infinite;
}

/* When flowing - more active animation */
.is-flowing .wave-1 {
  animation: flowWave 2s linear infinite, waveActive1 3s ease-in-out infinite;
  opacity: 1;
}

.is-flowing .wave-2 {
  animation: flowWave 2s linear infinite -0.4s, waveActive2 3.5s ease-in-out infinite;
  opacity: 0.75;
}

.is-flowing .wave-3 {
  animation: flowWave 2s linear infinite -0.8s, waveActive3 4s ease-in-out infinite;
  opacity: 0.5;
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
  animation: particleRise 2.5s ease-in-out infinite;
}

.is-flowing .p2 {
  animation: particleRise 2.5s ease-in-out infinite 0.8s;
}

.is-flowing .p3 {
  animation: particleRise 2.5s ease-in-out infinite 1.6s;
}

/* Status indicator dot */
.status-dot {
  position: absolute;
  bottom: 4px;
  right: 4px;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: #64748b;
  border: 2px solid #1e1b4b;
  transition: all 0.3s ease;
}

.status-dot.active {
  background: #22c55e;
  box-shadow: 0 0 8px rgba(34, 197, 94, 0.6);
  animation: pulse 1.5s ease-in-out infinite;
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
  background: linear-gradient(135deg, #1e1b4b 0%, #312e81 100%);
  color: #a5b4fc;
  cursor: pointer;
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow:
    0 4px 12px rgba(99, 102, 241, 0.15),
    inset 0 1px 0 rgba(255, 255, 255, 0.1);
}

.control-button:hover {
  background: linear-gradient(135deg, #312e81 0%, #4c1d95 100%);
  color: white;
  transform: translateY(-2px);
  box-shadow:
    0 8px 24px rgba(99, 102, 241, 0.25),
    inset 0 1px 0 rgba(255, 255, 255, 0.15);
}

.control-button.is-active {
  background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%);
  color: white;
  box-shadow:
    0 4px 16px rgba(99, 102, 241, 0.4),
    inset 0 1px 0 rgba(255, 255, 255, 0.2);
}

/* Animations */

/* Gentle breathing when idle */
@keyframes breathe {
  0%, 100% {
    transform: translateY(0);
  }
  50% {
    transform: translateY(3px);
  }
}

/* Subtle wave floating when idle */
@keyframes waveFloat1 {
  0%, 100% {
    transform: translateX(0);
  }
  50% {
    transform: translateX(5px);
  }
}

@keyframes waveFloat2 {
  0%, 100% {
    transform: translateX(0);
  }
  50% {
    transform: translateX(-5px);
  }
}

@keyframes waveFloat3 {
  0%, 100% {
    transform: translateX(0);
  }
  50% {
    transform: translateX(3px);
  }
}

/* Active flow - waves moving horizontally */
@keyframes flowWave {
  0% {
    transform: translateX(0);
  }
  100% {
    transform: translateX(-40px);
  }
}

/* Vertical movement when active */
@keyframes waveActive1 {
  0%, 100% {
    stroke-width: 6;
  }
  50% {
    stroke-width: 7;
  }
}

@keyframes waveActive2 {
  0%, 100% {
    stroke-width: 5;
  }
  50% {
    stroke-width: 6;
  }
}

@keyframes waveActive3 {
  0%, 100% {
    stroke-width: 4;
  }
  50% {
    stroke-width: 5;
  }
}

/* Particles rising like bubbles */
@keyframes particleRise {
  0% {
    transform: translateY(20px) scale(0);
    opacity: 0;
  }
  20% {
    opacity: 1;
  }
  80% {
    opacity: 0.8;
  }
  100% {
    transform: translateY(-30px) scale(0.5);
    opacity: 0;
  }
}

/* Status dot pulse */
@keyframes pulse {
  0%, 100% {
    transform: scale(1);
    box-shadow: 0 0 8px rgba(34, 197, 94, 0.6);
  }
  50% {
    transform: scale(1.15);
    box-shadow: 0 0 16px rgba(34, 197, 94, 0.8);
  }
}
</style>
