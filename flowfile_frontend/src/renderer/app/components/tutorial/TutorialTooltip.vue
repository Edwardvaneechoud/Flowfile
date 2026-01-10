<script setup lang="ts">
import { computed, ref, onMounted, onUnmounted, watch } from "vue";
import type { TutorialStep } from "../../stores/tutorial-store";

const props = defineProps<{
  step: TutorialStep;
  position: { x: number; y: number };
  isCenterMode: boolean;
  currentStepIndex: number;
  totalSteps: number;
  progress: number;
}>();

const emit = defineEmits<{
  (e: "next"): void;
  (e: "prev"): void;
  (e: "skip"): void;
  (e: "complete"): void;
}>();

const tooltipRef = ref<HTMLElement | null>(null);
const tooltipRect = ref<DOMRect | null>(null);

// Update tooltip rect after mount and on changes
function updateTooltipRect() {
  if (tooltipRef.value) {
    tooltipRect.value = tooltipRef.value.getBoundingClientRect();
  }
}

watch(() => props.step, () => {
  setTimeout(updateTooltipRect, 50);
}, { immediate: true });

onMounted(() => {
  setTimeout(updateTooltipRect, 50);
  window.addEventListener("resize", updateTooltipRect);
});

onUnmounted(() => {
  window.removeEventListener("resize", updateTooltipRect);
});

// Calculate final tooltip position with viewport bounds checking
const tooltipStyle = computed(() => {
  if (props.isCenterMode) {
    // Check if this step should be centered in screen
    if (props.step.centerInScreen) {
      return {
        position: "fixed" as const,
        left: "50%",
        top: "50%",
        right: "auto",
        bottom: "auto",
        transform: "translate(-50%, -50%)",
      };
    }
    // Position in bottom-right corner when no target is highlighted
    return {
      position: "fixed" as const,
      right: "24px",
      bottom: "24px",
      left: "auto",
      top: "auto",
      transform: "none",
    };
  }

  const tooltipWidth = tooltipRect.value?.width || 350;
  const tooltipHeight = tooltipRect.value?.height || 200;
  const padding = 20;
  const position = props.step.position || "bottom";

  let x = props.position.x;
  let y = props.position.y;
  let transform = "";

  switch (position) {
    case "top":
      x = props.position.x;
      y = props.position.y;
      transform = "translate(-50%, -100%)";
      break;
    case "bottom":
      x = props.position.x;
      y = props.position.y;
      transform = "translate(-50%, 0)";
      break;
    case "left":
      x = props.position.x;
      y = props.position.y;
      transform = "translate(-100%, -50%)";
      break;
    case "right":
      x = props.position.x;
      y = props.position.y;
      transform = "translate(0, -50%)";
      break;
  }

  // Bounds checking
  let finalX = x;
  let finalY = y;

  // After transform, check bounds
  let effectiveX = x;
  let effectiveY = y;

  if (transform.includes("-50%, 0")) {
    effectiveX = x - tooltipWidth / 2;
  } else if (transform.includes("-50%, -100%)")) {
    effectiveX = x - tooltipWidth / 2;
    effectiveY = y - tooltipHeight;
  } else if (transform.includes("-100%, -50%")) {
    effectiveX = x - tooltipWidth;
    effectiveY = y - tooltipHeight / 2;
  } else if (transform.includes("0, -50%")) {
    effectiveY = y - tooltipHeight / 2;
  }

  // Adjust if out of bounds
  if (effectiveX < padding) {
    finalX = x + (padding - effectiveX);
  }
  if (effectiveX + tooltipWidth > window.innerWidth - padding) {
    finalX = x - (effectiveX + tooltipWidth - window.innerWidth + padding);
  }
  if (effectiveY < padding) {
    finalY = y + (padding - effectiveY);
  }
  if (effectiveY + tooltipHeight > window.innerHeight - padding) {
    finalY = y - (effectiveY + tooltipHeight - window.innerHeight + padding);
  }

  return {
    position: "fixed" as const,
    left: `${finalX}px`,
    top: `${finalY}px`,
    transform,
  };
});

const isLastStep = computed(() => props.currentStepIndex === props.totalSteps - 1);
const showNextButton = computed(() => props.step.showNextButton !== false);
const showPrevButton = computed(() => props.step.showPrevButton !== false && props.currentStepIndex > 0);
const canSkip = computed(() => props.step.canSkip !== false);

// Action hint text based on step action
const actionHint = computed(() => {
  switch (props.step.action) {
    case "click":
      return "Click the highlighted element to continue";
    case "drag":
      return "Drag the highlighted element to the target area";
    case "input":
      return "Enter the required information";
    case "wait":
      return "Please wait...";
    default:
      return null;
  }
});

function handleNext() {
  if (isLastStep.value) {
    emit("complete");
  } else {
    emit("next");
  }
}
</script>

<template>
  <div ref="tooltipRef" class="tutorial-tooltip" :style="tooltipStyle">
    <!-- Progress bar -->
    <div class="tooltip-progress">
      <div class="progress-bar">
        <div class="progress-fill" :style="{ width: `${progress}%` }"></div>
      </div>
      <span class="progress-text">{{ currentStepIndex + 1 }} / {{ totalSteps }}</span>
    </div>

    <!-- Content -->
    <div class="tooltip-content">
      <h3 class="tooltip-title">{{ step.title }}</h3>
      <p class="tooltip-description" v-html="step.content"></p>

      <!-- Action hint -->
      <p v-if="actionHint && step.action !== 'observe'" class="tooltip-action-hint">
        <span class="action-icon material-icons">touch_app</span>
        {{ actionHint }}
      </p>
    </div>

    <!-- Navigation -->
    <div class="tooltip-navigation">
      <button v-if="canSkip" class="nav-btn skip-btn" @click="emit('skip')">
        Skip Tutorial
      </button>

      <div class="nav-main">
        <button v-if="showPrevButton" class="nav-btn prev-btn" @click="emit('prev')">
          <span class="material-icons">arrow_back</span>
          Back
        </button>

        <button v-if="showNextButton" class="nav-btn next-btn" @click="handleNext">
          {{ isLastStep ? "Finish" : "Next" }}
          <span v-if="!isLastStep" class="material-icons">arrow_forward</span>
          <span v-else class="material-icons">check</span>
        </button>
      </div>
    </div>
  </div>
</template>

<style scoped>
.tutorial-tooltip {
  width: 380px;
  max-width: calc(100vw - 40px);
  background: var(--color-background-primary, #ffffff);
  border-radius: 12px;
  box-shadow:
    0 20px 40px rgba(0, 0, 0, 0.2),
    0 0 0 1px rgba(0, 0, 0, 0.05);
  z-index: 100000;
  overflow: hidden;
  animation: tooltip-appear 0.3s ease-out;
  pointer-events: auto;
}

@keyframes tooltip-appear {
  from {
    opacity: 0;
  }
  to {
    opacity: 1;
  }
}

.tooltip-progress {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 12px 16px;
  background: var(--color-background-muted, #f5f5f5);
  border-bottom: 1px solid var(--color-border-light, #e0e0e0);
}

.progress-bar {
  flex: 1;
  height: 4px;
  background: var(--color-border-light, #e0e0e0);
  border-radius: 2px;
  overflow: hidden;
}

.progress-fill {
  height: 100%;
  background: var(--color-accent, #3b82f6);
  border-radius: 2px;
  transition: width 0.3s ease;
}

.progress-text {
  font-size: 12px;
  font-weight: 500;
  color: var(--color-text-secondary, #666);
  white-space: nowrap;
}

.tooltip-content {
  padding: 20px;
}

.tooltip-title {
  margin: 0 0 12px;
  font-size: 18px;
  font-weight: 600;
  color: var(--color-text-primary, #1a1a1a);
  line-height: 1.3;
}

.tooltip-description {
  margin: 0;
  font-size: 14px;
  line-height: 1.6;
  color: var(--color-text-secondary, #666);
}

.tooltip-description :deep(strong) {
  color: var(--color-text-primary, #1a1a1a);
  font-weight: 600;
}

.tooltip-description :deep(code) {
  background: var(--color-background-muted, #f5f5f5);
  padding: 2px 6px;
  border-radius: 4px;
  font-family: var(--font-family-mono, monospace);
  font-size: 13px;
}

.tooltip-action-hint {
  display: flex;
  align-items: center;
  gap: 8px;
  margin: 16px 0 0;
  padding: 10px 12px;
  background: var(--color-accent-subtle, rgba(59, 130, 246, 0.1));
  border-radius: 8px;
  font-size: 13px;
  color: var(--color-accent, #3b82f6);
  font-weight: 500;
}

.action-icon {
  font-size: 18px;
}

.tooltip-navigation {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 16px;
  background: var(--color-background-muted, #f5f5f5);
  border-top: 1px solid var(--color-border-light, #e0e0e0);
}

.nav-btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 8px 16px;
  border: none;
  border-radius: 8px;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s ease;
}

.skip-btn {
  background: transparent;
  color: var(--color-text-secondary, #666);
}

.skip-btn:hover {
  background: var(--color-background-tertiary, #eee);
  color: var(--color-text-primary, #1a1a1a);
}

.nav-main {
  display: flex;
  gap: 8px;
}

.prev-btn {
  background: var(--color-background-primary, #fff);
  color: var(--color-text-primary, #1a1a1a);
  border: 1px solid var(--color-border-primary, #ddd);
}

.prev-btn:hover {
  background: var(--color-background-tertiary, #eee);
}

.next-btn {
  background: var(--color-accent, #3b82f6);
  color: white;
}

.next-btn:hover {
  background: var(--color-accent-hover, #2563eb);
}

.nav-btn .material-icons {
  font-size: 18px;
}
</style>
