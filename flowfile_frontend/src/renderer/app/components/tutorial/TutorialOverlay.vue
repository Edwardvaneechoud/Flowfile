<script setup lang="ts">
import { ref, computed, watch, onMounted, onUnmounted, nextTick } from "vue";
import { useTutorialStore } from "../../stores/tutorial-store";
import { useFlowStore } from "../../stores/column-store";
import TutorialTooltip from "./TutorialTooltip.vue";

const tutorialStore = useTutorialStore();
const nodeStore = useFlowStore();

const targetRect = ref<DOMRect | null>(null);
const tooltipPosition = ref({ x: 0, y: 0 });
const previousFlowId = ref<number | null>(null);
const previousNodeCount = ref<number>(0);

// Compute spotlight position and size
const spotlightStyle = computed(() => {
  if (!targetRect.value || !tutorialStore.currentStep?.target) {
    return null;
  }

  const padding = tutorialStore.currentStep.highlightPadding ?? 8;
  const rect = targetRect.value;

  return {
    left: `${rect.left - padding}px`,
    top: `${rect.top - padding}px`,
    width: `${rect.width + padding * 2}px`,
    height: `${rect.height + padding * 2}px`,
    borderRadius: tutorialStore.currentStep.spotlightShape === "circle" ? "50%" : "8px",
  };
});

// Check if we should show center mode (no target element)
const isCenterMode = computed(() => {
  return !tutorialStore.currentStep?.target;
});

// Calculate tooltip position based on target and preferred position
function calculateTooltipPosition() {
  if (!tutorialStore.currentStep) return;

  const position = tutorialStore.currentStep.position || "bottom";

  if (isCenterMode.value || position === "center") {
    // Center the tooltip on screen
    tooltipPosition.value = {
      x: window.innerWidth / 2,
      y: window.innerHeight / 2,
    };
    return;
  }

  if (!targetRect.value) return;

  const rect = targetRect.value;
  const padding = 16;

  switch (position) {
    case "top":
      tooltipPosition.value = {
        x: rect.left + rect.width / 2,
        y: rect.top - padding,
      };
      break;
    case "bottom":
      tooltipPosition.value = {
        x: rect.left + rect.width / 2,
        y: rect.bottom + padding,
      };
      break;
    case "left":
      tooltipPosition.value = {
        x: rect.left - padding,
        y: rect.top + rect.height / 2,
      };
      break;
    case "right":
      tooltipPosition.value = {
        x: rect.right + padding,
        y: rect.top + rect.height / 2,
      };
      break;
  }
}

// Update target element position
function updateTargetPosition() {
  if (!tutorialStore.currentStep?.target) {
    targetRect.value = null;
    calculateTooltipPosition();
    return;
  }

  const targetElement = document.querySelector(tutorialStore.currentStep.target);
  if (targetElement) {
    targetRect.value = targetElement.getBoundingClientRect();
    calculateTooltipPosition();
  } else {
    targetRect.value = null;
  }
}

// Watch for step changes and wait for elements if needed
watch(
  () => tutorialStore.currentStep,
  async (newStep) => {
    if (!newStep) return;

    // Start/stop modal polling based on current step
    if (newStep.id === "click-quick-create") {
      startModalPolling();
    } else {
      stopModalPolling();
    }

    // If we need to wait for an element, poll for it
    if (newStep.waitForElement) {
      const maxAttempts = 50;
      let attempts = 0;

      while (attempts < maxAttempts) {
        const element = document.querySelector(newStep.waitForElement);
        if (element) break;
        await new Promise((resolve) => setTimeout(resolve, 100));
        attempts++;
      }
    }

    await nextTick();
    updateTargetPosition();
  },
  { immediate: true }
);

// Auto-advance when flow is created
watch(
  () => nodeStore.flowId,
  (newFlowId, oldFlowId) => {
    if (!tutorialStore.isActive) return;

    const currentStepId = tutorialStore.currentStep?.id;

    // If we're on the "confirm-create-flow" step and a flow was just created, advance
    if (currentStepId === "confirm-create-flow" && newFlowId && newFlowId > 0 && (!oldFlowId || oldFlowId <= 0)) {
      // Small delay to let the UI update
      setTimeout(() => {
        tutorialStore.nextStep();
      }, 500);
    }
  }
);

// Track modal polling state
let modalPollInterval: ReturnType<typeof setInterval> | null = null;
const modalWasInitiallyVisible = ref(false);

// Start polling for Quick Create modal when on the correct step
function startModalPolling() {
  if (modalPollInterval) return;

  // Check if modal is already visible when we start - if so, don't auto-advance
  const createFlowBtn = document.querySelector("[data-tutorial='create-flow-confirm-btn']");
  modalWasInitiallyVisible.value = createFlowBtn !== null;

  // If modal is already open, don't poll (user needs to close it first or click Next)
  if (modalWasInitiallyVisible.value) {
    return;
  }

  modalPollInterval = setInterval(() => {
    if (!tutorialStore.isActive) {
      stopModalPolling();
      return;
    }

    const currentStepId = tutorialStore.currentStep?.id;
    if (currentStepId !== "click-quick-create") {
      stopModalPolling();
      return;
    }

    // Check if the modal's Create Flow button is visible
    const createFlowBtn = document.querySelector("[data-tutorial='create-flow-confirm-btn']");
    if (createFlowBtn) {
      stopModalPolling();
      setTimeout(() => {
        tutorialStore.nextStep();
      }, 200);
    }
  }, 100);
}

function stopModalPolling() {
  if (modalPollInterval) {
    clearInterval(modalPollInterval);
    modalPollInterval = null;
  }
  modalWasInitiallyVisible.value = false;
}

// Check if we should start/stop modal polling based on current step
function checkForModalOpen() {
  if (!tutorialStore.isActive) return;

  const currentStepId = tutorialStore.currentStep?.id;
  if (currentStepId === "click-quick-create") {
    startModalPolling();
  } else {
    stopModalPolling();
  }
}

// Track if node settings was previously visible
const nodeSettingsWasVisible = ref(false);

// Auto-advance when node settings panel appears (for configure-manual-input step)
function checkForNodeSettings() {
  if (!tutorialStore.isActive) return;

  const currentStepId = tutorialStore.currentStep?.id;
  const nodeSettings = document.querySelector("#nodeSettings");
  const nodeSettingsIsVisible = nodeSettings !== null;

  // If we're on "configure-manual-input" step and nodeSettings just appeared, advance
  if (currentStepId === "configure-manual-input" && nodeSettingsIsVisible && !nodeSettingsWasVisible.value) {
    setTimeout(() => {
      tutorialStore.nextStep();
    }, 300);
  }

  nodeSettingsWasVisible.value = nodeSettingsIsVisible;
}

// Auto-advance when a node is added to the canvas
function checkForNewNodes() {
  if (!tutorialStore.isActive) return;

  const currentStepId = tutorialStore.currentStep?.id;
  const nodes = document.querySelectorAll(".vue-flow__node");
  const currentNodeCount = nodes.length;

  // If we're on a "drag" step and a new node was added, advance
  if (currentStepId === "drag-manual-input" && currentNodeCount > previousNodeCount.value) {
    setTimeout(() => {
      tutorialStore.nextStep();
    }, 300);
  }

  // Track for "drag-group-by" step
  if (currentStepId === "drag-group-by" && currentNodeCount > previousNodeCount.value) {
    setTimeout(() => {
      tutorialStore.nextStep();
    }, 300);
  }

  // Track for "drag-write-data" step
  if (currentStepId === "drag-write-data" && currentNodeCount > previousNodeCount.value) {
    setTimeout(() => {
      tutorialStore.nextStep();
    }, 300);
  }

  previousNodeCount.value = currentNodeCount;
}

// Handle window resize
function handleResize() {
  updateTargetPosition();
}

// Handle scroll
function handleScroll() {
  updateTargetPosition();
}

// Mutation observer to detect DOM changes
let mutationObserver: MutationObserver | null = null;

function setupMutationObserver() {
  mutationObserver = new MutationObserver(() => {
    updateTargetPosition();
    checkForModalOpen();
    checkForNodeSettings();
    checkForNewNodes();
  });

  mutationObserver.observe(document.body, {
    childList: true,
    subtree: true,
    attributes: true,
    attributeFilter: ["class", "style"],
  });
}

onMounted(() => {
  window.addEventListener("resize", handleResize);
  window.addEventListener("scroll", handleScroll, true);
  setupMutationObserver();
  updateTargetPosition();

  // Initialize node count
  previousNodeCount.value = document.querySelectorAll(".vue-flow__node").length;
});

onUnmounted(() => {
  window.removeEventListener("resize", handleResize);
  window.removeEventListener("scroll", handleScroll, true);
  if (mutationObserver) {
    mutationObserver.disconnect();
  }
  stopModalPolling();
});
</script>

<template>
  <Teleport to="body">
    <Transition name="tutorial-fade">
      <div
        v-if="tutorialStore.isActive && !tutorialStore.tutorialPaused"
        class="tutorial-overlay"
      >
        <!-- Backdrop with spotlight cutout - pointer-events: none allows clicks through -->
        <div class="tutorial-backdrop">
          <!-- SVG mask for spotlight effect -->
          <svg class="tutorial-mask" width="100%" height="100%">
            <defs>
              <mask id="spotlight-mask">
                <rect width="100%" height="100%" fill="white" />
                <rect
                  v-if="spotlightStyle"
                  :x="parseInt(spotlightStyle.left)"
                  :y="parseInt(spotlightStyle.top)"
                  :width="parseInt(spotlightStyle.width)"
                  :height="parseInt(spotlightStyle.height)"
                  :rx="spotlightStyle.borderRadius === '50%' ? parseInt(spotlightStyle.width) / 2 : 8"
                  fill="black"
                />
              </mask>
            </defs>
            <rect width="100%" height="100%" fill="rgba(0, 0, 0, 0.7)" mask="url(#spotlight-mask)" />
          </svg>
        </div>

        <!-- Spotlight border/highlight - visual only, no pointer events -->
        <div v-if="spotlightStyle" class="tutorial-spotlight" :style="spotlightStyle">
          <div class="spotlight-border"></div>
        </div>

        <!-- Tooltip - this is the only element that captures pointer events -->
        <TutorialTooltip
          v-if="tutorialStore.currentStep"
          :step="tutorialStore.currentStep"
          :position="tooltipPosition"
          :is-center-mode="isCenterMode"
          :current-step-index="tutorialStore.currentStepIndex"
          :total-steps="tutorialStore.totalSteps"
          :progress="tutorialStore.progress"
          @next="tutorialStore.nextStep"
          @prev="tutorialStore.prevStep"
          @skip="tutorialStore.endTutorial"
          @complete="tutorialStore.completeTutorial"
        />
      </div>
    </Transition>
  </Teleport>
</template>

<style scoped>
.tutorial-overlay {
  position: fixed;
  top: 0;
  left: 0;
  width: 100vw;
  height: 100vh;
  z-index: 10000;
  /* Allow clicks to pass through to the page */
  pointer-events: none;
}

.tutorial-backdrop {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  pointer-events: none;
}

.tutorial-mask {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
}

.tutorial-spotlight {
  position: fixed;
  pointer-events: none;
  z-index: 10001;
  transition: all 0.3s ease-out;
}

.spotlight-border {
  position: absolute;
  top: -2px;
  left: -2px;
  right: -2px;
  bottom: -2px;
  border: 2px solid var(--color-accent);
  border-radius: inherit;
  box-shadow:
    0 0 0 4px color-mix(in srgb, var(--color-accent) 30%, transparent),
    0 0 20px color-mix(in srgb, var(--color-accent) 40%, transparent);
  animation: spotlight-pulse 2s ease-in-out infinite;
}

@keyframes spotlight-pulse {
  0%,
  100% {
    box-shadow:
      0 0 0 4px color-mix(in srgb, var(--color-accent) 30%, transparent),
      0 0 20px color-mix(in srgb, var(--color-accent) 40%, transparent);
  }
  50% {
    box-shadow:
      0 0 0 6px color-mix(in srgb, var(--color-accent) 40%, transparent),
      0 0 30px color-mix(in srgb, var(--color-accent) 50%, transparent);
  }
}

/* Transition animations */
.tutorial-fade-enter-active,
.tutorial-fade-leave-active {
  transition: opacity 0.3s ease;
}

.tutorial-fade-enter-from,
.tutorial-fade-leave-to {
  opacity: 0;
}
</style>
