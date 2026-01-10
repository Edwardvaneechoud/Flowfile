<script setup lang="ts">
import { ref, computed, watch, onMounted, onUnmounted, nextTick } from "vue";
import { useTutorialStore } from "../../stores/tutorial-store";
import TutorialTooltip from "./TutorialTooltip.vue";

const tutorialStore = useTutorialStore();

const targetRect = ref<DOMRect | null>(null);
const tooltipPosition = ref({ x: 0, y: 0 });
const overlayRef = ref<HTMLElement | null>(null);

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

// Handle window resize
function handleResize() {
  updateTargetPosition();
}

// Handle scroll
function handleScroll() {
  updateTargetPosition();
}

// Handle click on overlay (to detect clicks on highlighted element)
function handleOverlayClick(event: MouseEvent) {
  if (!tutorialStore.currentStep?.target) return;

  const targetElement = document.querySelector(tutorialStore.currentStep.target) as HTMLElement;
  if (!targetElement) return;

  const rect = targetElement.getBoundingClientRect();
  const padding = tutorialStore.currentStep.highlightPadding ?? 8;

  // Check if click is within the spotlight area
  const isWithinSpotlight =
    event.clientX >= rect.left - padding &&
    event.clientX <= rect.right + padding &&
    event.clientY >= rect.top - padding &&
    event.clientY <= rect.bottom + padding;

  if (isWithinSpotlight) {
    // Let the click through to the actual element
    const elementAtPoint = document.elementFromPoint(event.clientX, event.clientY);

    // Temporarily hide overlay to get the actual element
    if (overlayRef.value) {
      overlayRef.value.style.pointerEvents = "none";
      const actualElement = document.elementFromPoint(event.clientX, event.clientY);
      overlayRef.value.style.pointerEvents = "";

      if (actualElement) {
        // Create and dispatch a click event
        const clickEvent = new MouseEvent("click", {
          bubbles: true,
          cancelable: true,
          view: window,
          clientX: event.clientX,
          clientY: event.clientY,
        });
        actualElement.dispatchEvent(clickEvent);

        // If this step expects a click action, complete it
        if (tutorialStore.currentStep?.action === "click") {
          tutorialStore.onActionCompleted();
        }
      }
    }
  }
}

// Mutation observer to detect DOM changes
let mutationObserver: MutationObserver | null = null;

function setupMutationObserver() {
  mutationObserver = new MutationObserver(() => {
    updateTargetPosition();
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
});

onUnmounted(() => {
  window.removeEventListener("resize", handleResize);
  window.removeEventListener("scroll", handleScroll, true);
  if (mutationObserver) {
    mutationObserver.disconnect();
  }
});
</script>

<template>
  <Teleport to="body">
    <Transition name="tutorial-fade">
      <div
        v-if="tutorialStore.isActive && !tutorialStore.tutorialPaused"
        ref="overlayRef"
        class="tutorial-overlay"
        @click="handleOverlayClick"
      >
        <!-- Backdrop with spotlight cutout -->
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

        <!-- Spotlight border/highlight -->
        <div v-if="spotlightStyle" class="tutorial-spotlight" :style="spotlightStyle">
          <div class="spotlight-border"></div>
        </div>

        <!-- Tooltip -->
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
  pointer-events: auto;
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
  border: 2px solid var(--color-accent, #3b82f6);
  border-radius: inherit;
  box-shadow:
    0 0 0 4px rgba(59, 130, 246, 0.3),
    0 0 20px rgba(59, 130, 246, 0.4);
  animation: spotlight-pulse 2s ease-in-out infinite;
}

@keyframes spotlight-pulse {
  0%,
  100% {
    box-shadow:
      0 0 0 4px rgba(59, 130, 246, 0.3),
      0 0 20px rgba(59, 130, 246, 0.4);
  }
  50% {
    box-shadow:
      0 0 0 6px rgba(59, 130, 246, 0.4),
      0 0 30px rgba(59, 130, 246, 0.5);
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
