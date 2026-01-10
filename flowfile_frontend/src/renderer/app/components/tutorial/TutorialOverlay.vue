<script setup lang="ts">
import { ref, computed, watch, onMounted, onUnmounted, nextTick } from "vue";
import { useTutorialStore } from "../../stores/tutorial-store";
import TutorialTooltip from "./TutorialTooltip.vue";

const tutorialStore = useTutorialStore();

const targetRect = ref<DOMRect | null>(null);
const tooltipPosition = ref({ x: 0, y: 0 });

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
