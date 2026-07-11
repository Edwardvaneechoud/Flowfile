<script setup lang="ts">
import { ref, computed, watch, onUnmounted } from "vue";
import { useRoute, useRouter } from "vue-router";

import { useTutorialStore } from "../../stores/tutorial-store";
import { requiresMet, resolveSelector, resolveTarget } from "./tutorial-engine";
import { useTutorialBridge } from "./useTutorialBridge";
import TutorialTooltip from "./TutorialTooltip.vue";

const tutorialStore = useTutorialStore();
const route = useRoute();
const router = useRouter();

useTutorialBridge();

const isDesignerPage = computed(() => route.name === "designer");
const overlayVisible = computed(
  () => isDesignerPage.value && tutorialStore.isActive && !tutorialStore.tutorialPaused,
);
const showPausedPill = computed(() => tutorialStore.isActive && !isDesignerPage.value);

// Off the designer route the tour waits (context still accrues); returning
// re-evaluates so anything done meanwhile is absorbed.
watch(
  [isDesignerPage, () => tutorialStore.isActive],
  ([onDesigner, active]) => {
    if (!active) return;
    tutorialStore.setSuspended(!onDesigner);
    if (onDesigner) {
      tutorialStore.resyncFromApp();
      tutorialStore.reevaluateCurrentStep();
    }
  },
  { immediate: true },
);

const targetRect = ref<DOMRect | null>(null);
const secondaryRect = ref<DOMRect | null>(null);
const tooltipPosition = ref({ x: 0, y: 0 });

const currentStep = computed(() => tutorialStore.currentStep);

// Step needs context from an earlier step that hasn't happened: render it
// centered and free ("described mode") instead of pointing at nothing.
const prereqMissing = computed(() =>
  currentStep.value ? !requiresMet(currentStep.value, tutorialStore.context) : false,
);

const targetSelector = computed(() => {
  if (!currentStep.value || prereqMissing.value) return null;
  return resolveTarget(currentStep.value, tutorialStore.context);
});

const secondarySelector = computed(() => {
  if (!currentStep.value || prereqMissing.value) return null;
  return resolveSelector(currentStep.value.secondaryTarget, tutorialStore.context);
});

const interactionMode = computed(() => {
  if (prereqMissing.value) return "free";
  return currentStep.value?.interaction ?? "modal";
});

const veilEnabled = computed(() => {
  if (!currentStep.value || prereqMissing.value) return false;
  return currentStep.value.veil ?? interactionMode.value !== "free";
});

const isCenterMode = computed(
  () => !targetSelector.value || currentStep.value?.tooltipCorner === true,
);

const stepSatisfied = computed(() => tutorialStore.currentStepSatisfied);

function ringStyle(rect: DOMRect | null) {
  if (!rect || !currentStep.value) return null;
  const padding = currentStep.value.highlightPadding ?? 8;
  return {
    left: `${rect.left - padding}px`,
    top: `${rect.top - padding}px`,
    width: `${rect.width + padding * 2}px`,
    height: `${rect.height + padding * 2}px`,
    borderRadius: currentStep.value.spotlightShape === "circle" ? "50%" : "8px",
  };
}

const spotlightStyle = computed(() => (targetSelector.value ? ringStyle(targetRect.value) : null));

const secondarySpotlightStyle = computed(() =>
  secondarySelector.value ? ringStyle(secondaryRect.value) : null,
);

// Same numbers as the veil cutout so visuals and interactivity always agree.
const holeRect = computed(() => {
  if (!targetRect.value || !currentStep.value) return null;
  const padding = currentStep.value.highlightPadding ?? 8;
  const rect = targetRect.value;
  return {
    top: rect.top - padding,
    left: rect.left - padding,
    width: rect.width + padding * 2,
    height: rect.height + padding * 2,
  };
});

const holeEdges = computed(() => {
  if (!holeRect.value) return null;
  const hole = holeRect.value;
  return {
    top: Math.max(hole.top, 0),
    left: Math.max(hole.left, 0),
    right: hole.left + hole.width,
    bottom: hole.top + hole.height,
    height: hole.height,
  };
});

// Estimated tooltip footprint used for side-flipping (matches its CSS width).
const TOOLTIP_WIDTH = 380;
const TOOLTIP_HEIGHT = 300;

const effectivePosition = ref<"top" | "bottom" | "left" | "right" | "center">("bottom");

// Flip to the opposite side when the preferred side lacks room — a clamped
// tooltip would otherwise cover the very element the step asks to click.
function pickSide(rect: DOMRect): "top" | "bottom" | "left" | "right" | "center" {
  const preferred = currentStep.value?.position || "bottom";
  const margin = 60;
  switch (preferred) {
    case "left":
      return rect.left - TOOLTIP_WIDTH - margin < 0 ? "right" : "left";
    case "right":
      return rect.right + TOOLTIP_WIDTH + margin > window.innerWidth ? "left" : "right";
    case "top":
      return rect.top - TOOLTIP_HEIGHT - margin < 0 ? "bottom" : "top";
    case "bottom":
      return rect.bottom + TOOLTIP_HEIGHT + margin > window.innerHeight ? "top" : "bottom";
    default:
      return preferred;
  }
}

function calculateTooltipPosition() {
  if (!currentStep.value) return;

  if (isCenterMode.value || currentStep.value.position === "center" || !targetRect.value) {
    effectivePosition.value = "center";
    tooltipPosition.value = {
      x: window.innerWidth / 2,
      y: window.innerHeight / 2,
    };
    return;
  }

  const rect = targetRect.value;
  const padding = 16;
  const side = pickSide(rect);
  effectivePosition.value = side;

  switch (side) {
    case "top":
      tooltipPosition.value = { x: rect.left + rect.width / 2, y: rect.top - padding };
      break;
    case "bottom":
      tooltipPosition.value = { x: rect.left + rect.width / 2, y: rect.bottom + padding };
      break;
    case "left":
      tooltipPosition.value = {
        x: rect.left - padding - 30,
        y: rect.top + rect.height / 2,
      };
      break;
    case "right":
      tooltipPosition.value = { x: rect.right + padding, y: rect.top + rect.height / 2 };
      break;
  }
}

function rectsAlmostEqual(a: DOMRect | null, b: DOMRect | null): boolean {
  if (a === null || b === null) return a === b;
  return (
    Math.abs(a.left - b.left) < 0.5 &&
    Math.abs(a.top - b.top) < 0.5 &&
    Math.abs(a.width - b.width) < 0.5 &&
    Math.abs(a.height - b.height) < 0.5
  );
}

// rAF loop follows canvas pans and node drags smoothly; a coarse interval
// backs it up where rAF is throttled (occluded/headless windows), and step
// changes update immediately so the spotlight never shows a stale target.
let rafId = 0;
let intervalId = 0;
let resyncIntervalId = 0;
let cachedElement: Element | null = null;
let cachedSelector: string | null = null;
let cachedSecondaryElement: Element | null = null;
let cachedSecondarySelector: string | null = null;

function setRect(rect: DOMRect | null) {
  if (rectsAlmostEqual(rect, targetRect.value)) return;
  targetRect.value = rect;
  calculateTooltipPosition();
}

function updateNow() {
  const selector = targetSelector.value;
  if (!selector) {
    cachedElement = null;
    cachedSelector = null;
    setRect(null);
  } else {
    if (selector !== cachedSelector || !cachedElement?.isConnected) {
      cachedElement = document.querySelector(selector);
      cachedSelector = selector;
    }
    setRect(cachedElement ? cachedElement.getBoundingClientRect() : null);
  }

  const secondary = secondarySelector.value;
  if (!secondary) {
    cachedSecondaryElement = null;
    cachedSecondarySelector = null;
    if (secondaryRect.value !== null) secondaryRect.value = null;
    return;
  }
  if (secondary !== cachedSecondarySelector || !cachedSecondaryElement?.isConnected) {
    cachedSecondaryElement = document.querySelector(secondary);
    cachedSecondarySelector = secondary;
  }
  const rect = cachedSecondaryElement ? cachedSecondaryElement.getBoundingClientRect() : null;
  if (!rectsAlmostEqual(rect, secondaryRect.value)) secondaryRect.value = rect;
}

function tick() {
  updateNow();
  rafId = requestAnimationFrame(tick);
}

function startTracking() {
  if (rafId) return;
  rafId = requestAnimationFrame(tick);
  intervalId = window.setInterval(updateNow, 250);
  // Catches canvas mutations that bypass notify() (undo, paste, edge delete,
  // flow reload) — see tutorialStore.resyncFromApp.
  resyncIntervalId = window.setInterval(() => tutorialStore.resyncFromApp(), 1000);
}

function stopTracking() {
  if (rafId) cancelAnimationFrame(rafId);
  if (intervalId) window.clearInterval(intervalId);
  if (resyncIntervalId) window.clearInterval(resyncIntervalId);
  rafId = 0;
  intervalId = 0;
  resyncIntervalId = 0;
  cachedElement = null;
  cachedSelector = null;
  cachedSecondaryElement = null;
  cachedSecondarySelector = null;
}

watch(
  overlayVisible,
  (visible) => {
    if (visible) {
      calculateTooltipPosition();
      startTracking();
    } else {
      stopTracking();
      targetRect.value = null;
    }
  },
  { immediate: true },
);

watch(targetSelector, () => updateNow());

onUnmounted(stopTracking);

function swallow(event: Event) {
  event.preventDefault();
  event.stopPropagation();
}

function resumeTutorial() {
  router.push({ name: "designer" });
}
</script>

<template>
  <Teleport to="body">
    <!-- No leave transition: a click-blocking layer must vanish with v-if
         immediately (transitionend never fires in throttled/hidden windows,
         which would leave an invisible blocker behind). -->
    <div v-if="overlayVisible" class="tutorial-overlay">
      <!-- Dark veil with spotlight cutout -->
      <div v-if="veilEnabled && spotlightStyle" class="tutorial-backdrop">
        <svg class="tutorial-mask" width="100%" height="100%">
          <defs>
            <mask id="spotlight-mask">
              <rect width="100%" height="100%" fill="white" />
              <rect
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
      <div v-else-if="veilEnabled" class="tutorial-backdrop tutorial-backdrop-full"></div>

      <!-- Interaction blockers. modal: everything; spotlight: everything but
             the hole. A spotlight step whose target is missing renders no
             blockers so the user is never trapped. -->
      <div
        v-if="interactionMode === 'modal'"
        class="tutorial-blocker tutorial-blocker--full"
        @mousedown="swallow"
        @click="swallow"
        @dblclick="swallow"
        @contextmenu="swallow"
        @wheel="swallow"
      ></div>
      <template v-else-if="interactionMode === 'spotlight' && holeEdges">
        <div
          class="tutorial-blocker"
          :style="{ top: 0, left: 0, right: 0, height: `${holeEdges.top}px` }"
          @mousedown="swallow"
          @click="swallow"
          @contextmenu="swallow"
        ></div>
        <div
          class="tutorial-blocker"
          :style="{
            top: `${holeEdges.top}px`,
            left: 0,
            width: `${holeEdges.left}px`,
            height: `${holeEdges.height}px`,
          }"
          @mousedown="swallow"
          @click="swallow"
          @contextmenu="swallow"
        ></div>
        <div
          class="tutorial-blocker"
          :style="{
            top: `${holeEdges.top}px`,
            left: `${holeEdges.right}px`,
            right: 0,
            height: `${holeEdges.height}px`,
          }"
          @mousedown="swallow"
          @click="swallow"
          @contextmenu="swallow"
        ></div>
        <div
          class="tutorial-blocker"
          :style="{ top: `${holeEdges.bottom}px`, left: 0, right: 0, bottom: 0 }"
          @mousedown="swallow"
          @click="swallow"
          @contextmenu="swallow"
        ></div>
      </template>

      <!-- Spotlight border - visual only -->
      <div v-if="spotlightStyle" class="tutorial-spotlight" :style="spotlightStyle">
        <div class="spotlight-border"></div>
      </div>
      <div
        v-if="secondarySpotlightStyle"
        class="tutorial-spotlight"
        :style="secondarySpotlightStyle"
      >
        <div class="spotlight-border"></div>
      </div>

      <TutorialTooltip
        v-if="currentStep"
        :step="currentStep"
        :position="tooltipPosition"
        :placement="effectivePosition"
        :is-center-mode="isCenterMode"
        :current-step-index="tutorialStore.currentStepIndex"
        :total-steps="tutorialStore.totalSteps"
        :progress="tutorialStore.progress"
        :satisfied="stepSatisfied"
        :prereq-missing="prereqMissing"
        :wrong-action-hint="tutorialStore.wrongActionHint"
        @next="tutorialStore.nextStep"
        @prev="tutorialStore.prevStep"
        @skip="tutorialStore.endTutorial"
        @complete="tutorialStore.completeTutorial"
      />
    </div>

    <div v-if="showPausedPill" class="tutorial-paused-pill">
      <span class="material-icons pill-icon">school</span>
      <span class="pill-text">Tutorial paused</span>
      <button class="pill-btn pill-btn--primary" @click="resumeTutorial">Resume</button>
      <button class="pill-btn" @click="tutorialStore.endTutorial">Exit</button>
    </div>
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
  /* Children opt in to pointer events individually */
  pointer-events: none;
  animation: tutorial-appear 0.3s ease;
}

@keyframes tutorial-appear {
  from {
    opacity: 0;
  }
  to {
    opacity: 1;
  }
}

.tutorial-backdrop {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  pointer-events: none;
}

.tutorial-backdrop-full {
  background: rgba(0, 0, 0, 0.7);
}

.tutorial-mask {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
}

.tutorial-blocker {
  position: fixed;
  pointer-events: auto;
  z-index: 10001;
}

.tutorial-blocker--full {
  inset: 0;
}

.tutorial-spotlight {
  position: fixed;
  pointer-events: none;
  z-index: 10002;
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

.tutorial-paused-pill {
  position: fixed;
  bottom: 24px;
  right: 24px;
  z-index: 10000;
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 14px;
  background: var(--color-background-primary, #fff);
  border: 1px solid var(--color-border-primary, #ddd);
  border-radius: 999px;
  box-shadow: 0 8px 20px rgba(0, 0, 0, 0.18);
  pointer-events: auto;
  animation: tutorial-appear 0.3s ease;
}

.pill-icon {
  font-size: 18px;
  color: var(--color-accent, #3b82f6);
}

.pill-text {
  font-size: 13px;
  font-weight: 500;
  color: var(--color-text-primary, #1a1a1a);
}

.pill-btn {
  padding: 5px 12px;
  border: 1px solid var(--color-border-primary, #ddd);
  border-radius: 999px;
  background: var(--color-background-primary, #fff);
  color: var(--color-text-primary, #1a1a1a);
  font-size: 12px;
  font-weight: 500;
  cursor: pointer;
}

.pill-btn:hover {
  background: var(--color-background-tertiary, #eee);
}

.pill-btn--primary {
  background: var(--color-accent, #3b82f6);
  border-color: var(--color-accent, #3b82f6);
  color: #fff;
}

.pill-btn--primary:hover {
  background: var(--color-accent-hover, #2563eb);
}
</style>
