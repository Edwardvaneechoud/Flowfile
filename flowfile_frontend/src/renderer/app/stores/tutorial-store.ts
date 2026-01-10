// Tutorial Store - Manages interactive tutorial state and progress
import { defineStore } from "pinia";
import { ref, computed } from "vue";

export interface TutorialStep {
  id: string;
  title: string;
  content: string;
  target?: string; // CSS selector for the element to highlight
  position?: "top" | "bottom" | "left" | "right" | "center";
  action?: "click" | "drag" | "input" | "wait" | "observe";
  actionTarget?: string; // CSS selector for the action target (e.g., where to drag to)
  actionData?: any; // Additional data for the action (e.g., node type to drag)
  waitForElement?: string; // Wait for this element to appear before showing step
  waitForEvent?: string; // Wait for this event before auto-advancing
  canSkip?: boolean;
  showNextButton?: boolean;
  showPrevButton?: boolean;
  onEnter?: () => void | Promise<void>;
  onExit?: () => void | Promise<void>;
  highlightPadding?: number;
  spotlightShape?: "rectangle" | "circle";
  centerInScreen?: boolean; // Show tooltip centered in screen instead of bottom-right corner
}

export interface Tutorial {
  id: string;
  name: string;
  description: string;
  steps: TutorialStep[];
}

export const useTutorialStore = defineStore("tutorial", () => {
  // State
  const isActive = ref(false);
  const currentTutorial = ref<Tutorial | null>(null);
  const currentStepIndex = ref(0);
  const isTransitioning = ref(false);
  const completedTutorials = ref<Set<string>>(new Set());
  const tutorialPaused = ref(false);

  // Computed
  const currentStep = computed<TutorialStep | null>(() => {
    if (!currentTutorial.value || currentStepIndex.value < 0) return null;
    return currentTutorial.value.steps[currentStepIndex.value] || null;
  });

  const totalSteps = computed(() => currentTutorial.value?.steps.length ?? 0);

  const progress = computed(() => {
    if (totalSteps.value === 0) return 0;
    return ((currentStepIndex.value + 1) / totalSteps.value) * 100;
  });

  const isFirstStep = computed(() => currentStepIndex.value === 0);
  const isLastStep = computed(() => currentStepIndex.value === totalSteps.value - 1);

  const hasNextStep = computed(() => currentStepIndex.value < totalSteps.value - 1);
  const hasPrevStep = computed(() => currentStepIndex.value > 0);

  // Actions
  async function startTutorial(tutorial: Tutorial) {
    currentTutorial.value = tutorial;
    currentStepIndex.value = 0;
    isActive.value = true;
    tutorialPaused.value = false;

    const step = currentTutorial.value.steps[0];
    if (step?.onEnter) {
      await step.onEnter();
    }
  }

  async function nextStep() {
    if (!currentTutorial.value || isTransitioning.value) return;
    if (currentStepIndex.value >= totalSteps.value - 1) {
      await completeTutorial();
      return;
    }

    isTransitioning.value = true;

    const currentStepObj = currentStep.value;
    if (currentStepObj?.onExit) {
      await currentStepObj.onExit();
    }

    currentStepIndex.value++;

    const nextStepObj = currentStep.value;
    if (nextStepObj?.onEnter) {
      await nextStepObj.onEnter();
    }

    isTransitioning.value = false;
  }

  async function prevStep() {
    if (!currentTutorial.value || isTransitioning.value || currentStepIndex.value <= 0) return;

    isTransitioning.value = true;

    const currentStepObj = currentStep.value;
    if (currentStepObj?.onExit) {
      await currentStepObj.onExit();
    }

    currentStepIndex.value--;

    const prevStepObj = currentStep.value;
    if (prevStepObj?.onEnter) {
      await prevStepObj.onEnter();
    }

    isTransitioning.value = false;
  }

  async function goToStep(index: number) {
    if (!currentTutorial.value || isTransitioning.value) return;
    if (index < 0 || index >= totalSteps.value) return;

    isTransitioning.value = true;

    const currentStepObj = currentStep.value;
    if (currentStepObj?.onExit) {
      await currentStepObj.onExit();
    }

    currentStepIndex.value = index;

    const newStepObj = currentStep.value;
    if (newStepObj?.onEnter) {
      await newStepObj.onEnter();
    }

    isTransitioning.value = false;
  }

  async function completeTutorial() {
    if (currentTutorial.value) {
      completedTutorials.value.add(currentTutorial.value.id);

      const lastStep = currentStep.value;
      if (lastStep?.onExit) {
        await lastStep.onExit();
      }
    }

    endTutorial();
  }

  function endTutorial() {
    isActive.value = false;
    currentTutorial.value = null;
    currentStepIndex.value = 0;
    isTransitioning.value = false;
    tutorialPaused.value = false;
  }

  function pauseTutorial() {
    tutorialPaused.value = true;
  }

  function resumeTutorial() {
    tutorialPaused.value = false;
  }

  function isTutorialCompleted(tutorialId: string): boolean {
    return completedTutorials.value.has(tutorialId);
  }

  function resetCompletedTutorials() {
    completedTutorials.value.clear();
  }

  // Trigger action completion (called when user completes the expected action)
  async function onActionCompleted() {
    if (!currentStep.value) return;

    // If the step has a waitForEvent, this is called when that event occurs
    // Auto-advance to next step
    if (currentStep.value.action && currentStep.value.action !== "observe") {
      await nextStep();
    }
  }

  return {
    // State
    isActive,
    currentTutorial,
    currentStepIndex,
    isTransitioning,
    completedTutorials,
    tutorialPaused,

    // Computed
    currentStep,
    totalSteps,
    progress,
    isFirstStep,
    isLastStep,
    hasNextStep,
    hasPrevStep,

    // Actions
    startTutorial,
    nextStep,
    prevStep,
    goToStep,
    completeTutorial,
    endTutorial,
    pauseTutorial,
    resumeTutorial,
    isTutorialCompleted,
    resetCompletedTutorials,
    onActionCompleted,
  };
});
