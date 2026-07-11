// Tutorial Store - orchestrates the event-driven interactive tutorial.
// Pure logic (event folding, matching, skipping) lives in tutorial-engine.ts.
import { defineStore } from "pinia";
import { ref, computed } from "vue";

import {
  applyEvent,
  computeEntryIndex,
  emptyContext,
  matchesAny,
  type Tutorial,
  type TutorialContext,
  type TutorialEvent,
  type TutorialStep,
} from "../components/tutorial/tutorial-engine";

export type { Tutorial, TutorialContext, TutorialEvent, TutorialStep };

const COMPLETED_KEY = "flowfile-tutorial-completed";

function loadCompleted(): Set<string> {
  try {
    const raw = globalThis.localStorage?.getItem(COMPLETED_KEY);
    return new Set(raw ? JSON.parse(raw) : []);
  } catch {
    return new Set();
  }
}

function persistCompleted(completed: Set<string>) {
  try {
    globalThis.localStorage?.setItem(COMPLETED_KEY, JSON.stringify([...completed]));
  } catch {
    /* localStorage unavailable */
  }
}

function sameNodesByItem(a: Record<string, number[]>, b: Record<string, number[]>): boolean {
  const keysA = Object.keys(a).filter((k) => a[k].length > 0);
  const keysB = Object.keys(b).filter((k) => b[k].length > 0);
  if (keysA.length !== keysB.length) return false;
  return keysA.every(
    (k) => b[k] && a[k].length === b[k].length && a[k].every((id) => b[k].includes(id)),
  );
}

function sameEdges(
  a: Array<{ sourceId: number; targetId: number }>,
  b: Array<{ sourceId: number; targetId: number }>,
): boolean {
  if (a.length !== b.length) return false;
  return a.every((e) => b.some((o) => o.sourceId === e.sourceId && o.targetId === e.targetId));
}

export const useTutorialStore = defineStore("tutorial", () => {
  // State
  const isActive = ref(false);
  const currentTutorial = ref<Tutorial | null>(null);
  const currentStepIndex = ref(0);
  const isTransitioning = ref(false);
  const completedTutorials = ref<Set<string>>(loadCompleted());
  const tutorialPaused = ref(false);
  // Active but off the designer route: context still accrues, advancing waits.
  const suspended = ref(false);
  const context = ref<TutorialContext>(emptyContext());
  const wrongActionHint = ref<string | null>(null);

  // Reads live app state (open flow, existing nodes/edges) at start so already-
  // done steps are satisfied from the first click. Registered by the bridge.
  let seedProvider: (() => Partial<TutorialContext>) | null = null;
  let advanceLock = false;

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

  const currentStepSatisfied = computed(
    () => currentStep.value?.isSatisfied?.(context.value) === true,
  );

  // Actions
  function registerSeedProvider(provider: () => Partial<TutorialContext>) {
    seedProvider = provider;
  }

  async function startTutorial(tutorial: Tutorial) {
    context.value = { ...emptyContext(), ...(seedProvider?.() ?? {}) };
    currentTutorial.value = tutorial;
    currentStepIndex.value = 0;
    isActive.value = true;
    tutorialPaused.value = false;
    suspended.value = false;
    wrongActionHint.value = null;
    await currentStep.value?.onEnter?.();
  }

  // Context is always recorded (even inactive/paused/suspended) so out-of-order
  // actions are remembered; advancing is gated on the current step's condition.
  function notify(event: TutorialEvent) {
    context.value = applyEvent(context.value, event);
    if (!isActive.value || tutorialPaused.value || suspended.value || isTransitioning.value) {
      return;
    }
    const step = currentStep.value;
    if (!step) return;
    if (step.advanceWhen && matchesAny(event, step.advanceWhen, context.value)) {
      wrongActionHint.value = null;
      void advanceFrom(step.id);
      return;
    }
    const hint = step.wrongActionHint?.(event, context.value);
    if (hint) wrongActionHint.value = hint;
  }

  // Once-only advance: the lock plus the step-id-still-current check make an
  // event and a Next click racing each other a no-op for the loser.
  async function advanceFrom(stepId: string) {
    if (advanceLock || isTransitioning.value) return;
    if (currentStep.value?.id !== stepId) return;
    advanceLock = true;
    try {
      await transitionTo(currentStepIndex.value + 1, 1);
    } finally {
      advanceLock = false;
    }
  }

  async function transitionTo(target: number, direction: 1 | -1) {
    if (!currentTutorial.value || isTransitioning.value) return;
    if (direction === 1 && target > totalSteps.value - 1) {
      await completeTutorial();
      return;
    }
    isTransitioning.value = true;
    try {
      await currentStep.value?.onExit?.();
      let idx = Math.min(Math.max(target, 0), totalSteps.value - 1);
      // Forward only: fast-forward past already-satisfied steps. Back always
      // lands exactly one step back so the user can re-read completed steps.
      if (direction === 1) {
        idx = computeEntryIndex(currentTutorial.value.steps, idx, context.value);
      }
      currentStepIndex.value = idx;
      wrongActionHint.value = null;
      await currentStep.value?.onEnter?.();
    } finally {
      isTransitioning.value = false;
    }
  }

  async function nextStep() {
    const stepId = currentStep.value?.id;
    if (stepId) await advanceFrom(stepId);
  }

  async function prevStep() {
    if (currentStepIndex.value <= 0) return;
    await transitionTo(currentStepIndex.value - 1, -1);
  }

  async function goToStep(index: number) {
    if (!currentTutorial.value) return;
    if (index < 0 || index >= totalSteps.value) return;
    await transitionTo(index, index > currentStepIndex.value ? 1 : -1);
  }

  // Called on resume/route-return: consume context accrued while suspended.
  function reevaluateCurrentStep() {
    if (!currentTutorial.value || isTransitioning.value) return;
    const idx = computeEntryIndex(currentTutorial.value.steps, currentStepIndex.value, context.value);
    if (idx !== currentStepIndex.value) void transitionTo(idx, 1);
  }

  // Re-derive canvas-owned context from live app state — heals paths that
  // mutate the canvas without notify() (undo, paste, edge delete, flow reload).
  function resyncFromApp() {
    if (!seedProvider) return;
    const seed = seedProvider();
    const nodesByItem = seed.nodesByItem ?? {};
    const edges = seed.edges ?? [];
    const ctx = context.value;
    const flowId = seed.flowId ?? ctx.flowId;
    if (
      flowId === ctx.flowId &&
      sameNodesByItem(nodesByItem, ctx.nodesByItem) &&
      sameEdges(edges, ctx.edges)
    ) {
      return;
    }
    const next: TutorialContext = { ...ctx, flowId, nodesByItem, edges };
    if (
      next.openSettingsNodeId !== null &&
      !Object.values(nodesByItem).some((ids) => ids.includes(next.openSettingsNodeId as number))
    ) {
      next.openSettingsNodeId = null;
    }
    context.value = next;
    if (isActive.value && !tutorialPaused.value && !suspended.value) reevaluateCurrentStep();
  }

  function setSuspended(value: boolean) {
    suspended.value = value;
  }

  async function completeTutorial() {
    if (currentTutorial.value) {
      completedTutorials.value.add(currentTutorial.value.id);
      persistCompleted(completedTutorials.value);
      await currentStep.value?.onExit?.();
    }
    endTutorial();
  }

  function endTutorial() {
    isActive.value = false;
    currentTutorial.value = null;
    currentStepIndex.value = 0;
    isTransitioning.value = false;
    tutorialPaused.value = false;
    suspended.value = false;
    context.value = emptyContext();
    wrongActionHint.value = null;
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
    persistCompleted(completedTutorials.value);
  }

  return {
    // State
    isActive,
    currentTutorial,
    currentStepIndex,
    isTransitioning,
    completedTutorials,
    tutorialPaused,
    suspended,
    context,
    wrongActionHint,

    // Computed
    currentStep,
    totalSteps,
    progress,
    isFirstStep,
    isLastStep,
    hasNextStep,
    hasPrevStep,
    currentStepSatisfied,

    // Actions
    registerSeedProvider,
    startTutorial,
    notify,
    nextStep,
    prevStep,
    goToStep,
    reevaluateCurrentStep,
    resyncFromApp,
    setSuspended,
    completeTutorial,
    endTutorial,
    pauseTutorial,
    resumeTutorial,
    isTutorialCompleted,
    resetCompletedTutorials,
  };
});
