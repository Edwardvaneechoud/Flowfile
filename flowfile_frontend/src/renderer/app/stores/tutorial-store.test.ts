// Unit tests for the tutorial store orchestration: guarded advancing,
// satisfied fast-forward, suspension, and completion persistence.
// Pinia is set up per-test for isolation; localStorage is a Map-backed fake.

import { setActivePinia, createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { useTutorialStore } from "./tutorial-store";
import type { Tutorial, TutorialStep } from "./tutorial-store";
import { hasNodeOfItem } from "../components/tutorial/tutorial-engine";

function fakeLocalStorage() {
  const map = new Map<string, string>();
  return {
    getItem: (k: string) => map.get(k) ?? null,
    setItem: (k: string, v: string) => void map.set(k, v),
    removeItem: (k: string) => void map.delete(k),
    clear: () => map.clear(),
  } as unknown as Storage;
}

function step(id: string, extra: Partial<TutorialStep> = {}): TutorialStep {
  return { id, title: id, content: "", ...extra };
}

function makeTutorial(steps: TutorialStep[]): Tutorial {
  return { id: "test-tutorial", name: "Test", description: "", steps };
}

const dragTutorial = () =>
  makeTutorial([
    step("welcome"),
    step("drag-a", {
      advanceWhen: { event: "node-added", nodeItem: "manual_input" },
      isSatisfied: (ctx) => hasNodeOfItem(ctx, "manual_input"),
    }),
    step("drag-b", {
      advanceWhen: { event: "node-added", nodeItem: "group_by" },
      isSatisfied: (ctx) => hasNodeOfItem(ctx, "group_by"),
    }),
    step("done"),
  ]);

beforeEach(() => {
  vi.stubGlobal("localStorage", fakeLocalStorage());
  setActivePinia(createPinia());
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("notify / advance", () => {
  it("advances exactly once on a matching event", async () => {
    const store = useTutorialStore();
    await store.startTutorial(dragTutorial());
    await store.nextStep();
    expect(store.currentStep?.id).toBe("drag-a");

    store.notify({ type: "node-added", nodeItem: "manual_input", nodeId: 1 });
    await vi.waitFor(() => expect(store.currentStep?.id).toBe("drag-b"));

    // replaying the same event must not advance drag-b (wrong item)
    store.notify({ type: "node-added", nodeItem: "manual_input", nodeId: 1 });
    expect(store.currentStep?.id).toBe("drag-b");
  });

  it("records context without advancing on a non-matching event", async () => {
    const store = useTutorialStore();
    await store.startTutorial(dragTutorial());
    await store.nextStep();

    store.notify({ type: "node-added", nodeItem: "group_by", nodeId: 2 });
    expect(store.currentStep?.id).toBe("drag-a");
    expect(store.context.nodesByItem.group_by).toEqual([2]);
  });

  it("skips a later step already satisfied by an out-of-order action", async () => {
    const store = useTutorialStore();
    await store.startTutorial(dragTutorial());
    await store.nextStep();

    // wrong node first (group_by), then the right one (manual_input)
    store.notify({ type: "node-added", nodeItem: "group_by", nodeId: 2 });
    store.notify({ type: "node-added", nodeItem: "manual_input", nodeId: 1 });
    await vi.waitFor(() => expect(store.currentStep?.id).toBe("done"));
  });

  it("does not advance while inactive, paused or suspended, but keeps context", async () => {
    const store = useTutorialStore();
    store.notify({ type: "node-added", nodeItem: "manual_input", nodeId: 1 });
    expect(store.context.nodesByItem.manual_input).toEqual([1]);

    await store.startTutorial(dragTutorial());
    expect(store.context.nodesByItem.manual_input).toBeUndefined();
    await store.nextStep();
    // drag-a satisfied context was reset at start; re-add while suspended
    store.setSuspended(true);
    store.notify({ type: "node-added", nodeItem: "manual_input", nodeId: 3 });
    expect(store.currentStep?.id).toBe("drag-a");

    store.setSuspended(false);
    store.reevaluateCurrentStep();
    await vi.waitFor(() => expect(store.currentStep?.id).toBe("drag-b"));
  });

  it("ignores events during a slow transition, then catches up on landing", async () => {
    let release: () => void = () => {};
    const gate = new Promise<void>((resolve) => (release = resolve));
    const tutorial = makeTutorial([
      step("welcome", { onExit: () => gate }),
      step("drag-a", {
        advanceWhen: { event: "node-added", nodeItem: "manual_input" },
        isSatisfied: (ctx) => hasNodeOfItem(ctx, "manual_input"),
      }),
      step("done"),
    ]);
    const store = useTutorialStore();
    await store.startTutorial(tutorial);

    const advancing = store.nextStep();
    // event lands while welcome -> drag-a transition awaits onExit
    store.notify({ type: "node-added", nodeItem: "manual_input", nodeId: 1 });
    release();
    await advancing;
    // entry skip consumed the satisfied drag-a
    expect(store.currentStep?.id).toBe("done");
  });
});

describe("navigation", () => {
  it("Back lands on the satisfied step without bouncing forward", async () => {
    const store = useTutorialStore();
    await store.startTutorial(dragTutorial());
    await store.nextStep();
    store.notify({ type: "node-added", nodeItem: "manual_input", nodeId: 1 });
    await vi.waitFor(() => expect(store.currentStep?.id).toBe("drag-b"));

    await store.prevStep();
    expect(store.currentStep?.id).toBe("drag-a");
    expect(store.currentStepSatisfied).toBe(true);

    await store.nextStep();
    expect(store.currentStep?.id).toBe("drag-b");
  });

  it("never fast-forwards past the last step", async () => {
    const tutorial = makeTutorial([
      step("a", { isSatisfied: () => true }),
      step("b", { isSatisfied: () => true }),
      step("end", { isSatisfied: () => true }),
    ]);
    const store = useTutorialStore();
    await store.startTutorial(tutorial);
    await store.nextStep();
    expect(store.currentStep?.id).toBe("end");
    expect(store.isActive).toBe(true);
  });

  it("finishing the last step completes the tutorial", async () => {
    const store = useTutorialStore();
    await store.startTutorial(makeTutorial([step("only")]));
    await store.nextStep();
    expect(store.isActive).toBe(false);
    expect(store.isTutorialCompleted("test-tutorial")).toBe(true);
  });
});

describe("wrong action hints", () => {
  it("surfaces and clears the hint", async () => {
    const tutorial = makeTutorial([
      step("drag-a", {
        advanceWhen: { event: "node-added", nodeItem: "manual_input" },
        isSatisfied: (ctx) => hasNodeOfItem(ctx, "manual_input"),
        wrongActionHint: (ev) =>
          ev.type === "node-added" ? `That's a ${ev.nodeItem} node` : null,
      }),
      step("done"),
    ]);
    const store = useTutorialStore();
    await store.startTutorial(tutorial);

    store.notify({ type: "node-added", nodeItem: "group_by", nodeId: 2 });
    expect(store.wrongActionHint).toBe("That's a group_by node");

    store.notify({ type: "node-added", nodeItem: "manual_input", nodeId: 1 });
    await vi.waitFor(() => expect(store.currentStep?.id).toBe("done"));
    expect(store.wrongActionHint).toBeNull();
  });
});

describe("resyncFromApp", () => {
  it("replaces canvas-owned context and un-satisfies steps after external mutation", async () => {
    const store = useTutorialStore();
    let seed: Record<string, unknown> = {};
    store.registerSeedProvider(() => seed);
    await store.startTutorial(dragTutorial());
    await store.nextStep();

    store.notify({ type: "node-added", nodeItem: "manual_input", nodeId: 1 });
    await vi.waitFor(() => expect(store.currentStep?.id).toBe("drag-b"));
    await store.prevStep();
    expect(store.currentStepSatisfied).toBe(true);

    // canvas mutated outside notify() (e.g. undo removed the node)
    seed = { nodesByItem: {}, edges: [] };
    store.resyncFromApp();
    expect(store.context.nodesByItem.manual_input ?? []).toEqual([]);
    expect(store.currentStepSatisfied).toBe(false);
  });

  it("fast-forwards when the canvas already satisfies the current step", async () => {
    const store = useTutorialStore();
    let seed: Record<string, unknown> = {};
    store.registerSeedProvider(() => seed);
    await store.startTutorial(dragTutorial());
    await store.nextStep();
    expect(store.currentStep?.id).toBe("drag-a");

    // canvas finished loading a flow that already contains the node
    seed = { nodesByItem: { manual_input: [4] }, edges: [] };
    store.resyncFromApp();
    await vi.waitFor(() => expect(store.currentStep?.id).toBe("drag-b"));
  });

  it("prunes openSettingsNodeId when its node left the canvas", async () => {
    const store = useTutorialStore();
    let seed: Record<string, unknown> = { nodesByItem: { manual_input: [1] }, edges: [] };
    store.registerSeedProvider(() => seed);
    await store.startTutorial(dragTutorial());
    store.notify({ type: "node-settings-opened", nodeId: 1 });
    expect(store.context.openSettingsNodeId).toBe(1);

    seed = { nodesByItem: {}, edges: [] };
    store.resyncFromApp();
    expect(store.context.openSettingsNodeId).toBeNull();
  });

  it("keeps event-only fields and is a no-op when the canvas is unchanged", async () => {
    const store = useTutorialStore();
    const seed = { nodesByItem: { manual_input: [1] }, edges: [] };
    store.registerSeedProvider(() => seed);
    await store.startTutorial(dragTutorial());
    store.notify({ type: "flow-run-started" });
    const before = store.context;
    store.resyncFromApp();
    expect(store.context).toBe(before);
    expect(store.context.runStarted).toBe(true);
  });
});

describe("seeding and persistence", () => {
  it("seeds context from the registered provider on start", async () => {
    const store = useTutorialStore();
    store.registerSeedProvider(() => ({
      flowId: 12,
      nodesByItem: { manual_input: [4] },
    }));
    await store.startTutorial(dragTutorial());
    expect(store.context.flowId).toBe(12);
    await store.nextStep();
    // drag-a satisfied via seed -> lands on drag-b
    expect(store.currentStep?.id).toBe("drag-b");
  });

  it("persists completion and hydrates a fresh store", async () => {
    const store = useTutorialStore();
    await store.startTutorial(makeTutorial([step("only")]));
    await store.completeTutorial();

    setActivePinia(createPinia());
    const fresh = useTutorialStore();
    expect(fresh.isTutorialCompleted("test-tutorial")).toBe(true);

    fresh.resetCompletedTutorials();
    setActivePinia(createPinia());
    expect(useTutorialStore().isTutorialCompleted("test-tutorial")).toBe(false);
  });

  it("survives a missing localStorage", async () => {
    vi.stubGlobal("localStorage", undefined);
    setActivePinia(createPinia());
    const store = useTutorialStore();
    await store.startTutorial(makeTutorial([step("only")]));
    await expect(store.completeTutorial()).resolves.toBeUndefined();
    expect(store.isTutorialCompleted("test-tutorial")).toBe(true);
  });
});
