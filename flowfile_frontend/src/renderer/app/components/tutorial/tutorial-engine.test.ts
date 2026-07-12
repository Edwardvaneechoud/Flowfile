import { describe, expect, it } from "vitest";

import {
  applyEvent,
  computeEntryIndex,
  emptyContext,
  firstNodeOfItem,
  hasEdgeBetweenItems,
  hasNodeOfItem,
  matches,
  matchesAny,
  requiresMet,
  resolveTarget,
  type TutorialContext,
  type TutorialStep,
} from "./tutorial-engine";

function ctxWith(events: Parameters<typeof applyEvent>[1][]): TutorialContext {
  return events.reduce(applyEvent, emptyContext());
}

describe("applyEvent", () => {
  it("records flow creation and opening", () => {
    expect(ctxWith([{ type: "flow-created", flowId: 7 }]).flowId).toBe(7);
    expect(ctxWith([{ type: "flow-opened", flowId: 9 }]).flowId).toBe(9);
  });

  it("does not mutate the input context", () => {
    const before = emptyContext();
    applyEvent(before, { type: "node-added", nodeItem: "manual_input", nodeId: 1 });
    expect(before.nodesByItem).toEqual({});
  });

  it("collects nodes by item and dedupes ids", () => {
    const ctx = ctxWith([
      { type: "node-added", nodeItem: "manual_input", nodeId: 1 },
      { type: "node-added", nodeItem: "manual_input", nodeId: 1 },
      { type: "node-added", nodeItem: "group_by", nodeId: 2 },
    ]);
    expect(ctx.nodesByItem).toEqual({ manual_input: [1], group_by: [2] });
  });

  it("prunes nodes, edges and open settings on node-removed", () => {
    const ctx = ctxWith([
      { type: "node-added", nodeItem: "manual_input", nodeId: 1 },
      { type: "node-added", nodeItem: "group_by", nodeId: 2 },
      { type: "edge-connected", sourceId: 1, targetId: 2 },
      { type: "node-settings-opened", nodeId: 1 },
      { type: "node-removed", nodeId: 1 },
    ]);
    expect(ctx.nodesByItem.manual_input).toEqual([]);
    expect(ctx.edges).toEqual([]);
    expect(ctx.openSettingsNodeId).toBeNull();
    expect(ctx.settingsEverOpenedFor).toEqual([1]);
  });

  it("dedupes edges", () => {
    const ctx = ctxWith([
      { type: "edge-connected", sourceId: 1, targetId: 2 },
      { type: "edge-connected", sourceId: 1, targetId: 2 },
    ]);
    expect(ctx.edges).toHaveLength(1);
  });

  it("tracks settings open/close with durable history", () => {
    const opened = ctxWith([{ type: "node-settings-opened", nodeId: 4 }]);
    expect(opened.openSettingsNodeId).toBe(4);
    const closed = applyEvent(opened, { type: "node-settings-closed" });
    expect(closed.openSettingsNodeId).toBeNull();
    expect(closed.settingsEverOpenedFor).toEqual([4]);
  });

  it("tracks run lifecycle", () => {
    const started = ctxWith([{ type: "flow-run-started" }]);
    expect(started.runStarted).toBe(true);
    expect(started.runCompleted).toBe(false);
    const done = applyEvent(started, { type: "flow-run-completed", success: true });
    expect(done.runCompleted).toBe(true);
    expect(done.runSucceeded).toBe(true);
    const restarted = applyEvent(done, { type: "flow-run-started" });
    expect(restarted.runCompleted).toBe(false);
  });
});

describe("matches", () => {
  it("filters node-added by item", () => {
    const ctx = emptyContext();
    const cond = { event: "node-added" as const, nodeItem: "manual_input" };
    expect(matches({ type: "node-added", nodeItem: "manual_input", nodeId: 1 }, cond, ctx)).toBe(
      true,
    );
    expect(matches({ type: "node-added", nodeItem: "group_by", nodeId: 1 }, cond, ctx)).toBe(false);
  });

  it("resolves betweenItems through nodesByItem", () => {
    const ctx = ctxWith([
      { type: "node-added", nodeItem: "manual_input", nodeId: 1 },
      { type: "node-added", nodeItem: "group_by", nodeId: 2 },
      { type: "node-added", nodeItem: "output", nodeId: 3 },
    ]);
    const cond = { event: "edge-connected" as const, betweenItems: ["manual_input", "group_by"] as [string, string] };
    expect(matches({ type: "edge-connected", sourceId: 1, targetId: 2 }, cond, ctx)).toBe(true);
    expect(matches({ type: "edge-connected", sourceId: 1, targetId: 3 }, cond, ctx)).toBe(false);
    expect(matches({ type: "edge-connected", sourceId: 2, targetId: 1 }, cond, ctx)).toBe(false);
    expect(matches({ type: "edge-connected", sourceId: 99, targetId: 2 }, cond, ctx)).toBe(false);
  });

  it("resolves nodeOfItem for settings events", () => {
    const ctx = ctxWith([{ type: "node-added", nodeItem: "manual_input", nodeId: 5 }]);
    const cond = { event: "node-settings-opened" as const, nodeOfItem: "manual_input" };
    expect(matches({ type: "node-settings-opened", nodeId: 5 }, cond, ctx)).toBe(true);
    expect(matches({ type: "node-settings-opened", nodeId: 6 }, cond, ctx)).toBe(false);
  });

  it("rejects mismatched event types", () => {
    expect(matches({ type: "flow-run-started" }, { event: "flow-saved" }, emptyContext())).toBe(
      false,
    );
  });

  it("matchesAny accepts any of several conditions", () => {
    const ctx = emptyContext();
    const conds = [{ event: "save-dialog-opened" as const }, { event: "flow-saved" as const }];
    expect(matchesAny({ type: "save-dialog-opened" }, conds, ctx)).toBe(true);
    expect(matchesAny({ type: "flow-saved", flowId: 1 }, conds, ctx)).toBe(true);
    expect(matchesAny({ type: "flow-run-started" }, conds, ctx)).toBe(false);
    expect(matchesAny({ type: "flow-saved", flowId: 1 }, { event: "flow-saved" }, ctx)).toBe(true);
  });
});

describe("dialog and settings state", () => {
  it("tracks the save dialog open/close and flow settings", () => {
    let ctx = applyEvent(emptyContext(), { type: "save-dialog-opened" });
    expect(ctx.saveDialogOpen).toBe(true);
    ctx = applyEvent(ctx, { type: "save-dialog-closed" });
    expect(ctx.saveDialogOpen).toBe(false);
    ctx = applyEvent(ctx, { type: "flow-settings-opened" });
    expect(ctx.flowSettingsOpened).toBe(true);
  });
});

describe("computeEntryIndex", () => {
  const step = (id: string, satisfied?: boolean): TutorialStep => ({
    id,
    title: id,
    content: "",
    ...(satisfied === undefined ? {} : { isSatisfied: () => satisfied }),
  });

  it("stays put when the step is not satisfied", () => {
    expect(computeEntryIndex([step("a", false), step("b")], 0, emptyContext())).toBe(0);
  });

  it("skips consecutive satisfied steps", () => {
    const steps = [step("a", true), step("b", true), step("c", false), step("d")];
    expect(computeEntryIndex(steps, 0, emptyContext())).toBe(2);
  });

  it("treats steps without isSatisfied as unsatisfied", () => {
    expect(computeEntryIndex([step("a"), step("b", true)], 0, emptyContext())).toBe(0);
  });

  it("never skips past the last step", () => {
    const steps = [step("a", true), step("b", true), step("c", true)];
    expect(computeEntryIndex(steps, 0, emptyContext())).toBe(2);
  });
});

describe("context helpers", () => {
  const ctx = ctxWith([
    { type: "node-added", nodeItem: "manual_input", nodeId: 1 },
    { type: "node-added", nodeItem: "group_by", nodeId: 2 },
    { type: "edge-connected", sourceId: 1, targetId: 2 },
  ]);

  it("firstNodeOfItem / hasNodeOfItem", () => {
    expect(firstNodeOfItem(ctx, "manual_input")).toBe(1);
    expect(firstNodeOfItem(ctx, "output")).toBeUndefined();
    expect(hasNodeOfItem(ctx, "group_by")).toBe(true);
    expect(hasNodeOfItem(ctx, "output")).toBe(false);
  });

  it("hasEdgeBetweenItems", () => {
    expect(hasEdgeBetweenItems(ctx, "manual_input", "group_by")).toBe(true);
    expect(hasEdgeBetweenItems(ctx, "group_by", "manual_input")).toBe(false);
  });

  it("requiresMet", () => {
    const step: TutorialStep = { id: "s", title: "", content: "", requires: ["manual_input"] };
    expect(requiresMet(step, ctx)).toBe(true);
    expect(requiresMet({ ...step, requires: ["output"] }, ctx)).toBe(false);
    expect(requiresMet({ id: "s", title: "", content: "" }, ctx)).toBe(true);
  });

  it("requiresMet treats 'flow' as an open-flow prerequisite", () => {
    const step: TutorialStep = { id: "s", title: "", content: "", requires: ["flow"] };
    expect(requiresMet(step, ctx)).toBe(false);
    expect(requiresMet(step, applyEvent(ctx, { type: "flow-created", flowId: 3 }))).toBe(true);
  });

  it("resolveTarget supports strings and context functions", () => {
    const fnStep: TutorialStep = {
      id: "s",
      title: "",
      content: "",
      target: (c) => {
        const id = firstNodeOfItem(c, "manual_input");
        return id === undefined ? null : `.vue-flow__node[data-id="${id}"]`;
      },
    };
    expect(resolveTarget(fnStep, ctx)).toBe('.vue-flow__node[data-id="1"]');
    expect(resolveTarget(fnStep, emptyContext())).toBeNull();
    expect(resolveTarget({ id: "s", title: "", content: "", target: ".x" }, ctx)).toBe(".x");
    expect(resolveTarget({ id: "s", title: "", content: "" }, ctx)).toBeNull();
  });
});
