// Pure tutorial engine: event folding, advance matching, and step skipping.
// No Vue/store/DOM imports so it runs in the node test environment.

export type TutorialEvent =
  | { type: "flow-created"; flowId: number }
  | { type: "flow-opened"; flowId: number }
  | { type: "flow-saved"; flowId: number }
  | { type: "node-added"; nodeItem: string; nodeId: number }
  | { type: "node-removed"; nodeId: number }
  | { type: "edge-connected"; sourceId: number; targetId: number }
  | { type: "node-settings-opened"; nodeId: number }
  | { type: "node-settings-closed" }
  | { type: "code-generator-opened" }
  | { type: "flow-run-started" }
  | { type: "flow-run-completed"; success: boolean };

export type TutorialEventType = TutorialEvent["type"];

export interface TutorialContext {
  flowId: number | null;
  flowSaved: boolean;
  nodesByItem: Record<string, number[]>;
  edges: Array<{ sourceId: number; targetId: number }>;
  openSettingsNodeId: number | null;
  settingsEverOpenedFor: number[];
  codeGeneratorOpened: boolean;
  runStarted: boolean;
  runCompleted: boolean;
  runSucceeded: boolean;
}

export interface StepAdvanceCondition {
  event: TutorialEventType;
  // node-added: palette item that must match
  nodeItem?: string;
  // edge-connected: [sourceItem, targetItem] resolved through ctx.nodesByItem
  betweenItems?: [string, string];
  // node-settings-opened: node must belong to this item
  nodeOfItem?: string;
}

export interface TutorialStep {
  id: string;
  title: string;
  content: string;
  target?: string | ((ctx: TutorialContext) => string | null);
  // second highlighted element (e.g. the other endpoint of a connection)
  secondaryTarget?: string | ((ctx: TutorialContext) => string | null);
  // park the tooltip in the corner so it can't cover canvas elements the step
  // needs (connect steps); the spotlight still points at the target
  tooltipCorner?: boolean;
  position?: "top" | "bottom" | "left" | "right" | "center";
  // modal: backdrop blocks all clicks; spotlight: only the target is clickable;
  // free: no click interception (drag/connect/drawer steps)
  interaction?: "modal" | "spotlight" | "free";
  // dark backdrop; defaults to true except on "free" steps
  veil?: boolean;
  advanceWhen?: StepAdvanceCondition;
  // convention: every step with advanceWhen defines isSatisfied, so events that
  // land mid-transition or while suspended are never lost
  isSatisfied?: (ctx: TutorialContext) => boolean;
  // Prerequisites; unmet -> described mode (centered, free, hint + Next)
  // instead of pointing at nothing. Entries are nodesByItem keys, plus the
  // special key "flow" meaning an open flow.
  requires?: string[];
  hint?: string;
  wrongActionHint?: (event: TutorialEvent, ctx: TutorialContext) => string | null;
  canSkip?: boolean;
  showNextButton?: boolean;
  showPrevButton?: boolean;
  nextLabel?: string;
  onEnter?: () => void | Promise<void>;
  onExit?: () => void | Promise<void>;
  highlightPadding?: number;
  spotlightShape?: "rectangle" | "circle";
  centerInScreen?: boolean;
}

export interface Tutorial {
  id: string;
  name: string;
  description: string;
  steps: TutorialStep[];
}

export function emptyContext(): TutorialContext {
  return {
    flowId: null,
    flowSaved: false,
    nodesByItem: {},
    edges: [],
    openSettingsNodeId: null,
    settingsEverOpenedFor: [],
    codeGeneratorOpened: false,
    runStarted: false,
    runCompleted: false,
    runSucceeded: false,
  };
}

export function applyEvent(ctx: TutorialContext, ev: TutorialEvent): TutorialContext {
  const next: TutorialContext = {
    ...ctx,
    nodesByItem: { ...ctx.nodesByItem },
    edges: [...ctx.edges],
    settingsEverOpenedFor: [...ctx.settingsEverOpenedFor],
  };
  switch (ev.type) {
    case "flow-created":
    case "flow-opened":
      next.flowId = ev.flowId;
      break;
    case "flow-saved":
      next.flowSaved = true;
      break;
    case "node-added": {
      const list = next.nodesByItem[ev.nodeItem] ?? [];
      if (!list.includes(ev.nodeId)) next.nodesByItem[ev.nodeItem] = [...list, ev.nodeId];
      break;
    }
    case "node-removed": {
      for (const item of Object.keys(next.nodesByItem)) {
        next.nodesByItem[item] = next.nodesByItem[item].filter((id) => id !== ev.nodeId);
      }
      next.edges = next.edges.filter((e) => e.sourceId !== ev.nodeId && e.targetId !== ev.nodeId);
      if (next.openSettingsNodeId === ev.nodeId) next.openSettingsNodeId = null;
      break;
    }
    case "edge-connected":
      if (!next.edges.some((e) => e.sourceId === ev.sourceId && e.targetId === ev.targetId)) {
        next.edges.push({ sourceId: ev.sourceId, targetId: ev.targetId });
      }
      break;
    case "node-settings-opened":
      next.openSettingsNodeId = ev.nodeId;
      if (!next.settingsEverOpenedFor.includes(ev.nodeId)) {
        next.settingsEverOpenedFor.push(ev.nodeId);
      }
      break;
    case "node-settings-closed":
      next.openSettingsNodeId = null;
      break;
    case "code-generator-opened":
      next.codeGeneratorOpened = true;
      break;
    case "flow-run-started":
      next.runStarted = true;
      next.runCompleted = false;
      break;
    case "flow-run-completed":
      next.runCompleted = true;
      next.runSucceeded = ev.success;
      break;
  }
  return next;
}

export function firstNodeOfItem(ctx: TutorialContext, item: string): number | undefined {
  return ctx.nodesByItem[item]?.[0];
}

export function hasNodeOfItem(ctx: TutorialContext, item: string): boolean {
  return (ctx.nodesByItem[item]?.length ?? 0) > 0;
}

export function hasEdgeBetweenItems(
  ctx: TutorialContext,
  sourceItem: string,
  targetItem: string,
): boolean {
  const sources = ctx.nodesByItem[sourceItem] ?? [];
  const targets = ctx.nodesByItem[targetItem] ?? [];
  return ctx.edges.some((e) => sources.includes(e.sourceId) && targets.includes(e.targetId));
}

// Match against ctx AFTER the event has been applied (notify applies first).
export function matches(
  ev: TutorialEvent,
  cond: StepAdvanceCondition,
  ctx: TutorialContext,
): boolean {
  if (ev.type !== cond.event) return false;
  if (cond.nodeItem !== undefined) {
    if (ev.type !== "node-added" || ev.nodeItem !== cond.nodeItem) return false;
  }
  if (cond.betweenItems !== undefined) {
    if (ev.type !== "edge-connected") return false;
    const [sourceItem, targetItem] = cond.betweenItems;
    if (!(ctx.nodesByItem[sourceItem] ?? []).includes(ev.sourceId)) return false;
    if (!(ctx.nodesByItem[targetItem] ?? []).includes(ev.targetId)) return false;
  }
  if (cond.nodeOfItem !== undefined) {
    if (ev.type !== "node-settings-opened") return false;
    if (!(ctx.nodesByItem[cond.nodeOfItem] ?? []).includes(ev.nodeId)) return false;
  }
  return true;
}

// Monotonic forward skip over satisfied steps; never skips the final step so
// the user always sees the completion screen and finishes themselves.
export function computeEntryIndex(
  steps: TutorialStep[],
  from: number,
  ctx: TutorialContext,
): number {
  let idx = Math.max(0, from);
  while (idx < steps.length - 1 && steps[idx].isSatisfied?.(ctx) === true) idx++;
  return Math.min(idx, Math.max(steps.length - 1, 0));
}

export function requiresMet(step: TutorialStep, ctx: TutorialContext): boolean {
  return (step.requires ?? []).every((item) =>
    item === "flow" ? (ctx.flowId ?? 0) > 0 : hasNodeOfItem(ctx, item),
  );
}

export function resolveSelector(
  target: string | ((ctx: TutorialContext) => string | null) | undefined,
  ctx: TutorialContext,
): string | null {
  if (target == null) return null;
  return typeof target === "function" ? target(ctx) : target;
}

export function resolveTarget(step: TutorialStep, ctx: TutorialContext): string | null {
  return resolveSelector(step.target, ctx);
}
