// The settle-then-decide half of an undo/redo press, free of stores so it can be unit-tested.
export type HistoryAction = "undo" | "redo";

export interface HistoryPress {
  flowId: () => number;
  /** canUndo / canRedo from the history state the channel applied. */
  canRun: (action: HistoryAction) => boolean;
  drawerHasPendingEdits: () => boolean;
  /** Close the settings drawer, saving the user's edits there; false when that save was refused. */
  closeDrawer: () => Promise<boolean>;
  flushPendingEdits: () => Promise<void>;
  whenMutationsIdle: () => Promise<void>;
}

/**
 * Settle everything before an undo/redo press and decide whether it runs. Pending canvas
 * edits are sent first. The settings drawer closes only when the press has something to do
 * (its unsaved edits count for undo: they become the step undone); a refused drawer save
 * keeps the drawer open with its draft and stops the press. Resolves the flow to act on,
 * or null.
 */
export async function prepareHistoryAction(
  action: HistoryAction,
  press: HistoryPress,
): Promise<number | null> {
  const flowId = press.flowId();
  if (!(flowId > 0)) return null;
  await press.flushPendingEdits();
  await press.whenMutationsIdle();
  if (press.flowId() !== flowId) return null;
  const drawerStep = action === "undo" && press.drawerHasPendingEdits();
  if (!press.canRun(action) && !drawerStep) return null;
  if (!(await press.closeDrawer())) return null;
  // Decide on the history the drawer's save (if any) left behind.
  await press.whenMutationsIdle();
  if (press.flowId() !== flowId || !press.canRun(action)) return null;
  return flowId;
}
