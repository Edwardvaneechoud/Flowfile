import { describe, expect, it, vi } from "vitest";
import { prepareHistoryAction, type HistoryAction, type HistoryPress } from "./historyAction";

const press = (overrides: Partial<HistoryPress> = {}) => {
  const state = { canUndo: true, canRedo: false };
  const deps: HistoryPress = {
    flowId: () => 7,
    canRun: (action: HistoryAction) => (action === "undo" ? state.canUndo : state.canRedo),
    drawerHasPendingEdits: () => false,
    closeDrawer: vi.fn(async () => true),
    flushPendingEdits: vi.fn(async () => undefined),
    whenMutationsIdle: vi.fn(async () => undefined),
    ...overrides,
  };
  return { deps, state };
};

describe("prepareHistoryAction", () => {
  it("closes the drawer and runs when there is something to undo", async () => {
    const { deps } = press();
    await expect(prepareHistoryAction("undo", deps)).resolves.toBe(7);
    expect(deps.flushPendingEdits).toHaveBeenCalledTimes(1);
    expect(deps.closeDrawer).toHaveBeenCalledTimes(1);
  });

  it("leaves the drawer open when there is nothing to undo or redo", async () => {
    const { deps, state } = press();
    state.canUndo = false;
    await expect(prepareHistoryAction("undo", deps)).resolves.toBeNull();
    await expect(prepareHistoryAction("redo", deps)).resolves.toBeNull();
    expect(deps.closeDrawer).not.toHaveBeenCalled();
  });

  it("undoes pending drawer edits even when the history was empty", async () => {
    const { deps, state } = press({ drawerHasPendingEdits: () => true });
    state.canUndo = false;
    deps.closeDrawer = vi.fn(async () => {
      state.canUndo = true;
      return true;
    });
    await expect(prepareHistoryAction("undo", deps)).resolves.toBe(7);
    expect(deps.closeDrawer).toHaveBeenCalledTimes(1);
  });

  it("does not close the drawer for redo because of pending edits", async () => {
    const { deps } = press({ drawerHasPendingEdits: () => true });
    await expect(prepareHistoryAction("redo", deps)).resolves.toBeNull();
    expect(deps.closeDrawer).not.toHaveBeenCalled();
  });

  it("aborts when the drawer's save is refused, so the drawer keeps its draft", async () => {
    const { deps } = press({
      drawerHasPendingEdits: () => true,
      closeDrawer: vi.fn(async () => false),
    });
    await expect(prepareHistoryAction("undo", deps)).resolves.toBeNull();
    expect(deps.closeDrawer).toHaveBeenCalledTimes(1);
  });

  it("stops when the user switched flows meanwhile", async () => {
    let flowId = 7;
    const { deps } = press({
      flowId: () => flowId,
      flushPendingEdits: vi.fn(async () => {
        flowId = 8;
      }),
    });
    await expect(prepareHistoryAction("undo", deps)).resolves.toBeNull();
    expect(deps.closeDrawer).not.toHaveBeenCalled();
  });

  it("does nothing without an open flow", async () => {
    const { deps } = press({ flowId: () => -1 });
    await expect(prepareHistoryAction("undo", deps)).resolves.toBeNull();
    expect(deps.flushPendingEdits).not.toHaveBeenCalled();
  });
});
