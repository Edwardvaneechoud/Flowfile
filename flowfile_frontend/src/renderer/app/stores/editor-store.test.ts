// Run flushes the open drawer through executeDrawCloseFunction, so it must await the save.

import { setActivePinia, createPinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";

const messageWarning = vi.hoisted(() => vi.fn());
const messageInfo = vi.hoisted(() => vi.fn());
vi.mock("element-plus", () => ({ ElMessage: { warning: messageWarning, info: messageInfo } }));

import { useEditorStore } from "./editor-store";

describe("executeDrawCloseFunction", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
  });

  it("resolves true when no drawer registered a save", async () => {
    expect(await useEditorStore().executeDrawCloseFunction()).toBe(true);
  });

  it("waits for the registered save to finish before resolving", async () => {
    const store = useEditorStore();
    let finished = false;
    store.setCloseFunction(async () => {
      await new Promise((resolve) => setTimeout(resolve, 5));
      finished = true;
      return true;
    });

    expect(await store.executeDrawCloseFunction()).toBe(true);
    expect(finished).toBe(true);
  });

  it("treats a save that returns nothing as saved", async () => {
    const store = useEditorStore();
    store.setCloseFunction(vi.fn(async () => undefined));
    expect(await store.executeDrawCloseFunction()).toBe(true);
  });

  it("resolves false when the save was refused and keeps the registration", async () => {
    const store = useEditorStore();
    const save = vi.fn(async () => false);
    store.setCloseFunction(save);

    expect(await store.executeDrawCloseFunction()).toBe(false);
    expect(save).toHaveBeenCalledOnce();
    expect(store.drawCloseFunction).toBe(save);
  });

  it("propagates a save that throws", async () => {
    const store = useEditorStore();
    store.setCloseFunction(async () => {
      throw new Error("boom");
    });
    await expect(store.executeDrawCloseFunction()).rejects.toThrow("boom");
  });
});

// Canvas clicks, node switches and minimize call this before the drawer lets go of its node.
describe("saveDrawerBeforeLeave", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    messageWarning.mockClear();
    messageInfo.mockClear();
  });

  const openDrawer = (save: () => Promise<unknown>) => {
    const store = useEditorStore();
    store.isDrawerOpen = true;
    store.setCloseFunction(save);
    return store;
  };

  it("lets go without saving when no settings drawer is open", async () => {
    const store = useEditorStore();
    const save = vi.fn(async () => false);
    store.setCloseFunction(save);

    expect(await store.saveDrawerBeforeLeave()).toBe(true);
    expect(save).not.toHaveBeenCalled();
  });

  it("saves, then clears the registration so the drawer's cleanup does not save twice", async () => {
    const save = vi.fn(async () => true);
    const store = openDrawer(save);

    expect(await store.saveDrawerBeforeLeave()).toBe(true);
    expect(save).toHaveBeenCalledOnce();
    expect(store.drawCloseFunction).toBeNull();
    expect(await store.executeDrawCloseFunction()).toBe(true);
    expect(save).toHaveBeenCalledOnce();
  });

  it("keeps the drawer and its save when the save is refused, and says why", async () => {
    const save = vi.fn(async () => false);
    const store = openDrawer(save);

    expect(await store.saveDrawerBeforeLeave()).toBe(false);
    expect(store.isDrawerOpen).toBe(true);
    expect(store.drawCloseFunction).toBe(save);
    expect(messageWarning).toHaveBeenCalledWith(
      expect.objectContaining({ message: expect.stringMatching(/could not be saved/) }),
    );
  });

  it("treats a save that throws as refused", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const store = openDrawer(async () => {
      throw new Error("boom");
    });

    expect(await store.saveDrawerBeforeLeave()).toBe(false);
    expect(store.drawCloseFunction).not.toBeNull();
  });

  it("discards the refused edits on a second attempt to leave, so nothing traps the user", async () => {
    const save = vi.fn(async () => false);
    const store = openDrawer(save);

    expect(await store.saveDrawerBeforeLeave()).toBe(false);
    expect(messageWarning).toHaveBeenCalledWith(
      expect.objectContaining({ message: expect.stringMatching(/click away again to discard/) }),
    );
    expect(await store.saveDrawerBeforeLeave()).toBe(true);
    expect(save).toHaveBeenCalledTimes(2);
    expect(store.drawCloseFunction).toBeNull();
    expect(messageInfo).toHaveBeenCalledWith(
      expect.objectContaining({ message: expect.stringMatching(/discarded/) }),
    );
  });

  it("re-arms the refusal when another node's drawer registers", async () => {
    const store = openDrawer(async () => false);
    expect(await store.saveDrawerBeforeLeave()).toBe(false);

    store.setCloseFunction(async () => false);
    expect(await store.saveDrawerBeforeLeave()).toBe(false);
  });

  it("shares one save between concurrent attempts, e.g. the clicks of a double-click", async () => {
    let finish: (saved: boolean) => void = () => undefined;
    const save = vi.fn(() => new Promise<boolean>((resolve) => (finish = resolve)));
    const store = openDrawer(save);

    const attempts = [
      store.saveDrawerBeforeLeave(),
      store.saveDrawerBeforeLeave(),
      store.saveDrawerBeforeLeave(),
    ];
    finish(false);

    expect(await Promise.all(attempts)).toEqual([false, false, false]);
    expect(save).toHaveBeenCalledOnce();
    expect(messageWarning).toHaveBeenCalledOnce();
  });

  it("keeps the edits while a flow runs and says they can be applied after the run", async () => {
    const save = vi.fn(async () => false);
    const store = openDrawer(save);
    store.isRunning = true;

    expect(await store.saveDrawerBeforeLeave()).toBe(false);
    expect(store.isDrawerOpen).toBe(true);
    expect(store.drawCloseFunction).toBe(save);
    expect(messageWarning).toHaveBeenCalledWith(
      expect.objectContaining({
        message: expect.stringMatching(/flow is running.*changes are kept.*Apply when the run/s),
      }),
    );
    expect(messageInfo).not.toHaveBeenCalled();
  });

  it("still discards on a second attempt to leave during the run", async () => {
    const store = openDrawer(async () => false);
    store.isRunning = true;

    expect(await store.saveDrawerBeforeLeave()).toBe(false);
    expect(await store.saveDrawerBeforeLeave()).toBe(true);
    expect(store.drawCloseFunction).toBeNull();
    expect(messageInfo).toHaveBeenCalledWith(
      expect.objectContaining({ message: expect.stringMatching(/discarded/) }),
    );
  });

  it("saves the kept edits on the next leave once the run has finished", async () => {
    const save = vi.fn(async () => !useEditorStore().isRunning);
    const store = openDrawer(save);
    store.isRunning = true;
    expect(await store.saveDrawerBeforeLeave()).toBe(false);

    store.isRunning = false;
    expect(await store.saveDrawerBeforeLeave()).toBe(true);
    expect(save).toHaveBeenCalledTimes(2);
    expect(store.drawCloseFunction).toBeNull();
    expect(messageInfo).not.toHaveBeenCalled();
  });

  it("warns again, instead of discarding, when the save is refused for another reason after the run", async () => {
    const store = openDrawer(async () => false);
    store.isRunning = true;
    expect(await store.saveDrawerBeforeLeave()).toBe(false);

    store.isRunning = false;
    expect(await store.saveDrawerBeforeLeave()).toBe(false);
    expect(messageWarning).toHaveBeenLastCalledWith(
      expect.objectContaining({ message: expect.stringMatching(/could not be saved/) }),
    );
    expect(messageInfo).not.toHaveBeenCalled();
    expect(await store.saveDrawerBeforeLeave()).toBe(true);
  });

  it("warns first in the next run after Run's pre-run save of the kept edits succeeded", async () => {
    let accept = false;
    const store = openDrawer(async () => accept);
    store.isRunning = true;
    expect(await store.saveDrawerBeforeLeave()).toBe(false);

    store.isRunning = false;
    accept = true;
    expect(await store.executeDrawCloseFunction()).toBe(true);

    store.isRunning = true;
    accept = false;
    expect(await store.saveDrawerBeforeLeave()).toBe(false);
    expect(messageWarning).toHaveBeenCalledTimes(2);
    expect(messageInfo).not.toHaveBeenCalled();
    expect(store.drawCloseFunction).not.toBeNull();
  });

  it("warns first again after a save outside a leave, such as Apply, succeeded", async () => {
    const store = openDrawer(async () => false);
    expect(await store.saveDrawerBeforeLeave()).toBe(false);

    store.disarmRefusedSave();
    expect(await store.saveDrawerBeforeLeave()).toBe(false);
    expect(messageWarning).toHaveBeenCalledTimes(2);
    expect(messageInfo).not.toHaveBeenCalled();
  });

  it("discards on the next leave after Run's pre-run save was refused", async () => {
    const store = openDrawer(async () => false);
    store.rememberRefusedSave(store.drawCloseFunction);

    expect(await store.saveDrawerBeforeLeave()).toBe(true);
    expect(messageWarning).not.toHaveBeenCalled();
    expect(messageInfo).toHaveBeenCalledOnce();
  });
});

// Undo/redo and the drawer's cleanup run the close save once; a refused undo close may re-arm it.
describe("executeDrawCloseFunctionOnce", () => {
  const SettingsComponent = { name: "Settings" };

  beforeEach(() => {
    setActivePinia(createPinia());
    useEditorStore().activeDrawerComponent = SettingsComponent;
  });

  it("runs the close save once and forgets it", async () => {
    const store = useEditorStore();
    const close = vi.fn(async () => true);
    store.setCloseFunction(close);
    await expect(store.executeDrawCloseFunctionOnce()).resolves.toBe(true);
    await expect(store.executeDrawCloseFunctionOnce()).resolves.toBeUndefined();
    expect(close).toHaveBeenCalledTimes(1);
  });

  it("re-arms a refused close only for a caller that keeps the drawer open", async () => {
    const store = useEditorStore();
    const close = vi.fn(async () => false);
    store.setCloseFunction(close, () => true);
    await expect(store.executeDrawCloseFunctionOnce({ userEditsOnly: true }, true)).resolves.toBe(
      false,
    );
    expect(store.drawCloseFunction).toBe(close);
    expect(store.hasPendingDrawerEdits()).toBe(true);

    await expect(store.executeDrawCloseFunctionOnce()).resolves.toBe(false);
    expect(store.drawCloseFunction).toBeNull();
    expect(store.hasPendingDrawerEdits()).toBe(false);
  });

  it("does not re-arm over a drawer that registered while the save ran", async () => {
    const store = useEditorStore();
    const next = vi.fn();
    store.setCloseFunction(async () => {
      store.setCloseFunction(next);
      return false;
    });
    await store.executeDrawCloseFunctionOnce(undefined, true);
    expect(store.drawCloseFunction).toBe(next);
  });

  it("does not re-arm when the drawer was closed while the save ran", async () => {
    const store = useEditorStore();
    store.setCloseFunction(async () => {
      store.activeDrawerComponent = null;
      return false;
    });
    await store.executeDrawCloseFunctionOnce(undefined, true);
    expect(store.drawCloseFunction).toBeNull();
  });

  it("treats a close that throws as refused", async () => {
    const store = useEditorStore();
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const close = vi.fn(async () => {
      throw new Error("boom");
    });
    store.setCloseFunction(close);
    await expect(store.executeDrawCloseFunctionOnce(undefined, true)).resolves.toBe(false);
    expect(store.drawCloseFunction).toBe(close);
  });
});
