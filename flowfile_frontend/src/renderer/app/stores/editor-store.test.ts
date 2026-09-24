import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { useEditorStore } from "./editor-store";

const SettingsComponent = { name: "Settings" };

beforeEach(() => {
  setActivePinia(createPinia());
  useEditorStore().activeDrawerComponent = SettingsComponent;
});

describe("editor-store drawer close function", () => {
  it("runs the close save once and forgets it", async () => {
    const store = useEditorStore();
    const close = vi.fn(async () => true);
    store.setCloseFunction(close);
    await expect(store.executeDrawCloseFunction()).resolves.toBe(true);
    await expect(store.executeDrawCloseFunction()).resolves.toBeUndefined();
    expect(close).toHaveBeenCalledTimes(1);
  });

  it("re-arms a refused close only for a caller that keeps the drawer open", async () => {
    const store = useEditorStore();
    const close = vi.fn(async () => false);
    store.setCloseFunction(close, () => true);
    await expect(store.executeDrawCloseFunction({ userEditsOnly: true }, true)).resolves.toBe(
      false,
    );
    expect(store.drawCloseFunction).toBe(close);
    expect(store.hasPendingDrawerEdits()).toBe(true);

    await expect(store.executeDrawCloseFunction()).resolves.toBe(false);
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
    await store.executeDrawCloseFunction(undefined, true);
    expect(store.drawCloseFunction).toBe(next);
  });

  it("does not re-arm when the drawer was closed while the save ran", async () => {
    const store = useEditorStore();
    store.setCloseFunction(async () => {
      store.activeDrawerComponent = null;
      return false;
    });
    await store.executeDrawCloseFunction(undefined, true);
    expect(store.drawCloseFunction).toBeNull();
  });

  it("treats a close that throws as refused", async () => {
    const store = useEditorStore();
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const close = vi.fn(async () => {
      throw new Error("boom");
    });
    store.setCloseFunction(close);
    await expect(store.executeDrawCloseFunction(undefined, true)).resolves.toBe(false);
    expect(store.drawCloseFunction).toBe(close);
  });
});
