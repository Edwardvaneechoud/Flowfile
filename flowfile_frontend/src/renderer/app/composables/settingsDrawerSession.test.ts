// @vitest-environment happy-dom
import { describe, expect, it, vi } from "vitest";
import { createDrawerSession, isDrawerEdit } from "./settingsDrawerSession";

const session = (
  push: () => unknown = () => true,
  { nodeExists = () => true, isSetup }: { nodeExists?: () => boolean; isSetup?: boolean } = {},
) => {
  const pushSpy = vi.fn(push);
  const drawer = createDrawerSession({ push: pushSpy, nodeExists });
  // What the drawer reads from the node's load-time GET /node response.
  if (isSetup !== undefined) drawer.loaded(isSetup);
  return { drawer, push: pushSpy };
};

const keydown = (key: string, target: Element, modifiers: Partial<KeyboardEvent> = {}) =>
  ({
    type: "keydown",
    key,
    metaKey: false,
    ctrlKey: false,
    shiftKey: false,
    altKey: false,
    defaultPrevented: false,
    target,
    ...modifiers,
  }) as unknown as KeyboardEvent;

describe("createDrawerSession", () => {
  it("closes a configured node the user only looked at without saving (fix-ups stay local)", async () => {
    const { drawer, push } = session(() => true, { isSetup: true });
    await expect(drawer.close()).resolves.toBeUndefined();
    expect(push).not.toHaveBeenCalled();
  });

  it("configures a never-configured node on a normal close, not when closing to undo", async () => {
    const { drawer, push } = session(() => true, { isSetup: false });
    await expect(drawer.close({ userEditsOnly: true })).resolves.toBeUndefined();
    expect(push).not.toHaveBeenCalled();
    await expect(drawer.close()).resolves.toBe(true);
    await drawer.close();
    expect(push).toHaveBeenCalledTimes(1);
  });

  it("takes never-configured only from the load-time state, whatever the draft says", async () => {
    // A component may show a proposal or a local default; only the response's is_setup counts.
    const { drawer, push } = session(() => true, { isSetup: false });
    drawer.loaded(true);
    await expect(drawer.close()).resolves.toBe(true);
    expect(push).toHaveBeenCalledTimes(1);
  });

  it("saves nothing by default before the node's state has loaded", async () => {
    const { drawer, push } = session();
    await expect(drawer.close()).resolves.toBeUndefined();
    expect(push).not.toHaveBeenCalled();
  });

  it("ignores a load that lands after the node was already saved", async () => {
    const { drawer, push } = session();
    drawer.noteEdit();
    await drawer.save();
    drawer.loaded(false);
    await drawer.close();
    expect(push).toHaveBeenCalledTimes(1);
  });

  it("stays never-configured when its close save was refused", async () => {
    const results = [false, true];
    const { drawer, push } = session(() => results.shift(), { isSetup: false });
    await expect(drawer.close()).resolves.toBe(false);
    await expect(drawer.close()).resolves.toBe(true);
    expect(push).toHaveBeenCalledTimes(2);
  });

  it("reports pending user edits until they are saved", async () => {
    const { drawer } = session(() => true, { isSetup: false });
    expect(drawer.hasPendingEdits()).toBe(false);
    drawer.noteEdit();
    expect(drawer.hasPendingEdits()).toBe(true);
    await drawer.save();
    expect(drawer.hasPendingEdits()).toBe(false);
  });

  it("still saves user edits when closing to undo", async () => {
    const { drawer, push } = session();
    drawer.noteEdit();
    await expect(drawer.close({ userEditsOnly: true })).resolves.toBe(true);
    expect(push).toHaveBeenCalledTimes(1);
  });

  it("saves on close after the user worked on the settings", async () => {
    const { drawer, push } = session();
    drawer.noteEdit();
    await expect(drawer.close()).resolves.toBe(true);
    expect(push).toHaveBeenCalledTimes(1);
    await drawer.close();
    expect(push).toHaveBeenCalledTimes(1);
  });

  it("discards the edits of a node that is no longer on the canvas", async () => {
    const { drawer, push } = session(() => true, {
      nodeExists: () => false,
      isSetup: false,
    });
    drawer.noteEdit();
    await expect(drawer.close()).resolves.toBeUndefined();
    expect(push).not.toHaveBeenCalled();
  });

  it("leaves nothing pending after Apply", async () => {
    const { drawer, push } = session();
    drawer.noteEdit();
    await drawer.save();
    await drawer.close();
    expect(push).toHaveBeenCalledTimes(1);
  });

  it("keeps edits pending when the save was refused", async () => {
    const results = [false, true];
    const { drawer, push } = session(() => results.shift());
    drawer.noteEdit();
    await expect(drawer.save()).resolves.toBe(false);
    await expect(drawer.close()).resolves.toBe(true);
    expect(push).toHaveBeenCalledTimes(2);
  });

  it("keeps an edit made while a save was in flight", async () => {
    const pending: ((saved: boolean) => void)[] = [];
    const { drawer, push } = session(
      () => new Promise<boolean>((resolve) => pending.push(resolve)),
    );
    drawer.noteEdit();
    const applying = drawer.save();
    drawer.noteEdit();
    pending.shift()?.(true);
    await applying;
    const closing = drawer.close();
    pending.shift()?.(true);
    await closing;
    expect(push).toHaveBeenCalledTimes(2);
  });
});

describe("isDrawerEdit", () => {
  it("counts pointer, input and typing events", () => {
    const field = document.createElement("div");
    expect(isDrawerEdit({ type: "pointerdown" } as Event, true)).toBe(true);
    expect(isDrawerEdit({ type: "change" } as Event, true)).toBe(true);
    expect(isDrawerEdit(keydown("a", field), true)).toBe(true);
  });

  it("ignores a bare modifier and the canvas undo shortcut", () => {
    const button = document.createElement("button");
    document.body.appendChild(button);
    expect(isDrawerEdit(keydown("Meta", button, { metaKey: true }), true)).toBe(false);
    expect(isDrawerEdit(keydown("z", button, { metaKey: true }), true)).toBe(false);
  });

  it("counts Cmd+Z inside a text field: that is the field's own undo", () => {
    const input = document.createElement("input");
    document.body.appendChild(input);
    expect(isDrawerEdit(keydown("z", input, { metaKey: true }), true)).toBe(true);
  });
});
