// @vitest-environment happy-dom
// Web mode's pop-out windows: `window.open` handles by kind and flow, a closed poll, the opener messages.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { SelectionMessage } from "./popoutWindow";

type FakeHandle = {
  closed: boolean;
  focus: ReturnType<typeof vi.fn>;
  close: ReturnType<typeof vi.fn>;
  postMessage: ReturnType<typeof vi.fn>;
  location: { href: string; assign: ReturnType<typeof vi.fn> };
};

const fakeHandle = (href = "about:blank"): FakeHandle => {
  const handle: FakeHandle = {
    closed: false,
    focus: vi.fn(),
    close: vi.fn(() => {
      handle.closed = true;
    }),
    postMessage: vi.fn(),
    location: { href, assign: vi.fn() },
  };
  return handle;
};

const selection: SelectionMessage = { type: "selection", previewNodeId: 2, selectedNodeIds: [2, 3] };

const load = async () => {
  vi.resetModules();
  return (await import("./desktop")).desktop;
};

const web = (hash: string, name: string) => ({ hash, name });
const pageUrl = (hash: string) => `${window.location.origin}${window.location.pathname}${hash}`;

// Every subscription is undone after its test: a module instance's wire listener must not outlive it.
const unlisteners: Array<() => void> = [];
const on = async (subscription: Promise<() => void>): Promise<() => void> => {
  const off = await subscription;
  unlisteners.push(off);
  return off;
};

const post = (data: unknown, origin = window.location.origin) =>
  window.dispatchEvent(new MessageEvent("message", { data, origin }));

describe("desktop pop-out windows (web mode)", () => {
  let openSpy: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.useFakeTimers();
    openSpy = vi.fn();
    vi.stubGlobal("open", openSpy);
  });

  afterEach(() => {
    for (const off of unlisteners.splice(0)) off();
    window.name = "";
    vi.restoreAllMocks();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("opens a blank named window on the url and focuses it on a second open", async () => {
    const desktop = await load();
    const handle = fakeHandle();
    openSpy.mockReturnValue(handle);

    await desktop.openPopoutWindow("notebook", 4, web("#/notebook?flow=4", "flowfile-notebook-4"));
    expect(openSpy).toHaveBeenCalledWith("", "flowfile-notebook-4", expect.any(String));
    expect(handle.location.assign).toHaveBeenCalledWith(pageUrl("#/notebook?flow=4"));
    expect(await desktop.listPopoutWindows()).toEqual([{ kind: "notebook", flowId: 4 }]);

    await desktop.openPopoutWindow("notebook", 4, web("#/notebook?flow=4", "flowfile-notebook-4"));
    expect(openSpy).toHaveBeenCalledOnce();
    expect(handle.focus).toHaveBeenCalledOnce();
  });

  it("keeps the kinds of one flow apart", async () => {
    const desktop = await load();
    openSpy.mockImplementation(() => fakeHandle());

    await desktop.openPopoutWindow("notebook", 4, web("n", "flowfile-notebook-4"));
    await desktop.openPopoutWindow("logs", 4, web("l", "flowfile-logs-4"));
    expect(openSpy).toHaveBeenCalledTimes(2);
    expect(await desktop.listPopoutWindows()).toEqual([
      { kind: "notebook", flowId: 4 },
      { kind: "logs", flowId: 4 },
    ]);

    await desktop.closePopoutWindow("logs", 4);
    expect(await desktop.listPopoutWindows()).toEqual([{ kind: "notebook", flowId: 4 }]);
  });

  it("focuses a named window that survived a reload instead of reloading it", async () => {
    const desktop = await load();
    const handle = fakeHandle(`${window.location.origin}/#/notebook?flow=4`);
    openSpy.mockReturnValue(handle);

    await desktop.openPopoutWindow("notebook", 4, web("#/notebook?flow=4", "flowfile-notebook-4"));
    expect(handle.location.assign).not.toHaveBeenCalled();
    expect(handle.focus).toHaveBeenCalledOnce();
  });

  it("rejects when the browser blocks the window", async () => {
    const desktop = await load();
    openSpy.mockReturnValue(null);

    await expect(
      desktop.openPopoutWindow("notebook", 4, web("#/notebook?flow=4", "n")),
    ).rejects.toThrow("The browser blocked the Notebook window");
    expect(await desktop.listPopoutWindows()).toEqual([]);
  });

  it("reports a window the user closed, once, on the next poll", async () => {
    const desktop = await load();
    const handle = fakeHandle();
    openSpy.mockReturnValue(handle);
    const onClosed = vi.fn();
    const unlisten = await on(desktop.onPopoutWindowClosed(onClosed));

    await desktop.openPopoutWindow("notebook", 4, web("#/notebook?flow=4", "flowfile-notebook-4"));
    vi.advanceTimersByTime(1000);
    expect(onClosed).not.toHaveBeenCalled();

    handle.closed = true;
    vi.advanceTimersByTime(1000);
    expect(onClosed).toHaveBeenCalledExactlyOnceWith({ kind: "notebook", flowId: 4 });
    expect(await desktop.listPopoutWindows()).toEqual([]);

    vi.advanceTimersByTime(3000);
    expect(onClosed).toHaveBeenCalledOnce();
    unlisten();
  });

  it("closing on request does not report the window as closed by the user", async () => {
    const desktop = await load();
    const handle = fakeHandle();
    openSpy.mockReturnValue(handle);
    const onClosed = vi.fn();
    await on(desktop.onPopoutWindowClosed(onClosed));

    await desktop.openPopoutWindow("notebook", 4, web("#/notebook?flow=4", "flowfile-notebook-4"));
    await desktop.closePopoutWindow("notebook", 4);
    vi.advanceTimersByTime(1000);
    expect(handle.close).toHaveBeenCalledOnce();
    expect(onClosed).not.toHaveBeenCalled();
  });

  it("accepts only a well-formed return message from this origin", async () => {
    const desktop = await load();
    const onReturned = vi.fn();
    const unlisten = await on(desktop.onPopoutWindowReturned(onReturned));
    const valid = { type: "flowfile:popout", event: "returned", kind: "notebook", flowId: 4 };

    post(valid, "https://elsewhere.example");
    post({ ...valid, type: "flowfile:notebook-return" });
    post({ ...valid, event: "opened" });
    post({ ...valid, kind: "settings" });
    post({ ...valid, flowId: "4" });
    post({ ...valid, event: "rekeyed", to: 9 });
    post(null);
    post("flowfile:popout");
    expect(onReturned).not.toHaveBeenCalled();

    post(valid);
    expect(onReturned).toHaveBeenCalledExactlyOnceWith({ kind: "notebook", flowId: 4 });

    unlisten();
    post(valid);
    expect(onReturned).toHaveBeenCalledOnce();
  });

  it("a pop-out's return reaches its opener's listener and closes the pop-out", async () => {
    const desktop = await load();
    const postMessage = vi.fn();
    const close = vi.fn();
    vi.stubGlobal("opener", { postMessage });
    vi.stubGlobal("close", close);
    const onReturned = vi.fn();
    await on(desktop.onPopoutWindowReturned(onReturned));

    await desktop.returnPopoutToDesigner("logs", 7);
    expect(postMessage).toHaveBeenCalledWith(expect.any(Object), window.location.origin);
    expect(close).toHaveBeenCalledOnce();

    post(postMessage.mock.calls[0][0]);
    expect(onReturned).toHaveBeenCalledExactlyOnceWith({ kind: "logs", flowId: 7 });
  });

  it("a rekeyed message moves the handle to the new flow before anyone hears of it", async () => {
    const desktop = await load();
    const handle = fakeHandle();
    openSpy.mockReturnValueOnce(handle).mockReturnValueOnce(fakeHandle());
    const onClosed = vi.fn();
    await on(desktop.onPopoutWindowClosed(onClosed));
    let openDuringHandler: Promise<unknown> | null = null;
    const onRekeyed = vi.fn(() => {
      openDuringHandler = desktop.listPopoutWindows();
    });
    const unlisten = await on(desktop.onPopoutWindowRekeyed(onRekeyed));
    await desktop.openPopoutWindow("notebook", 4, web("#/notebook?flow=4", "flowfile-notebook-4"));
    await desktop.openPopoutWindow("logs", 4, web("#/popout/logs?flow=4", "flowfile-logs-4"));

    post({ type: "flowfile:popout", event: "rekeyed", kind: "notebook", flowId: 4, to: 9 });
    expect(onRekeyed).toHaveBeenCalledExactlyOnceWith({ kind: "notebook", from: 4, to: 9 });
    const moved = [
      { kind: "logs", flowId: 4 },
      { kind: "notebook", flowId: 9 },
    ];
    expect(await openDuringHandler).toEqual(moved);
    expect(await desktop.listPopoutWindows()).toEqual(moved);

    await desktop.openPopoutWindow("notebook", 9, web("#/notebook?flow=9", "flowfile-notebook-9"));
    expect(openSpy).toHaveBeenCalledTimes(2);
    expect(handle.focus).toHaveBeenCalledOnce();

    handle.closed = true;
    vi.advanceTimersByTime(1000);
    expect(onClosed).toHaveBeenCalledExactlyOnceWith({ kind: "notebook", flowId: 9 });

    unlisten();
    post({ type: "flowfile:popout", event: "rekeyed", kind: "logs", flowId: 4, to: 9 });
    expect(onRekeyed).toHaveBeenCalledOnce();
  });

  it("ignores a rekeyed message without a new id and one for an untracked window", async () => {
    const desktop = await load();
    openSpy.mockImplementation(() => fakeHandle());
    const onRekeyed = vi.fn();
    await on(desktop.onPopoutWindowRekeyed(onRekeyed));
    await desktop.openPopoutWindow("notebook", 4, web("#/notebook?flow=4", "flowfile-notebook-4"));

    post({ type: "flowfile:popout", event: "rekeyed", kind: "notebook", flowId: 4 });
    post({ type: "flowfile:popout", event: "rekeyed", kind: "notebook", flowId: 4, to: "9" });
    expect(onRekeyed).not.toHaveBeenCalled();
    expect(await desktop.listPopoutWindows()).toEqual([{ kind: "notebook", flowId: 4 }]);

    post({ type: "flowfile:popout", event: "rekeyed", kind: "table", flowId: 4, to: 9 });
    expect(onRekeyed).not.toHaveBeenCalled();
    expect(await desktop.listPopoutWindows()).toEqual([{ kind: "notebook", flowId: 4 }]);
  });

  it("refuses a rekey into a flow that already has a window of that kind", async () => {
    const desktop = await load();
    openSpy.mockImplementation(() => fakeHandle());
    const onRekeyed = vi.fn();
    await on(desktop.onPopoutWindowRekeyed(onRekeyed));
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    await desktop.openPopoutWindow("notebook", 4, web("#/notebook?flow=4", "flowfile-notebook-4"));
    await desktop.openPopoutWindow("notebook", 9, web("#/notebook?flow=9", "flowfile-notebook-9"));

    post({ type: "flowfile:popout", event: "rekeyed", kind: "notebook", flowId: 4, to: 9 });
    expect(onRekeyed).not.toHaveBeenCalled();
    expect(console.warn).toHaveBeenCalledOnce();
    expect(await desktop.listPopoutWindows()).toEqual([
      { kind: "notebook", flowId: 4 },
      { kind: "notebook", flowId: 9 },
    ]);
  });

  it("listens for opener messages only while someone subscribes", async () => {
    const desktop = await load();
    const added = vi.spyOn(window, "addEventListener");
    const removed = vi.spyOn(window, "removeEventListener");
    const messages = (spy: typeof added) => spy.mock.calls.filter(([type]) => type === "message");

    const offReturned = await on(desktop.onPopoutWindowReturned(vi.fn()));
    const offClosed = await on(desktop.onPopoutWindowClosed(vi.fn()));
    expect(messages(added)).toHaveLength(1);
    offReturned();
    expect(messages(removed)).toHaveLength(0);
    offClosed();
    expect(messages(removed)).toHaveLength(1);

    await on(desktop.onPopoutWindowRekeyed(vi.fn()));
    expect(messages(added)).toHaveLength(2);
  });

  it("answers a held window's ready report and not an untracked one's", async () => {
    const desktop = await load();
    openSpy.mockReturnValue(fakeHandle());
    const onReady = vi.fn();
    const unlisten = await on(desktop.onPopoutWindowReady(onReady));
    await desktop.openPopoutWindow("table", 4, web("#/popout/table?flow=4", "flowfile-table-4"));

    post({ type: "flowfile:popout", event: "ready", kind: "table", flowId: 9 });
    post({ type: "flowfile:popout", event: "ready", kind: "logs", flowId: 4 });
    expect(onReady).not.toHaveBeenCalled();
    post({ type: "flowfile:popout", event: "ready", kind: "table", flowId: 4 });
    expect(onReady).toHaveBeenCalledExactlyOnceWith({ kind: "table", flowId: 4 });

    unlisten();
    post({ type: "flowfile:popout", event: "ready", kind: "table", flowId: 4 });
    expect(onReady).toHaveBeenCalledOnce();
  });

  it("posts a message to the window it holds and to no other", async () => {
    const desktop = await load();
    const handle = fakeHandle();
    openSpy.mockReturnValue(handle);
    await desktop.postToPopoutWindow("table", 4, selection);
    expect(handle.postMessage).not.toHaveBeenCalled();

    await desktop.openPopoutWindow("table", 4, web("#/popout/table?flow=4", "flowfile-table-4"));
    await desktop.postToPopoutWindow("table", 4, selection);
    expect(handle.postMessage).toHaveBeenCalledExactlyOnceWith(
      { type: "flowfile:popout-message", kind: "table", flowId: 4, message: selection },
      window.location.origin,
    );

    handle.closed = true;
    await desktop.postToPopoutWindow("table", 4, selection);
    expect(handle.postMessage).toHaveBeenCalledOnce();
  });

  it("a window hears well-formed messages from its origin until it stops listening", async () => {
    const desktop = await load();
    const onMessage = vi.fn();
    const off = await desktop.onPopoutMessage(onMessage);
    const wire = { type: "flowfile:popout-message", kind: "table", flowId: 4, message: selection };

    post(wire, "https://elsewhere.example");
    post({ ...wire, type: "flowfile:popout" });
    post({ ...wire, kind: "settings" });
    post({ ...wire, message: { ...selection, previewNodeId: "2" } });
    post({ ...wire, message: undefined });
    expect(onMessage).not.toHaveBeenCalled();

    post(wire);
    expect(onMessage).toHaveBeenCalledExactlyOnceWith(selection);

    off();
    post(wire);
    expect(onMessage).toHaveBeenCalledOnce();
  });

  it("a pop-out's ready report reaches its opener's listener", async () => {
    const desktop = await load();
    openSpy.mockReturnValue(fakeHandle());
    const postMessage = vi.fn();
    vi.stubGlobal("opener", { postMessage });
    const onReady = vi.fn();
    await on(desktop.onPopoutWindowReady(onReady));
    await desktop.openPopoutWindow("table", 4, web("#/popout/table?flow=4", "flowfile-table-4"));

    await desktop.reportPopoutReady("table", 4);
    expect(postMessage).toHaveBeenCalledExactlyOnceWith(
      { type: "flowfile:popout", event: "ready", kind: "table", flowId: 4 },
      window.location.origin,
    );
    post(postMessage.mock.calls[0][0]);
    expect(onReady).toHaveBeenCalledExactlyOnceWith({ kind: "table", flowId: 4 });
  });

  it("a pop-out's Save As renames the window and reaches its opener's listener", async () => {
    const desktop = await load();
    openSpy.mockReturnValue(fakeHandle());
    const postMessage = vi.fn();
    vi.stubGlobal("opener", { postMessage });
    const onRekeyed = vi.fn();
    await on(desktop.onPopoutWindowRekeyed(onRekeyed));
    await desktop.openPopoutWindow("notebook", 4, web("#/notebook?flow=4", "flowfile-notebook-4"));

    await desktop.rekeyPopoutWindow("notebook", 4, 9);
    expect(window.name).toBe("flowfile-notebook-9");
    expect(postMessage).toHaveBeenCalledWith(expect.any(Object), window.location.origin);

    post(postMessage.mock.calls[0][0]);
    expect(onRekeyed).toHaveBeenCalledExactlyOnceWith({ kind: "notebook", from: 4, to: 9 });
    expect(await desktop.listPopoutWindows()).toEqual([{ kind: "notebook", flowId: 9 }]);
  });
});
