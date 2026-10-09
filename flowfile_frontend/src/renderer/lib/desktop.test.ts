// @vitest-environment happy-dom
// Web mode's pop-out windows: `window.open` handles by kind and flow, a closed poll, one opener message.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

type FakeHandle = {
  closed: boolean;
  focus: ReturnType<typeof vi.fn>;
  close: ReturnType<typeof vi.fn>;
  location: { href: string; assign: ReturnType<typeof vi.fn> };
};

const fakeHandle = (href = "about:blank"): FakeHandle => {
  const handle: FakeHandle = {
    closed: false,
    focus: vi.fn(),
    close: vi.fn(() => {
      handle.closed = true;
    }),
    location: { href, assign: vi.fn() },
  };
  return handle;
};

const load = async () => {
  vi.resetModules();
  return (await import("./desktop")).desktop;
};

const web = (url: string, name: string) => ({ url, name });

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
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("opens a blank named window on the url and focuses it on a second open", async () => {
    const desktop = await load();
    const handle = fakeHandle();
    openSpy.mockReturnValue(handle);

    await desktop.openPopoutWindow("notebook", 4, web("u4", "flowfile-notebook-4"));
    expect(openSpy).toHaveBeenCalledWith("", "flowfile-notebook-4", expect.any(String));
    expect(handle.location.assign).toHaveBeenCalledWith("u4");
    expect(await desktop.listPopoutWindows()).toEqual([{ kind: "notebook", flowId: 4 }]);

    await desktop.openPopoutWindow("notebook", 4, web("u4", "flowfile-notebook-4"));
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

    await desktop.openPopoutWindow("notebook", 4, web("u4", "flowfile-notebook-4"));
    expect(handle.location.assign).not.toHaveBeenCalled();
    expect(handle.focus).toHaveBeenCalledOnce();
  });

  it("rejects when the browser blocks the window", async () => {
    const desktop = await load();
    openSpy.mockReturnValue(null);

    await expect(desktop.openPopoutWindow("notebook", 4, web("u4", "n"))).rejects.toThrow(
      "The browser blocked the Notebook window",
    );
    expect(await desktop.listPopoutWindows()).toEqual([]);
  });

  it("reports a window the user closed, once, on the next poll", async () => {
    const desktop = await load();
    const handle = fakeHandle();
    openSpy.mockReturnValue(handle);
    const onClosed = vi.fn();
    const unlisten = await desktop.onPopoutWindowClosed(onClosed);

    await desktop.openPopoutWindow("notebook", 4, web("u4", "flowfile-notebook-4"));
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
    await desktop.onPopoutWindowClosed(onClosed);

    await desktop.openPopoutWindow("notebook", 4, web("u4", "flowfile-notebook-4"));
    await desktop.closePopoutWindow("notebook", 4);
    vi.advanceTimersByTime(1000);
    expect(handle.close).toHaveBeenCalledOnce();
    expect(onClosed).not.toHaveBeenCalled();
  });

  it("accepts only a well-formed return message from this origin", async () => {
    const desktop = await load();
    const onReturned = vi.fn();
    const unlisten = await desktop.onPopoutWindowReturned(onReturned);
    const valid = { type: "flowfile:popout", event: "returned", kind: "notebook", flowId: 4 };

    post(valid, "https://elsewhere.example");
    post({ ...valid, type: "flowfile:notebook-return" });
    post({ ...valid, event: "opened" });
    post({ ...valid, kind: "settings" });
    post({ ...valid, flowId: "4" });
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
    await desktop.onPopoutWindowReturned(onReturned);

    await desktop.returnPopoutToDesigner("logs", 7);
    expect(postMessage).toHaveBeenCalledWith(expect.any(Object), window.location.origin);
    expect(close).toHaveBeenCalledOnce();

    post(postMessage.mock.calls[0][0]);
    expect(onReturned).toHaveBeenCalledExactlyOnceWith({ kind: "logs", flowId: 7 });
  });
});
