import { describe, expect, it, vi } from "vitest";
import {
  exitNativeFullscreen,
  isPresentQuery,
  requestNativeFullscreen,
  viewerQuery,
  withPresentQuery,
} from "./presentMode";

describe("isPresentQuery", () => {
  it("reads ?present=1", () => {
    expect(isPresentQuery({ present: "1" })).toBe(true);
    expect(isPresentQuery({ present: "true" })).toBe(true);
    expect(isPresentQuery({ present: ["1"] })).toBe(true);
    expect(isPresentQuery({ present: "0" })).toBe(false);
    expect(isPresentQuery({ present: null })).toBe(false);
    expect(isPresentQuery({ f: "abc" })).toBe(false);
    expect(isPresentQuery({})).toBe(false);
  });
});

describe("withPresentQuery", () => {
  it("adds and removes the key and keeps the filter state", () => {
    expect(withPresentQuery({ f: "abc" }, true)).toEqual({ f: "abc", present: "1" });
    expect(withPresentQuery({ f: "abc", present: "1" }, false)).toEqual({ f: "abc" });
    expect(withPresentQuery({}, false)).toEqual({});
  });

  it("does not change the query it was given", () => {
    const query = { present: "1" };
    withPresentQuery(query, false);
    expect(query).toEqual({ present: "1" });
  });
});

describe("viewerQuery", () => {
  it("writes filters, tab and present mode together and keeps every other key", () => {
    expect(viewerQuery({ x: "1" }, { filters: "abc", tab: "t2", present: true })).toEqual({
      x: "1",
      f: "abc",
      tab: "t2",
      present: "1",
    });
  });

  it("sets each key from the state, not from a query that missed a pending write", () => {
    expect(viewerQuery({ tab: "t1" }, { filters: "abc", tab: "t2", present: true })).toEqual({
      f: "abc",
      tab: "t2",
      present: "1",
    });
  });

  it("leaving present mode removes only its key; empty state removes the others", () => {
    const query = { f: "abc", tab: "t2", present: "1" };
    expect(viewerQuery(query, { filters: "abc", tab: "t2", present: false })).toEqual({
      f: "abc",
      tab: "t2",
    });
    expect(viewerQuery(query, { filters: null, tab: null, present: false })).toEqual({});
    expect(query).toEqual({ f: "abc", tab: "t2", present: "1" });
  });
});

describe("requestNativeFullscreen", () => {
  it("asks the element when the document allows it", async () => {
    const requestFullscreen = vi.fn(() => Promise.resolve());
    const el = { requestFullscreen } as unknown as Element;
    expect(await requestNativeFullscreen(el, { fullscreenEnabled: true })).toBe(true);
    expect(requestFullscreen).toHaveBeenCalledTimes(1);
  });

  it("resolves false without the API, when it is disabled or refused", async () => {
    const refused = { requestFullscreen: () => Promise.reject(new TypeError("denied")) };
    expect(
      await requestNativeFullscreen(refused as unknown as Element, { fullscreenEnabled: true }),
    ).toBe(false);
    const allowed = vi.fn(() => Promise.resolve());
    expect(
      await requestNativeFullscreen({ requestFullscreen: allowed } as unknown as Element, {
        fullscreenEnabled: false,
      }),
    ).toBe(false);
    expect(allowed).not.toHaveBeenCalled();
    expect(await requestNativeFullscreen({} as Element, { fullscreenEnabled: true })).toBe(false);
    expect(await requestNativeFullscreen(null, { fullscreenEnabled: true })).toBe(false);
  });
});

describe("exitNativeFullscreen", () => {
  it("exits only when something is fullscreen and swallows a failure", async () => {
    const exitFullscreen = vi.fn(() => Promise.resolve());
    await exitNativeFullscreen({ fullscreenElement: null, exitFullscreen });
    expect(exitFullscreen).not.toHaveBeenCalled();
    await exitNativeFullscreen({ fullscreenElement: {} as Element, exitFullscreen });
    expect(exitFullscreen).toHaveBeenCalledTimes(1);
    await expect(
      exitNativeFullscreen({
        fullscreenElement: {} as Element,
        exitFullscreen: () => Promise.reject(new Error("not fullscreen")),
      }),
    ).resolves.toBeUndefined();
  });
});
