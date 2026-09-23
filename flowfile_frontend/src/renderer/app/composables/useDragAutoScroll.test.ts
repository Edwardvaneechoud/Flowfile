// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { canScrollBy, edgeScrollDelta, useDragAutoScroll } from "./useDragAutoScroll";

describe("edgeScrollDelta", () => {
  const band = { top: 100, bottom: 400 };

  it("is zero outside the band and in its middle", () => {
    expect(edgeScrollDelta(50, band)).toBe(0);
    expect(edgeScrollDelta(450, band)).toBe(0);
    expect(edgeScrollDelta(250, band)).toBe(0);
  });

  it("scrolls up near the top edge and down near the bottom, faster closer to the edge", () => {
    expect(edgeScrollDelta(100, band)).toBeLessThan(0);
    expect(edgeScrollDelta(400, band)).toBeGreaterThan(0);
    expect(Math.abs(edgeScrollDelta(100, band))).toBeGreaterThan(
      Math.abs(edgeScrollDelta(140, band)),
    );
    expect(edgeScrollDelta(400, band)).toBeGreaterThan(edgeScrollDelta(360, band));
  });

  it("caps the step at maxStep", () => {
    expect(edgeScrollDelta(100, band, 48, 14)).toBe(-14);
    expect(edgeScrollDelta(400, band, 48, 14)).toBe(14);
  });

  it("still scrolls down near the bottom of a box shorter than two edge zones", () => {
    const short = { top: 250, bottom: 301 };
    expect(edgeScrollDelta(295, short)).toBeGreaterThan(0);
    expect(edgeScrollDelta(254, short)).toBeLessThan(0);
    expect(edgeScrollDelta(275.5, short)).toBe(0);
  });
});

describe("useDragAutoScroll", () => {
  const VIEWPORT = 200;
  const CONTENT = 1000;
  let rafQueue: FrameRequestCallback[];

  const scroller = (top: number, height: number) => {
    const el = document.createElement("div");
    let value = 0;
    Object.defineProperty(el, "scrollTop", {
      configurable: true,
      get: () => value,
      set: (next: number) => {
        value = Math.max(0, Math.min(next, CONTENT - VIEWPORT));
      },
    });
    Object.defineProperty(el, "scrollHeight", { configurable: true, get: () => CONTENT });
    Object.defineProperty(el, "clientHeight", { configurable: true, get: () => VIEWPORT });
    el.getBoundingClientRect = () =>
      ({ top, bottom: top + height, left: 0, right: 300, width: 300, height }) as DOMRect;
    return el;
  };

  const stepFrames = (count: number) => {
    for (let i = 0; i < count; i++) {
      const queued = rafQueue;
      rafQueue = [];
      queued.forEach((cb) => cb(0));
    }
  };

  const dragOverAt = (clientY: number, clientX = 100) =>
    document.dispatchEvent(new MouseEvent("dragover", { bubbles: true, clientX, clientY }));

  beforeEach(() => {
    rafQueue = [];
    let seq = 1;
    vi.stubGlobal("requestAnimationFrame", (cb: FrameRequestCallback) => {
      rafQueue.push(cb);
      return seq++;
    });
    vi.stubGlobal("cancelAnimationFrame", () => {
      rafQueue = [];
    });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("scrolls the container under the pointer while it hovers an edge, and stops on stop()", () => {
    const el = scroller(0, VIEWPORT);
    const controller = useDragAutoScroll({ containers: () => [el] });
    controller.start();

    dragOverAt(VIEWPORT - 4);
    stepFrames(3);
    const afterBottom = el.scrollTop;
    expect(afterBottom).toBeGreaterThan(0);

    dragOverAt(VIEWPORT / 2);
    stepFrames(3);
    expect(el.scrollTop).toBe(afterBottom);

    dragOverAt(4);
    stepFrames(3);
    expect(el.scrollTop).toBeLessThan(afterBottom);

    controller.stop();
    const frozen = el.scrollTop;
    dragOverAt(VIEWPORT - 4);
    stepFrames(3);
    expect(el.scrollTop).toBe(frozen);
    expect(rafQueue).toHaveLength(0);
  });

  it("falls through to the outer scroller once the inner one is exhausted", () => {
    const inner = scroller(0, VIEWPORT);
    const outer = scroller(0, VIEWPORT);
    inner.scrollTop = CONTENT;
    expect(canScrollBy(inner, 1)).toBe(false);

    const controller = useDragAutoScroll({ containers: () => [inner, outer] });
    controller.start();
    dragOverAt(VIEWPORT - 4);
    stepFrames(2);
    expect(outer.scrollTop).toBeGreaterThan(0);
    controller.stop();
  });

  it("ignores a container the pointer is not over horizontally", () => {
    const el = scroller(0, VIEWPORT);
    const controller = useDragAutoScroll({ containers: () => [el] });
    controller.start();
    dragOverAt(VIEWPORT - 4, 900);
    stepFrames(2);
    expect(el.scrollTop).toBe(0);
    controller.stop();
  });
});
