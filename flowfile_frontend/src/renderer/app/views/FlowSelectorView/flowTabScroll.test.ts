import { describe, expect, it } from "vitest";

import { overflowState, wheelScrollDelta } from "./flowTabScroll";

const overflowing = { scrollLeft: 0, clientWidth: 500, scrollWidth: 1200 };
const fitting = { scrollLeft: 0, clientWidth: 500, scrollWidth: 400 };
const wheel = (deltaY: number, extra: Partial<Parameters<typeof wheelScrollDelta>[0]> = {}) => ({
  deltaX: 0,
  deltaY,
  deltaMode: 0,
  ctrlKey: false,
  ...extra,
});

describe("overflowState", () => {
  it("shows only the right arrow at the start", () => {
    expect(overflowState(overflowing)).toEqual({ left: false, right: true });
  });

  it("shows both arrows in the middle", () => {
    expect(overflowState({ ...overflowing, scrollLeft: 300 })).toEqual({ left: true, right: true });
  });

  it("shows only the left arrow at the end, tolerating sub-pixel rounding", () => {
    expect(overflowState({ ...overflowing, scrollLeft: 700 })).toEqual({
      left: true,
      right: false,
    });
    expect(overflowState({ ...overflowing, scrollLeft: 699.5 })).toEqual({
      left: true,
      right: false,
    });
  });

  it("shows no arrows when everything fits", () => {
    expect(overflowState(fitting)).toEqual({ left: false, right: false });
  });
});

describe("wheelScrollDelta", () => {
  it("turns a vertical pixel wheel into a horizontal scroll", () => {
    expect(wheelScrollDelta(wheel(120), overflowing)).toBe(120);
    expect(wheelScrollDelta(wheel(-40), overflowing)).toBe(-40);
  });

  it("leaves pinch-to-zoom (ctrlKey) to the browser", () => {
    expect(wheelScrollDelta(wheel(120, { ctrlKey: true }), overflowing)).toBeNull();
  });

  it("does nothing when the strip does not overflow", () => {
    expect(wheelScrollDelta(wheel(120), fitting)).toBeNull();
  });

  it("leaves already-horizontal gestures to native scrolling", () => {
    expect(wheelScrollDelta(wheel(10, { deltaX: 50 }), overflowing)).toBeNull();
    expect(wheelScrollDelta(wheel(10, { deltaX: 10 }), overflowing)).toBeNull();
  });

  it("scales line and page delta modes to pixels", () => {
    expect(wheelScrollDelta(wheel(3, { deltaMode: 1 }), overflowing)).toBe(48);
    expect(wheelScrollDelta(wheel(1, { deltaMode: 2 }), overflowing)).toBe(500);
  });
});
