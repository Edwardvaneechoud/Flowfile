import { describe, expect, it } from "vitest";

import {
  CODE_DOCK_MAX_STRETCH,
  CODE_DOCK_MIN_WIDTH,
  clampDockWidth,
  dragWidth,
  stretch,
} from "./codeDockResize";

describe("clampDockWidth", () => {
  it("keeps the pane between its minimum and the canvas's share of the window", () => {
    expect(clampDockWidth(100, 1600)).toBe(360);
    expect(clampDockWidth(2000, 1600)).toBe(1240);
    expect(clampDockWidth(500, 1600)).toBe(500);
  });

  it("never goes under the minimum in a window too narrow for both", () => {
    expect(clampDockWidth(500, 700)).toBe(CODE_DOCK_MIN_WIDTH);
  });
});

describe("dragWidth", () => {
  it("follows the pointer within the clamp", () => {
    expect(dragWidth(600, -100, 1600)).toEqual({ width: 500, closeArmed: false });
    expect(dragWidth(600, 2000, 1600)).toEqual({ width: 1240, closeArmed: false });
  });

  it("rubber-bands past the minimum and arms a close only past the threshold", () => {
    const soft = dragWidth(360, -10, 1600);
    expect(soft).toEqual({ width: Math.round(360 - stretch(10)), closeArmed: false });
    expect(soft.width).toBeLessThan(360);
    const armed = dragWidth(360, -100, 1600);
    expect(armed.closeArmed).toBe(true);
    expect(armed.width).toBeGreaterThanOrEqual(360 - CODE_DOCK_MAX_STRETCH);
  });

  it("never gives way by more than the maximum stretch however far the drag goes", () => {
    expect(stretch(100)).toBeLessThan(CODE_DOCK_MAX_STRETCH);
    expect(stretch(1e6)).toBeLessThanOrEqual(CODE_DOCK_MAX_STRETCH);
  });
});
