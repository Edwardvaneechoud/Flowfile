import { describe, expect, it } from "vitest";
import { computeDropCell } from "./gridDrop";

// Defaults matching DashboardCanvas: 12 cols, row height 40, margin [8,8],
// default tile width 6. A 1000px-wide grid → colW = (1000 - 8*13)/12 = 74.6667.
const base = { rect: { left: 0, top: 0, width: 1000 }, cols: 12, rowHeight: 40, margin: 8, w: 6 };

describe("computeDropCell", () => {
  it("rounds a mid-grid drop to the nearest cell", () => {
    expect(computeDropCell({ ...base, clientX: 400, clientY: 200 })).toEqual({ x: 5, y: 4 });
  });

  it("clamps x so a 6-wide tile never overflows past the last column", () => {
    expect(computeDropCell({ ...base, clientX: 990, clientY: 0 }).x).toBe(6); // cols - w
  });

  it("clamps negative offsets to the origin", () => {
    expect(computeDropCell({ ...base, clientX: 0, clientY: 0 })).toEqual({ x: 0, y: 0 });
  });

  it("subtracts the grid's left/top offset (scroll-corrected via getBoundingClientRect)", () => {
    // Same relative pointer as the mid-grid case, but the grid box is offset.
    const offset = { ...base, rect: { left: 100, top: 50, width: 1000 } };
    expect(computeDropCell({ ...offset, clientX: 500, clientY: 250 })).toEqual({ x: 5, y: 4 });
  });

  it("falls back to the origin when the grid has no measurable width", () => {
    expect(
      computeDropCell({ ...base, rect: { left: 0, top: 0, width: 0 }, clientX: 50, clientY: 50 }),
    ).toEqual({
      x: 0,
      y: 0,
    });
  });
});
