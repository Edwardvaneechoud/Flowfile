import { describe, expect, it } from "vitest";
import { nearestPointIndex, sparklineGeometry, trendFits } from "./sparkline";

const DAY = 86_400_000;

describe("sparklineGeometry", () => {
  it("places points by time and value inside the padding", () => {
    const g = sparklineGeometry(
      [
        { t: 0, value: 0 },
        { t: DAY, value: 10 },
        { t: 2 * DAY, value: 5 },
      ],
      108,
      48,
      4,
    );
    expect(g.points).toEqual([
      { x: 4, y: 44 },
      { x: 54, y: 4 },
      { x: 104, y: 24 },
    ]);
    expect(g.line).toBe("M4 44 L54 4 L104 24");
    expect(g.area).toBe("M4 44 L54 4 L104 24 L104 44 L4 44 Z");
  });

  it("leaves a missing bucket as a wider step, not a hidden one", () => {
    // Months Jan, Feb, (no Mar), Apr: the Feb→Apr step is twice the Jan→Feb step.
    const g = sparklineGeometry(
      [
        { t: 0, value: 1 },
        { t: 1 * 30 * DAY, value: 2 },
        { t: 3 * 30 * DAY, value: 3 },
      ],
      98,
      20,
      4,
    );
    const [a, b, c] = g.points.map((p) => p.x);
    expect(c - b).toBeCloseTo(2 * (b - a), 5);
  });

  it("puts a single point in the middle and draws no line for it", () => {
    const g = sparklineGeometry([{ t: 5, value: 7 }], 100, 40, 4);
    expect(g.points).toEqual([{ x: 50, y: 20 }]);
    expect(g.line).toBe("");
    expect(g.area).toBe("");
  });

  it("draws a flat series at mid-height and nothing without room or points", () => {
    const g = sparklineGeometry(
      [
        { t: 0, value: 3 },
        { t: DAY, value: 3 },
      ],
      100,
      40,
    );
    expect(g.points.map((p) => p.y)).toEqual([20, 20]);
    expect(sparklineGeometry([], 100, 40).line).toBe("");
    expect(sparklineGeometry([{ t: 0, value: 1 }], 0, 40).points).toEqual([]);
  });
});

describe("nearestPointIndex", () => {
  const points = [
    { x: 0, y: 0 },
    { x: 10, y: 0 },
    { x: 40, y: 0 },
  ];

  it("finds the point closest in x", () => {
    expect(nearestPointIndex(points, -5)).toBe(0);
    expect(nearestPointIndex(points, 6)).toBe(1);
    expect(nearestPointIndex(points, 30)).toBe(2);
    expect(nearestPointIndex([], 3)).toBe(-1);
  });
});

describe("trendFits", () => {
  it("shows the line only when the number keeps its size", () => {
    expect(trendFits(150, null, true)).toBe(true);
    expect(trendFits(70, null, false)).toBe(false);
    expect(trendFits(110, 72, false)).toBe(false);
    expect(trendFits(160, 72, false)).toBe(true);
    expect(trendFits(110, 20, true)).toBe(true);
  });
});
