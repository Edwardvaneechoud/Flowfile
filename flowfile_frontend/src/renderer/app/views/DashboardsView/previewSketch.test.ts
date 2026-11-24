import { describe, expect, it } from "vitest";
import {
  arcSlices,
  areaToPoints,
  barHeights,
  boxes,
  hashSeed,
  heatCells,
  linePoints,
  mapDots,
  scatterDots,
  seededRandom,
  sketchKind,
} from "./previewSketch";

describe("sketchKind", () => {
  it("maps drawn marks to sketches and falls back to bars", () => {
    expect(sketchKind("line")).toBe("line");
    expect(sketchKind("arc")).toBe("arc");
    expect(sketchKind("poi")).toBe("map");
    expect(sketchKind("tick")).toBe("point");
    expect(sketchKind("rect")).toBe("heatmap");
    expect(sketchKind(null)).toBe("bar");
    expect(sketchKind("something new")).toBe("bar");
  });
});

describe("seeded shapes", () => {
  it("are the same for the same seed and differ between seeds", () => {
    expect(barHeights(7)).toEqual(barHeights(7));
    expect(linePoints(7)).toEqual(linePoints(7));
    expect(barHeights(7)).not.toEqual(barHeights(8));
  });

  it("draw from a generator in [0, 1), including seed 0", () => {
    for (const seed of [0, 1, 99999]) {
      const rand = seededRandom(seed);
      for (let i = 0; i < 50; i++) {
        const v = rand();
        expect(v).toBeGreaterThanOrEqual(0);
        expect(v).toBeLessThan(1);
      }
    }
  });

  it("stay inside their boxes", () => {
    for (let seed = 0; seed < 30; seed++) {
      for (const h of barHeights(seed)) expect(h).toBeGreaterThanOrEqual(25);
      for (const [x, y] of linePoints(seed)) {
        expect(x).toBeGreaterThanOrEqual(0);
        expect(x).toBeLessThanOrEqual(100);
        expect(y).toBeGreaterThanOrEqual(6);
        expect(y).toBeLessThanOrEqual(46);
      }
      for (const d of [...scatterDots(seed), ...mapDots(seed)]) {
        expect(d.x).toBeGreaterThanOrEqual(6);
        expect(d.x).toBeLessThanOrEqual(94);
        expect(d.y).toBeGreaterThanOrEqual(8);
        expect(d.y).toBeLessThanOrEqual(92);
      }
      for (const b of boxes(seed)) {
        expect(b.low).toBeLessThanOrEqual(b.q1);
        expect(b.q1).toBeLessThan(b.q3);
        expect(b.q3).toBeLessThanOrEqual(b.high);
      }
      expect(heatCells(seed)).toHaveLength(24);
    }
  });

  it("splits an arc into slices that fill the circle, largest first", () => {
    const slices = arcSlices(3);
    const total = slices.reduce((sum, s) => sum + s.share, 0);
    expect(total).toBeCloseTo(1);
    expect(slices[0].offset).toBe(0);
    for (let i = 1; i < slices.length; i++) {
      expect(slices[i].share).toBeLessThanOrEqual(slices[i - 1].share);
      expect(slices[i].offset).toBeCloseTo(slices[i - 1].offset + slices[i - 1].share);
    }
  });

  it("closes an area down to the baseline", () => {
    const points = linePoints(1, 3);
    expect(areaToPoints(points).startsWith("0,50 ")).toBe(true);
    expect(areaToPoints(points).endsWith(" 100,50")).toBe(true);
    expect(areaToPoints([])).toBe("");
  });

  it("hashes string ids to stable non-negative seeds", () => {
    expect(hashSeed("tile-a")).toBe(hashSeed("tile-a"));
    expect(hashSeed("tile-a")).not.toBe(hashSeed("tile-b"));
    expect(hashSeed("")).toBe(0);
  });
});
