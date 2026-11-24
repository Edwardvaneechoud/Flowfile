/** What a chart tile's sketch draws on a library card when its chart has no saved picture. */
export type SketchKind = "bar" | "line" | "area" | "point" | "arc" | "boxplot" | "map" | "heatmap";

const KIND_BY_MARK: Record<string, SketchKind> = {
  bar: "bar",
  line: "line",
  area: "area",
  point: "point",
  circle: "point",
  tick: "point",
  square: "point",
  arc: "arc",
  boxplot: "boxplot",
  poi: "map",
  choropleth: "map",
  rect: "heatmap",
  table: "heatmap",
};

/** The sketch for a chart's drawn mark (`CatalogVisualization.chart_type`); unknown or null is a bar chart. */
export function sketchKind(chartType: string | null | undefined): SketchKind {
  return (chartType && KIND_BY_MARK[chartType]) || "bar";
}

/** A small deterministic generator, so a chart keeps the same sketch on every render. */
export function seededRandom(seed: number): () => number {
  let state = (Math.abs(Math.floor(seed)) * 2654435761 + 1013904223) >>> 0 || 1;
  return () => {
    state ^= state << 13;
    state >>>= 0;
    state ^= state >>> 17;
    state ^= state << 5;
    state >>>= 0;
    return state / 4294967296;
  };
}

/** A stable number from a string id, for tiles that have no chart id to seed from. */
export function hashSeed(id: string): number {
  let hash = 0;
  for (let i = 0; i < id.length; i++) hash = (hash * 31 + id.charCodeAt(i)) | 0;
  return Math.abs(hash);
}

/** Bar heights as percentages of the plot, between 25 and 95. */
export function barHeights(seed: number): number[] {
  const rand = seededRandom(seed);
  const count = 5 + Math.floor(rand() * 3);
  return Array.from({ length: count }, () => Math.round(25 + rand() * 70));
}

/** A wandering series in a 100 x 50 box (y down), drifting upward so it reads as a trend. */
export function linePoints(seed: number, count = 9): Array<[number, number]> {
  const rand = seededRandom(seed);
  let y = 30 + rand() * 12;
  return Array.from({ length: count }, (_, i) => {
    y = Math.min(46, Math.max(6, y + (rand() - 0.6) * 12));
    return [Math.round((i / (count - 1)) * 1000) / 10, Math.round(y * 10) / 10];
  });
}

/** SVG `points` for a line, and the same line closed down to the baseline for an area. */
export function lineToPoints(points: Array<[number, number]>): string {
  return points.map(([x, y]) => `${x},${y}`).join(" ");
}

export function areaToPoints(points: Array<[number, number]>): string {
  if (!points.length) return "";
  return `0,50 ${lineToPoints(points)} 100,50`;
}

export interface SketchDot {
  /** Percentages of the plot. */
  x: number;
  y: number;
  /** Index into the categorical palette. */
  slot: number;
}

/** Scattered dots with a loose upward trend, in up to three series. */
export function scatterDots(seed: number, count = 16): SketchDot[] {
  const rand = seededRandom(seed);
  return Array.from({ length: count }, () => {
    const x = 6 + rand() * 88;
    const y = Math.min(92, Math.max(8, 88 - x * 0.7 + (rand() - 0.5) * 40));
    return { x: Math.round(x), y: Math.round(y), slot: Math.floor(rand() * 3) };
  });
}

/** Points clustered around a few centres, like locations on a map. */
export function mapDots(seed: number, count = 18): SketchDot[] {
  const rand = seededRandom(seed);
  const centres = Array.from({ length: 3 }, () => [20 + rand() * 60, 25 + rand() * 50]);
  return Array.from({ length: count }, (_, i) => {
    const [cx, cy] = centres[i % centres.length];
    return {
      x: Math.round(Math.min(94, Math.max(6, cx + (rand() - 0.5) * 30))),
      y: Math.round(Math.min(92, Math.max(8, cy + (rand() - 0.5) * 30))),
      slot: i % centres.length,
    };
  });
}

export interface ArcSlice {
  /** Fraction of the circle, the slices summing to 1. */
  share: number;
  /** Fraction of the circle before this slice. */
  offset: number;
  slot: number;
}

/** Three to four slices, largest first. */
export function arcSlices(seed: number): ArcSlice[] {
  const rand = seededRandom(seed);
  const count = 3 + Math.floor(rand() * 2);
  const weights = Array.from({ length: count }, () => 0.4 + rand()).sort((a, b) => b - a);
  const total = weights.reduce((sum, w) => sum + w, 0);
  let offset = 0;
  return weights.map((w, slot) => {
    const share = w / total;
    const slice = { share, offset, slot };
    offset += share;
    return slice;
  });
}

export interface SketchBox {
  /** Percentages of the plot height, measured from the bottom. */
  low: number;
  q1: number;
  q3: number;
  high: number;
}

export function boxes(seed: number): SketchBox[] {
  const rand = seededRandom(seed);
  return Array.from({ length: 4 }, () => {
    const mid = 30 + rand() * 40;
    const half = 8 + rand() * 10;
    return {
      low: Math.round(Math.max(5, mid - half - 8 - rand() * 10)),
      q1: Math.round(mid - half),
      q3: Math.round(mid + half),
      high: Math.round(Math.min(95, mid + half + 8 + rand() * 10)),
    };
  });
}

/** Cell strengths from 0 to 1 for a 6 x 4 heatmap, row by row. */
export function heatCells(seed: number): number[] {
  const rand = seededRandom(seed);
  return Array.from({ length: 24 }, () => Math.round(rand() * 100) / 100);
}
