/** Geometry for a KPI's trend line: pure, so it is testable and needs no chart library. */

export interface SparkInput {
  t: number;
  value: number;
}

export interface SparkPoint {
  x: number;
  y: number;
}

export interface SparkGeometry {
  points: SparkPoint[];
  /** SVG path of the line; empty for fewer than two points. */
  line: string;
  /** The line closed down to the bottom edge, for the soft area under it. */
  area: string;
}

const round = (n: number): number => Math.round(n * 100) / 100;

/**
 * Points placed by time across ``width`` and by value across ``height``, inside ``pad`` on every
 * side so a 2px line and the end marker are not clipped. A missing bucket leaves a wider step,
 * never a hidden one. A flat series sits mid-height; a single point sits in the middle.
 */
export const sparklineGeometry = (
  series: SparkInput[],
  width: number,
  height: number,
  pad = 4,
): SparkGeometry => {
  if (!series.length || width <= 0 || height <= 0) return { points: [], line: "", area: "" };
  const innerW = Math.max(0, width - 2 * pad);
  const innerH = Math.max(0, height - 2 * pad);
  const t0 = series[0].t;
  const tSpan = series[series.length - 1].t - t0;
  let lo = Infinity;
  let hi = -Infinity;
  for (const p of series) {
    lo = Math.min(lo, p.value);
    hi = Math.max(hi, p.value);
  }
  const vSpan = hi - lo;
  const points = series.map((p) => ({
    x: round(pad + (tSpan > 0 ? ((p.t - t0) / tSpan) * innerW : innerW / 2)),
    y: round(pad + (vSpan > 0 ? (1 - (p.value - lo) / vSpan) * innerH : innerH / 2)),
  }));
  if (points.length < 2) return { points, line: "", area: "" };
  const line = points.map((p, i) => `${i ? "L" : "M"}${p.x} ${p.y}`).join(" ");
  const bottom = round(height - pad);
  const area = `${line} L${points[points.length - 1].x} ${bottom} L${points[0].x} ${bottom} Z`;
  return { points, line, area };
};

/** Index of the point nearest ``x`` (points are in x order); -1 when there are none. */
export const nearestPointIndex = (points: SparkPoint[], x: number): number => {
  let best = -1;
  let bestDist = Infinity;
  points.forEach((p, i) => {
    const d = Math.abs(p.x - x);
    if (d < bestDist) {
      best = i;
      bestDist = d;
    }
  });
  return best;
};

/** Smallest box height, in px, a KPI tile needs to show the line under its number. */
export const TREND_MIN_HEIGHT = 22;
const TILE_PADDING = 20;
const LABEL_HEIGHT = 16;
const DELTA_HEIGHT = 18;
const AUTO_VALUE_MIN = 28;

/**
 * Whether a KPI tile of ``tileHeight`` px has room for the trend under its number: the number
 * keeps its own size (``valuePx`` for a fixed one, the auto fit's smallest otherwise), so on a
 * tile too small for both the line gives way.
 */
export const trendFits = (
  tileHeight: number,
  valuePx: number | null,
  hasDeltaRow: boolean,
): boolean => {
  const value = valuePx == null ? AUTO_VALUE_MIN : valuePx * 1.1;
  const used = TILE_PADDING + LABEL_HEIGHT + value + (hasDeltaRow ? DELTA_HEIGHT : 0);
  return tileHeight - used >= TREND_MIN_HEIGHT;
};
