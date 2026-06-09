export interface GridDropParams {
  /** Bounding box of the grid root (`.vgl-layout`), viewport-relative. */
  rect: { left: number; top: number; width: number }
  clientX: number
  clientY: number
  cols: number
  rowHeight: number
  /** Single-axis margin (grid uses `[margin, margin]`). */
  margin: number
  /** Width of the tile being dropped, in grid columns. */
  w: number
}

/**
 * Convert a pointer position into a grid cell, mirroring grid-layout-plus's
 * own `calcXY` (round-to-cell against the `.vgl-layout` box). The returned
 * `x` is clamped so a `w`-wide tile never overflows past `cols`.
 *
 * Ported verbatim from flowfile_frontend/.../DashboardsView/gridDrop.ts.
 */
export function computeDropCell({
  rect,
  clientX,
  clientY,
  cols,
  rowHeight,
  margin,
  w,
}: GridDropParams): { x: number; y: number } {
  // Degenerate grid (not laid out yet): drop at the origin.
  if (rect.width <= 0 || cols <= 0) return { x: 0, y: 0 }
  const colW = (rect.width - margin * (cols + 1)) / cols
  const denomX = colW + margin
  const denomY = rowHeight + margin
  const relX = clientX - rect.left
  const relY = clientY - rect.top
  let x = denomX > 0 ? Math.round((relX - margin) / denomX) : 0
  let y = denomY > 0 ? Math.round((relY - margin) / denomY) : 0
  x = Math.max(0, Math.min(x, cols - w))
  y = Math.max(0, y)
  return { x, y }
}
