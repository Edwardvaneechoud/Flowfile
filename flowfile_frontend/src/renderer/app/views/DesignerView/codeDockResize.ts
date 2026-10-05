/** Code-dock width math, kept pure so the resize gesture is unit-testable. */
export const CODE_DOCK_MIN_WIDTH = 360;
export const CODE_DOCK_CLOSE_DRAG = 100;
export const CODE_DOCK_MAX_STRETCH = 48;
/** The canvas keeps at least this much of the window. */
const CANVAS_MIN_WIDTH = 360;

/** A width the pane may settle at: at least the minimum, even in a window too narrow for both. */
export const clampDockWidth = (width: number, innerWidth: number): number =>
  Math.round(
    Math.min(
      Math.max(width, CODE_DOCK_MIN_WIDTH),
      Math.max(CODE_DOCK_MIN_WIDTH, innerWidth - CANVAS_MIN_WIDTH),
    ),
  );

/** How far the pane gives way when dragged `overshoot` px past its minimum. */
export const stretch = (overshoot: number): number =>
  CODE_DOCK_MAX_STRETCH * (1 - Math.exp(-overshoot / CODE_DOCK_MAX_STRETCH));

/** The width shown mid-drag: rubber-bands past the minimum (arming a close), clamped otherwise. */
export const dragWidth = (
  startWidth: number,
  dx: number,
  innerWidth: number,
): { width: number; closeArmed: boolean } => {
  const width = startWidth + dx;
  const overshoot = CODE_DOCK_MIN_WIDTH - width;
  if (overshoot > 0) {
    return {
      width: Math.round(CODE_DOCK_MIN_WIDTH - stretch(overshoot)),
      closeArmed: overshoot >= CODE_DOCK_CLOSE_DRAG,
    };
  }
  return { width: clampDockWidth(width, innerWidth), closeArmed: false };
};
