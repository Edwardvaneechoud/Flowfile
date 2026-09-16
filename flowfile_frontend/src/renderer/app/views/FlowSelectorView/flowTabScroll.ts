/**
 * Pure overflow-scroll rules for the flow tab strip, kept free of DOM/Vue
 * so they can be unit-tested in the node vitest environment.
 */
export interface StripMetrics {
  scrollLeft: number;
  clientWidth: number;
  scrollWidth: number;
}

export interface WheelLike {
  deltaX: number;
  deltaY: number;
  deltaMode: number;
  ctrlKey: boolean;
}

const DOM_DELTA_LINE = 1;
const DOM_DELTA_PAGE = 2;
const LINE_HEIGHT_PX = 16;

export const stripMetrics = (el: StripMetrics): StripMetrics => ({
  scrollLeft: el.scrollLeft,
  clientWidth: el.clientWidth,
  scrollWidth: el.scrollWidth,
});

export const overflowState = (m: StripMetrics): { left: boolean; right: boolean } => ({
  left: m.scrollLeft > 0,
  right: m.scrollLeft + m.clientWidth < m.scrollWidth - 1,
});

/**
 * Horizontal pixel delta a wheel event should apply to the strip, or null when
 * the event must be left to the browser (pinch-zoom, no overflow, or a gesture
 * that is already horizontal).
 */
export const wheelScrollDelta = (event: WheelLike, m: StripMetrics): number | null => {
  if (event.ctrlKey) return null;
  if (m.scrollWidth <= m.clientWidth) return null;
  if (Math.abs(event.deltaY) <= Math.abs(event.deltaX)) return null;
  if (event.deltaMode === DOM_DELTA_LINE) return event.deltaY * LINE_HEIGHT_PX;
  if (event.deltaMode === DOM_DELTA_PAGE) return event.deltaY * m.clientWidth;
  return event.deltaY;
};
