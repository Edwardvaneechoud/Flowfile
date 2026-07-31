// Pure placement math for the column-stats popover. No Vue, no DOM: plain
// numbers in, plain numbers out, so it unit-tests in node env.
//
// The side is chosen once from the anchor, and the panel pins the edge that
// touches the anchor (`top` when below, `bottom` when above). A later
// content-height change therefore grows or shrinks the free edge and can never
// move the popover — which is what lets the first paint be the final one, with
// no measuring pass and no reposition.

/** Panel width. Owned here, not in CSS, so the left clamp can never desync. */
export const PANEL_WIDTH = 320;
/** Keep-out band along every viewport edge. */
export const MARGIN = 10;
/** Gap between the anchor and the panel. */
export const OFFSET = 6;
// Tallest ordinary body (badges + null bar + counts + bound rows). Only decides
// which side reads better; underestimating is harmless because maxHeight scrolls.
const PREFERRED_HEIGHT = 320;

/** The subset of DOMRect the math needs — a DOMRect satisfies this as-is. */
export interface AnchorBox {
  top: number;
  bottom: number;
  left: number;
  width: number;
}

export interface Viewport {
  width: number;
  height: number;
}

export interface Placement {
  side: "below" | "above";
  left: number;
  /** px from the viewport top; null when the panel is pinned by `bottom`. */
  top: number | null;
  /** px from the viewport bottom; null when the panel is pinned by `top`. */
  bottom: number | null;
  maxHeight: number;
}

/**
 * Where to put the popover for a given anchor, without knowing its height.
 * Drops down by default and flips up only when below cannot hold a full panel
 * and above is roomier.
 */
export function computePlacement(
  anchor: AnchorBox,
  viewport: Viewport,
  width = PANEL_WIDTH,
): Placement {
  const centered = anchor.left + anchor.width / 2 - width / 2;
  // Math.max outermost: a viewport narrower than the panel pins to the left
  // margin instead of going negative.
  const left = Math.max(MARGIN, Math.min(centered, viewport.width - width - MARGIN));

  const spaceBelow = viewport.height - anchor.bottom - OFFSET - MARGIN;
  const spaceAbove = anchor.top - OFFSET - MARGIN;
  const below = spaceBelow >= PREFERRED_HEIGHT || spaceBelow >= spaceAbove;

  return below
    ? {
        side: "below",
        left,
        top: anchor.bottom + OFFSET,
        bottom: null,
        maxHeight: Math.max(0, spaceBelow),
      }
    : {
        side: "above",
        left,
        top: null,
        bottom: viewport.height - anchor.top + OFFSET,
        maxHeight: Math.max(0, spaceAbove),
      };
}
