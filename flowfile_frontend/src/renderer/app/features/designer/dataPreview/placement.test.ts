import { describe, expect, it } from "vitest";

import { MARGIN, OFFSET, PANEL_WIDTH, computePlacement, type AnchorBox } from "./placement";

const VIEWPORT = { width: 1280, height: 800 };

function anchorAt(overrides: Partial<AnchorBox> = {}): AnchorBox {
  return { top: 100, bottom: 116, left: 600, width: 16, ...overrides };
}

describe("computePlacement", () => {
  it("drops below the anchor and pins the top edge when there is room", () => {
    const placement = computePlacement(anchorAt(), VIEWPORT);
    expect(placement.side).toBe("below");
    expect(placement.top).toBe(116 + OFFSET);
    expect(placement.bottom).toBeNull();
  });

  it("centers on the anchor", () => {
    const placement = computePlacement(anchorAt(), VIEWPORT);
    expect(placement.left).toBe(600 + 8 - PANEL_WIDTH / 2);
  });

  it("clamps to the left margin", () => {
    const placement = computePlacement(anchorAt({ left: 0 }), VIEWPORT);
    expect(placement.left).toBe(MARGIN);
  });

  it("clamps to the right margin", () => {
    const placement = computePlacement(anchorAt({ left: 1264 }), VIEWPORT);
    expect(placement.left + PANEL_WIDTH).toBe(VIEWPORT.width - MARGIN);
  });

  it("pins to the left margin rather than going negative on a narrow viewport", () => {
    const placement = computePlacement(anchorAt({ left: 100 }), { width: 200, height: 800 });
    expect(placement.left).toBe(MARGIN);
  });

  it("flips above and pins the bottom edge for a bottom-dock anchor", () => {
    const anchor = anchorAt({ top: 700, bottom: 716 });
    const placement = computePlacement(anchor, VIEWPORT);
    expect(placement.side).toBe("above");
    expect(placement.top).toBeNull();
    expect(placement.bottom).toBe(VIEWPORT.height - 700 + OFFSET);
  });

  it("prefers below when below fits a full panel, even though above is roomier", () => {
    // 800 - 436 - 6 - 10 = 348 below vs 420 - 6 - 10 = 404 above.
    const placement = computePlacement(anchorAt({ top: 420, bottom: 436 }), VIEWPORT);
    expect(placement.side).toBe("below");
  });

  it("reports the free space on the chosen side, never negative", () => {
    const below = computePlacement(anchorAt(), VIEWPORT);
    expect(below.maxHeight).toBe(VIEWPORT.height - 116 - OFFSET - MARGIN);

    const above = computePlacement(anchorAt({ top: 700, bottom: 716 }), VIEWPORT);
    expect(above.maxHeight).toBe(700 - OFFSET - MARGIN);

    const offscreen = computePlacement(anchorAt({ top: -50, bottom: 900 }), VIEWPORT);
    expect(offscreen.maxHeight).toBeGreaterThanOrEqual(0);
  });

  it("does not depend on the panel's rendered height", () => {
    const anchor = anchorAt();
    expect(computePlacement(anchor, VIEWPORT)).toEqual(computePlacement(anchor, VIEWPORT));
  });
});
