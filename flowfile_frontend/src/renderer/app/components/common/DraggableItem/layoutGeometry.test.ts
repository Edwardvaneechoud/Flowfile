import { describe, expect, it } from "vitest";

import {
  MINIMIZED_HEADER_PX,
  MIN_PANEL_H,
  clampRectToBounds,
  computeLayout,
  defaultIntent,
  intentFromRect,
  isContainerUsable,
  snapSideForRect,
  type Bounds,
  type PanelConfig,
  type PanelIntent,
  type RenderRect,
} from "./layoutGeometry";

const container: Bounds = { width: 1200, height: 900 };

// Shaped like the designer's panels (drawerRegistry + Canvas defaults); the
// non-zero v offset exercises the offset math the real configs default to 0.
const rightDrawerCfg: PanelConfig = {
  defaultDock: "right",
  h: { behaviour: "fixed", defaultSize: 600, defaultOffset: 0 },
  v: { behaviour: "scale", defaultSize: 700, defaultOffset: 8 },
};

const bottomDockCfg: PanelConfig = {
  defaultDock: "bottom",
  h: { behaviour: "scale", defaultSize: null, defaultOffset: 180 },
  v: { behaviour: "fixed", defaultSize: 225, defaultOffset: 0 },
};

const paletteCfg: PanelConfig = {
  defaultDock: "left",
  h: { behaviour: "fixed", defaultSize: 230, defaultOffset: 0 },
  v: { behaviour: "scale", defaultSize: null, defaultOffset: 0 },
};

const freeCfg: PanelConfig = {
  defaultDock: "free",
  h: { behaviour: "fixed", defaultSize: 400, defaultOffset: 100 },
  v: { behaviour: "fixed", defaultSize: 300, defaultOffset: 100 },
};

const intentWith = (cfg: PanelConfig, patch: Partial<PanelIntent>): PanelIntent => ({
  ...defaultIntent(cfg),
  ...patch,
});

describe("isContainerUsable", () => {
  it("rejects zero, tiny, and non-finite bounds", () => {
    expect(isContainerUsable({ width: 0, height: 0 })).toBe(false);
    expect(isContainerUsable({ width: 199, height: 900 })).toBe(false);
    expect(isContainerUsable({ width: 1200, height: 149 })).toBe(false);
    expect(isContainerUsable({ width: NaN, height: 900 })).toBe(false);
  });

  it("accepts the floor and above", () => {
    expect(isContainerUsable({ width: 200, height: 150 })).toBe(true);
    expect(isContainerUsable(container)).toBe(true);
  });
});

describe("computeLayout — docked", () => {
  it("right dock defaults: flush right, default width/offset/height", () => {
    const rect = computeLayout(defaultIntent(rightDrawerCfg), rightDrawerCfg, container, false);
    expect(rect).toEqual({ left: 600, top: 8, width: 600, height: 700 });
  });

  it("right dock with a user gap derives height as a span", () => {
    const intent = intentWith(rightDrawerCfg, {
      v: { size: null, offset: 8, gap: 200 },
    });
    const rect = computeLayout(intent, rightDrawerCfg, container, false);
    expect(rect).toEqual({ left: 600, top: 8, width: 600, height: 692 });
  });

  it("scale-gap invariance: a transient container shrink cannot corrupt recovery", () => {
    const intent = intentWith(rightDrawerCfg, {
      v: { size: null, offset: 8, gap: 200 },
    });
    const before = computeLayout(intent, rightDrawerCfg, container, false);
    const shrunk = computeLayout(intent, rightDrawerCfg, { width: 1200, height: 300 }, false);
    // Render floor engages during the shrink but the intent is untouched...
    expect(shrunk?.height).toBe(MIN_PANEL_H);
    // ...so restoring the container restores the exact original rect.
    const after = computeLayout(intent, rightDrawerCfg, container, false);
    expect(after).toEqual(before);
  });

  it("returns null for an unusable container (caller keeps last good rect)", () => {
    const intent = defaultIntent(rightDrawerCfg);
    expect(computeLayout(intent, rightDrawerCfg, { width: 0, height: 0 }, false)).toBeNull();
    expect(computeLayout(intent, rightDrawerCfg, { width: 180, height: 900 }, false)).toBeNull();
  });

  it("bottom dock defaults: flush bottom, span width from the left offset", () => {
    const rect = computeLayout(defaultIntent(bottomDockCfg), bottomDockCfg, container, false);
    expect(rect).toEqual({ left: 180, top: 675, width: 1020, height: 225 });
  });

  it("bottom dock minimized positions by the header height", () => {
    const rect = computeLayout(defaultIntent(bottomDockCfg), bottomDockCfg, container, true);
    expect(rect?.top).toBe(900 - MINIMIZED_HEADER_PX);
    expect(rect?.height).toBe(225);
  });

  it("left palette defaults to full height at the left edge", () => {
    const rect = computeLayout(defaultIntent(paletteCfg), paletteCfg, container, false);
    expect(rect).toEqual({ left: 0, top: 0, width: 230, height: 900 });
  });

  it("a fill axis stretches to the container and ignores a stored gap", () => {
    const cfg: PanelConfig = {
      defaultDock: "top",
      h: { behaviour: "fill", defaultSize: null, defaultOffset: 20 },
      v: { behaviour: "fixed", defaultSize: 200, defaultOffset: 0 },
    };
    const intent = intentWith(cfg, { h: { size: null, offset: null, gap: 300 } });
    const rect = computeLayout(intent, cfg, container, false);
    expect(rect).toEqual({ left: 20, top: 0, width: 1180, height: 200 });
  });

  it("clamps an oversized panel to the container", () => {
    const intent = intentWith(rightDrawerCfg, {
      h: { size: 5000, offset: null, gap: null },
    });
    const rect = computeLayout(intent, rightDrawerCfg, container, false);
    expect(rect?.width).toBe(1200);
    expect(rect?.left).toBe(0);
  });
});

describe("computeLayout — fullscreen and free", () => {
  it("fullscreen fills the container", () => {
    const intent = intentWith(rightDrawerCfg, { fullScreen: true });
    expect(computeLayout(intent, rightDrawerCfg, container, false)).toEqual({
      left: 0,
      top: 0,
      width: 1200,
      height: 900,
    });
  });

  it("free near-anchored uses offsets", () => {
    const intent = intentWith(freeCfg, {
      dock: "free",
      h: { size: 400, offset: 50, gap: null },
      v: { size: 300, offset: 70, gap: null },
    });
    expect(computeLayout(intent, freeCfg, container, false)).toEqual({
      left: 50,
      top: 70,
      width: 400,
      height: 300,
    });
  });

  it("free far-anchored keeps the gap to the far edge", () => {
    const intent = intentWith(freeCfg, {
      dock: "free",
      h: { size: 400, offset: null, gap: 50 },
      v: { size: 300, offset: null, gap: 60 },
    });
    expect(computeLayout(intent, freeCfg, container, false)).toEqual({
      left: 750,
      top: 540,
      width: 400,
      height: 300,
    });
  });

  it("free far-anchored minimized positions by the header height", () => {
    const intent = intentWith(freeCfg, {
      dock: "free",
      h: { size: 400, offset: null, gap: 0 },
      v: { size: 300, offset: null, gap: 0 },
    });
    const rect = computeLayout(intent, freeCfg, container, true);
    expect(rect?.top).toBe(900 - MINIMIZED_HEADER_PX);
  });

  it("free panels are clamped fully inside the container", () => {
    const intent = intentWith(freeCfg, {
      dock: "free",
      h: { size: 400, offset: 5000, gap: null },
      v: { size: 300, offset: 5000, gap: null },
    });
    expect(computeLayout(intent, freeCfg, container, false)).toEqual({
      left: 800,
      top: 600,
      width: 400,
      height: 300,
    });
  });
});

describe("intentFromRect", () => {
  it("right dock records width as size and the vertical span as offset+gap", () => {
    const rect: RenderRect = { left: 600, top: 8, width: 600, height: 692 };
    const intent = intentFromRect(rect, "right", rightDrawerCfg, container);
    expect(intent).toEqual({
      dock: "right",
      h: { size: 600, offset: null, gap: null },
      v: { size: null, offset: 8, gap: 200 },
      fullScreen: false,
    });
  });

  it("bottom dock records height as size and the horizontal span", () => {
    const rect: RenderRect = { left: 180, top: 675, width: 1020, height: 225 };
    const intent = intentFromRect(rect, "bottom", bottomDockCfg, container);
    expect(intent).toEqual({
      dock: "bottom",
      h: { size: null, offset: 180, gap: 0 },
      v: { size: 225, offset: null, gap: null },
      fullScreen: false,
    });
  });

  it("free anchors each axis by the panel-center half", () => {
    const nearRect: RenderRect = { left: 100, top: 100, width: 300, height: 200 };
    expect(intentFromRect(nearRect, "free", freeCfg, container)).toEqual({
      dock: "free",
      h: { size: 300, offset: 100, gap: null },
      v: { size: 200, offset: 100, gap: null },
      fullScreen: false,
    });
    const farRect: RenderRect = { left: 800, top: 650, width: 300, height: 200 };
    expect(intentFromRect(farRect, "free", freeCfg, container)).toEqual({
      dock: "free",
      h: { size: 300, offset: null, gap: 100 },
      v: { size: 200, offset: null, gap: 50 },
      fullScreen: false,
    });
  });

  it("round-trips through computeLayout for in-bounds rects", () => {
    const cases: Array<{ rect: RenderRect; dock: PanelIntent["dock"]; cfg: PanelConfig }> = [
      { rect: { left: 640, top: 8, width: 560, height: 700 }, dock: "right", cfg: rightDrawerCfg },
      {
        rect: { left: 200, top: 660, width: 1000, height: 240 },
        dock: "bottom",
        cfg: bottomDockCfg,
      },
      { rect: { left: 0, top: 40, width: 260, height: 860 }, dock: "left", cfg: paletteCfg },
      { rect: { left: 750, top: 540, width: 400, height: 300 }, dock: "free", cfg: freeCfg },
      { rect: { left: 50, top: 70, width: 400, height: 300 }, dock: "free", cfg: freeCfg },
    ];
    for (const { rect, dock, cfg } of cases) {
      const intent = intentFromRect(rect, dock, cfg, container);
      expect(computeLayout(intent, cfg, container, false)).toEqual(rect);
    }
  });
});

describe("clampRectToBounds", () => {
  it("pulls an off-canvas rect fully inside (so commits never store negatives)", () => {
    expect(clampRectToBounds({ left: -80, top: -20, width: 400, height: 300 }, container)).toEqual({
      left: 0,
      top: 0,
      width: 400,
      height: 300,
    });
    expect(clampRectToBounds({ left: 1100, top: 850, width: 400, height: 300 }, container)).toEqual(
      { left: 800, top: 600, width: 400, height: 300 },
    );
  });

  it("shrinks a rect larger than the container", () => {
    expect(clampRectToBounds({ left: 0, top: 0, width: 2000, height: 1200 }, container)).toEqual({
      left: 0,
      top: 0,
      width: 1200,
      height: 900,
    });
  });

  it("leaves an in-bounds rect untouched", () => {
    const rect: RenderRect = { left: 50, top: 60, width: 300, height: 200 };
    expect(clampRectToBounds(rect, container)).toEqual(rect);
  });

  it("clamps a minimized rect by its on-screen header height", () => {
    // Full height 500 would force top <= 400; the visible 35px strip fits
    // down to 900 - 35 = 865.
    const rect: RenderRect = { left: 50, top: 700, width: 300, height: 500 };
    expect(clampRectToBounds(rect, container, true).top).toBe(700);
    expect(clampRectToBounds({ ...rect, top: 880 }, container, true).top).toBe(865);
    expect(clampRectToBounds(rect, container, false).top).toBe(400);
  });
});

describe("minimized free-panel round-trip (drag-release must not teleport)", () => {
  it("a minimized panel released in the lower half re-renders at the same top", () => {
    // Stored height 500, minimized (35px on screen), released at top=500.
    const released: RenderRect = { left: 300, top: 500, width: 300, height: 500 };
    const settled = clampRectToBounds(released, container, true);
    const intent = intentFromRect(settled, "free", freeCfg, container, true);
    const rendered = computeLayout(intent, freeCfg, container, true);
    expect(rendered?.top).toBe(500);
    expect(rendered?.left).toBe(300);
    // The stored expanded height survives the round-trip untouched.
    expect(intent.v.size).toBe(500);
  });
});

describe("snapSideForRect", () => {
  const allowed: Array<"top" | "bottom" | "left" | "right"> = ["right", "bottom", "left", "top"];

  it("snaps to a side within the zone and not outside it", () => {
    const nearRight: RenderRect = { left: 770, top: 300, width: 400, height: 200 };
    expect(snapSideForRect(nearRight, container, allowed)).toBe("right");
    const farFromAll: RenderRect = { left: 400, top: 300, width: 300, height: 200 };
    expect(snapSideForRect(farFromAll, container, allowed)).toBeNull();
  });

  it("the nearest edge wins at a corner", () => {
    // 20px from the bottom, 35px from the right.
    const corner: RenderRect = { left: 765, top: 680, width: 400, height: 200 };
    expect(snapSideForRect(corner, container, allowed)).toBe("bottom");
  });

  it("only allowed sides are considered", () => {
    const nearLeft: RenderRect = { left: 10, top: 300, width: 300, height: 200 };
    expect(snapSideForRect(nearLeft, container, ["right"])).toBeNull();
    expect(snapSideForRect(nearLeft, container, ["left"])).toBe("left");
  });

  it("ties resolve in allowed order", () => {
    // 30px from both the right and the bottom edge.
    const tie: RenderRect = { left: 770, top: 670, width: 400, height: 200 };
    expect(snapSideForRect(tie, container, ["right", "bottom"])).toBe("right");
    expect(snapSideForRect(tie, container, ["bottom", "right"])).toBe("bottom");
  });
});
