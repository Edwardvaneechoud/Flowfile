// Pure geometry for the canvas overlay panels. No Vue, no DOM.
//
// The core contract of the overlay system: localStorage holds USER INTENT
// (chosen dock side, sizes, offsets — written only on explicit gestures) and
// the on-screen rect is DERIVED here from intent × container every time. A
// mis-measured container can therefore produce at worst one bad frame, never
// a bad save: recovery is automatic once the container reports sane bounds.

export type AxisBehaviour = "scale" | "fixed" | "fill";
export type DockSide = "top" | "bottom" | "left" | "right";
export type PanelDock = DockSide | "free";

export interface Bounds {
  width: number;
  height: number;
}

export interface RenderRect {
  left: number;
  top: number;
  width: number;
  height: number;
}

// Per-axis user intent. Exactly which fields apply depends on the dock mode
// and the axis behaviour:
//  - "fixed" axis: `size` in px.
//  - "scale" axis (docked): the panel spans [offset, containerExtent - gap];
//    `size` is derived, so a stale container can never corrupt it.
//  - "fill" axis: spans [offset, containerExtent]; user gap is ignored.
//  - free mode: `size` in px, positioned by `offset` (near-edge anchored) OR
//    `gap` (far-edge anchored) — never both.
// null = "the user never chose" → fall back to the panel's config default.
export interface AxisIntent {
  size: number | null;
  offset: number | null;
  gap: number | null;
}

export interface PanelIntent {
  dock: PanelDock;
  h: AxisIntent;
  v: AxisIntent;
  fullScreen: boolean;
}

export interface AxisConfig {
  behaviour: AxisBehaviour;
  defaultSize: number | null;
  defaultOffset: number;
}

export interface PanelConfig {
  defaultDock: PanelDock;
  h: AxisConfig;
  v: AxisConfig;
}

// Interactive resize floors. A persisted extent at or below these is provably
// not user-produced (gesture handlers clamp above them), which is what lets
// the v3 migration treat such records as corruption.
export const MIN_PANEL_W = 150;
export const MIN_PANEL_H = 100;

// Rendered height of a minimized panel (header only, CSS-driven).
export const MINIMIZED_HEADER_PX = 35;

// A docked panel's header must travel this far before it pops out to free
// mode — deliberate undocking, not cursor jitter (the old 4px threshold let a
// sloppy dblclick permanently undock a drawer).
export const POP_OUT_THRESHOLD_PX = 48;

// Releasing a dragged panel with an edge inside this zone re-docks it to that
// side (magnetic snap-back).
export const SNAP_ZONE_PX = 40;

// Below this the container is treated as unreliable (mid-layout, mid-route
// transition, pre-window-restore) and no geometry is derived — the caller
// keeps its last good rect. A real canvas is always far larger.
export const MIN_CONTAINER_W = 200;
export const MIN_CONTAINER_H = 150;

export const isContainerUsable = (c: Bounds): boolean =>
  Number.isFinite(c.width) &&
  Number.isFinite(c.height) &&
  c.width >= MIN_CONTAINER_W &&
  c.height >= MIN_CONTAINER_H;

export const defaultIntent = (cfg: PanelConfig): PanelIntent => ({
  dock: cfg.defaultDock,
  h: { size: null, offset: null, gap: null },
  v: { size: null, offset: null, gap: null },
  fullScreen: false,
});

const clamp = (value: number, min: number, max: number): number =>
  Math.max(min, Math.min(value, max));

// Span axes (scale/fill): position from the near edge, size to the far edge.
const spanAxis = (
  intent: AxisIntent,
  cfg: AxisConfig,
  extent: number,
): { offset: number; size: number } => {
  const offset = clamp(intent.offset ?? cfg.defaultOffset, 0, extent);
  let size: number;
  if (cfg.behaviour === "fill") {
    size = extent - offset;
  } else if (intent.gap !== null) {
    size = extent - offset - intent.gap;
  } else if (cfg.defaultSize !== null) {
    size = cfg.defaultSize;
  } else {
    size = extent - offset;
  }
  return { offset, size };
};

const fixedAxisSize = (intent: AxisIntent, cfg: AxisConfig, fallback: number): number =>
  intent.size ?? cfg.defaultSize ?? fallback;

export function computeLayout(
  intent: PanelIntent,
  cfg: PanelConfig,
  container: Bounds,
  minimized: boolean,
): RenderRect | null {
  if (!isContainerUsable(container)) return null;

  if (intent.fullScreen) {
    return { left: 0, top: 0, width: container.width, height: container.height };
  }

  let left: number;
  let top: number;
  let width: number;
  let height: number;

  const verticalDockedAxis = (): { top: number; height: number } => {
    if (cfg.v.behaviour === "fixed") {
      return {
        top: clamp(intent.v.offset ?? cfg.v.defaultOffset, 0, container.height),
        height: fixedAxisSize(intent.v, cfg.v, 300),
      };
    }
    const { offset, size } = spanAxis(intent.v, cfg.v, container.height);
    return { top: offset, height: size };
  };

  const horizontalDockedAxis = (): { left: number; width: number } => {
    if (cfg.h.behaviour === "fixed") {
      return {
        left: clamp(intent.h.offset ?? cfg.h.defaultOffset, 0, container.width),
        width: fixedAxisSize(intent.h, cfg.h, 300),
      };
    }
    const { offset, size } = spanAxis(intent.h, cfg.h, container.width);
    return { left: offset, width: size };
  };

  switch (intent.dock) {
    case "right": {
      width = fixedAxisSize(intent.h, cfg.h, 300);
      width = clamp(width, MIN_PANEL_W, container.width);
      left = container.width - width;
      ({ top, height } = verticalDockedAxis());
      break;
    }
    case "left": {
      width = fixedAxisSize(intent.h, cfg.h, 300);
      width = clamp(width, MIN_PANEL_W, container.width);
      left = 0;
      ({ top, height } = verticalDockedAxis());
      break;
    }
    case "bottom": {
      height = fixedAxisSize(intent.v, cfg.v, 300);
      height = clamp(height, MIN_PANEL_H, container.height);
      const effHeight = minimized ? MINIMIZED_HEADER_PX : height;
      top = container.height - effHeight;
      ({ left, width } = horizontalDockedAxis());
      break;
    }
    case "top": {
      height = fixedAxisSize(intent.v, cfg.v, 300);
      height = clamp(height, MIN_PANEL_H, container.height);
      top = 0;
      ({ left, width } = horizontalDockedAxis());
      break;
    }
    case "free":
    default: {
      width = fixedAxisSize(intent.h, cfg.h, 400);
      height = fixedAxisSize(intent.v, cfg.v, 300);
      width = clamp(width, MIN_PANEL_W, container.width);
      height = clamp(height, MIN_PANEL_H, container.height);
      const effHeight = minimized ? MINIMIZED_HEADER_PX : height;
      left =
        intent.h.gap !== null
          ? container.width - width - intent.h.gap
          : (intent.h.offset ?? cfg.h.defaultOffset);
      top =
        intent.v.gap !== null
          ? container.height - effHeight - intent.v.gap
          : (intent.v.offset ?? cfg.v.defaultOffset);
      break;
    }
  }

  width = clamp(width, MIN_PANEL_W, container.width);
  height = clamp(height, MIN_PANEL_H, container.height);
  const effHeight = minimized ? MINIMIZED_HEADER_PX : height;
  left = clamp(left, 0, Math.max(0, container.width - width));
  top = clamp(top, 0, Math.max(0, container.height - effHeight));

  return { left, top, width, height };
}

// Pull a rect fully inside the container. Gesture rects are deliberately
// unclamped while dragging (panels may hang off-canvas mid-gesture, as
// before); the commit must record the settled, in-bounds geometry — negative
// offsets would be rejected by sanitizeIntent on the next load.
export function clampRectToBounds(
  rect: RenderRect,
  container: Bounds,
  minimized = false,
): RenderRect {
  const width = Math.min(rect.width, container.width);
  const height = Math.min(rect.height, container.height);
  // A minimized panel renders as a 35px header while rect.height keeps the
  // stored expanded size — clamp its position by what's actually on screen.
  const effHeight = minimized ? MINIMIZED_HEADER_PX : height;
  return {
    width,
    height,
    left: clamp(rect.left, 0, Math.max(0, container.width - width)),
    top: clamp(rect.top, 0, Math.max(0, container.height - effHeight)),
  };
}

// Gesture-stop inverse of computeLayout: capture the final on-screen rect as
// user intent (sizes for fixed axes, offset+gap spans for scale/fill axes,
// anchored offsets for free mode).
export function intentFromRect(
  rect: RenderRect,
  dock: PanelDock,
  cfg: PanelConfig,
  container: Bounds,
  minimized = false,
): PanelIntent {
  const spanIntentH = (): AxisIntent => ({
    size: null,
    offset: rect.left,
    gap: container.width - rect.left - rect.width,
  });
  const spanIntentV = (): AxisIntent => ({
    size: null,
    offset: rect.top,
    gap: container.height - rect.top - rect.height,
  });

  let h: AxisIntent;
  let v: AxisIntent;

  switch (dock) {
    case "right":
    case "left":
      h = { size: rect.width, offset: null, gap: null };
      v =
        cfg.v.behaviour === "fixed"
          ? { size: rect.height, offset: rect.top, gap: null }
          : spanIntentV();
      break;
    case "top":
    case "bottom":
      v = { size: rect.height, offset: null, gap: null };
      h =
        cfg.h.behaviour === "fixed"
          ? { size: rect.width, offset: rect.left, gap: null }
          : spanIntentH();
      break;
    case "free":
    default: {
      // Anchor each axis to the container half the panel's center sits in, so
      // a right/bottom-hugging panel keeps hugging as the container resizes.
      // Vertical anchoring uses the on-screen height (35px when minimized) —
      // computeLayout positions with the same effHeight, so the round-trip
      // must too or a minimized drop in the lower half jumps on release.
      const effHeight = minimized ? MINIMIZED_HEADER_PX : rect.height;
      const farH = rect.left + rect.width / 2 > container.width / 2;
      const farV = rect.top + effHeight / 2 > container.height / 2;
      h = {
        size: rect.width,
        offset: farH ? null : rect.left,
        gap: farH ? container.width - rect.left - rect.width : null,
      };
      v = {
        size: rect.height,
        offset: farV ? null : rect.top,
        gap: farV ? container.height - rect.top - effHeight : null,
      };
      break;
    }
  }

  return { dock, h, v, fullScreen: false };
}

// Which dock side (if any) a released rect should snap to. Nearest edge wins;
// ties resolve in `allowed` order.
export function snapSideForRect(
  rect: RenderRect,
  container: Bounds,
  allowed: DockSide[],
): DockSide | null {
  const distances: Record<DockSide, number> = {
    left: rect.left,
    right: container.width - (rect.left + rect.width),
    top: rect.top,
    bottom: container.height - (rect.top + rect.height),
  };
  let best: DockSide | null = null;
  for (const side of allowed) {
    const d = distances[side];
    if (d <= SNAP_ZONE_PX && (best === null || d < distances[best])) {
      best = side;
    }
  }
  return best;
}
