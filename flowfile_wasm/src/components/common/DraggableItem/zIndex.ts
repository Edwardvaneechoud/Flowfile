// Centralised z-index hierarchy for the canvas overlay system.
//
// Layered low to high. Use these constants instead of magic numbers so the
// stacking order stays coherent when new overlays are added.
export const Z_INDEX = {
  // Normal floating panels (DraggableItem). bringToFront walks BASE..MAX
  // before normalizing back to BASE.
  PANEL_BASE: 100,
  PANEL_MAX: 200,

  // A panel in fullscreen mode covers the other panels in the canvas region.
  FULLSCREEN: 250,

  // Canvas-pinned menus (the VueFlow pane/context menu) float above the panels.
  CONTEXT_MENU: 10000,

  // The floating layout-controls widget — sits above the whole canvas overlay
  // system (panels, fullscreen, context menu) so Reset Layout / Fit stay
  // reachable, but below app-level tooltips/toasts.
  FLOATING_WIDGET: 20000,
} as const

export type ZIndexKey = keyof typeof Z_INDEX
