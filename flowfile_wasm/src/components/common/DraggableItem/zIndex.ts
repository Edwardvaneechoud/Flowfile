// Centralised z-index hierarchy for the canvas overlay system.
//
// Layered low to high. Use these constants instead of magic numbers so the
// stacking order remains coherent when new overlays are added.
export const Z_INDEX = {
  // Normal floating panels (DraggableItem). bringToFront walks BASE..MAX
  // before normalizing back to BASE.
  PANEL_BASE: 100,
  PANEL_MAX: 200,

  // Floating widgets that should sit above panels but below fullscreen
  // (e.g. the layout-controls trigger button).
  FLOATING_WIDGET: 200,

  // A panel in fullscreen mode covers everything else in the canvas region.
  FULLSCREEN: 250,

  // Canvas-pinned controls that follow VueFlow stacking. Sit above panels
  // but below fullscreen so a maximised panel still occludes them.
  UNDO_REDO: 1000,
  CONTEXT_MENU: 1000,

  // Tooltips and toasts are app-level — they intentionally float above the
  // canvas overlay system.
  TOOLTIP: 100000,
} as const

export type ZIndexKey = keyof typeof Z_INDEX
