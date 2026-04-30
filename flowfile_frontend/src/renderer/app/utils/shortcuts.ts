// Display labels for keyboard modifiers — Mac glyphs on macOS, named labels
// everywhere else. Not used for event handling itself; the keydown handlers
// accept either `event.metaKey`/`event.ctrlKey` and read `event.shiftKey`
// directly. These are purely for rendering in popovers / tooltips so what
// the user reads matches what their muscle memory actually presses.
export const IS_MAC =
  typeof navigator !== "undefined" && /Mac|iPhone|iPad|iPod/.test(navigator.platform);

export const MODIFIER_LABEL = IS_MAC ? "⌘" : "Ctrl";
export const SHIFT_LABEL = IS_MAC ? "⇧" : "Shift";
