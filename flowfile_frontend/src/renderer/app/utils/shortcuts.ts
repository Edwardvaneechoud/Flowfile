// Display label for the primary keyboard modifier — `⌘` on macOS, `Ctrl`
// everywhere else. Not used for event handling itself; the keydown handlers
// already accept either `event.metaKey` or `event.ctrlKey`. This is purely
// for rendering in popovers / tooltips so what the user reads matches what
// their muscle memory actually presses.
const isMac = typeof navigator !== "undefined" && /Mac|iPhone|iPad|iPod/.test(navigator.platform);

export const MODIFIER_LABEL = isMac ? "⌘" : "Ctrl";
