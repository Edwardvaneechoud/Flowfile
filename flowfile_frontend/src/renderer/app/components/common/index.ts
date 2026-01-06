// Common components barrel export
export { default as FileBrowser } from "./FileBrowser/fileBrowser.vue";
export { default as DraggableItem } from "./DraggableItem/DraggableItem.vue";
export { default as LayoutControls } from "./DraggableItem/layoutControls.vue";

// Re-export types and utilities
export * from "./DraggableItem/stateStore";
export * from "./FileBrowser/types";
export * from "./FileBrowser/constants";
