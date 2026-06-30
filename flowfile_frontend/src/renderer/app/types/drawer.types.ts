import type { Component } from "vue";
import type { AxisBehaviour } from "../components/common/DraggableItem/stateStore";
import type { useEditorStore } from "../stores/editor-store";
import type { useNodeStore } from "../stores/column-store";
import type { useFlowStore } from "../stores/flow-store";
import type { useDrawerStore } from "../stores/drawer-store";

// Store singletons handed to every registry predicate so the registry module
// itself imports zero stores at runtime (pure data + closures).
export interface DrawerCtx {
  editor: ReturnType<typeof useEditorStore>;
  node: ReturnType<typeof useNodeStore>;
  flow: ReturnType<typeof useFlowStore>;
  drawer: ReturnType<typeof useDrawerStore>;
}

// The minimal {id,label} shape DraggableItem renders in its header strip.
export interface DrawerTab {
  id: string;
  label: string;
}

export interface DrawerTabDef {
  id: string;
  label: string;
  component: Component; // registry markRaw()s it
  visibleWhen: (ctx: DrawerCtx) => boolean;
  // Pull focus to this tab when the signal goes true (for always-present tabs
  // that never "appear", e.g. Code grabbing focus on Ctrl+G).
  focusWhen?: (ctx: DrawerCtx) => boolean;
  props?: (ctx: DrawerCtx) => Record<string, unknown>;
  remountKey?: (ctx: DrawerCtx) => string | number; // omit ⇒ singleton (kept mounted)
}

export interface DrawerDef {
  id: string; // DraggableItem id + localStorage + bringToFront key
  side: "right" | "bottom";
  initialWidth?: number;
  initialHeight?: number;
  initialLeft?: number;
  widthBehaviour?: AxisBehaviour;
  heightBehaviour?: AxisBehaviour;
  allowFullScreen?: boolean;
  tabs: DrawerTabDef[];
  // Default drawer visibility = "≥1 tab visible". Override when a tab is a
  // permanent home tab (e.g. the bottom dock's Data placeholder).
  visibleWhen?: (ctx: DrawerCtx) => boolean;
  onMinimize?: (ctx: DrawerCtx) => void;
}
