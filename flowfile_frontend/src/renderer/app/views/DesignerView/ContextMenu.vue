<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, ref, watch } from "vue";

import { ContextMenuAction } from "./types";

const props = withDefaults(
  defineProps<{
    x: number;
    y: number;
    targetType: "node" | "edge" | "pane" | "selection";
    targetId?: string;
    targetInGroup?: boolean;
    /** Whether the node-copy buffer is armed; "Paste Node" is hidden otherwise. */
    canPasteNode?: boolean;
    /** Node target only: the node is a run_flow node ("Go to Flow" entry). */
    isRunFlowNode?: boolean;
    /** Node target only: the node is currently executing. */
    isRunning?: boolean;
    /** Node target only: cache_results flag; null while hydration is in flight. */
    cacheResults?: boolean | null;
    onClose: () => void;
  }>(),
  {
    targetId: "",
    targetInGroup: false,
    canPasteNode: false,
    isRunFlowNode: false,
    isRunning: false,
    cacheResults: null,
  },
);

const emit = defineEmits(["action"]);

const menuRef = ref<HTMLElement | null>(null);

// Lucide-style line icons (inner <svg> markup) drawn into a shared svg wrapper.
const ICONS: Record<string, string> = {
  fitView:
    '<path d="M8 3H5a2 2 0 0 0-2 2v3"/><path d="M21 8V5a2 2 0 0 0-2-2h-3"/><path d="M3 16v3a2 2 0 0 0 2 2h3"/><path d="M16 21h3a2 2 0 0 0 2-2v-3"/>',
  zoomIn:
    '<circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/><line x1="11" y1="8" x2="11" y2="14"/><line x1="8" y1="11" x2="14" y2="11"/>',
  zoomOut:
    '<circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/><line x1="8" y1="11" x2="14" y2="11"/>',
  paste:
    '<rect x="8" y="2" width="8" height="4" rx="1" ry="1"/><path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2"/>',
  group:
    '<path d="M5 3a2 2 0 0 0-2 2"/><path d="M19 3a2 2 0 0 1 2 2"/><path d="M21 19a2 2 0 0 1-2 2"/><path d="M5 21a2 2 0 0 1-2-2"/><path d="M9 3h1"/><path d="M9 21h1"/><path d="M14 3h1"/><path d="M14 21h1"/><path d="M3 9v1"/><path d="M21 9v1"/><path d="M3 14v1"/><path d="M21 14v1"/>',
  ungroup:
    '<rect width="8" height="6" x="5" y="4" rx="1"/><rect width="8" height="6" x="11" y="14" rx="1"/>',
  document:
    '<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><path d="M14 2v6h6"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/><line x1="10" y1="9" x2="8" y2="9"/>',
  sparkles:
    '<path d="M9.937 15.5A2 2 0 0 0 8.5 14.063l-6.135-1.582a.5.5 0 0 1 0-.962L8.5 9.936A2 2 0 0 0 9.937 8.5l1.582-6.135a.5.5 0 0 1 .962 0L14.063 8.5A2 2 0 0 0 15.5 9.937l6.135 1.581a.5.5 0 0 1 0 .964L15.5 14.063a2 2 0 0 0-1.437 1.437l-1.582 6.135a.5.5 0 0 1-.962 0z"/><path d="M20 3v4"/><path d="M22 5h-4"/><path d="M4 17v2"/><path d="M5 18H3"/>',
  lineage:
    '<circle cx="18" cy="5" r="3"/><circle cx="6" cy="12" r="3"/><circle cx="18" cy="19" r="3"/><line x1="8.59" y1="13.51" x2="15.42" y2="17.49"/><line x1="15.41" y1="6.51" x2="8.59" y2="10.49"/>',
  settings:
    '<circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/>',
  table:
    '<rect x="3" y="3" width="18" height="18" rx="2" ry="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="3" y1="15" x2="21" y2="15"/><line x1="9" y1="3" x2="9" y2="21"/>',
  externalLink:
    '<path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/>',
  arrowRight: '<path d="M5 12h14"/><path d="m12 5 7 7-7 7"/>',
  play: '<polygon points="5 3 19 12 5 21 5 3"/>',
  refresh:
    '<path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/><path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16"/><path d="M16 21h5v-5"/>',
  copy: '<rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>',
  trash:
    '<path d="M3 6h18"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"/><path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>',
};

interface MenuEntry {
  id: string;
  label: string;
  icon: string;
  danger?: boolean;
  disabled?: boolean;
  divider?: boolean;
}

const divider = (id: string): MenuEntry => ({ id, label: "", icon: "", divider: true });

// A node target renders the full per-node action set (this menu is the only
// right-click surface for nodes — NodeWrapper no longer has its own menu).
// Pane/selection targets keep the canvas-level actions.
const menuEntries = computed<MenuEntry[]>(() => {
  if (props.targetType === "node") {
    const entries: MenuEntry[] = [
      { id: "open-node-settings", label: "Edit", icon: "settings" },
      { id: "view-node-data", label: "View Data", icon: "table" },
    ];
    if (props.isRunFlowNode) {
      entries.push({ id: "open-target-flow", label: "Go to Flow", icon: "externalLink" });
    }
    entries.push(
      { id: "suggest-next-node", label: "Suggest next node…", icon: "arrowRight" },
      { id: "open-node-docs", label: "Read more", icon: "document" },
      divider("d1"),
      {
        id: "run-node",
        label: props.isRunning ? "Running…" : "Run Now",
        icon: "play",
        disabled: props.isRunning,
      },
      {
        id: "toggle-cache",
        label:
          props.cacheResults === null
            ? "Cache results…"
            : props.cacheResults
              ? "Disable Cache"
              : "Enable Cache",
        icon: "refresh",
        disabled: props.cacheResults === null,
      },
      divider("d2"),
      props.targetInGroup
        ? { id: "remove-from-group", label: "Remove from group", icon: "ungroup" }
        : { id: "group-selection", label: "Group selected nodes", icon: "group" },
      { id: "ask-lineage-node", label: "Ask about this node's lineage…", icon: "lineage" },
      divider("d3"),
      { id: "copy-node", label: "Copy", icon: "copy" },
      { id: "delete-node", label: "Delete", icon: "trash", danger: true },
    );
    return entries;
  }

  const entries: MenuEntry[] = [
    { id: "fit-view", label: "Fit View", icon: "fitView" },
    { id: "zoom-in", label: "Zoom In", icon: "zoomIn" },
    { id: "zoom-out", label: "Zoom Out", icon: "zoomOut" },
  ];
  if (props.canPasteNode) {
    entries.push({ id: "paste-node", label: "Paste Node", icon: "paste" });
  }
  if (props.targetType === "pane" || props.targetType === "selection") {
    entries.push({ id: "group-selection", label: "Group selected nodes", icon: "group" });
  }
  if (props.targetType === "pane") {
    entries.push(
      { id: "generate-documentation", label: "Generate documentation", icon: "document" },
      { id: "add-descriptions-all", label: "Add description to all nodes", icon: "sparkles" },
      { id: "ask-lineage", label: "Ask about lineage…", icon: "lineage" },
    );
  }
  return entries;
});

const handleAction = (entry: MenuEntry): void => {
  if (entry.divider || entry.disabled) return;
  emit("action", {
    actionId: entry.id,
    targetType: props.targetType,
    targetId: props.targetId,
    // Raw coords, not the clamped ones — paste-node projects them into flow
    // space, so they must stay where the user actually clicked.
    position: { x: props.x, y: props.y },
  } as ContextMenuAction);
  props.onClose();
};

// The page never scrolls (html/body/#app are overflow:hidden), so a menu
// opened near an edge would be unreachable rather than scrolled into view —
// clamp it fully inside the viewport. Runs pre-paint (nextTick), so there is
// no visible jump.
const coords = ref({ x: props.x, y: props.y });

const clampToViewport = () => {
  const el = menuRef.value;
  if (!el) return;
  const { width, height } = el.getBoundingClientRect();
  coords.value = {
    x: Math.max(10, Math.min(props.x, window.innerWidth - width - 10)),
    y: Math.max(10, Math.min(props.y, window.innerHeight - height - 10)),
  };
};

// Re-clamp whenever the anchor or the rendered entry set changes — the menu
// may stay mounted across two consecutive right-clicks on different targets.
watch(
  () => [props.x, props.y, props.targetType, props.targetId],
  () => {
    coords.value = { x: props.x, y: props.y };
    void nextTick(clampToViewport);
  },
);

// Capture-phase pointerdown: VueFlow's pane preventDefaults pointerdown, which
// suppresses the compatibility mousedown — a bubble mousedown listener never
// fires for canvas clicks, leaving the menu stuck open. pointerdown also fires
// for right-clicks (click does not), so a right-click elsewhere dismisses too.
const handlePointerDownOutside = (event: PointerEvent) => {
  if (menuRef.value && !menuRef.value.contains(event.target as Node)) {
    props.onClose();
  }
};

const handleKeyDown = (event: KeyboardEvent) => {
  if (event.key === "Escape") {
    props.onClose();
  }
};

onMounted(() => {
  void nextTick(clampToViewport);
  document.addEventListener("pointerdown", handlePointerDownOutside, true);
  document.addEventListener("keydown", handleKeyDown);
});

onUnmounted(() => {
  document.removeEventListener("pointerdown", handlePointerDownOutside, true);
  document.removeEventListener("keydown", handleKeyDown);
});
</script>

<template>
  <div
    ref="menuRef"
    class="context-menu"
    :style="{
      left: `${coords.x}px`,
      top: `${coords.y}px`,
    }"
  >
    <div class="context-menu-header">
      <span>{{
        targetType === "node"
          ? "Node Actions"
          : targetType === "edge"
            ? "Edge Actions"
            : "Canvas Actions"
      }}</span>
    </div>
    <div class="context-menu-items">
      <template v-for="entry in menuEntries" :key="entry.id">
        <div v-if="entry.divider" class="context-menu-divider"></div>
        <div
          v-else
          class="context-menu-item"
          :class="{ disabled: entry.disabled, 'context-menu-item-danger': entry.danger }"
          @click="handleAction(entry)"
        >
          <!-- eslint-disable vue/no-v-html -- ICONS is a trusted internal constant of static SVG paths -->
          <span class="context-menu-icon" aria-hidden="true">
            <svg
              width="16"
              height="16"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
              v-html="ICONS[entry.icon]"
            ></svg>
          </span>
          <!-- eslint-enable vue/no-v-html -->
          <span>{{ entry.label }}</span>
        </div>
      </template>
    </div>
  </div>
</template>

<style scoped>
/* Context menu styles are now centralized in styles/components/_context-menu.css */
/* Component-specific overrides only */
.context-menu {
  min-width: 200px;
}

/* The node-target menu is taller than the shared 300px cap; viewport clamping
   already keeps it on-screen, so let it grow instead of scrolling. */
.context-menu-items {
  max-height: min(80vh, 560px);
}

.context-menu-icon {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
}

.context-menu-item-danger {
  color: var(--color-danger);
}

.context-menu-item-danger:hover {
  background-color: var(--color-danger-light);
}
</style>
