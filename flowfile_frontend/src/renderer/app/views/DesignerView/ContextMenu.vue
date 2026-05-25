<script setup lang="ts">
import { ref, onMounted, onUnmounted } from "vue";
import { ContextMenuAction } from "./types";

const props = defineProps({
  x: {
    type: Number,
    required: true,
  },
  y: {
    type: Number,
    required: true,
  },
  targetType: {
    type: String,
    required: true,
    validator: (value: string) => ["node", "edge", "pane", "selection"].includes(value),
  },
  targetId: {
    type: String,
    default: "",
  },
  targetInGroup: {
    type: Boolean,
    default: false,
  },
  onClose: {
    type: Function,
    required: true,
  },
});

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
};

// Define menu actions based on the target type. Pane-only entries pick up
// the "Generate documentation" affordance and the new "Ask about
// lineage" affordance; the per-node "Ask about this node's lineage"
// joins the node-target entries. The four pre-existing canvas actions
// still render against node/edge targets — that's a known pre-existing
// inconsistency, fixing it is out of scope for.
const getMenuActions = () => {
  const baseActions = [
    { id: "fit-view", label: "Fit View", icon: "🔍" },
    { id: "zoom-in", label: "Zoom In", icon: "🔍+" },
    { id: "zoom-out", label: "Zoom Out", icon: "🔍-" },
    { id: "paste-node", label: "Paste Node", icon: "📋" },
  ];
  if (props.targetType === "pane") {
    baseActions.push({
      id: "group-selection",
      label: "Group selected nodes",
      icon: "🗂️",
    });
    baseActions.push({
      id: "generate-documentation",
      label: "Generate documentation",
      icon: "📝",
    });
    baseActions.push({
      id: "add-descriptions-all",
      label: "Add description to all nodes",
      icon: "✨",
    });
    baseActions.push({
      id: "ask-lineage",
      label: "Ask about lineage…",
      icon: "🔎",
    });
  }
  if (props.targetType === "selection") {
    baseActions.push({
      id: "group-selection",
      label: "Group selected nodes",
      icon: "🗂️",
    });
  }
  if (props.targetType === "node") {
    if (props.targetInGroup) {
      baseActions.push({
        id: "remove-from-group",
        label: "Remove from group",
        icon: "🗂️",
      });
    } else {
      baseActions.push({
        id: "group-selection",
        label: "Group selected nodes",
        icon: "🗂️",
      });
    }
    baseActions.push({
      id: "ask-lineage-node",
      label: "Ask about this node's lineage…",
      icon: "🔎",
    });
  }
  return baseActions;
};

const handleAction = (actionId: string): void => {
  emit("action", {
    actionId,
    targetType: props.targetType as "node" | "edge" | "pane" | "selection",
    targetId: props.targetId,
    position: { x: props.x, y: props.y },
  } as ContextMenuAction);
  props.onClose();
};

// Close the menu when clicking outside
const handleClickOutside = (event: MouseEvent) => {
  if (menuRef.value && !menuRef.value.contains(event.target as Node)) {
    props.onClose();
  }
};

// Close the menu when pressing Escape
const handleKeyDown = (event: KeyboardEvent) => {
  if (event.key === "Escape") {
    props.onClose();
  }
};

onMounted(() => {
  document.addEventListener("mousedown", handleClickOutside);
  document.addEventListener("keydown", handleKeyDown);
});

onUnmounted(() => {
  document.removeEventListener("mousedown", handleClickOutside);
  document.removeEventListener("keydown", handleKeyDown);
});
</script>

<template>
  <div
    ref="menuRef"
    class="context-menu"
    :style="{
      left: `${x}px`,
      top: `${y}px`,
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
      <div
        v-for="action in getMenuActions()"
        :key="action.id"
        class="context-menu-item"
        @click="handleAction(action.id)"
      >
        <span class="context-menu-icon">{{ action.icon }}</span>
        <span>{{ action.label }}</span>
      </div>
    </div>
  </div>
</template>

<style scoped>
/* Context menu styles are now centralized in styles/components/_context-menu.css */
/* Component-specific overrides only */
.context-menu {
  min-width: 200px;
}
</style>
