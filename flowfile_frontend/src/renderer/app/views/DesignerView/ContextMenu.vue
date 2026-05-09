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
    validator: (value: string) => ["node", "edge", "pane"].includes(value),
  },
  targetId: {
    type: String,
    default: "",
  },
  onClose: {
    type: Function,
    required: true,
  },
});

const emit = defineEmits(["action"]);

const menuRef = ref<HTMLElement | null>(null);

// Define menu actions based on the target type. Pane-only entries pick up
// the "Generate documentation" affordance (W50) and the new "Ask about
// lineage" affordance (W51); the per-node "Ask about this node's lineage"
// joins the node-target entries. The four pre-existing canvas actions
// still render against node/edge targets — that's a known pre-existing
// inconsistency, fixing it is out of scope for W51.
const getMenuActions = () => {
  const baseActions = [
    { id: "fit-view", label: "Fit View", icon: "🔍" },
    { id: "zoom-in", label: "Zoom In", icon: "🔍+" },
    { id: "zoom-out", label: "Zoom Out", icon: "🔍-" },
    { id: "paste-node", label: "Paste Node", icon: "📋" },
  ];
  if (props.targetType === "pane") {
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
  if (props.targetType === "node") {
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
    targetType: props.targetType as "node" | "edge" | "pane",
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
