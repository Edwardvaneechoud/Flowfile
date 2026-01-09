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

// Define menu actions based on the target type
const getMenuActions = () => {
  return [
    { id: "fit-view", label: "Fit View", icon: "🔍" },
    { id: "zoom-in", label: "Zoom In", icon: "🔍+" },
    { id: "zoom-out", label: "Zoom Out", icon: "🔍-" },
    { id: "paste-node", label: "Paste Node", icon: "📋" },
  ];
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
.context-menu {
  position: fixed;
  min-width: 200px;
  background-color: var(--color-background-primary);
  border-radius: var(--border-radius-md);
  box-shadow: var(--shadow-lg);
  z-index: var(--z-index-canvas-context-menu, 100002);
  overflow: hidden;
  border: 1px solid var(--color-border-primary);
}

.context-menu-header {
  padding: var(--spacing-2) var(--spacing-3);
  font-weight: var(--font-weight-semibold);
  background-color: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-primary);
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
}

.context-menu-items {
  max-height: 300px;
  overflow-y: auto;
}

.context-menu-item {
  display: flex;
  align-items: center;
  padding: var(--spacing-2) var(--spacing-3);
  cursor: pointer;
  transition: background-color var(--transition-fast);
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
}

.context-menu-item:hover {
  background-color: var(--color-background-tertiary);
}

.context-menu-icon {
  margin-right: var(--spacing-2);
  width: 20px;
  text-align: center;
  color: var(--color-text-secondary);
}
</style>
