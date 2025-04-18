<script setup lang="ts">
import { ref, onMounted, onUnmounted, defineProps, defineEmits } from "vue";
import { useNodeStore } from "../../../../stores/column-store";
import { useVueFlow } from "@vue-flow/core";
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
const nodeStore = useNodeStore();

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
  background-color: white;
  border-radius: 4px;
  box-shadow: 0 2px 10px rgba(0, 0, 0, 0.2);
  z-index: 10000;
  overflow: hidden;
}

.context-menu-header {
  padding: 8px 12px;
  font-weight: bold;
  background-color: #f5f5f5;
  border-bottom: 1px solid #e0e0e0;
  font-size: 14px;
}

.context-menu-items {
  max-height: 300px;
  overflow-y: auto;
}

.context-menu-item {
  display: flex;
  align-items: center;
  padding: 8px 12px;
  cursor: pointer;
  transition: background-color 0.2s;
  font-size: 14px;
}

.context-menu-item:hover {
  background-color: #f5f5f5;
}

.context-menu-icon {
  margin-right: 8px;
  width: 20px;
  text-align: center;
}
</style>
