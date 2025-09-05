<template>
  <div
    class="layout-widget-wrapper"
    :style="{
      left: position.x + 'px',
      top: position.y + 'px',
    }"
  >
    <Transition name="panel-fade">
      <div v-if="isOpen" class="panel" :style="panelStyle">
        <div class="panel-header">
          <span class="panel-title">Layout Controls</span>
          <button class="close-btn" title="Close" @click="isOpen = false">✕</button>
        </div>
        <div class="panel-body">
          <button class="control-btn" @click="runAction(arrangeLayout, 'tile')">
            <span class="icon">⊞</span> Tile Layout
          </button>
          <button class="control-btn" @click="runAction(arrangeLayout, 'cascade')">
            <span class="icon">◫</span> Cascade
          </button>
          <button class="control-btn" @click="runAction(resetLayout)">
            <span class="icon">↺</span> Reset Layout
          </button>
          <button class="control-btn accent" @click="runAction(resetLayoutGraph)">
            <span class="icon">⟲</span> Reset Layout Graph
          </button>
        </div>
      </div>
    </Transition>
    <button
      class="trigger-btn"
      :class="{ 'is-open': isOpen }"
      title="Layout Controls"
      @mousedown="handleMouseDown"
      @click="handleClick"
    >
      <svg class="layout-icon" viewBox="0 0 24 24" width="24" height="24">
        <rect x="2" y="2" width="8" height="6" fill="currentColor" opacity="0.9" />
        <rect x="12" y="2" width="8" height="6" fill="currentColor" opacity="0.7" />
        <rect x="2" y="10" width="8" height="10" fill="currentColor" opacity="0.7" />
        <rect x="12" y="10" width="8" height="10" fill="currentColor" opacity="0.9" />
      </svg>
    </button>
  </div>
</template>

<script setup lang="ts">
// UPDATED: Added 'computed' and defineEmits
import { ref, onMounted, onBeforeUnmount, computed } from "vue";
import { useItemStore } from "./stateStore";

// Define emits for parent component communication
const emit = defineEmits<{
  'reset-layout-graph': []
}>();

const itemStore = useItemStore();
const isOpen = ref(false);

// Position state
const position = ref({ x: window.innerWidth - 80, y: window.innerHeight - 80 });
const isDragging = ref(false);
const hasDragged = ref(false);
const dragStart = ref({ x: 0, y: 0 });
const initialPosition = ref({ x: 0, y: 0 });

// --- NEW: Function to handle window resizing ---
const handleViewportResize = () => {
  const buttonSize = 45;
  const boundaryMargin = 10; // Margin from the edge of the viewport
  position.value.x = Math.max(
    boundaryMargin,
    Math.min(window.innerWidth - buttonSize - boundaryMargin, position.value.x),
  );
  position.value.y = Math.max(
    boundaryMargin,
    Math.min(window.innerHeight - buttonSize - boundaryMargin, position.value.y),
  );
};

// --- NEW: Computed property for dynamic panel positioning ---
const panelStyle = computed(() => {
  const style: { [key: string]: string } = {};
  const isRightHalf = position.value.x > window.innerWidth / 2;
  const isBottomHalf = position.value.y > window.innerHeight / 2;

  // Position horizontally
  if (isRightHalf) {
    style.right = "60px"; // Open to the left of the button
  } else {
    style.left = "60px"; // Open to the right of the button
  }

  // Position vertically
  if (isBottomHalf) {
    style.bottom = "0px"; // Align with the bottom of the button
  } else {
    style.top = "0px"; // Align with the top of the button
  }

  return style;
});

// Load saved position from localStorage
onMounted(() => {
  const savedPosition = localStorage.getItem("layoutControlsPosition");
  if (savedPosition) {
    const parsed = JSON.parse(savedPosition);
    position.value = parsed;
    handleViewportResize(); // Run once on load to ensure it's within bounds
  }
  // NEW: Add the resize listener
  window.addEventListener("resize", handleViewportResize);
});

// Save position to localStorage
const savePosition = () => {
  localStorage.setItem("layoutControlsPosition", JSON.stringify(position.value));
};

// Handle mouse down - prepare for potential drag
const handleMouseDown = (e: MouseEvent) => {
  e.preventDefault();
  hasDragged.value = false;

  if (isOpen.value) {
    return;
  }

  isDragging.value = true;
  dragStart.value = {
    x: e.clientX,
    y: e.clientY,
  };
  initialPosition.value = {
    x: position.value.x,
    y: position.value.y,
  };

  document.addEventListener("mousemove", onDrag);
  document.addEventListener("mouseup", stopDrag);
};

// Handle click - open panel only if we didn't drag
const handleClick = (e: MouseEvent) => {
  e.preventDefault();
  e.stopPropagation();

  if (!hasDragged.value) {
    isOpen.value = !isOpen.value;
  }
};

const onDrag = (e: MouseEvent) => {
  if (!isDragging.value) return;

  const deltaX = e.clientX - dragStart.value.x;
  const deltaY = e.clientY - dragStart.value.y;

  if (Math.abs(deltaX) > 5 || Math.abs(deltaY) > 5) {
    hasDragged.value = true;
  }

  if (hasDragged.value) {
    let newX = initialPosition.value.x + deltaX;
    let newY = initialPosition.value.y + deltaY;

    // Keep within viewport bounds
    const buttonSize = 45;
    const boundaryMargin = 10; // UPDATED: Margin from edge

    // UPDATED: Clamping logic now includes the margin
    newX = Math.max(
      boundaryMargin,
      Math.min(window.innerWidth - buttonSize - boundaryMargin, newX),
    );
    newY = Math.max(
      boundaryMargin,
      Math.min(window.innerHeight - buttonSize - boundaryMargin, newY),
    );

    position.value = { x: newX, y: newY };
  }
};

const stopDrag = () => {
  if (isDragging.value) {
    isDragging.value = false;
    if (hasDragged.value) {
      savePosition();
    }
    document.removeEventListener("mousemove", onDrag);
    document.removeEventListener("mouseup", stopDrag);
  }
};

// Helper function to run an action and then close the panel
const runAction = <T extends any[]>(action: (...args: T) => void, ...args: T) => {
  action(...args);
  isOpen.value = false;
};

// Arrange all windows in a specific layout
const arrangeLayout = (layout: "tile" | "cascade") => {
  itemStore.arrangeItems(layout);
};

// Sync all groups
const syncAllGroups = () => {
  if (itemStore.groups) {
    Object.keys(itemStore.groups).forEach((groupName) => {
      itemStore.syncGroupDimensions(groupName);
    });
  }
};

// Reset layout using the store's resetLayout method without reloading
const resetLayout = () => {
  itemStore.resetLayout();
};

// NEW: Reset layout graph - emits event to parent
const resetLayoutGraph = () => {
  emit('reset-layout-graph');
};

// Cleanup
onBeforeUnmount(() => {
  document.removeEventListener("mousemove", onDrag);
  document.removeEventListener("mouseup", stopDrag);
  // UPDATED: This now correctly removes the listener
  window.removeEventListener("resize", handleViewportResize);
});
</script>

<style scoped>
.layout-widget-wrapper {
  position: fixed;
  z-index: 20000;
}

.trigger-btn {
  width: 45px;
  height: 45px;
  border-radius: 50%;
  border: none;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  box-shadow: 0 3px 10px rgba(0, 0, 0, 0.2);
  cursor: move;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
  position: relative;
  overflow: hidden;
}

.trigger-btn:hover {
  width: 55px;
  height: 55px;
  box-shadow: 0 6px 20px rgba(102, 126, 234, 0.4);
}

.trigger-btn.is-open {
  cursor: pointer;
}

.trigger-btn::before {
  content: "";
  position: absolute;
  top: 50%;
  left: 50%;
  width: 0;
  height: 0;
  border-radius: 50%;
  background: rgba(255, 255, 255, 0.2);
  transform: translate(-50%, -50%);
  transition:
    width 0.6s,
    height 0.6s;
}

.trigger-btn:hover::before {
  width: 100px;
  height: 100px;
}

.layout-icon {
  color: white;
  transition: transform 0.3s ease;
  pointer-events: none;
}

.trigger-btn:hover .layout-icon {
  transform: scale(1.1);
}

.trigger-btn.is-open .layout-icon {
  transform: rotate(90deg);
}

.panel {
  position: absolute;
  /* REMOVED: right: 60px; */
  /* REMOVED: bottom: 0; */
  width: 250px;
  background: white;
  border-radius: 12px;
  box-shadow: 0 5px 25px rgba(0, 0, 0, 0.15);
  overflow: hidden;
  display: flex;
  flex-direction: column;
  backdrop-filter: blur(10px);
  background: rgba(255, 255, 255, 0.98);
}

.panel-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px 16px;
  border-bottom: 1px solid #f0f0f0;
  background: linear-gradient(135deg, #667eea10 0%, #764ba210 100%);
}

.panel-title {
  font-weight: 600;
  font-size: 16px;
  color: #333;
  user-select: none;
}

.close-btn {
  background: none;
  border: none;
  font-size: 20px;
  color: #888;
  cursor: pointer;
  padding: 0;
  line-height: 1;
  width: 24px;
  height: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 4px;
  transition: all 0.2s;
}

.close-btn:hover {
  color: #333;
  background: rgba(0, 0, 0, 0.05);
}

.panel-body {
  padding: 16px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.control-btn {
  background-color: #f8f9fa;
  color: #333;
  border: 1px solid #e0e0e0;
  border-radius: 8px;
  padding: 10px 16px;
  cursor: pointer;
  font-size: 14px;
  font-weight: 500;
  transition: all 0.2s ease;
  display: flex;
  align-items: center;
  gap: 8px;
  text-align: left;
  position: relative;
  overflow: hidden;
}

.control-btn::before {
  content: "";
  position: absolute;
  top: 0;
  left: -100%;
  width: 100%;
  height: 100%;
  background: linear-gradient(90deg, transparent, rgba(102, 126, 234, 0.1), transparent);
  transition: left 0.5s;
}

.control-btn:hover::before {
  left: 100%;
}

.control-btn:hover {
  background-color: #f0f2ff;
  border-color: #667eea;
  transform: translateX(2px);
}

/* Accent button style for Reset Layout Graph */
.control-btn.accent {
  background: linear-gradient(135deg, #667eea15 0%, #764ba215 100%);
  border-color: #764ba2;
}

.control-btn.accent:hover {
  background: linear-gradient(135deg, #667eea25 0%, #764ba225 100%);
  border-color: #667eea;
}

.control-btn .icon {
  font-size: 16px;
  min-width: 20px;
}

/* Vue Transition Styles */
.panel-fade-enter-active,
.panel-fade-leave-active {
  transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
}

.panel-fade-enter-from {
  opacity: 0;
  transform: translateY(10px) scale(0.95);
}

.panel-fade-leave-to {
  opacity: 0;
  transform: translateY(-10px) scale(0.95);
}
</style>