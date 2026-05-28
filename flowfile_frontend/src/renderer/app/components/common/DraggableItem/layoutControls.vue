<template>
  <div
    ref="wrapperRef"
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
            <span class="icon">↺</span> Reset window Layout
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
import { ref, onMounted, onBeforeUnmount, computed, useTemplateRef } from "vue";
import { useItemStore } from "./stateStore";

// Define emits for parent component communication
const emit = defineEmits<{
  (e: "reset-layout-graph"): void;
}>();

const itemStore = useItemStore();
const isOpen = ref(false);

const wrapperRef = useTemplateRef<HTMLElement>("wrapperRef");

// The widget now lives inside the canvas <main> element. Bounds (and the
// pinned bottom-right anchor) come from <main>, not the viewport — otherwise
// the button would always sit at the absolute bottom of the screen, possibly
// behind toolbars or off-canvas.
const getCanvasBounds = () => {
  const main = wrapperRef.value?.closest("main") as HTMLElement | null;
  if (main) {
    return { width: main.clientWidth, height: main.clientHeight };
  }
  return { width: window.innerWidth, height: window.innerHeight };
};

// Default-position constants. Anchor is "just right of the Data Actions
// panel" (`Canvas.vue` mounts it at `initial-width="230"`), bottom-aligned
// so the button doesn't compete vertically with the right-docked node
// settings / AI assistant drawers. ``LAYOUT_CONTROLS_DEFAULT_X`` is
// clamped against the canvas width below so a narrow window can't push
// the button off-screen.
const DATA_ACTIONS_PANEL_WIDTH = 230;
const LAYOUT_CONTROLS_GAP = 10;
const LAYOUT_CONTROLS_DEFAULT_X = DATA_ACTIONS_PANEL_WIDTH + LAYOUT_CONTROLS_GAP;

// localStorage key. Bumped from ``layoutControlsPosition`` (V1) when the
// default moved from bottom-right to bottom-left next to the Data Actions
// panel — existing users would otherwise see their old saved bottom-right
// position and miss the new default. V1 entries are left orphaned in
// localStorage; harmless and reversible if the default is ever changed back.
const LAYOUT_CONTROLS_STORAGE_KEY = "layoutControlsPositionV2";

// Position state — initialised in onMounted from saved storage or the
// computed bottom-left default; the inline value below is a safe
// pre-mount placeholder that ``handleViewportResize`` / ``onMounted``
// override before paint.
const position = ref({ x: LAYOUT_CONTROLS_DEFAULT_X, y: window.innerHeight - 80 });
const isDragging = ref(false);
const hasDragged = ref(false);
const dragStart = ref({ x: 0, y: 0 });
const initialPosition = ref({ x: 0, y: 0 });

// --- Function to handle window resizing ---
// Always reset to bottom-left (next to Data Actions) on resize so the
// button stays accessible even when the canvas shrinks below the
// previously-saved coordinate.
const handleViewportResize = () => {
  const buttonSize = 45;
  const boundaryMargin = 10;
  const bounds = getCanvasBounds();
  // Clamp against the canvas width so a very narrow window doesn't put
  // the button beyond the right edge.
  const maxX = Math.max(boundaryMargin, bounds.width - buttonSize - boundaryMargin);
  position.value.x = Math.min(LAYOUT_CONTROLS_DEFAULT_X, maxX);
  position.value.y = bounds.height - buttonSize - boundaryMargin;
  savePosition();
};

// --- NEW: Computed property for dynamic panel positioning ---
const panelStyle = computed(() => {
  const style: { [key: string]: string } = {};
  const bounds = getCanvasBounds();
  const isRightHalf = position.value.x > bounds.width / 2;
  const isBottomHalf = position.value.y > bounds.height / 2;

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

// Load saved position from localStorage - but always ensure it's in valid bounds
onMounted(() => {
  const buttonSize = 45;
  const boundaryMargin = 10;
  const savedPosition = localStorage.getItem(LAYOUT_CONTROLS_STORAGE_KEY);
  const bounds = getCanvasBounds();
  const maxX = bounds.width - buttonSize - boundaryMargin;
  const maxY = bounds.height - buttonSize - boundaryMargin;
  // Default anchor: bottom-left next to Data Actions, clamped if the
  // canvas is narrower than the panel + gap.
  const defaultX = Math.min(LAYOUT_CONTROLS_DEFAULT_X, Math.max(boundaryMargin, maxX));

  if (savedPosition) {
    const parsed = JSON.parse(savedPosition);

    if (
      parsed.x <= maxX &&
      parsed.y <= maxY &&
      parsed.x >= boundaryMargin &&
      parsed.y >= boundaryMargin
    ) {
      position.value = parsed;
    } else {
      // Reset to default if saved position is out of bounds
      position.value.x = defaultX;
      position.value.y = maxY;
      savePosition();
    }
  } else {
    // Default to bottom-left corner of the canvas, next to Data Actions.
    position.value.x = defaultX;
    position.value.y = maxY;
  }

  window.addEventListener("resize", handleViewportResize);
});

// Save position to localStorage
const savePosition = () => {
  localStorage.setItem(LAYOUT_CONTROLS_STORAGE_KEY, JSON.stringify(position.value));
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

    // Keep within canvas bounds (the <main> element, not the viewport).
    const buttonSize = 45;
    const boundaryMargin = 10;
    const bounds = getCanvasBounds();

    newX = Math.max(boundaryMargin, Math.min(bounds.width - buttonSize - boundaryMargin, newX));
    newY = Math.max(boundaryMargin, Math.min(bounds.height - buttonSize - boundaryMargin, newY));

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

// Reset layout using the store's resetLayout method without reloading
const resetLayout = () => {
  itemStore.resetLayout();
};

// NEW: Reset layout graph - emits event to parent
const resetLayoutGraph = () => {
  emit("reset-layout-graph");
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
  position: absolute;
  /* See zIndex.ts: FLOATING_WIDGET (200) — above panels, below fullscreen. */
  z-index: 200;
}

.trigger-btn {
  width: 45px;
  height: 45px;
  border-radius: 50%;
  border: none;
  background: linear-gradient(
    135deg,
    var(--color-gradient-purple-start) 0%,
    var(--color-gradient-purple-end) 100%
  );
  box-shadow: var(--shadow-md);
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
  box-shadow: var(--shadow-lg);
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
  background: var(--color-white-alpha-20);
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
  color: var(--color-text-inverse);
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
  width: 250px;
  background: var(--color-background-primary);
  border-radius: 12px;
  box-shadow: var(--shadow-xl);
  overflow: hidden;
  display: flex;
  flex-direction: column;
  backdrop-filter: blur(10px);
  border: 1px solid var(--color-border-primary);
}

.panel-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px 16px;
  border-bottom: 1px solid var(--color-border-primary);
  background: var(--color-background-secondary);
}

.panel-title {
  font-weight: 600;
  font-size: 16px;
  color: var(--color-text-primary);
  user-select: none;
}

.close-btn {
  background: none;
  border: none;
  font-size: 20px;
  color: var(--color-text-muted);
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
  color: var(--color-text-primary);
  background: var(--color-background-hover);
}

.panel-body {
  padding: 16px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.control-btn {
  background-color: var(--color-background-secondary);
  color: var(--color-text-primary);
  border: 1px solid var(--color-border-primary);
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
  background: linear-gradient(
    90deg,
    transparent,
    var(--color-focus-ring-purple-light),
    transparent
  );
  transition: left 0.5s;
}

.control-btn:hover::before {
  left: 100%;
}

.control-btn:hover {
  background-color: var(--color-background-hover);
  border-color: var(--color-accent);
  transform: translateX(2px);
}

/* Accent button style for Reset Layout Graph */
.control-btn.accent {
  background: linear-gradient(
    135deg,
    var(--color-focus-ring-purple-light) 0%,
    var(--color-focus-ring-purple-light) 100%
  );
  border-color: var(--color-gradient-purple-end);
}

.control-btn.accent:hover {
  background: linear-gradient(
    135deg,
    var(--color-focus-ring-purple) 0%,
    var(--color-focus-ring-purple) 100%
  );
  border-color: var(--color-gradient-purple-start);
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
