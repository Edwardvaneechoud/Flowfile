<template>
  <div
    :id="props.id"
    class="overlay"
    :class="{
      'no-transition': isResizing,
      minimized: isMinimized,
      'in-group': itemState.group,
      synced: itemState.syncDimensions,
    }"
    :style="{
      width: isMinimized ? 'auto' : itemState.width + 'px',
      height: isMinimized ? 'auto' : itemState.height + 'px',
      top: itemState.top + 'px',
      left: itemState.left + 'px',
      zIndex: itemState.zIndex,
    }"
  >
    <div class="header" @mousedown="startMove">
      <button
        v-if="allowMinimizing"
        class="minimal-button"
        data-tooltip="true"
        :title="isMinimized ? 'Maximize' : 'Minimize'"
        @click="toggleMinimize"
      >
        <span class="icon">{{ isMinimized ? "+" : "−" }}</span>
      </button>

      <button
        v-if="showRight && itemState.stickynessPosition !== 'right'"
        class="minimal-button"
        data-tooltip="true"
        title="Move to Right"
        @click="moveToRight"
      >
        <span class="icon">→</span>
      </button>
      <button
        v-if="showBottom && itemState.stickynessPosition !== 'bottom'"
        class="minimal-button"
        data-tooltip="true"
        title="Move to Bottom"
        @click="moveToBottom"
      >
        <span class="icon">↓</span>
      </button>
      <button
        v-if="showLeft && itemState.stickynessPosition !== 'left'"
        class="minimal-button"
        data-tooltip="true"
        title="Move to Left"
        @click="moveToLeft"
      >
        <span class="icon">←</span>
      </button>
      <button
        v-if="showTop && itemState.stickynessPosition !== 'top'"
        class="minimal-button"
        data-tooltip="true"
        title="Move to Top"
        @click="moveToTop"
      >
        <span class="icon">↑</span>
      </button>
      <button
        v-if="allowFullScreen && !itemState.fullScreen"
        class="minimal-button"
        data-tooltip="true"
        data-tooltip-text="Toggle Full Screen"
        @click="toggleFullScreen"
      >
        <span class="icon">⬜</span>
      </button>
      <button
        v-if="allowFullScreen && itemState.fullScreen"
        class="minimal-button"
        data-tooltip="true"
        data-tooltip-text="Exit Full Screen"
        @click="toggleFullScreen"
      >
        <span class="icon">❐</span>
      </button>
      <span class="group-badge" @mousedown="startMove">
        {{ title }}
      </span>
    </div>

    <div class="content" @click="registerClick">
      <slot v-if="!isMinimized"></slot>
    </div>

    <div
      class="draggable-line right-vertical"
      @mousedown.stop="startResizeRight"
      @mouseenter="resizeOnEnter($event, 'right')"
    ></div>
    <div
      class="draggable-line bottom-horizontal"
      @mousedown.stop="startResizeBottom"
      @mouseenter="resizeOnEnter($event, 'bottom')"
    ></div>
    <div
      class="draggable-line top-horizontal"
      @mousedown.stop="startResizeTop"
      @mouseenter="resizeOnEnter($event, 'top')"
    ></div>
    <div
      class="draggable-line left-vertical"
      @mousedown.stop="startResizeLeft"
      @mouseenter="resizeOnEnter($event, 'left')"
    ></div>
  </div>
</template>

<script setup lang="ts">
import {
  ref,
  onMounted,
  onBeforeUnmount,
  defineExpose,
  defineProps,
  getCurrentInstance,
  nextTick,
  watch,
} from "vue";
import { useItemStore } from "./stateStore";
import type { ItemLayout } from "./stateStore";

const props = defineProps({
  id: {
    type: String,
    required: true,
  },
  showLeft: {
    type: Boolean,
    default: false,
  },
  showTop: {
    type: Boolean,
    default: false,
  },
  showRight: {
    type: Boolean,
    default: false,
  },
  showBottom: {
    type: Boolean,
    default: false,
  },
  showPresets: {
    type: Boolean,
    default: false,
  },
  initialPosition: {
    type: String as () => "top" | "bottom" | "left" | "right" | "free",
    default: "free",
  },
  initialHeight: {
    type: Number,
    default: null,
  },
  initialWidth: {
    type: Number,
    default: null,
  },

  initialLeft: {
    type: Number,
    default: null,
  },
  initialTop: {
    type: Number,
    default: null,
  },
  allowMinimizing: {
    type: Boolean,
    default: true,
  },
  title: {
    type: String,
    default: "",
  },
  onMinize: {
    type: Function,
    default: null,
  },
  allowFreeMove: {
    type: Boolean,
    default: true,
  },
  allowFullScreen: {
    type: Boolean,
    default: false,
  },
  group: {
    type: String,
    default: null,
  },
  syncDimensions: {
    type: Boolean,
    default: false,
  },
  preventOverlap: {
    type: Boolean,
    default: false,
  },
});

const itemStore = useItemStore();
const itemState = ref(
  itemStore.items[props.id] || {
    width: props.initialWidth || 400,
    height: props.initialHeight || 300,
    left: props.initialLeft || 100, // Used corrected prop
    top: props.initialTop || 100,
    group: props.group,
    syncDimensions: props.syncDimensions,
    zIndex: 100,
  },
);

const isDragging = ref(false);
const isResizing = ref(false);
const startX = ref(0);
const startY = ref(0);
const startWidth = ref(0);
const startHeight = ref(0);
const startLeft = ref(0);
const startTop = ref(0);
const isMinimized = ref(false);
const instance = getCurrentInstance();
const activeLine = ref<HTMLElement | null>(null);
let resizeTimeout: ReturnType<typeof setTimeout>;

const resizeDirection = ref<"top" | "bottom" | "left" | "right" | null>(null);
const initialGroupStates = ref<
  Record<string, { top: number; left: number; width: number; height: number }>
>({});

const resizeDelay = ref<ReturnType<typeof setTimeout> | null>(null);
const resizeOnEnter = (e: MouseEvent, position: "top" | "bottom" | "left" | "right") => {
  if (resizeDelay.value) clearTimeout(resizeDelay.value);
  resizeDelay.value = setTimeout(() => {
    if (itemStore.inResizing && !isResizing.value) {
      switch (position) {
        case "right":
          startResizeRight(e);
          break;
        case "bottom":
          startResizeBottom(e);
          break;
        case "top":
          startResizeTop(e);
          break;
        case "left":
          startResizeLeft(e);
          break;
      }
    }
  }, 200);
};

const savePositionAndSize = () => {
  itemStore.setItemState(props.id, {
    width: itemState.value.width,
    height: itemState.value.height,
    left: itemState.value.left,
    top: itemState.value.top,
    stickynessPosition: itemState.value.stickynessPosition,
    fullWidth: itemState.value.fullWidth,
    fullHeight: itemState.value.fullHeight,
    zIndex: itemState.value.zIndex,
    fullScreen: itemState.value.fullScreen,
    group: itemState.value.group,
    syncDimensions: itemState.value.syncDimensions,
  });

  itemStore.saveItemState(props.id);

  if (itemState.value.group && itemState.value.syncDimensions && isResizing.value) {
    const groupItems = itemStore.groups[itemState.value.group];
    if (groupItems) {
      const initialActiveState = initialGroupStates.value[props.id];
      if (!initialActiveState) return;

      const deltaX = itemState.value.left - initialActiveState.left;
      const deltaY = itemState.value.top - initialActiveState.top;

      groupItems.forEach((itemId) => {
        if (itemId === props.id) return;

        const initialItemState = initialGroupStates.value[itemId];
        if (itemStore.items[itemId]?.syncDimensions && initialItemState) {
          const updates: Partial<ItemLayout> = {
            width: itemState.value.width,
            height: itemState.value.height,
          };

          if (resizeDirection.value === "top") {
            updates.top = initialItemState.top + deltaY;
          }
          if (resizeDirection.value === "left") {
            updates.left = initialItemState.left + deltaX;
          }

          itemStore.setItemState(itemId, updates);
          itemStore.saveItemState(itemId);
        }
      });
    }
  }
};

const loadPositionAndSize = () => {
  itemStore.loadItemState(props.id);
  if (itemStore.items[props.id]) {
    itemState.value = itemStore.items[props.id];
  }
};

const toggleMinimize = () => {
  if (!isMinimized.value && props.onMinize) {
    props.onMinize();
  }
  isMinimized.value = !isMinimized.value;
};

const handleReziging = (e: MouseEvent) => {
  activeLine.value = e.target as HTMLElement;
  activeLine.value.classList.add("resizing-highlight-line");
  isResizing.value = true;
  itemStore.inResizing = true;
};

const toggleFullScreen = () => {
  itemStore.toggleFullScreen(props.id);
  loadPositionAndSize();
};

const captureGroupInitialStates = () => {
  if (itemState.value.group && itemState.value.syncDimensions) {
    initialGroupStates.value = {};
    const groupItems = itemStore.groups[itemState.value.group];
    if (groupItems) {
      groupItems.forEach((id) => {
        const item = itemStore.items[id];
        if (item) {
          initialGroupStates.value[id] = {
            top: item.top,
            left: item.left,
            width: item.width,
            height: item.height,
          };
        }
      });
    }
  }
};

const startResizeRight = (e: MouseEvent) => {
  e.preventDefault();
  handleReziging(e);
  resizeDirection.value = "right";
  captureGroupInitialStates();
  startX.value = e.clientX;
  startWidth.value = itemState.value.width;
  document.addEventListener("mousemove", onResizeWidth);
  document.addEventListener("mouseup", stopResize);
};

const onResizeWidth = (e: MouseEvent) => {
  if (isResizing.value) {
    const deltaX = e.clientX - startX.value;
    const newWidth = startWidth.value + deltaX;
    if (newWidth > 100 && newWidth < window.innerWidth) {
      itemState.value.width = newWidth;
      savePositionAndSize();
    }
  }
};

const startResizeBottom = (e: MouseEvent) => {
  e.preventDefault();
  handleReziging(e);
  resizeDirection.value = "bottom";
  captureGroupInitialStates();
  startY.value = e.clientY;
  startHeight.value = itemState.value.height;
  document.addEventListener("mousemove", onResizeHeight);
  document.addEventListener("mouseup", stopResize);
};

const onResizeHeight = (e: MouseEvent) => {
  if (isResizing.value) {
    const deltaY = e.clientY - startY.value;
    const newHeight = startHeight.value + deltaY;
    if (newHeight > 100 && newHeight < window.innerHeight - 100) {
      itemState.value.height = newHeight;
      savePositionAndSize();
    }
  }
};

const startResizeTop = (e: MouseEvent) => {
  e.preventDefault();
  handleReziging(e);
  resizeDirection.value = "top";
  captureGroupInitialStates();
  startY.value = e.clientY;
  startTop.value = itemState.value.top;
  startHeight.value = itemState.value.height;
  document.addEventListener("mousemove", onResizeTop);
  document.addEventListener("mouseup", stopResize);
};

const onResizeTop = (e: MouseEvent) => {
  if (isResizing.value) {
    const deltaY = e.clientY - startY.value;
    const newTop = startTop.value + deltaY;
    const newHeight = startHeight.value - deltaY;
    if (newHeight > 100 && newHeight < window.innerHeight - 100 && newTop >= 0) {
      itemState.value.top = newTop;
      itemState.value.height = newHeight;
      savePositionAndSize();
    }
  }
};

const startResizeLeft = (e: MouseEvent) => {
  e.preventDefault();
  handleReziging(e);
  resizeDirection.value = "left";
  captureGroupInitialStates();
  startX.value = e.clientX;
  startLeft.value = itemState.value.left;
  startWidth.value = itemState.value.width;
  document.addEventListener("mousemove", onResizeLeft);
  document.addEventListener("mouseup", stopResize);
};

const onResizeLeft = (e: MouseEvent) => {
  if (isResizing.value) {
    const deltaX = e.clientX - startX.value;
    const newLeft = startLeft.value + deltaX;
    const newWidth = startWidth.value - deltaX;
    if (newWidth > 100 && newWidth < window.innerWidth - 100) {
      itemState.value.left = newLeft;
      itemState.value.width = newWidth;
      savePositionAndSize();
    }
  }
};

const stopResize = () => {
  if (isResizing.value) {
    isResizing.value = false;
    resizeDirection.value = null;
    initialGroupStates.value = {};
    if (activeLine.value) {
      activeLine.value.classList.remove("resizing-highlight-line");
    }
    itemStore.inResizing = false;
    document.removeEventListener("mousemove", onResizeWidth);
    document.removeEventListener("mousemove", onResizeHeight);
    document.removeEventListener("mousemove", onResizeTop);
    document.removeEventListener("mousemove", onResizeLeft);
  }
};

const startMove = (e: MouseEvent) => {
  registerClick();
  if (!props.allowFreeMove) return;
  e.preventDefault();
  if (
    (e.target as HTMLElement).classList.contains("icon") ||
    (e.target as HTMLElement).classList.contains("minimal-button")
  )
    return;

  isDragging.value = true;
  startX.value = e.clientX;
  startY.value = e.clientY;
  startLeft.value = itemState.value.left;
  startTop.value = itemState.value.top;
  document.addEventListener("mousemove", onMove);
  document.addEventListener("mouseup", stopMove);
  itemState.value.stickynessPosition = "free";
};

const onMove = (e: MouseEvent) => {
  if (isDragging.value) {
    const deltaX = e.clientX - startX.value;
    const deltaY = e.clientY - startY.value;
    itemState.value.left = startLeft.value + deltaX;
    itemState.value.top = startTop.value + deltaY;
  }
};

const stopMove = () => {
  if (isDragging.value) {
    isDragging.value = false;
    document.removeEventListener("mousemove", onMove);
    document.removeEventListener("mouseup", stopMove);

    savePositionAndSize();

    if (props.preventOverlap) {
      itemStore.preventOverlap(props.id);
      loadPositionAndSize();
    }
  }
};

const moveToRight = () => {
  const parentElement = instance?.parent?.vnode.el as HTMLElement | null;
  if (parentElement) {
    const parentRight = parentElement.offsetLeft + parentElement.offsetWidth;
    itemState.value.left = parentRight - itemState.value.width;
    itemState.value.top = parentElement.offsetTop;
    itemState.value.stickynessPosition = "right";
    if (itemState.value.fullHeight) {
      itemState.value.height = parentElement.offsetHeight;
    }
    savePositionAndSize();
  }
};

const moveToBottom = () => {
  const parentElement = instance?.parent?.vnode.el as HTMLElement | null;
  if (parentElement) {
    const parentBottom = parentElement.offsetTop + parentElement.offsetHeight;
    itemState.value.left = parentElement.offsetLeft + props.initialLeft;
    itemState.value.top = parentBottom - (itemState.value.height + props.initialTop);
    itemState.value.stickynessPosition = "bottom";
    if (itemState.value.fullWidth) {
      itemState.value.width = parentElement.offsetWidth - props.initialLeft;
    }
    savePositionAndSize();
  }
};

const moveToLeft = () => {
  const parentElement = instance?.parent?.vnode.el as HTMLElement | null;
  if (parentElement) {
    itemState.value.left = parentElement.offsetLeft;
    itemState.value.top = parentElement.offsetTop;
    itemState.value.stickynessPosition = "left";
    if (itemState.value.fullHeight) {
      itemState.value.height = parentElement.offsetHeight;
    }
    savePositionAndSize();
  }
};

const moveToTop = () => {
  const parentElement = instance?.parent?.vnode.el as HTMLElement | null;
  if (parentElement) {
    itemState.value.left = parentElement.offsetLeft;
    itemState.value.top = parentElement.offsetTop;
    itemState.value.stickynessPosition = "top";
    if (itemState.value.fullWidth) {
      itemState.value.width = parentElement.offsetWidth;
    }
    savePositionAndSize();
  }
};

const applyStickyPosition = () => {
  const parentElement = instance?.parent?.vnode.el as HTMLElement | null;
  if (!parentElement) {
    console.warn(`No parent element found for ${props.id}`);
    return;
  }

  switch (itemState.value.stickynessPosition) {
    case "top":
      itemState.value.left = parentElement.offsetLeft;
      itemState.value.top = parentElement.offsetTop;
      if (itemState.value.fullWidth) {
        itemState.value.width = parentElement.offsetWidth;
      }
      break;

    case "bottom":
      itemState.value.left = parentElement.offsetLeft + (props.initialLeft || 0);
      itemState.value.top =
        parentElement.offsetTop +
        parentElement.offsetHeight -
        itemState.value.height -
        (props.initialTop || 0);
      if (itemState.value.fullWidth) {
        itemState.value.width = parentElement.offsetWidth - (props.initialLeft || 0);
      }
      break;

    case "left":
      itemState.value.left = parentElement.offsetLeft;
      itemState.value.top = parentElement.offsetTop + (props.initialTop || 0);
      if (itemState.value.fullHeight) {
        itemState.value.height = parentElement.offsetHeight - (props.initialTop || 0);
      }
      break;

    case "right":
      itemState.value.left =
        parentElement.offsetLeft + parentElement.offsetWidth - itemState.value.width;
      itemState.value.top = parentElement.offsetTop + (props.initialTop || 0);
      if (itemState.value.fullHeight) {
        itemState.value.height = parentElement.offsetHeight - (props.initialTop || 0);
      }
      break;

    case "free":
    default:
      // Keep current position
      break;
  }

  // Save the new position
  savePositionAndSize();
};

const calculateWidth = () => {
  if (props.initialWidth) {
    return props.initialWidth;
  } else if (props.initialPosition === "top" || props.initialPosition === "bottom") {
    return instance?.parent?.vnode.el?.offsetWidth - props.initialLeft || 300;
  } else return 300;
};

const calculateHeight = () => {
  if (props.initialHeight) {
    return props.initialHeight;
  } else if (props.initialPosition === "left" || props.initialPosition === "right") {
    return instance?.parent?.vnode.el?.offsetHeight - props.initialHeight || 300;
  } else return 300;
};

const handleResize = () => {
  clearTimeout(resizeTimeout);
  resizeTimeout = setTimeout(applyStickyPosition, 1);
};

const parentResizeObserver = new ResizeObserver(() => {
  handleResize();
});

const observeParentResize = () => {
  const parentElement = instance?.parent?.vnode.el as HTMLElement | null;
  if (parentElement) {
    parentResizeObserver.observe(parentElement);
  }
};

const registerClick = () => {
  itemStore.clickOnItem(props.id);
};

const setFullScreen = (makeFull: boolean) => {
  itemStore.setFullScreen(props.id, makeFull);
  loadPositionAndSize();
};

watch(
  () => itemStore.items[props.id],
  (newState) => {
    if (newState) {
      if (isDragging.value || isResizing.value) {
        itemState.value.zIndex = newState.zIndex;
      } else {
        itemState.value = { ...newState };
      }
    }
  },
  { deep: true },
);

watch(
  () => ({ group: props.group, syncDimensions: props.syncDimensions }),
  ({ group, syncDimensions }) => {
    itemStore.setItemState(props.id, {
      group,
      syncDimensions,
    });
    itemState.value.group = group;
    itemState.value.syncDimensions = syncDimensions;
  },
);

nextTick().then(() => {
  observeParentResize();
});

onMounted(() => {
  // Calculate initial values based on props
  const initialWidth = calculateWidth();
  const initialHeight = calculateHeight();
  const initialLeft = props.initialLeft || 100;
  const initialTop = props.initialTop || 100;

  // IMPORTANT: Register the true initial state FIRST
  itemStore.registerInitialState(props.id, {
    width: initialWidth,
    height: initialHeight,
    left: initialLeft,
    top: initialTop,
    stickynessPosition: props.initialPosition,
    fullWidth: !props.initialWidth,
    fullHeight: !props.initialHeight,
    group: props.group,
    syncDimensions: props.syncDimensions,
  });

  // Check if there's a saved state
  const hasSavedState = localStorage.getItem(`overlayPositionAndSize_${props.id}`) !== null;

  if (!hasSavedState) {
    // No saved state, use initial values
    itemStore.setItemState(props.id, {
      width: initialWidth,
      height: initialHeight,
      left: initialLeft,
      top: initialTop,
      fullHeight: !props.initialHeight,
      fullWidth: !props.initialWidth,
      stickynessPosition: props.initialPosition,
      group: props.group,
      syncDimensions: props.syncDimensions,
    });
    itemState.value = itemStore.items[props.id];

    // Apply sticky position after DOM is ready
    if (props.initialPosition !== "free") {
      nextTick(() => {
        applyStickyPosition();
      });
    }
  } else {
    // Load saved state
    loadPositionAndSize();

    // If the saved state has a sticky position, re-apply it
    if (itemState.value.stickynessPosition && itemState.value.stickynessPosition !== "free") {
      nextTick(() => {
        applyStickyPosition();
      });
    }
  }

  // Listen for layout reset events
  const handleLayoutReset = () => {
    // Get the updated state from the store
    itemState.value = { ...itemStore.items[props.id] };

    // Re-apply sticky position if not "free"
    if (itemState.value.stickynessPosition && itemState.value.stickynessPosition !== "free") {
      nextTick(() => {
        applyStickyPosition();
      });
    }
  };

  window.addEventListener("layout-reset", handleLayoutReset);
  document.addEventListener("mouseup", stopResize);

  // Store the event handler for cleanup
  (window as any)[`resetHandler_${props.id}`] = handleLayoutReset;
});

defineExpose({
  setFullScreen,
});

// Update the onBeforeUnmount to clean up the event listener
onBeforeUnmount(() => {
  const handler = (window as any)[`resetHandler_${props.id}`];
  if (handler) {
    window.removeEventListener("layout-reset", handler);
    delete (window as any)[`resetHandler_${props.id}`];
  }
  document.removeEventListener("mouseup", stopResize);
  document.removeEventListener("mousemove", onMove);
  document.removeEventListener("mouseup", stopMove);
  document.removeEventListener("mousemove", onResizeWidth);
  document.removeEventListener("mousemove", onResizeHeight);
  document.removeEventListener("mousemove", onResizeTop);
  document.removeEventListener("mousemove", onResizeLeft);
});
</script>

<style scoped>
/* (Styles are unchanged) */
.minimal-button {
  background: none;
  border: none;
  padding: 4px;
  margin: 0 2px;
  font-size: 16px;
  cursor: pointer;
  color: var(--color-text-primary);
  position: relative;
  background-color: var(--color-background-tertiary);
  border-radius: 4px;
  width: 25px;
  height: 25px;
}
.minimal-button[data-tooltip="true"]::after {
  content: attr(data-tooltip-text);
  position: absolute;
  top: calc(100% + 5px);
  left: 50%;
  transform: translateX(-50%);
  background-color: var(--color-gray-800);
  color: var(--color-text-inverse);
  padding: 4px 8px;
  border-radius: 4px;
  white-space: nowrap;
  font-size: 12px;
  opacity: 0;
  visibility: hidden;
  transition:
    opacity 0.2s,
    visibility 0.2s;
  pointer-events: none;
  z-index: 100000;
}
.minimal-button[data-tooltip="true"]::before {
  content: "";
  position: absolute;
  top: 100%;
  left: 50%;
  transform: translateX(-50%);
  border-width: 4px;
  border-style: solid;
  border-color: transparent transparent var(--color-gray-800) transparent;
  opacity: 0;
  visibility: hidden;
  transition:
    opacity 0.2s,
    visibility 0.2s;
  pointer-events: none;
  z-index: 100000;
}
.minimal-button[data-tooltip="true"]:hover::after,
.minimal-button[data-tooltip="true"]:hover::before {
  opacity: 1;
  visibility: visible;
}
.minimal-button .icon {
  font-size: 16px;
}
.minimal-button:hover {
  color: var(--color-text-primary);
  background-color: var(--color-background-hover);
}
.group-badge {
  background-color: var(--color-accent);
  color: var(--color-text-inverse);
  padding: 2px 6px;
  border-radius: 3px;
  font-size: 11px;
  margin-right: 4px;
  cursor: pointer;
  transition: background-color 0.2s;
  user-select: none;
}
.title-text {
  flex-grow: 1;
  padding: 0 8px;
  font-size: 14px;
  color: var(--color-text-primary);
}
.overlay.minimized {
  width: auto !important;
  height: 35px !important;
  cursor: default;
}
.overlay {
  position: absolute;
  width: auto;
  max-width: 100%;
  height: auto;
  max-height: 100%;
  box-sizing: border-box;
  background-color: var(--color-background-primary);
  box-shadow: var(--shadow-md);
  border-radius: 8px;
  display: flex;
  flex-direction: column;
  cursor: move;
  transition: border-color 0.2s;
  overflow: hidden;
  border: 1px solid var(--color-border-primary);
}
.no-transition {
  transition: none !important;
}
.header {
  display: flex;
  justify-content: flex-start;
  align-items: center;
  width: 100%;
  padding: 4px;
  border-top-left-radius: 6px;
  border-top-right-radius: 6px;
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-primary);
  min-height: 35px;
  box-sizing: border-box;
  overflow: hidden;
}
.content {
  flex-grow: 1;
  overflow: auto;
  padding: 10px;
  max-height: calc(100% - 50px);
  box-sizing: border-box;
}
.draggable-line {
  position: absolute;
  opacity: 1;
}
.draggable-line.right-vertical {
  top: 0;
  right: 0;
  width: 5px;
  height: 100%;
  cursor: ew-resize;
}
.draggable-line.left-vertical {
  top: 0;
  left: 0;
  width: 5px;
  height: 100%;
  cursor: ew-resize;
}
.draggable-line.bottom-horizontal {
  bottom: 0;
  left: 0;
  width: 100%;
  height: 5px;
  cursor: ns-resize;
}
.draggable-line.top-horizontal {
  top: 0;
  left: 0;
  width: 100%;
  height: 5px;
  cursor: ns-resize;
}
.resizing-highlight-line {
  background-color: #080b0e43;
}
.draggable-line:hover {
  background-color: #2196f330;
}
</style>
