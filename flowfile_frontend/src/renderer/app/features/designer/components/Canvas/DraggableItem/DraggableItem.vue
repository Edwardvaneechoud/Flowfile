<template>
  <div
    class="overlay"
    :class="{ 'no-transition': isResizing, minimized: isMinimized }"
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
        :title="isMinimized ? 'Maximize' : 'Minimize'"
        @click="toggleMinimize"
      >
        <span class="icon">{{ isMinimized ? "+" : "−" }}</span>
      </button>
      <button
        v-if="showRight && itemState.stickynessPosition !== 'right'"
        class="minimal-button"
        title="Move to Right"
        @click="moveToRight"
      >
        <span class="icon">→</span>
      </button>
      <button
        v-if="showBottom && itemState.stickynessPosition !== 'bottom'"
        class="minimal-button"
        title="Move to Bottom"
        @click="moveToBottom"
      >
        <span class="icon">↓</span>
      </button>
      <button
        v-if="showLeft && itemState.stickynessPosition !== 'left'"
        class="minimal-button"
        title="Move to Left"
        @click="moveToLeft"
      >
        <span class="icon">←</span>
      </button>
      <button
        v-if="showTop && itemState.stickynessPosition !== 'top'"
        class="minimal-button"
        title="Move to Top"
        @click="moveToTop"
      >
        <span class="icon">↑</span>
      </button>
      <button
        v-if="allowFullScreen && !itemState.fullScreen"
        class="minimal-button"
        title="Toggle Full Screen"
        @click="toggleFullScreen"
      >
        <span class="icon">⬜</span>
      </button>
      <button
        v-if="allowFullScreen && itemState.fullScreen"
        class="minimal-button"
        title="To Small Screen"
        @click="toggleFullScreen"
      >
        <span class="icon">❐</span>
      </button>
      {{ title }}
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
import { ref, onMounted, onBeforeUnmount, defineProps, getCurrentInstance, nextTick } from "vue";
import { useItemStore } from "./stateStore";

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
  initalLeft: {
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
});

const itemStore = useItemStore();
const itemState = ref(
  itemStore.items[props.id] || {
    width: props.initialWidth,
    height: props.initialHeight,
    left: 100,
    top: 100,
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

const savePositionAndSize = () => {
  itemStore.setItemState(props.id, itemState.value);
  itemStore.saveItemState(props.id);
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
  if (itemState.value.fullScreen) {
    itemState.value.fullScreen = false;
    itemState.value.width = itemState.value.prevWidth || 300;
    itemState.value.height = itemState.value.prevHeight || 300;
    itemState.value.left = itemState.value.prevLeft || 100;
    itemState.value.top = itemState.value.prevTop || 100;
  } else {
    itemState.value.fullScreen = true;
    itemState.value.prevWidth = itemState.value.width;
    itemState.value.prevHeight = itemState.value.height;
    itemState.value.prevLeft = itemState.value.left;
    itemState.value.prevTop = itemState.value.top;
    itemState.value.width = window.innerWidth - 100;
    itemState.value.height = window.innerHeight - 100;
    itemState.value.left = 50;
    itemState.value.top = 50;
  }
};

const startResizeRight = (e: MouseEvent) => {
  e.preventDefault();
  handleReziging(e);
  startX.value = e.clientX;
  startWidth.value = itemState.value.width;
  document.addEventListener("mousemove", onResizeWidth);
  document.addEventListener("mouseup", stopResize);
};

const onResizeWidth = (e: MouseEvent) => {
  if (isResizing.value) {
    const deltaX = e.clientX - startX.value;
    const newWidth = startWidth.value + deltaX;
    if (newWidth > 100 && newWidth < window.innerWidth - 100) {
      itemState.value.width = newWidth;
      savePositionAndSize();
    }
  }
};

const startResizeBottom = (e: MouseEvent) => {
  e.preventDefault();
  handleReziging(e);
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
    if (newHeight > 100 && newHeight < window.innerHeight - 100) {
      itemState.value.top = newTop;
      itemState.value.height = newHeight;
      savePositionAndSize();
    }
  }
};

const startResizeLeft = (e: MouseEvent) => {
  e.preventDefault();
  handleReziging(e);
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
  isResizing.value = false;
  if (activeLine.value) {
    activeLine.value.classList.remove("resizing-highlight-line");
  }
  itemStore.inResizing = false;
  document.removeEventListener("mousemove", onResizeWidth);
  document.removeEventListener("mousemove", onResizeHeight);
  document.removeEventListener("mousemove", onResizeTop);
  document.removeEventListener("mousemove", onResizeLeft);
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
    savePositionAndSize();
  }
};

const stopMove = () => {
  isDragging.value = false;
  document.removeEventListener("mousemove", onMove);
  document.removeEventListener("mouseup", stopMove);
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
    itemState.value.left = parentElement.offsetLeft;
    itemState.value.top = parentBottom - itemState.value.height;
    itemState.value.stickynessPosition = "bottom";
    if (itemState.value.fullWidth) {
      itemState.value.width = parentElement.offsetWidth;
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
  switch (itemState.value.stickynessPosition) {
    case "top":
      moveToTop();
      break;
    case "bottom":
      moveToBottom();
      break;
    case "left":
      moveToLeft();
      break;
    case "right":
      moveToRight();
      break;
    case "free":
    default:
      // Leave default free position
      break;
  }
};

const calculateWidth = () => {
  if (props.initialWidth) {
    return props.initialWidth;
  } else if (props.initialPosition === "top" || props.initialPosition === "bottom") {
    return instance?.parent?.vnode.el?.offsetWidth || 300;
  } else return 300;
};

const calculateHeight = () => {
  if (props.initialHeight) {
    return props.initialHeight;
  } else if (props.initialPosition === "left" || props.initialPosition === "right") {
    return instance?.parent?.vnode.el?.offsetHeight || 300;
  } else return 300;
};

const handleResize = () => {
  clearTimeout(resizeTimeout);
  resizeTimeout = setTimeout(applyStickyPosition, 1); // Adjust the delay as needed
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
  // Only toggle if the state needs changing
  if (itemState.value.fullScreen !== makeFull) {
    toggleFullScreen();
  }
};


onMounted(() => {
  if (!itemStore.items[props.id]) {
    itemStore.setItemState(props.id, {
      width: calculateWidth(),
      height: calculateHeight(),
      left: props.initalLeft || 100,
      top: props.initialTop || 100,
      fullHeight: !props.initialHeight,
      fullWidth: !props.initialWidth,
    });
    itemState.value = itemStore.items[props.id];
    itemState.value.stickynessPosition = props.initialPosition;
    if (itemState.value.stickynessPosition !== "free") {
      applyStickyPosition();
    }
  } else {
    loadPositionAndSize();
  }
  document.addEventListener("mouseup", stopResize);
  document.addEventListener("mouseup", stopMove);
});
nextTick().then(() => {
  observeParentResize();
});

onBeforeUnmount(() => {
  document.removeEventListener("mouseup", stopResize);
  document.removeEventListener("mouseup", stopMove);
});

defineExpose({
  width: itemState.value.width,
  height: itemState.value.height,
  isDragging,
  isResizing,
  startX,
  startY,
  startWidth,
  startHeight,
  startLeft,
  startTop,
  left: itemState.value.left,
  top: itemState.value.top,
  startResizeRight,
  onResizeWidth,
  startResizeBottom,
  onResizeHeight,
  startResizeTop,
  onResizeTop,
  startResizeLeft,
  onResizeLeft,
  stopResize,
  startMove,
  onMove,
  stopMove,
  moveToRight,
  moveToBottom,
  moveToLeft,
  moveToTop,
  setHeight: (value: number) => (itemState.value.height = value),
  setWitdh: (value: number) => (itemState.value.width = value),
  setFullScreen,
});
</script>

<style scoped>
.minimal-button {
  background: none;
  border: none;
  padding: 4px;
  margin: 0 2px;
  font-size: 16px;
  cursor: pointer;
  color: #333;
  position: relative;
  background-color: #b6c1ff;
  border-radius: 4px;
  width: 25px;
  height: 25px;
}

.minimal-button .icon {
  font-size: 16px;
}

.minimal-button:hover {
  color: #000;
}

.minimal-button::after {
  content: attr(title);
  position: absolute;
  bottom: 100%;
  left: 50%;
  transform: translateX(-50%);
  background-color: #333;
  color: #fff;
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
}

.minimal-button::before {
  content: "";
  position: absolute;
  bottom: calc(100% - 4px);
  left: 50%;
  transform: translateX(-50%);
  border-width: 4px;
  border-style: solid;
  border-color: transparent transparent #333 transparent;
  opacity: 0;
  visibility: hidden;
  transition:
    opacity 0.2s,
    visibility 0.2s;
  pointer-events: none;
}

.minimal-button:hover::after,
.minimal-button:hover::before {
  opacity: 1;
  visibility: visible;
}

.move-handle {
  cursor: grab;
}

.minimized-title {
  cursor: pointer;
  background-color: #f0f0f0;
  color: #333;
  padding: 4px 10px;
  border-radius: 4px;
  display: flex;
  justify-content: center;
  align-items: center;
  font-size: 14px;
}

.overlay.minimized {
  width: 150px;
  height: 30px;
  cursor: default;
}

.overlay {
  position: absolute;
  width: auto;
  max-width: 100%;
  height: auto;
  max-height: 100%;
  box-sizing: border-box;
  background-color: #f9f9f9;
  box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
  border-radius: 8px;
  display: flex;
  flex-direction: column;
  cursor: move;
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
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  background-color: #fff;
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
</style>
