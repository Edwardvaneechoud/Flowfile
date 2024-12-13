<template>
  <div
    v-if="nodeStore.isDrawerOpen"
    class="overlay"
    :class="{ 'no-transition': isResizing }"
    :style="{
      width: width + 'px',
      height: height + 'px',
      top: top + 'px',
      left: left + 'px',
    }"
    @mousedown="startMove"
  >
    <div class="header">
      <button class="move-button" @click="moveToRight">
        <span class="icon">&#10140;</span>
      </button>
    </div>
    <div id="nodesettings" class="content"></div>
    <div class="draggable-line vertical" @mousedown.stop="startResizeWidth"></div>
    <div class="draggable-line horizontal" @mousedown.stop="startResizeHeight"></div>
    <div class="draggable-line top-horizontal" @mousedown.stop="startResizeTop"></div>
    <div class="draggable-line left-vertical" @mousedown.stop="startResizeLeft"></div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onBeforeUnmount } from "vue";
import { useNodeStore } from "../../../stores/column-store";

const nodeStore = useNodeStore();
const width = ref(400);
const height = ref(300);
const isDragging = ref(false);
const isResizing = ref(false);
const startX = ref(0);
const startY = ref(0);
const startWidth = ref(0);
const startHeight = ref(0);
const startLeft = ref(0);
const startTop = ref(0);
const left = ref(100);
const top = ref(100);

const startResizeWidth = (e: MouseEvent) => {
  e.preventDefault();
  isResizing.value = true;
  startX.value = e.clientX;
  startWidth.value = width.value;
  document.addEventListener("mousemove", onResizeWidth);
  document.addEventListener("mouseup", stopResize);
};

const onResizeWidth = (e: MouseEvent) => {
  if (isResizing.value) {
    const deltaX = e.clientX - startX.value;
    const newWidth = startWidth.value + deltaX;
    if (newWidth > 100 && newWidth < window.innerWidth - 100) {
      width.value = newWidth;
    }
  }
};

const startResizeHeight = (e: MouseEvent) => {
  e.preventDefault();
  isResizing.value = true;
  startY.value = e.clientY;
  startHeight.value = height.value;
  document.addEventListener("mousemove", onResizeHeight);
  document.addEventListener("mouseup", stopResize);
};

const onResizeHeight = (e: MouseEvent) => {
  if (isResizing.value) {
    const deltaY = e.clientY - startY.value;
    const newHeight = startHeight.value + deltaY;
    if (newHeight > 100 && newHeight < window.innerHeight - 100) {
      height.value = newHeight;
    }
  }
};

const startResizeTop = (e: MouseEvent) => {
  e.preventDefault();
  isResizing.value = true;
  startY.value = e.clientY;
  startTop.value = top.value;
  startHeight.value = height.value;
  document.addEventListener("mousemove", onResizeTop);
  document.addEventListener("mouseup", stopResize);
};

const onResizeTop = (e: MouseEvent) => {
  if (isResizing.value) {
    const deltaY = e.clientY - startY.value;
    const newTop = startTop.value + deltaY;
    const newHeight = startHeight.value - deltaY;
    if (newHeight > 100 && newHeight < window.innerHeight - 100) {
      top.value = newTop;
      height.value = newHeight;
    }
  }
};

const startResizeLeft = (e: MouseEvent) => {
  e.preventDefault();
  isResizing.value = true;
  startX.value = e.clientX;
  startLeft.value = left.value;
  startWidth.value = width.value;
  document.addEventListener("mousemove", onResizeLeft);
  document.addEventListener("mouseup", stopResize);
};

const onResizeLeft = (e: MouseEvent) => {
  if (isResizing.value) {
    const deltaX = e.clientX - startX.value;
    const newLeft = startLeft.value + deltaX;
    const newWidth = startWidth.value - deltaX;
    if (newWidth > 100 && newWidth < window.innerWidth - 100) {
      left.value = newLeft;
      width.value = newWidth;
    }
  }
};

const stopResize = () => {
  isResizing.value = false;
  document.removeEventListener("mousemove", onResizeWidth);
  document.removeEventListener("mousemove", onResizeHeight);
  document.removeEventListener("mousemove", onResizeTop);
  document.removeEventListener("mousemove", onResizeLeft);
  document.removeEventListener("mouseup", stopResize);
};

const startMove = (e: MouseEvent) => {
  if ((e.target as HTMLElement).classList.contains("draggable-line")) return; // Prevent move when resizing
  e.preventDefault();
  isDragging.value = true;
  startX.value = e.clientX;
  startY.value = e.clientY;
  startLeft.value = left.value;
  startTop.value = top.value;
  document.addEventListener("mousemove", onMove);
  document.addEventListener("mouseup", stopMove);
};

const onMove = (e: MouseEvent) => {
  if (isDragging.value) {
    const deltaX = e.clientX - startX.value;
    const deltaY = e.clientY - startY.value;
    left.value = startLeft.value + deltaX;
    top.value = startTop.value + deltaY;
  }
};

const stopMove = () => {
  isDragging.value = false;
  document.removeEventListener("mousemove", onMove);
  document.removeEventListener("mouseup", stopMove);
};

const moveToRight = () => {
  width.value = 500;
  height.value = window.innerHeight;
  left.value = window.innerWidth - 500;
  top.value = 0;
};

onMounted(() => {
  document.addEventListener("mouseup", stopResize);
  document.addEventListener("mouseup", stopMove);
});

onBeforeUnmount(() => {
  document.removeEventListener("mouseup", stopResize);
  document.removeEventListener("mouseup", stopMove);
});
</script>

<style scoped>
.overlay {
  position: absolute;
  cursor: move;
  background-color: #f9f9f9;
  box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
  border-radius: 8px;
  display: flex;
  flex-direction: column;
}

.no-transition {
  transition: none !important;
}

.header {
  display: flex;
  justify-content: flex-start;
  align-items: center;
  width: 100%;
  padding: 1px;
  border-top-left-radius: 8px;
  border-top-right-radius: 8px;
}

.content {
  flex-grow: 1;
  overflow: auto;
  padding: 10px;
}

.draggable-line {
  position: absolute;
  opacity: 1;
}

.draggable-line.vertical {
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

.draggable-line.horizontal {
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

.move-button {
  background-color: #4caf50;
  color: white;
  border: none;
  padding: 10px 20px;
  font-size: 14px;
  font-weight: bold;
  border-radius: 4px;
  cursor: pointer;
  box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
  transition:
    background-color 0.3s,
    box-shadow 0.3s;
}

.move-button .icon {
  margin-right: 8px;
  font-size: 16px;
}

.move-button:hover {
  background-color: #45a049;
  box-shadow: 0 8px 16px rgba(0, 0, 0, 0.2);
}
</style>
