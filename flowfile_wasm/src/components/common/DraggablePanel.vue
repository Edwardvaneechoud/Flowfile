<template>
  <div
    class="draggable-panel"
    :class="{ minimized: isMinimized, resizing: isResizing }"
    :style="panelStyle"
    ref="panelRef"
    @mousedown.stop="bringToFront"
  >
    <div class="panel-header" @mousedown="startMove">
      <button class="header-btn" @click.stop="toggleMinimize" :title="isMinimized ? 'Expand' : 'Minimize'">
        {{ isMinimized ? '+' : '−' }}
      </button>
      <span class="panel-title">{{ title }}</span>
      <button v-if="onClose" class="header-btn close-btn" @click.stop="onClose" title="Close">
        ×
      </button>
    </div>
    <div v-if="!isMinimized" class="panel-content">
      <slot></slot>
    </div>
    <!-- Resize handles -->
    <div v-if="!isMinimized" class="resize-handle right" @mousedown.stop="startResize('right')"></div>
    <div v-if="!isMinimized" class="resize-handle bottom" @mousedown.stop="startResize('bottom')"></div>
    <div v-if="!isMinimized" class="resize-handle left" @mousedown.stop="startResize('left')"></div>
    <div v-if="!isMinimized" class="resize-handle top" @mousedown.stop="startResize('top')"></div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import { getPanelState, savePanelState, clearPanelState, type PanelState } from '../../stores/panel-store'

// Global z-index counter shared across all DraggablePanel instances
let globalMaxZIndex = 100

interface Props {
  title: string
  panelId?: string
  initialPosition?: 'left' | 'right' | 'bottom' | 'top'
  initialWidth?: number
  initialHeight?: number
  initialLeft?: number
  initialTop?: number
  defaultZIndex?: number
  onClose?: () => void
}

const props = withDefaults(defineProps<Props>(), {
  initialPosition: 'right',
  initialWidth: 400,
  initialHeight: 300,
  initialLeft: 0,
  initialTop: 50,
  defaultZIndex: 100
})

const panelRef = ref<HTMLElement | null>(null)

// Panel state
const width = ref(props.initialWidth)
const height = ref(props.initialHeight)
const left = ref(props.initialLeft)
const top = ref(props.initialTop)
const isMinimized = ref(false)
const isResizing = ref(false)
const isDragging = ref(false)

// Initialize zIndex with defaultZIndex prop, update global counter if needed
const initialZIndex = Math.max(props.defaultZIndex, globalMaxZIndex + 1)
globalMaxZIndex = Math.max(globalMaxZIndex, initialZIndex)
const zIndex = ref(initialZIndex)

// Drag state
const startX = ref(0)
const startY = ref(0)
const startWidth = ref(0)
const startHeight = ref(0)
const startLeft = ref(0)
const startTop = ref(0)
const resizeDirection = ref<string | null>(null)

// Save current panel state to storage
function saveCurrentState() {
  if (!props.panelId) return

  const state: PanelState = {
    width: width.value,
    height: height.value,
    left: left.value,
    top: top.value,
    isMinimized: isMinimized.value,
    zIndex: zIndex.value
  }
  savePanelState(props.panelId, state)
}

// Reset panel to default position based on initialPosition prop
function resetToDefault() {
  const vh = window.innerHeight
  const vw = window.innerWidth

  // Clear saved state so panel uses defaults
  if (props.panelId) {
    clearPanelState(props.panelId)
  }

  // Reset to initial dimensions
  width.value = props.initialWidth
  height.value = props.initialHeight
  isMinimized.value = false

  // Calculate position based on initialPosition prop
  switch (props.initialPosition) {
    case 'right':
      left.value = vw - width.value
      top.value = props.initialTop
      height.value = vh - props.initialTop
      break
    case 'left':
      left.value = 0
      top.value = props.initialTop
      height.value = vh - props.initialTop
      break
    case 'bottom':
      left.value = props.initialLeft
      top.value = vh - height.value
      width.value = vw - props.initialLeft
      break
    case 'top':
      left.value = props.initialLeft
      top.value = props.initialTop
      width.value = vw - props.initialLeft
      break
  }
}

// Handle window resize - reset panels to default positions
function handleWindowResize() {
  resetToDefault()
}

// Compute initial position based on prop
onMounted(() => {
  const vh = window.innerHeight
  const vw = window.innerWidth

  // Try to restore saved state first
  if (props.panelId) {
    const savedState = getPanelState(props.panelId)
    if (savedState) {
      // Validate saved position is still within viewport
      const validLeft = Math.max(0, Math.min(savedState.left, vw - 100))
      const validTop = Math.max(0, Math.min(savedState.top, vh - 50))
      const validWidth = Math.min(savedState.width, vw - validLeft)
      const validHeight = Math.min(savedState.height, vh - validTop)

      width.value = validWidth
      height.value = validHeight
      left.value = validLeft
      top.value = validTop
      isMinimized.value = savedState.isMinimized

      // Restore zIndex if saved, and update global counter
      if (savedState.zIndex !== undefined) {
        zIndex.value = savedState.zIndex
        globalMaxZIndex = Math.max(globalMaxZIndex, savedState.zIndex)
      }
      return
    }
  }

  // No saved state, use initial position based on prop
  switch (props.initialPosition) {
    case 'right':
      left.value = vw - width.value
      top.value = props.initialTop
      height.value = vh - props.initialTop
      break
    case 'left':
      left.value = 0
      top.value = props.initialTop
      height.value = vh - props.initialTop
      break
    case 'bottom':
      left.value = props.initialLeft
      top.value = vh - height.value
      width.value = vw - props.initialLeft
      break
    case 'top':
      left.value = props.initialLeft
      top.value = props.initialTop
      width.value = vw - props.initialLeft
      break
  }

  // Add window resize listener to reset panels to default when window size changes
  window.addEventListener('resize', handleWindowResize)
})

// Watch for changes to initialTop and update position (only if no saved state)
watch(() => props.initialTop, (newTop) => {
  // Don't update position if panel has saved state (user has manually positioned it)
  if (props.panelId && getPanelState(props.panelId)) {
    return
  }

  const vh = window.innerHeight
  top.value = newTop

  // Adjust height for left/right panels
  if (props.initialPosition === 'left' || props.initialPosition === 'right') {
    height.value = vh - newTop
  }
})

const panelStyle = computed(() => ({
  width: isMinimized.value ? 'auto' : `${width.value}px`,
  height: isMinimized.value ? 'auto' : `${height.value}px`,
  left: `${left.value}px`,
  top: `${top.value}px`,
  zIndex: zIndex.value
}))

function bringToFront() {
  // Only update if this panel is not already at the top
  if (zIndex.value <= globalMaxZIndex) {
    globalMaxZIndex++
    zIndex.value = globalMaxZIndex
    saveCurrentState()
  }
}

function toggleMinimize() {
  isMinimized.value = !isMinimized.value
  saveCurrentState()
}

function startMove(e: MouseEvent) {
  if ((e.target as HTMLElement).classList.contains('header-btn')) return

  isDragging.value = true
  startX.value = e.clientX
  startY.value = e.clientY
  startLeft.value = left.value
  startTop.value = top.value

  document.addEventListener('mousemove', onMove)
  document.addEventListener('mouseup', stopMove)
}

function onMove(e: MouseEvent) {
  if (!isDragging.value) return

  const dx = e.clientX - startX.value
  const dy = e.clientY - startY.value

  left.value = Math.max(0, startLeft.value + dx)
  top.value = Math.max(0, startTop.value + dy)
}

function stopMove() {
  isDragging.value = false
  document.removeEventListener('mousemove', onMove)
  document.removeEventListener('mouseup', stopMove)
  saveCurrentState()
}

function startResize(direction: string) {
  isResizing.value = true
  resizeDirection.value = direction
  startX.value = event instanceof MouseEvent ? event.clientX : 0
  startY.value = event instanceof MouseEvent ? event.clientY : 0
  startWidth.value = width.value
  startHeight.value = height.value
  startLeft.value = left.value
  startTop.value = top.value

  document.addEventListener('mousemove', onResize)
  document.addEventListener('mouseup', stopResize)
}

function onResize(e: MouseEvent) {
  if (!isResizing.value) return

  const dx = e.clientX - startX.value
  const dy = e.clientY - startY.value

  switch (resizeDirection.value) {
    case 'right':
      width.value = Math.max(200, startWidth.value + dx)
      break
    case 'left':
      const newWidthL = startWidth.value - dx
      if (newWidthL >= 200) {
        width.value = newWidthL
        left.value = startLeft.value + dx
      }
      break
    case 'bottom':
      height.value = Math.max(100, startHeight.value + dy)
      break
    case 'top':
      const newHeightT = startHeight.value - dy
      if (newHeightT >= 100) {
        height.value = newHeightT
        top.value = startTop.value + dy
      }
      break
  }
}

function stopResize() {
  isResizing.value = false
  resizeDirection.value = null
  document.removeEventListener('mousemove', onResize)
  document.removeEventListener('mouseup', stopResize)
  saveCurrentState()
}

onUnmounted(() => {
  document.removeEventListener('mousemove', onMove)
  document.removeEventListener('mouseup', stopMove)
  document.removeEventListener('mousemove', onResize)
  document.removeEventListener('mouseup', stopResize)
  window.removeEventListener('resize', handleWindowResize)
})
</script>

<style scoped>
.draggable-panel {
  position: fixed;
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  box-shadow: var(--shadow-lg);
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.draggable-panel.resizing {
  user-select: none;
}

.draggable-panel.minimized {
  height: auto !important;
}

.panel-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  background: var(--bg-tertiary);
  border-bottom: 1px solid var(--border-light);
  cursor: move;
  user-select: none;
}

.header-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 24px;
  height: 24px;
  background: var(--bg-secondary);
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-size: 16px;
  color: var(--text-primary);
  transition: background 0.15s;
}

.header-btn:hover {
  background: var(--bg-hover);
}

.close-btn {
  margin-left: auto;
}

.panel-title {
  font-size: 13px;
  font-weight: 500;
  color: var(--text-primary);
  background: var(--accent-color);
  color: white;
  padding: 2px 8px;
  border-radius: 4px;
}

.panel-content {
  flex: 1;
  overflow: auto;
  padding: 12px;
}

.resize-handle {
  position: absolute;
  opacity: 0;
  transition: opacity 0.15s;
  pointer-events: none;
}

.draggable-panel:hover .resize-handle {
  opacity: 1;
  pointer-events: auto;
}

.resize-handle.right {
  top: 0;
  right: 0;
  width: 4px;
  height: 100%;
  cursor: ew-resize;
}

.resize-handle.left {
  top: 0;
  left: 0;
  width: 4px;
  height: 100%;
  cursor: ew-resize;
}

.resize-handle.bottom {
  bottom: 0;
  left: 0;
  width: 100%;
  height: 4px;
  cursor: ns-resize;
}

.resize-handle.top {
  top: 0;
  left: 0;
  width: 100%;
  height: 4px;
  cursor: ns-resize;
}

.resize-handle:hover {
  background: var(--accent-color);
  opacity: 0.5;
}
</style>
