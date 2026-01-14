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
          <button class="close-btn" title="Close" @click="isOpen = false">×</button>
        </div>
        <div class="panel-body">
          <button class="control-btn" @click="runAction(resetLayout)">
            <span class="icon">↺</span> Reset Panel Layout
          </button>
          <button class="control-btn" @click="runAction(fitToScreen)">
            <span class="icon">⊡</span> Fit to Screen
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
      <svg class="layout-icon" viewBox="0 0 24 24" width="20" height="20">
        <rect x="2" y="2" width="8" height="6" fill="currentColor" opacity="0.9" />
        <rect x="12" y="2" width="8" height="6" fill="currentColor" opacity="0.7" />
        <rect x="2" y="10" width="8" height="10" fill="currentColor" opacity="0.7" />
        <rect x="12" y="10" width="8" height="10" fill="currentColor" opacity="0.9" />
      </svg>
    </button>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, computed } from 'vue'
import { clearAllPanelStates } from '../../stores/panel-store'

const emit = defineEmits<{
  (e: 'reset-layout'): void
  (e: 'fit-to-screen'): void
}>()

const isOpen = ref(false)

// Position state
const BUTTON_SIZE = 40
const BOUNDARY_MARGIN = 10
const position = ref({ x: window.innerWidth - BUTTON_SIZE - BOUNDARY_MARGIN, y: window.innerHeight - BUTTON_SIZE - BOUNDARY_MARGIN })
const isDragging = ref(false)
const hasDragged = ref(false)
const dragStart = ref({ x: 0, y: 0 })
const initialPosition = ref({ x: 0, y: 0 })

// Handle window resizing - reset to bottom-right corner
const handleViewportResize = () => {
  position.value.x = window.innerWidth - BUTTON_SIZE - BOUNDARY_MARGIN
  position.value.y = window.innerHeight - BUTTON_SIZE - BOUNDARY_MARGIN
  savePosition()
}

// Computed property for dynamic panel positioning
const panelStyle = computed(() => {
  const style: { [key: string]: string } = {}
  const isRightHalf = position.value.x > window.innerWidth / 2
  const isBottomHalf = position.value.y > window.innerHeight / 2

  // Position horizontally
  if (isRightHalf) {
    style.right = '50px'
  } else {
    style.left = '50px'
  }

  // Position vertically
  if (isBottomHalf) {
    style.bottom = '0px'
  } else {
    style.top = '0px'
  }

  return style
})

// Load saved position from localStorage
onMounted(() => {
  const savedPosition = localStorage.getItem('layoutControlsPosition')

  if (savedPosition) {
    const parsed = JSON.parse(savedPosition)
    const maxX = window.innerWidth - BUTTON_SIZE - BOUNDARY_MARGIN
    const maxY = window.innerHeight - BUTTON_SIZE - BOUNDARY_MARGIN

    if (
      parsed.x <= maxX &&
      parsed.y <= maxY &&
      parsed.x >= BOUNDARY_MARGIN &&
      parsed.y >= BOUNDARY_MARGIN
    ) {
      position.value = parsed
    } else {
      position.value.x = maxX
      position.value.y = maxY
      savePosition()
    }
  }

  window.addEventListener('resize', handleViewportResize)
})

// Save position to localStorage
const savePosition = () => {
  localStorage.setItem('layoutControlsPosition', JSON.stringify(position.value))
}

// Handle mouse down - prepare for potential drag
const handleMouseDown = (e: MouseEvent) => {
  e.preventDefault()
  hasDragged.value = false

  if (isOpen.value) {
    return
  }

  isDragging.value = true
  dragStart.value = {
    x: e.clientX,
    y: e.clientY,
  }
  initialPosition.value = {
    x: position.value.x,
    y: position.value.y,
  }

  document.addEventListener('mousemove', onDrag)
  document.addEventListener('mouseup', stopDrag)
}

// Handle click - open panel only if we didn't drag
const handleClick = (e: MouseEvent) => {
  e.preventDefault()
  e.stopPropagation()

  if (!hasDragged.value) {
    isOpen.value = !isOpen.value
  }
}

const onDrag = (e: MouseEvent) => {
  if (!isDragging.value) return

  const deltaX = e.clientX - dragStart.value.x
  const deltaY = e.clientY - dragStart.value.y

  if (Math.abs(deltaX) > 5 || Math.abs(deltaY) > 5) {
    hasDragged.value = true
  }

  if (hasDragged.value) {
    let newX = initialPosition.value.x + deltaX
    let newY = initialPosition.value.y + deltaY

    // Keep within viewport bounds
    newX = Math.max(BOUNDARY_MARGIN, Math.min(window.innerWidth - BUTTON_SIZE - BOUNDARY_MARGIN, newX))
    newY = Math.max(BOUNDARY_MARGIN, Math.min(window.innerHeight - BUTTON_SIZE - BOUNDARY_MARGIN, newY))

    position.value = { x: newX, y: newY }
  }
}

const stopDrag = () => {
  if (isDragging.value) {
    isDragging.value = false
    if (hasDragged.value) {
      savePosition()
    }
    document.removeEventListener('mousemove', onDrag)
    document.removeEventListener('mouseup', stopDrag)
  }
}

// Helper function to run an action and then close the panel
const runAction = <T extends any[]>(action: (...args: T) => void, ...args: T) => {
  action(...args)
  isOpen.value = false
}

// Reset all panel layouts to defaults
const resetLayout = () => {
  clearAllPanelStates()
  emit('reset-layout')
}

// Fit panels to screen
const fitToScreen = () => {
  emit('fit-to-screen')
}

// Cleanup
onUnmounted(() => {
  document.removeEventListener('mousemove', onDrag)
  document.removeEventListener('mouseup', stopDrag)
  window.removeEventListener('resize', handleViewportResize)
})
</script>

<style scoped>
.layout-widget-wrapper {
  position: fixed;
  z-index: 20000;
}

.trigger-btn {
  width: 40px;
  height: 40px;
  border-radius: 50%;
  border: none;
  background: linear-gradient(135deg, var(--accent-color) 0%, var(--accent-hover) 100%);
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
  transform: scale(1.1);
  box-shadow: var(--shadow-lg);
}

.trigger-btn.is-open {
  cursor: pointer;
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
  width: 200px;
  background: var(--bg-secondary);
  border-radius: 8px;
  box-shadow: var(--shadow-lg);
  overflow: hidden;
  display: flex;
  flex-direction: column;
  border: 1px solid var(--border-color);
}

.panel-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 10px 12px;
  border-bottom: 1px solid var(--border-light);
  background: var(--bg-tertiary);
}

.panel-title {
  font-weight: 600;
  font-size: 13px;
  color: var(--text-primary);
  user-select: none;
}

.close-btn {
  background: none;
  border: none;
  font-size: 18px;
  color: var(--text-secondary);
  cursor: pointer;
  padding: 0;
  line-height: 1;
  width: 20px;
  height: 20px;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 4px;
  transition: all 0.2s;
}

.close-btn:hover {
  color: var(--text-primary);
  background: var(--bg-hover);
}

.panel-body {
  padding: 12px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.control-btn {
  background-color: var(--bg-tertiary);
  color: var(--text-primary);
  border: 1px solid var(--border-color);
  border-radius: 6px;
  padding: 8px 12px;
  cursor: pointer;
  font-size: 13px;
  font-weight: 500;
  transition: all 0.2s ease;
  display: flex;
  align-items: center;
  gap: 8px;
  text-align: left;
}

.control-btn:hover {
  background-color: var(--bg-hover);
  border-color: var(--accent-color);
  transform: translateX(2px);
}

.control-btn .icon {
  font-size: 14px;
  min-width: 18px;
}

/* Vue Transition Styles */
.panel-fade-enter-active,
.panel-fade-leave-active {
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
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
