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
    <div class="header" @mousedown="startMove" @dblclick="handleHeaderDblClick">
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
      <div v-if="tabs.length" class="dragitem-tabs" @mousedown.stop>
        <button
          v-for="t in tabs"
          :key="t.id"
          class="dragitem-tab"
          :class="{ active: t.id === activeTab }"
          @click="emit('update:activeTab', t.id)"
        >
          {{ t.label }}
        </button>
      </div>
      <div v-else-if="title" class="dragitem-tabs" @mousedown="startMove">
        <span class="dragitem-tab dragitem-tab--static active">{{ title }}</span>
      </div>
    </div>

    <div class="content" @click="registerClick">
      <slot v-if="!isMinimized"></slot>
    </div>

    <!-- Optional sticky footer (e.g. node-settings Apply bar), pinned below the
         scrolling content. Only rendered when a `footer` slot is provided. -->
    <div v-if="!isMinimized && $slots.footer" class="footer">
      <slot name="footer"></slot>
    </div>

    <div
      class="draggable-line right-vertical"
      @mousedown.stop="startResizeRight"
      @mouseenter="resizeOnEnter($event, 'right')"
      @dblclick.stop="handleResizeBarDblClick"
    ></div>
    <div
      class="draggable-line bottom-horizontal"
      @mousedown.stop="startResizeBottom"
      @mouseenter="resizeOnEnter($event, 'bottom')"
      @dblclick.stop="handleResizeBarDblClick"
    ></div>
    <div
      class="draggable-line top-horizontal"
      @mousedown.stop="startResizeTop"
      @mouseenter="resizeOnEnter($event, 'top')"
      @dblclick.stop="handleResizeBarDblClick"
    ></div>
    <div
      class="draggable-line left-vertical"
      @mousedown.stop="startResizeLeft"
      @mouseenter="resizeOnEnter($event, 'left')"
      @dblclick.stop="handleResizeBarDblClick"
    ></div>
  </div>
</template>

<script setup lang="ts">
import {
  ref,
  computed,
  onMounted,
  onBeforeUnmount,
  defineExpose,
  defineProps,
  getCurrentInstance,
  nextTick,
  watch,
} from 'vue'
import { useItemStore } from './stateStore'
import type { ItemLayout, AxisBehaviour } from './stateStore'

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
    type: String as () => 'top' | 'bottom' | 'left' | 'right' | 'free',
    default: 'free',
  },
  initialHeight: {
    type: Number,
    default: null,
  },
  initialWidth: {
    type: Number,
    default: null,
  },
  // Per-axis resize response (see AxisBehaviour).
  widthBehaviour: {
    type: String as () => AxisBehaviour,
    default: null,
  },
  heightBehaviour: {
    type: String as () => AxisBehaviour,
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
    default: '',
  },
  onMinimize: {
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
  // Tab strip rendered in the header. Empty ⇒ the `title` shows as a single
  // static tab (so every panel header looks the same).
  tabs: {
    type: Array as () => { id: string; label: string }[],
    default: () => [],
  },
  activeTab: {
    type: String,
    default: '',
  },
})

const emit = defineEmits(['update:activeTab'])

const itemStore = useItemStore()
const itemState = ref(
  itemStore.items[props.id] || {
    width: props.initialWidth || 400,
    height: props.initialHeight || 300,
    left: props.initialLeft || 100,
    top: props.initialTop || 100,
    group: props.group,
    syncDimensions: props.syncDimensions,
    zIndex: 100,
  },
)

// Unset ⇒ legacy rule: "fill" when no initial size was given, else "fixed".
const resolvedWidthBehaviour = computed<AxisBehaviour>(
  () => props.widthBehaviour ?? (props.initialWidth ? 'fixed' : 'fill'),
)
const resolvedHeightBehaviour = computed<AxisBehaviour>(
  () => props.heightBehaviour ?? (props.initialHeight ? 'fixed' : 'fill'),
)

const isDragging = ref(false)
const isResizing = ref(false)
const startX = ref(0)
const startY = ref(0)
const startWidth = ref(0)
const startHeight = ref(0)
const startLeft = ref(0)
const startTop = ref(0)
const isMinimized = ref(false)
const instance = getCurrentInstance()
const activeLine = ref<HTMLElement | null>(null)
let resizeTimeout: ReturnType<typeof setTimeout>

const resizeDirection = ref<'top' | 'bottom' | 'left' | 'right' | null>(null)
const initialGroupStates = ref<
  Record<string, { top: number; left: number; width: number; height: number }>
>({})

// Track previous container dimensions for proportional repositioning. Seeded in
// onMounted from the live container — panels are absolutely positioned inside
// the canvas container, so every bounds calculation must use the container, not
// the window (which is taller by the page header).
const prevContainerWidth = ref(0)
const prevContainerHeight = ref(0)

// Container bounds captured once at the start of a resize gesture. The container
// can't change mid-drag, and reading it per-mousemove would force a reflow.
const resizeBounds = ref<{ width: number; height: number }>({ width: 0, height: 0 })

const resizeDelay = ref<ReturnType<typeof setTimeout> | null>(null)
const resizeOnEnter = (e: MouseEvent, position: 'top' | 'bottom' | 'left' | 'right') => {
  if (resizeDelay.value) clearTimeout(resizeDelay.value)
  resizeDelay.value = setTimeout(() => {
    if (itemStore.inResizing && !isResizing.value) {
      switch (position) {
        case 'right':
          startResizeRight(e)
          break
        case 'bottom':
          startResizeBottom(e)
          break
        case 'top':
          startResizeTop(e)
          break
        case 'left':
          startResizeLeft(e)
          break
      }
    }
  }, 200)
}

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
  })

  itemStore.saveItemState(props.id)

  if (itemState.value.group && itemState.value.syncDimensions && isResizing.value) {
    const groupItems = itemStore.groups[itemState.value.group]
    if (groupItems) {
      const initialActiveState = initialGroupStates.value[props.id]
      if (!initialActiveState) return

      const deltaX = itemState.value.left - initialActiveState.left
      const deltaY = itemState.value.top - initialActiveState.top

      groupItems.forEach((itemId) => {
        if (itemId === props.id) return

        const initialItemState = initialGroupStates.value[itemId]
        if (itemStore.items[itemId]?.syncDimensions && initialItemState) {
          const updates: Partial<ItemLayout> = {
            width: itemState.value.width,
            height: itemState.value.height,
          }

          if (resizeDirection.value === 'top') {
            updates.top = initialItemState.top + deltaY
          }
          if (resizeDirection.value === 'left') {
            updates.left = initialItemState.left + deltaX
          }

          itemStore.setItemState(itemId, updates)
          itemStore.saveItemState(itemId)
        }
      })
    }
  }
}

const loadPositionAndSize = () => {
  itemStore.loadItemState(props.id)
  if (itemStore.items[props.id]) {
    itemState.value = itemStore.items[props.id]
  }
}

const toggleMinimize = () => {
  if (!isMinimized.value && props.onMinimize) {
    props.onMinimize()
  }
  isMinimized.value = !isMinimized.value
}

const handleReziging = (e: MouseEvent) => {
  activeLine.value = e.target as HTMLElement
  activeLine.value.classList.add('resizing-highlight-line')
  isResizing.value = true
  itemStore.inResizing = true
}

// Re-anchor the "scale" baseline to the current container after a fullscreen
// round-trip changes the panel size out-of-band — otherwise the next resize
// applies a phantom delta to the restored size.
const syncScaleBaseline = () => {
  const c = getStickyContainer()
  prevContainerWidth.value = c.width
  prevContainerHeight.value = c.height
}

const toggleFullScreen = () => {
  itemStore.toggleFullScreen(props.id)
  loadPositionAndSize()
  syncScaleBaseline()
}

const handleResizeBarDblClick = (e: MouseEvent) => {
  // Silent no-op when fullscreen is disabled (e.g. the left palette).
  if (!props.allowFullScreen) return
  // Don't toggle while a resize gesture is mid-flight.
  if (isResizing.value) return
  e.preventDefault()
  toggleFullScreen()
}

const handleHeaderDblClick = (e: MouseEvent) => {
  // Same gesture as the resize bars, but mounted on the title bar — easier to
  // hit than the 5px-wide edge handles. Ignore dblclicks that land on the
  // header buttons so rapidly clicking minimize/move buttons doesn't also
  // toggle fullscreen.
  if (!props.allowFullScreen) return
  if (isDragging.value || isResizing.value) return
  const target = e.target as HTMLElement | null
  if (target?.closest('button')) return
  e.preventDefault()
  toggleFullScreen()
}

const captureGroupInitialStates = () => {
  if (itemState.value.group && itemState.value.syncDimensions) {
    initialGroupStates.value = {}
    const groupItems = itemStore.groups[itemState.value.group]
    if (groupItems) {
      groupItems.forEach((id) => {
        const item = itemStore.items[id]
        if (item) {
          initialGroupStates.value[id] = {
            top: item.top,
            left: item.left,
            width: item.width,
            height: item.height,
          }
        }
      })
    }
  }
}

const startResizeRight = (e: MouseEvent) => {
  registerClick()
  e.preventDefault()
  handleReziging(e)
  resizeDirection.value = 'right'
  captureGroupInitialStates()
  resizeBounds.value = getStickyContainer()
  startX.value = e.clientX
  startWidth.value = itemState.value.width
  document.addEventListener('mousemove', onResizeWidth)
  document.addEventListener('mouseup', stopResize)
}

const onResizeWidth = (e: MouseEvent) => {
  if (isResizing.value) {
    const deltaX = e.clientX - startX.value
    const newWidth = startWidth.value + deltaX
    if (newWidth > 100 && newWidth < resizeBounds.value.width) {
      itemState.value.width = newWidth
      savePositionAndSize()
    }
  }
}

const startResizeBottom = (e: MouseEvent) => {
  registerClick()
  e.preventDefault()
  handleReziging(e)
  resizeDirection.value = 'bottom'
  captureGroupInitialStates()
  resizeBounds.value = getStickyContainer()
  startY.value = e.clientY
  startHeight.value = itemState.value.height
  document.addEventListener('mousemove', onResizeHeight)
  document.addEventListener('mouseup', stopResize)
}

const onResizeHeight = (e: MouseEvent) => {
  if (isResizing.value) {
    const deltaY = e.clientY - startY.value
    const newHeight = startHeight.value + deltaY
    if (newHeight > 100 && newHeight < resizeBounds.value.height) {
      itemState.value.height = newHeight
      savePositionAndSize()
    }
  }
}

const startResizeTop = (e: MouseEvent) => {
  registerClick()
  e.preventDefault()
  handleReziging(e)
  resizeDirection.value = 'top'
  captureGroupInitialStates()
  resizeBounds.value = getStickyContainer()
  startY.value = e.clientY
  startTop.value = itemState.value.top
  startHeight.value = itemState.value.height
  document.addEventListener('mousemove', onResizeTop)
  document.addEventListener('mouseup', stopResize)
}

const onResizeTop = (e: MouseEvent) => {
  if (isResizing.value) {
    const deltaY = e.clientY - startY.value
    const newTop = startTop.value + deltaY
    const newHeight = startHeight.value - deltaY
    if (newHeight > 100 && newHeight < resizeBounds.value.height && newTop >= 0) {
      itemState.value.top = newTop
      itemState.value.height = newHeight
      savePositionAndSize()
    }
  }
}

const startResizeLeft = (e: MouseEvent) => {
  registerClick()
  e.preventDefault()
  handleReziging(e)
  resizeDirection.value = 'left'
  captureGroupInitialStates()
  resizeBounds.value = getStickyContainer()
  startX.value = e.clientX
  startLeft.value = itemState.value.left
  startWidth.value = itemState.value.width
  document.addEventListener('mousemove', onResizeLeft)
  document.addEventListener('mouseup', stopResize)
}

const onResizeLeft = (e: MouseEvent) => {
  if (isResizing.value) {
    const deltaX = e.clientX - startX.value
    const newLeft = startLeft.value + deltaX
    const newWidth = startWidth.value - deltaX
    if (newWidth > 100 && newWidth < resizeBounds.value.width) {
      itemState.value.left = newLeft
      itemState.value.width = newWidth
      savePositionAndSize()
    }
  }
}

const stopResize = () => {
  if (isResizing.value) {
    isResizing.value = false
    resizeDirection.value = null
    initialGroupStates.value = {}
    if (activeLine.value) {
      activeLine.value.classList.remove('resizing-highlight-line')
    }
    itemStore.inResizing = false
    document.removeEventListener('mousemove', onResizeWidth)
    document.removeEventListener('mousemove', onResizeHeight)
    document.removeEventListener('mousemove', onResizeTop)
    document.removeEventListener('mousemove', onResizeLeft)
    itemStore.flushItemState(props.id)
  }
}

// Pixel threshold a mousedown must travel before we treat the gesture as a
// drag. Without this, a header dblclick (two mousedowns + tiny cursor
// jitter) would fire onMove with deltas of 1–2 px, drift the panel, and
// then snapshot the drifted position into prev{Top,Left} on
// `setFullScreen(true)` — so dblclick → fullscreen → dblclick restored
// the panel slightly lower each cycle. Matches the standard OS drag
// threshold so deliberate drags still feel responsive.
const DRAG_THRESHOLD_PX = 4
let dragActivated = false

const startMove = (e: MouseEvent) => {
  registerClick()
  if (!props.allowFreeMove) return
  e.preventDefault()
  if (
    (e.target as HTMLElement).classList.contains('icon') ||
    (e.target as HTMLElement).classList.contains('minimal-button')
  )
    return

  isDragging.value = true
  dragActivated = false
  startX.value = e.clientX
  startY.value = e.clientY
  startLeft.value = itemState.value.left
  startTop.value = itemState.value.top
  document.addEventListener('mousemove', onMove)
  document.addEventListener('mouseup', stopMove)
  // stickynessPosition is flipped to "free" only once the threshold trips
  // (inside onMove) — a dblclick gesture must not silently un-stick a
  // sticky-positioned panel.
}

const onMove = (e: MouseEvent) => {
  if (!isDragging.value) return
  const deltaX = e.clientX - startX.value
  const deltaY = e.clientY - startY.value
  if (!dragActivated) {
    if (Math.abs(deltaX) < DRAG_THRESHOLD_PX && Math.abs(deltaY) < DRAG_THRESHOLD_PX) return
    dragActivated = true
    itemState.value.stickynessPosition = 'free'
  }
  itemState.value.left = startLeft.value + deltaX
  itemState.value.top = startTop.value + deltaY
}

const stopMove = () => {
  if (isDragging.value) {
    isDragging.value = false
    document.removeEventListener('mousemove', onMove)
    document.removeEventListener('mouseup', stopMove)

    // No-op if the gesture stayed under the drag threshold — nothing
    // changed, so there's nothing to persist (and persisting a drifted
    // mid-dblclick position is exactly the bug this guards against).
    if (dragActivated) {
      savePositionAndSize()
      // Make the final position durable immediately — debounced writes would
      // be lost if the user closes the tab within 250 ms of releasing the drag.
      itemStore.flushItemState(props.id)
    }
    dragActivated = false
  }
}

// Returns the element panels are actually positioned against (the overlay's
// offsetParent — the canvas container). Sticky/move math is relative to this
// element's INSIDE, so `(left=0, top=0)` is its top-left corner. Falls back
// to the Vue parent root only if the overlay isn't mounted yet.
const getStickyContainer = (): { width: number; height: number } => {
  const overlayEl = document.getElementById(props.id)
  const offsetParent = overlayEl?.offsetParent as HTMLElement | null
  const ref = offsetParent ?? (instance?.parent?.vnode.el as HTMLElement | null)
  if (ref) {
    return { width: ref.clientWidth, height: ref.clientHeight }
  }
  return { width: window.innerWidth, height: window.innerHeight }
}

const moveToRight = () => {
  const c = getStickyContainer()
  itemState.value.left = c.width - itemState.value.width
  itemState.value.top = 0
  itemState.value.stickynessPosition = 'right'
  if (resolvedHeightBehaviour.value === 'fill') {
    itemState.value.height = c.height
  }
  savePositionAndSize()
}

const moveToBottom = () => {
  const c = getStickyContainer()
  itemState.value.left = props.initialLeft || 0
  itemState.value.top = c.height - (itemState.value.height + (props.initialTop || 0))
  itemState.value.stickynessPosition = 'bottom'
  if (resolvedWidthBehaviour.value === 'fill') {
    itemState.value.width = c.width - (props.initialLeft || 0)
  }
  savePositionAndSize()
}

const moveToLeft = () => {
  const c = getStickyContainer()
  itemState.value.left = 0
  itemState.value.top = 0
  itemState.value.stickynessPosition = 'left'
  if (resolvedHeightBehaviour.value === 'fill') {
    itemState.value.height = c.height
  }
  savePositionAndSize()
}

const moveToTop = () => {
  const c = getStickyContainer()
  itemState.value.left = 0
  itemState.value.top = 0
  itemState.value.stickynessPosition = 'top'
  if (resolvedWidthBehaviour.value === 'fill') {
    itemState.value.width = c.width
  }
  savePositionAndSize()
}

const applyStickyPosition = () => {
  const c = getStickyContainer()
  // Bail before any mutation if the container isn't laid out yet — clamping
  // against a 0-sized box would collapse the panel.
  if (c.width <= 0 || c.height <= 0) return

  // Resizing while fullscreen should keep the panel filling the canvas, not run
  // the shrink/reposition math below.
  if (itemState.value.fullScreen) {
    itemState.value.width = c.width
    itemState.value.height = c.height
    itemState.value.left = 0
    itemState.value.top = 0
    prevContainerWidth.value = c.width
    prevContainerHeight.value = c.height
    savePositionAndSize()
    return
  }

  const wB = resolvedWidthBehaviour.value
  const hB = resolvedHeightBehaviour.value

  // "scale": absorb the container delta to keep a constant gap to the edge.
  // Runs before effHeight/shrink-clamp so the position math sees the new size.
  if (wB === 'scale' && prevContainerWidth.value > 0) {
    itemState.value.width = Math.max(
      100,
      itemState.value.width + (c.width - prevContainerWidth.value),
    )
  }
  if (hB === 'scale' && prevContainerHeight.value > 0) {
    itemState.value.height = Math.max(
      100,
      itemState.value.height + (c.height - prevContainerHeight.value),
    )
  }

  // A minimized panel renders as a ~35px header (CSS overrides its size), so its
  // stored height is the pre-collapse value — use the rendered height for the
  // vertical position math instead.
  const effHeight = isMinimized.value ? 35 : itemState.value.height

  // Shrink an expanded panel to fit a smaller canvas before positioning it.
  if (!isMinimized.value) {
    itemState.value.width = Math.min(itemState.value.width, c.width)
    itemState.value.height = Math.min(itemState.value.height, c.height)
  }

  switch (itemState.value.stickynessPosition) {
    case 'top':
      itemState.value.left = props.initialLeft || 0
      itemState.value.top = 0
      if (wB === 'fill') {
        itemState.value.width = c.width - (props.initialLeft || 0)
      }
      break

    case 'bottom':
      itemState.value.left = props.initialLeft || 0
      itemState.value.top = Math.max(0, c.height - effHeight - (props.initialTop || 0))
      if (wB === 'fill') {
        itemState.value.width = c.width - (props.initialLeft || 0)
      }
      break

    case 'left':
      itemState.value.left = 0
      if (hB === 'scale') {
        // Preserve the user's vertical offset — the scale step already kept the
        // height gap; only keep it on-screen (mirrors the "free" branch).
        itemState.value.top = Math.max(
          0,
          Math.min(itemState.value.top, Math.max(0, c.height - effHeight)),
        )
      } else {
        itemState.value.top = props.initialTop || 0
        if (hB === 'fill') {
          itemState.value.height = c.height - (props.initialTop || 0)
        }
      }
      break

    case 'right':
      itemState.value.left = Math.max(0, c.width - itemState.value.width)
      if (hB === 'scale') {
        itemState.value.top = Math.max(
          0,
          Math.min(itemState.value.top, Math.max(0, c.height - effHeight)),
        )
      } else {
        itemState.value.top = props.initialTop || 0
        if (hB === 'fill') {
          itemState.value.height = c.height - (props.initialTop || 0)
        }
      }
      break

    case 'free':
    default: {
      if (prevContainerWidth.value > 0 && prevContainerHeight.value > 0) {
        // Use the panel's CENTER, not its top-left corner, to decide alignment.
        // A wide panel docked right (left ≈ half the canvas) would otherwise fail
        // the `left > width/2` test and stop following the right edge as the
        // canvas grows — leaving it stranded "in the middle".
        const wasRightAligned =
          itemState.value.left + itemState.value.width / 2 > prevContainerWidth.value / 2
        const wasBottomAligned =
          itemState.value.top + effHeight / 2 > prevContainerHeight.value / 2

        // Don't re-anchor a "scale" axis — the scale step already tracked it.
        if (wasRightAligned && wB !== 'scale') {
          const distanceFromRight =
            prevContainerWidth.value - itemState.value.left - itemState.value.width
          itemState.value.left = Math.max(0, c.width - itemState.value.width - distanceFromRight)
        }

        if (wasBottomAligned && hB !== 'scale') {
          const distanceFromBottom = prevContainerHeight.value - itemState.value.top - effHeight
          itemState.value.top = Math.max(0, c.height - effHeight - distanceFromBottom)
        }
      }

      // Keep the panel inside the canvas: fully when expanded; header-visible
      // when minimized (its rendered width is content-driven, so fall back to a
      // minimum-visible margin rather than the stale stored width).
      const minVisible = 100
      const maxLeft = Math.max(0, c.width - (isMinimized.value ? minVisible : itemState.value.width))
      const maxTop = Math.max(0, c.height - effHeight)
      itemState.value.left = Math.max(0, Math.min(itemState.value.left, maxLeft))
      itemState.value.top = Math.max(0, Math.min(itemState.value.top, maxTop))
      break
    }
  }

  prevContainerWidth.value = c.width
  prevContainerHeight.value = c.height

  savePositionAndSize()
}

// Fill-axis default sizes read from the canvas container (the overlay's
// offsetParent), NOT instance.parent — the latter points at this overlay once
// the panel is nested in a wrapper, which self-references to a tiny size and
// breaks the reset-layout defaults.
const calculateWidth = () => {
  if (props.initialWidth) {
    return props.initialWidth
  } else if (props.initialPosition === 'top' || props.initialPosition === 'bottom') {
    return Math.max(300, getStickyContainer().width - (props.initialLeft || 0))
  } else return 300
}

const calculateHeight = () => {
  if (props.initialHeight) {
    return props.initialHeight
  } else if (props.initialPosition === 'left' || props.initialPosition === 'right') {
    return Math.max(300, getStickyContainer().height - (props.initialTop || 0))
  } else return 300
}

const handleResize = () => {
  clearTimeout(resizeTimeout)
  resizeTimeout = setTimeout(applyStickyPosition, 1)
}

const parentResizeObserver = new ResizeObserver(() => {
  handleResize()
})

const observeParentResize = () => {
  // Observe the exact element bounds are read from (the overlay's offsetParent,
  // i.e. the canvas container), falling back to the Vue parent root.
  const overlayEl = document.getElementById(props.id)
  const parentElement =
    (overlayEl?.offsetParent as HTMLElement | null) ??
    (instance?.parent?.vnode.el as HTMLElement | null)
  if (parentElement) {
    parentResizeObserver.observe(parentElement)
  }
}

const handleWindowResize = () => {
  handleResize()
}

const registerClick = () => {
  itemStore.clickOnItem(props.id)
}

const setFullScreen = (makeFull: boolean) => {
  itemStore.setFullScreen(props.id, makeFull)
  loadPositionAndSize()
  syncScaleBaseline()
}

watch(
  () => itemStore.items[props.id],
  (newState) => {
    if (newState) {
      if (isDragging.value || isResizing.value) {
        itemState.value.zIndex = newState.zIndex
      } else {
        itemState.value = { ...newState }
      }
    }
  },
  { deep: true },
)

watch(
  () => ({ group: props.group, syncDimensions: props.syncDimensions }),
  ({ group, syncDimensions }) => {
    itemStore.setItemState(props.id, {
      group,
      syncDimensions,
    })
    itemState.value.group = group
    itemState.value.syncDimensions = syncDimensions
  },
)

nextTick().then(() => {
  observeParentResize()
})

// Instance-scoped reset handler. Replaces a previous global registry on
// `(window as any)[resetHandler_${id}]` that leaked references when the
// component unmounted without running its cleanup.
let layoutResetHandler: (() => void) | null = null

const clampStateToViewport = (state: ItemLayout) => {
  const c = getStickyContainer()
  // Bail before first layout — clamping against a 0-sized box would collapse the
  // panel; the post-mount applyStickyPosition (nextTick) re-clamps once laid out.
  if (c.width <= 0 || c.height <= 0) return
  const minVisible = 100
  state.width = Math.max(150, Math.min(state.width, c.width))
  state.height = Math.max(100, Math.min(state.height, Math.max(150, c.height)))
  state.left = Math.max(0, Math.min(state.left, Math.max(0, c.width - minVisible)))
  state.top = Math.max(0, Math.min(state.top, Math.max(0, c.height - minVisible)))
}

onMounted(() => {
  // Capture the canvas container size now (not at module-eval time) so the
  // proportional repositioning math in applyStickyPosition stays accurate.
  const initialBounds = getStickyContainer()
  prevContainerWidth.value = initialBounds.width
  prevContainerHeight.value = initialBounds.height

  const initialWidth = calculateWidth()
  const initialHeight = calculateHeight()
  const initialLeft = props.initialLeft || 100
  const initialTop = props.initialTop || 100

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
  })

  const hasSavedState = itemStore.hasSavedState(props.id)

  if (!hasSavedState) {
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
    })
    itemState.value = itemStore.items[props.id]

    if (props.initialPosition !== 'free') {
      nextTick(() => {
        applyStickyPosition()
      })
    }
  } else {
    loadPositionAndSize()
    // Saved width/height/left/top from a different viewport size can place
    // the panel partially or fully off-screen. Clamp before first paint.
    clampStateToViewport(itemState.value)
    itemStore.setItemState(props.id, { ...itemState.value })

    if (itemState.value.stickynessPosition && itemState.value.stickynessPosition !== 'free') {
      nextTick(() => {
        applyStickyPosition()
      })
    }
  }

  layoutResetHandler = () => {
    itemState.value = { ...itemStore.items[props.id] }

    // Ensure stickynessPosition is restored from initial state (props.initialPosition)
    // so sticky items snap back to their original position.
    const initialStickyPosition =
      itemStore.initialItemStates[props.id]?.stickynessPosition || props.initialPosition
    if (initialStickyPosition && initialStickyPosition !== 'free') {
      itemState.value.stickynessPosition = initialStickyPosition
      nextTick(() => {
        applyStickyPosition()
      })
    }
  }

  window.addEventListener('layout-reset', layoutResetHandler)
  window.addEventListener('resize', handleWindowResize)
  document.addEventListener('mouseup', stopResize)
})

defineExpose({
  setFullScreen,
})

onBeforeUnmount(() => {
  if (layoutResetHandler) {
    window.removeEventListener('layout-reset', layoutResetHandler)
    layoutResetHandler = null
  }
  window.removeEventListener('resize', handleWindowResize)
  parentResizeObserver.disconnect()
  document.removeEventListener('mouseup', stopResize)
  document.removeEventListener('mousemove', onMove)
  document.removeEventListener('mouseup', stopMove)
  document.removeEventListener('mousemove', onResizeWidth)
  document.removeEventListener('mousemove', onResizeHeight)
  document.removeEventListener('mousemove', onResizeTop)
  document.removeEventListener('mousemove', onResizeLeft)
})
</script>

<style scoped>
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

/* VS Code-style tab strip in the header. `align-self: stretch` + negative
   vertical margin makes the tabs fill the 35px header so the active underline
   meets the header's bottom border. Used for both multi-tab and single-title. */
.dragitem-tabs {
  display: flex;
  align-self: stretch;
  margin: -4px 0 -4px 4px;
}
.dragitem-tab {
  display: flex;
  align-items: center;
  height: 100%;
  padding: 0 12px;
  border: none;
  border-bottom: 2px solid transparent;
  background: transparent;
  color: var(--color-text-secondary);
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  white-space: nowrap;
  cursor: pointer;
  transition:
    color 0.15s ease,
    background 0.15s ease;
}
button.dragitem-tab:hover {
  color: var(--color-text-primary);
  background: var(--color-background-hover);
}
.dragitem-tab.active {
  color: var(--color-text-primary);
  border-bottom-color: var(--color-accent);
}
.dragitem-tab--static {
  cursor: move;
  user-select: none;
  font-size: 10px;
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
  flex: 1;
  min-height: 0;
  overflow: auto;
  padding: 10px;
  box-sizing: border-box;
}
/* Sticky footer below the scrolling content (opt-in via the `footer` slot). */
.footer {
  flex-shrink: 0;
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  padding: 6px 12px;
  border-top: 1px solid var(--color-border-primary);
  background: var(--color-background-secondary);
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
