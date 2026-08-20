<template>
  <div
    :id="props.id"
    class="overlay"
    :class="{
      'no-transition': isResizing,
      minimized: isMinimized,
    }"
    :style="{
      width: isMinimized ? 'auto' : rect.width + 'px',
      height: isMinimized ? 'auto' : rect.height + 'px',
      top: rect.top + 'px',
      left: rect.left + 'px',
      zIndex: itemStore.zIndexFor(props.id),
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
        <span class="icon">{{ isMinimized ? '+' : '−' }}</span>
      </button>

      <button
        v-if="showRight && intent.dock !== 'right'"
        class="minimal-button"
        data-tooltip="true"
        title="Move to Right"
        @click="dockTo('right')"
      >
        <span class="icon">→</span>
      </button>
      <button
        v-if="showBottom && intent.dock !== 'bottom'"
        class="minimal-button"
        data-tooltip="true"
        title="Move to Bottom"
        @click="dockTo('bottom')"
      >
        <span class="icon">↓</span>
      </button>
      <button
        v-if="showLeft && intent.dock !== 'left'"
        class="minimal-button"
        data-tooltip="true"
        title="Move to Left"
        @click="dockTo('left')"
      >
        <span class="icon">←</span>
      </button>
      <button
        v-if="showTop && intent.dock !== 'top'"
        class="minimal-button"
        data-tooltip="true"
        title="Move to Top"
        @click="dockTo('top')"
      >
        <span class="icon">↑</span>
      </button>
      <button
        v-if="allowFullScreen && !intent.fullScreen"
        class="minimal-button"
        data-tooltip="true"
        data-tooltip-text="Toggle Full Screen"
        @click="toggleFullScreen"
      >
        <span class="icon">⬜</span>
      </button>
      <button
        v-if="allowFullScreen && intent.fullScreen"
        class="minimal-button"
        data-tooltip="true"
        data-tooltip-text="Exit Full Screen"
        @click="toggleFullScreen"
      >
        <span class="icon">❐</span>
      </button>
      <div v-if="tabs.length" class="dragitem-tabs" role="tablist" @mousedown.stop>
        <button
          v-for="t in tabs"
          :key="t.id"
          role="tab"
          class="dragitem-tab"
          :class="{ active: t.id === activeTab }"
          :aria-selected="t.id === activeTab"
          :title="t.title"
          @click="emit('update:activeTab', t.id)"
        >
          {{ t.label }}
        </button>
      </div>
      <div v-else-if="title" class="dragitem-tabs" @mousedown="startMove">
        <span class="dragitem-tab dragitem-tab--static active">{{ title }}</span>
      </div>
      <!-- Host-supplied header buttons (e.g. the Code panel's run/export).
           mousedown.stop so pressing one never starts a panel drag. -->
      <div v-if="$slots.actions && !isMinimized" class="dragitem-actions" @mousedown.stop>
        <slot name="actions"></slot>
      </div>
      <button
        v-if="onClose"
        class="minimal-button close-button"
        data-tooltip="true"
        title="Close"
        @click="handleClose"
      >
        <span class="icon">×</span>
      </button>
    </div>

    <div class="content" :class="{ flush: flushContent }" @click="registerClick">
      <slot v-if="!isMinimized"></slot>
    </div>

    <!-- Optional sticky footer (e.g. node-settings Apply bar), pinned below the
         scrolling content. Only rendered when a `footer` slot is provided. -->
    <div v-if="!isMinimized && $slots.footer" class="footer">
      <slot name="footer"></slot>
    </div>

    <div
      class="draggable-line right-vertical"
      @mousedown.stop="beginResize($event, 'right')"
      @mouseenter="resizeOnEnter($event, 'right')"
      @dblclick.stop="handleResizeBarDblClick"
    ></div>
    <div
      class="draggable-line bottom-horizontal"
      @mousedown.stop="beginResize($event, 'bottom')"
      @mouseenter="resizeOnEnter($event, 'bottom')"
      @dblclick.stop="handleResizeBarDblClick"
    ></div>
    <div
      class="draggable-line top-horizontal"
      @mousedown.stop="beginResize($event, 'top')"
      @mouseenter="resizeOnEnter($event, 'top')"
      @dblclick.stop="handleResizeBarDblClick"
    ></div>
    <div
      class="draggable-line left-vertical"
      @mousedown.stop="beginResize($event, 'left')"
      @mouseenter="resizeOnEnter($event, 'left')"
      @dblclick.stop="handleResizeBarDblClick"
    ></div>
  </div>
</template>

<script setup lang="ts">
// Renders one overlay panel from persisted USER INTENT (store) derived
// against the shared canvas container — see layoutGeometry.ts for the
// intent-vs-derived contract. During a gesture the local gestureRect
// overrides the derived rect at 60Hz; intent is committed once, at mouseup.
import { computed, onBeforeUnmount, ref, watch, watchEffect } from 'vue'

import {
  clampRectToBounds,
  computeLayout,
  defaultIntent,
  intentFromRect,
  isContainerUsable,
  snapSideForRect,
  type AxisBehaviour,
  type DockSide,
  type PanelConfig,
  type PanelDock,
  type PanelIntent,
  type RenderRect,
} from './layoutGeometry'
import { useItemStore } from './stateStore'
import { useDraggablePosition } from './useDraggablePosition'
import { useDraggableResize } from './useDraggableResize'

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
  // When provided, a "×" close button appears in the header (distinct from the
  // "−" minimize/collapse button).
  onClose: {
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
  // Opt-in: hand the whole content box to the slot (no padding, no scroller)
  // for panels that own their own chrome and scrollers.
  flushContent: {
    type: Boolean,
    default: false,
  },
  // Tab strip rendered in the header. Empty ⇒ the `title` shows as a single
  // static tab (so every panel header looks the same).
  tabs: {
    type: Array as () => { id: string; label: string; title?: string }[],
    default: () => [],
  },
  activeTab: {
    type: String,
    default: '',
  },
})

const emit = defineEmits(['update:activeTab'])

const itemStore = useItemStore()

// Unset ⇒ legacy rule: "fill" when no initial size was given, else "fixed".
const resolvedWidthBehaviour = computed<AxisBehaviour>(
  () => props.widthBehaviour ?? (props.initialWidth ? 'fixed' : 'fill'),
)
const resolvedHeightBehaviour = computed<AxisBehaviour>(
  () => props.heightBehaviour ?? (props.initialHeight ? 'fixed' : 'fill'),
)

const panelCfg = computed<PanelConfig>(() => ({
  defaultDock: props.initialPosition as PanelDock,
  h: {
    behaviour: resolvedWidthBehaviour.value,
    defaultSize: props.initialWidth ?? null,
    defaultOffset: props.initialLeft ?? (props.initialPosition === 'free' ? 100 : 0),
  },
  v: {
    behaviour: resolvedHeightBehaviour.value,
    defaultSize: props.initialHeight ?? null,
    defaultOffset: props.initialTop ?? (props.initialPosition === 'free' ? 100 : 0),
  },
}))

// Registration needs no DOM; doing it in setup means the first render already
// has the loaded (or migrated) intent. The watch keeps the store's config
// fresh for resetLayout when reactive defaults (e.g. Canvas height override)
// change — re-registering is idempotent for the intent itself.
itemStore.registerPanel(props.id, panelCfg.value)
watch(panelCfg, (cfg) => itemStore.registerPanel(props.id, cfg))

const intent = computed<PanelIntent>(
  () => itemStore.items[props.id] ?? defaultIntent(panelCfg.value),
)

// Persisted (wasm keeps the collapse across reloads, unlike the desktop app).
const isMinimized = computed(() => itemStore.isMinimized(props.id))
const gestureRect = ref<RenderRect | null>(null)
const lastGoodRect = ref<RenderRect | null>(null)

const derivedRect = computed<RenderRect | null>(() =>
  computeLayout(intent.value, panelCfg.value, itemStore.containerBounds, isMinimized.value),
)

watchEffect(() => {
  if (derivedRect.value && !gestureRect.value) {
    lastGoodRect.value = derivedRect.value
  }
})

const rect = computed<RenderRect>(
  () =>
    gestureRect.value ??
    derivedRect.value ??
    lastGoodRect.value ?? {
      // Pre-container fallback (first frame only): defaults, unclamped — the
      // .overlay max-width/height CSS caps any overflow.
      left: panelCfg.value.h.defaultOffset,
      top: panelCfg.value.v.defaultOffset,
      width: panelCfg.value.h.defaultSize ?? 300,
      height: panelCfg.value.v.defaultSize ?? 300,
    },
)

const allowedDockSides = computed<DockSide[]>(() => {
  const sides = new Set<DockSide>()
  if (props.showRight) sides.add('right')
  if (props.showBottom) sides.add('bottom')
  if (props.showLeft) sides.add('left')
  if (props.showTop) sides.add('top')
  if (props.initialPosition !== 'free') sides.add(props.initialPosition as DockSide)
  return [...sides]
})

const registerClick = () => itemStore.clickOnItem(props.id)

const commitRect = (finalRect: RenderRect, dock: PanelDock) => {
  const bounds = { ...itemStore.containerBounds }
  // A broken container measurement would turn the rect into garbage intent —
  // drop the gesture instead (the derived rect simply resumes).
  if (!isContainerUsable(bounds)) return
  const settled = clampRectToBounds(finalRect, bounds, isMinimized.value)
  itemStore.commitIntent(
    props.id,
    intentFromRect(settled, dock, panelCfg.value, bounds, isMinimized.value),
  )
}

const dockTo = (side: DockSide) => {
  // While fullscreen the derived rect is the container — view state, not a
  // layout to commit. Exit fullscreen instead; the panel then docks normally.
  if (intent.value.fullScreen) {
    itemStore.setFullScreen(props.id, false)
    return
  }
  commitRect(rect.value, side)
}

const {
  isResizing,
  beginResize,
  resizeOnEnter,
  teardown: teardownResize,
} = useDraggableResize({
  gestureRect,
  getRect: () => ({ ...rect.value }),
  getBounds: () => ({ ...itemStore.containerBounds }),
  isEnabled: () => !intent.value.fullScreen,
  setSharedResizing: (value) => {
    itemStore.inResizing = value
  },
  isSharedResizing: () => itemStore.inResizing,
  onStart: registerClick,
  onCommit: (finalRect) => commitRect(finalRect, intent.value.dock),
})

const {
  isDragging,
  startMove,
  teardown: teardownMove,
} = useDraggablePosition({
  // A fullscreen panel is not draggable — its rect is view state and a drag
  // commit would persist container-sized geometry as layout.
  allowFreeMove: () => props.allowFreeMove && !intent.value.fullScreen,
  gestureRect,
  getRect: () => ({ ...rect.value }),
  getDock: () => intent.value.dock,
  onStart: registerClick,
  onCommit: (finalRect) => {
    const bounds = { ...itemStore.containerBounds }
    if (!isContainerUsable(bounds)) return
    // Magnetic snap-back: releasing near an allowed edge re-docks (VSCode-
    // style); otherwise the panel becomes a free intent.
    const snapSide = snapSideForRect(finalRect, bounds, allowedDockSides.value)
    commitRect(finalRect, snapSide ?? 'free')
  },
})

const toggleMinimize = () => {
  if (!isMinimized.value && props.onMinimize) {
    props.onMinimize()
  }
  itemStore.setMinimized(props.id, !isMinimized.value)
}

const handleClose = () => {
  props.onClose?.()
}

const setFullScreen = (makeFull: boolean) => {
  itemStore.setFullScreen(props.id, makeFull)
}

const toggleFullScreen = () => {
  itemStore.toggleFullScreen(props.id)
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

onBeforeUnmount(() => {
  teardownResize()
  teardownMove()
})

defineExpose({
  setFullScreen,
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
/* Close button is pushed to the far right of the header, past the tab strip.
   flex-shrink: 0 keeps it reachable at MIN_PANEL_W; the tab strip shrinks
   and ellipsizes instead. */
.close-button {
  margin-left: auto;
  flex-shrink: 0;
}
/* With header actions present they take the free space instead, so the close
   button stays pinned next to them rather than splitting the gap. */
.dragitem-actions {
  display: flex;
  align-items: center;
  gap: 4px;
  margin-left: auto;
  padding-left: 8px;
  flex-shrink: 0;
}
.dragitem-actions ~ .close-button {
  margin-left: 4px;
}
.close-button:hover {
  color: var(--color-text-inverse);
  background-color: var(--color-danger);
}

/* VS Code-style tab strip in the header. `align-self: stretch` + negative
   vertical margin makes the tabs fill the 35px header so the active underline
   meets the header's bottom border. Used for both multi-tab and single-title. */
.dragitem-tabs {
  display: flex;
  align-self: stretch;
  margin: -4px 0 -4px 4px;
  min-width: 0;
  overflow: hidden;
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
.content.flush {
  display: flex;
  flex-direction: column;
  overflow: hidden;
  padding: 0;
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
