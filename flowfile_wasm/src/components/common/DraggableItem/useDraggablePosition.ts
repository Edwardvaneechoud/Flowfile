import { ref, type Ref } from 'vue'

import { POP_OUT_THRESHOLD_PX, type PanelDock, type RenderRect } from './layoutGeometry'

// Free panels keep the small jitter guard: a header dblclick (two mousedowns
// + 1-2px cursor drift) must not register as a move.
const FREE_DRAG_THRESHOLD_PX = 4

interface PositionOptions {
  allowFreeMove: () => boolean
  gestureRect: Ref<RenderRect | null>
  getRect: () => RenderRect
  getDock: () => PanelDock
  onStart: () => void
  onCommit: (rect: RenderRect) => void
}

// Header-drag gesture. Docked panels pop out only after a deliberate pull
// (POP_OUT_THRESHOLD_PX); the commit handler decides whether the released
// rect snaps back into a dock or becomes a free intent.
export function useDraggablePosition(opts: PositionOptions) {
  const isDragging = ref(false)
  let dragActivated = false
  let startX = 0
  let startY = 0
  let anchorRect: RenderRect | null = null
  let anchorX = 0
  let anchorY = 0

  const startMove = (e: MouseEvent) => {
    opts.onStart()
    if (!opts.allowFreeMove()) return
    e.preventDefault()
    const target = e.target as HTMLElement
    if (target.classList.contains('icon') || target.classList.contains('minimal-button')) {
      return
    }
    isDragging.value = true
    dragActivated = false
    startX = e.clientX
    startY = e.clientY
    document.addEventListener('mousemove', onMove)
    document.addEventListener('mouseup', stopMove)
  }

  const onMove = (e: MouseEvent) => {
    if (!isDragging.value) return
    if (!dragActivated) {
      const dx = Math.abs(e.clientX - startX)
      const dy = Math.abs(e.clientY - startY)
      const threshold = opts.getDock() !== 'free' ? POP_OUT_THRESHOLD_PX : FREE_DRAG_THRESHOLD_PX
      if (dx < threshold && dy < threshold) return
      dragActivated = true
      // Anchor at activation so the panel doesn't jump by the threshold.
      anchorRect = { ...opts.getRect() }
      anchorX = e.clientX
      anchorY = e.clientY
      opts.gestureRect.value = { ...anchorRect }
      return
    }
    if (!anchorRect) return
    opts.gestureRect.value = {
      ...anchorRect,
      left: anchorRect.left + (e.clientX - anchorX),
      top: anchorRect.top + (e.clientY - anchorY),
    }
  }

  const stopMove = () => {
    if (!isDragging.value) return
    isDragging.value = false
    document.removeEventListener('mousemove', onMove)
    document.removeEventListener('mouseup', stopMove)
    if (!dragActivated) return // under-threshold: nothing changed
    dragActivated = false
    anchorRect = null
    const rect = opts.gestureRect.value
    if (rect) opts.onCommit(rect)
    opts.gestureRect.value = null
  }

  const teardown = () => {
    document.removeEventListener('mousemove', onMove)
    document.removeEventListener('mouseup', stopMove)
  }

  return { isDragging, startMove, teardown }
}
