import { ref, type Ref } from 'vue'

import { MIN_PANEL_H, MIN_PANEL_W, type Bounds, type RenderRect } from './layoutGeometry'

export type ResizeEdge = 'top' | 'bottom' | 'left' | 'right'

interface ResizeOptions {
  gestureRect: Ref<RenderRect | null>
  getRect: () => RenderRect
  getBounds: () => Bounds
  // Gate: e.g. a fullscreen panel must not be resizable — its derived rect is
  // view state, and committing it would overwrite the remembered layout.
  isEnabled: () => boolean
  // Shared cross-panel flag: entering another panel's resize bar while a
  // resize is in flight hands the gesture over (resizeOnEnter).
  setSharedResizing: (value: boolean) => void
  isSharedResizing: () => boolean
  onStart: () => void
  onCommit: (rect: RenderRect) => void
}

const clamp = (value: number, min: number, max: number): number =>
  Math.max(min, Math.min(value, max))

// Per-edge resize gestures. The gesture mutates only the shared gestureRect
// (render-side override); intent is committed once, at mouseup.
export function useDraggableResize(opts: ResizeOptions) {
  const isResizing = ref(false)
  const edge = ref<ResizeEdge | null>(null)
  const activeLine = ref<HTMLElement | null>(null)
  let start = { x: 0, y: 0, rect: { left: 0, top: 0, width: 0, height: 0 } as RenderRect }
  let bounds: Bounds = { width: 0, height: 0 }
  let hoverDelay: ReturnType<typeof setTimeout> | null = null
  let moved = false

  const beginResize = (e: MouseEvent, which: ResizeEdge) => {
    opts.onStart()
    if (!opts.isEnabled()) return
    e.preventDefault()
    moved = false
    activeLine.value = e.target as HTMLElement
    activeLine.value.classList.add('resizing-highlight-line')
    isResizing.value = true
    opts.setSharedResizing(true)
    edge.value = which
    // Container can't change mid-drag; reading it per-mousemove would reflow.
    bounds = opts.getBounds()
    start = { x: e.clientX, y: e.clientY, rect: { ...opts.getRect() } }
    opts.gestureRect.value = { ...start.rect }
    document.addEventListener('mousemove', onResizeMove)
    document.addEventListener('mouseup', stopResize)
  }

  const onResizeMove = (e: MouseEvent) => {
    if (!isResizing.value) return
    const dx = e.clientX - start.x
    const dy = e.clientY - start.y
    const rect = { ...start.rect }
    switch (edge.value) {
      case 'right':
        rect.width = clamp(
          start.rect.width + dx,
          MIN_PANEL_W,
          Math.max(MIN_PANEL_W, bounds.width - start.rect.left),
        )
        break
      case 'bottom':
        rect.height = clamp(
          start.rect.height + dy,
          MIN_PANEL_H,
          Math.max(MIN_PANEL_H, bounds.height - start.rect.top),
        )
        break
      case 'left': {
        // Keep the right edge fixed.
        const rightEdge = start.rect.left + start.rect.width
        rect.width = clamp(start.rect.width - dx, MIN_PANEL_W, Math.max(MIN_PANEL_W, rightEdge))
        rect.left = rightEdge - rect.width
        break
      }
      case 'top': {
        // Keep the bottom edge fixed.
        const bottomEdge = start.rect.top + start.rect.height
        rect.height = clamp(start.rect.height - dy, MIN_PANEL_H, Math.max(MIN_PANEL_H, bottomEdge))
        rect.top = bottomEdge - rect.height
        break
      }
    }
    if (
      rect.left !== start.rect.left ||
      rect.top !== start.rect.top ||
      rect.width !== start.rect.width ||
      rect.height !== start.rect.height
    ) {
      moved = true
    }
    opts.gestureRect.value = rect
  }

  const stopResize = () => {
    if (!isResizing.value) return
    isResizing.value = false
    edge.value = null
    activeLine.value?.classList.remove('resizing-highlight-line')
    activeLine.value = null
    opts.setSharedResizing(false)
    document.removeEventListener('mousemove', onResizeMove)
    document.removeEventListener('mouseup', stopResize)
    const rect = opts.gestureRect.value
    opts.gestureRect.value = null
    // A zero-move click is not a gesture: committing it would freeze derived
    // defaults into explicit intent (and break dblclick-fullscreen, whose two
    // clicks would each commit the fullscreen rect as layout).
    if (rect && moved) opts.onCommit(rect)
    moved = false
  }

  const resizeOnEnter = (e: MouseEvent, which: ResizeEdge) => {
    if (hoverDelay) clearTimeout(hoverDelay)
    hoverDelay = setTimeout(() => {
      if (opts.isSharedResizing() && !isResizing.value) {
        beginResize(e, which)
      }
    }, 200)
  }

  const teardown = () => {
    if (hoverDelay) clearTimeout(hoverDelay)
    // Unmount mid-gesture: the store-wide flag outlives this component and
    // would otherwise arm phantom hover-started resizes on other panels.
    if (isResizing.value) {
      isResizing.value = false
      activeLine.value?.classList.remove('resizing-highlight-line')
      activeLine.value = null
      opts.setSharedResizing(false)
    }
    document.removeEventListener('mousemove', onResizeMove)
    document.removeEventListener('mouseup', stopResize)
  }

  return { isResizing, beginResize, resizeOnEnter, stopResize, teardown }
}
