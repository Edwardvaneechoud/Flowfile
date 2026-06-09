import { ref } from 'vue'

/**
 * Shared drag state for dashboard tiles (drag a saved viz / a text block from
 * the sidebar onto the canvas). Ported from flowfile_frontend, with viz ids as
 * strings (WASM uses UUIDs, not DB row numbers).
 */

export const VIZ_MIME = 'application/flowfile-viz'
export const TEXT_MIME = 'application/flowfile-text'

const draggedVizId = ref<string | null>(null)
const isDraggingViz = ref(false)
const isDraggingText = ref(false)

export function useDashboardDragAndDrop() {
  const onDragEnd = () => {
    isDraggingViz.value = false
    isDraggingText.value = false
    draggedVizId.value = null
  }

  // `drop` doesn't fire when the gesture is cancelled (released off-canvas),
  // but `dragend` always does — use it to reset the shared flags.
  const armDragEnd = (event: DragEvent) => {
    if (event.dataTransfer) event.dataTransfer.effectAllowed = 'copy'
    document.addEventListener('dragend', onDragEnd, { once: true })
  }

  const onVizDragStart = (event: DragEvent, vizId: string) => {
    event.dataTransfer?.setData(VIZ_MIME, vizId)
    draggedVizId.value = vizId
    isDraggingViz.value = true
    armDragEnd(event)
  }

  const onTextDragStart = (event: DragEvent) => {
    event.dataTransfer?.setData(TEXT_MIME, '1')
    isDraggingText.value = true
    armDragEnd(event)
  }

  return {
    draggedVizId,
    isDraggingViz,
    isDraggingText,
    onVizDragStart,
    onTextDragStart,
    onDragEnd,
  }
}
