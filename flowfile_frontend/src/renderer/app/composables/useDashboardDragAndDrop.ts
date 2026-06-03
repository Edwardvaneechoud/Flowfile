import { ref } from "vue";

export const VIZ_MIME = "application/flowfile-viz";
export const TEXT_MIME = "application/flowfile-text";

const draggedVizId = ref<number | null>(null);
const isDraggingViz = ref(false);
const isDraggingText = ref(false);

export function useDashboardDragAndDrop() {
  const onDragEnd = () => {
    isDraggingViz.value = false;
    isDraggingText.value = false;
    draggedVizId.value = null;
  };

  // `drop` doesn't fire when the gesture is cancelled (released off-canvas),
  // but `dragend` always does — use it to reset the shared flags.
  const armDragEnd = (event: DragEvent) => {
    if (event.dataTransfer) event.dataTransfer.effectAllowed = "copy";
    document.addEventListener("dragend", onDragEnd, { once: true });
  };

  const onVizDragStart = (event: DragEvent, vizId: number) => {
    event.dataTransfer?.setData(VIZ_MIME, String(vizId));
    draggedVizId.value = vizId;
    isDraggingViz.value = true;
    armDragEnd(event);
  };

  const onTextDragStart = (event: DragEvent) => {
    event.dataTransfer?.setData(TEXT_MIME, "1");
    isDraggingText.value = true;
    armDragEnd(event);
  };

  return {
    draggedVizId,
    isDraggingViz,
    isDraggingText,
    onVizDragStart,
    onTextDragStart,
    onDragEnd,
  };
}
