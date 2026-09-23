// Edge auto-scroll for HTML5 drag-and-drop. The browser fires `dragover` while
// a drag is in flight but never scrolls a container for you, so a list taller
// than its box is unreachable past the fold without this.
import { getCurrentInstance, onBeforeUnmount } from "vue";

const EDGE_PX = 48;
const MAX_STEP_PX = 14;

export interface ScrollBand {
  top: number;
  bottom: number;
}

/** Pixels to scroll this frame: negative near the top edge, positive near the bottom, 0 elsewhere. */
export function edgeScrollDelta(
  pointerY: number,
  band: ScrollBand,
  edge = EDGE_PX,
  maxStep = MAX_STEP_PX,
): number {
  if (pointerY < band.top || pointerY > band.bottom) return 0;
  // A box shorter than two edge zones would otherwise let the top zone win everywhere.
  const zone = Math.min(edge, (band.bottom - band.top) / 2);
  if (zone <= 0) return 0;
  const fromTop = pointerY - band.top;
  const fromBottom = band.bottom - pointerY;
  if (fromTop < zone && fromTop <= fromBottom) {
    return -Math.ceil(((zone - fromTop) / zone) * maxStep);
  }
  if (fromBottom < zone) return Math.ceil(((zone - fromBottom) / zone) * maxStep);
  return 0;
}

export function canScrollBy(el: HTMLElement, delta: number): boolean {
  if (delta < 0) return el.scrollTop > 0;
  return el.scrollTop + el.clientHeight < el.scrollHeight - 1;
}

export function findScrollParent(el: HTMLElement | null): HTMLElement | null {
  let node = el;
  while (node) {
    const overflowY = getComputedStyle(node).overflowY;
    if (overflowY === "auto" || overflowY === "scroll") return node;
    node = node.parentElement;
  }
  return null;
}

export interface DragAutoScrollOptions {
  /** Candidate scrollers, innermost first. The first one under the pointer that can still scroll wins. */
  containers: () => (HTMLElement | null | undefined)[];
}

export interface DragAutoScrollController {
  /** Call from `dragstart`. */
  start(): void;
  /** Call from `dragend`/`drop`; idempotent. */
  stop(): void;
}

export function useDragAutoScroll(options: DragAutoScrollOptions): DragAutoScrollController {
  let rafId: number | null = null;
  let pointer: { x: number; y: number } | null = null;

  const onDragOver = (event: DragEvent) => {
    pointer = { x: event.clientX, y: event.clientY };
  };

  const step = () => {
    rafId = null;
    if (pointer) {
      for (const el of options.containers()) {
        if (!el) continue;
        const rect = el.getBoundingClientRect();
        if (pointer.x < rect.left || pointer.x > rect.right) continue;
        const delta = edgeScrollDelta(pointer.y, rect);
        if (delta !== 0 && canScrollBy(el, delta)) {
          el.scrollTop += delta;
          break;
        }
      }
    }
    rafId = requestAnimationFrame(step);
  };

  const start = () => {
    if (rafId !== null) return;
    pointer = null;
    // Capture phase: a target that stops propagation must not blind the scroller.
    document.addEventListener("dragover", onDragOver, true);
    rafId = requestAnimationFrame(step);
  };

  const stop = () => {
    document.removeEventListener("dragover", onDragOver, true);
    if (rafId !== null) cancelAnimationFrame(rafId);
    rafId = null;
    pointer = null;
  };

  if (getCurrentInstance()) onBeforeUnmount(stop);

  return { start, stop };
}
