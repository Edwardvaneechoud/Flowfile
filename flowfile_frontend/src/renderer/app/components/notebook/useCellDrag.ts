// Pointer-driven cell reordering: the handle owns the gesture and the host only
// renders a drop indicator — nothing in the cell list moves until the commit.
import { getCurrentInstance, onBeforeUnmount, ref, type Ref } from "vue";

const ACTIVATE_THRESHOLD_PX = 5;
const AUTOSCROLL_EDGE_PX = 40;
const AUTOSCROLL_MAX_STEP_PX = 12;

export interface CellBound {
  top: number;
  bottom: number;
}

export interface CellDragOptions {
  getHost: () => HTMLElement | null;
  getScrollContainer?: () => HTMLElement | null;
  onCommit: (cellId: string, targetIndex: number) => void;
  isDisabled?: () => boolean;
}

export interface CellDragController {
  onHandlePointerDown(cellId: string, ev: PointerEvent): void;
  draggingId: Ref<string | null>;
  dropSlot: Ref<number | null>;
  indicatorTop: Ref<number | null>;
  cancel(): void;
}

export function computeDropSlot(bounds: CellBound[], y: number): number {
  let slot = 0;
  for (const bound of bounds) {
    if ((bound.top + bound.bottom) / 2 < y) slot += 1;
  }
  return slot;
}

export function finalIndexFromSlot(slot: number, draggedIndex: number): number {
  return slot > draggedIndex ? slot - 1 : slot;
}

// Starts at `el` itself: a host that is its own scroller (the catalog notebook's
// `.nb-cells`) must resolve to itself, not to a non-scrolling ancestor.
export function findScrollParent(el: HTMLElement | null): HTMLElement | null {
  let node = el ?? null;
  while (node) {
    const overflowY = getComputedStyle(node).overflowY;
    if (overflowY === "auto" || overflowY === "scroll") return node;
    node = node.parentElement;
  }
  return null;
}

export function useCellDrag(opts: CellDragOptions): CellDragController {
  const draggingId = ref<string | null>(null);
  const dropSlot = ref<number | null>(null);
  const indicatorTop = ref<number | null>(null);

  let pointerId: number | null = null;
  let cellId: string | null = null;
  let captureEl: Element | null = null;
  let active = false;
  let startX = 0;
  let startY = 0;
  let lastClientY = 0;
  let bounds: CellBound[] = [];
  let draggedIndex = -1;
  let rafId: number | null = null;
  let previousUserSelect = "";

  function scrollContainer(): HTMLElement | null {
    const host = opts.getHost();
    return opts.getScrollContainer?.() ?? findScrollParent(host) ?? host;
  }

  function updateSlot() {
    const host = opts.getHost();
    if (!host || bounds.length === 0) return;
    const hostRect = host.getBoundingClientRect();
    const y = lastClientY - hostRect.top + host.scrollTop;
    const slot = computeDropSlot(bounds, y);
    dropSlot.value = slot;
    indicatorTop.value = slot < bounds.length ? bounds[slot].top : bounds[bounds.length - 1].bottom;
  }

  function autoscrollStep() {
    rafId = null;
    if (!active) return;
    const container = scrollContainer();
    if (container) {
      const rect = container.getBoundingClientRect();
      const overTop = AUTOSCROLL_EDGE_PX - Math.max(0, lastClientY - rect.top);
      const overBottom = AUTOSCROLL_EDGE_PX - Math.max(0, rect.bottom - lastClientY);
      let delta = 0;
      if (overTop > 0) delta = -Math.ceil((overTop / AUTOSCROLL_EDGE_PX) * AUTOSCROLL_MAX_STEP_PX);
      else if (overBottom > 0)
        delta = Math.ceil((overBottom / AUTOSCROLL_EDGE_PX) * AUTOSCROLL_MAX_STEP_PX);
      if (delta !== 0) {
        container.scrollTop += delta;
        updateSlot();
      }
    }
    rafId = requestAnimationFrame(autoscrollStep);
  }

  function activate(): boolean {
    const host = opts.getHost();
    if (!host || cellId === null) return false;
    const hostRect = host.getBoundingClientRect();
    const cells = Array.from(host.querySelectorAll<HTMLElement>(":scope > [data-cell-id]"));
    // Host-content coordinates, measured once, so autoscrolling can't invalidate them.
    bounds = cells.map((el) => {
      const rect = el.getBoundingClientRect();
      return {
        top: rect.top - hostRect.top + host.scrollTop,
        bottom: rect.bottom - hostRect.top + host.scrollTop,
      };
    });
    draggedIndex = cells.findIndex((el) => el.getAttribute("data-cell-id") === cellId);
    if (draggedIndex < 0) return false;
    active = true;
    draggingId.value = cellId;
    previousUserSelect = document.body.style.userSelect;
    document.body.style.userSelect = "none";
    host.classList.add("nb-dragging");
    rafId = requestAnimationFrame(autoscrollStep);
    return true;
  }

  function detachListeners() {
    window.removeEventListener("pointermove", onPointerMove);
    window.removeEventListener("pointerup", onPointerUp);
    window.removeEventListener("pointercancel", onPointerCancel);
    window.removeEventListener("lostpointercapture", onLostPointerCapture);
    window.removeEventListener("keydown", onKeyDown);
  }

  function endGesture() {
    detachListeners();
    if (rafId !== null) {
      cancelAnimationFrame(rafId);
      rafId = null;
    }
    if (captureEl && pointerId !== null && typeof captureEl.releasePointerCapture === "function") {
      try {
        captureEl.releasePointerCapture(pointerId);
      } catch {
        // capture was already lost
      }
    }
    if (active) {
      document.body.style.userSelect = previousUserSelect;
      opts.getHost()?.classList.remove("nb-dragging");
    }
    active = false;
    pointerId = null;
    cellId = null;
    captureEl = null;
    bounds = [];
    draggedIndex = -1;
    draggingId.value = null;
    dropSlot.value = null;
    indicatorTop.value = null;
  }

  function onPointerMove(ev: PointerEvent) {
    if (ev.pointerId !== pointerId) return;
    lastClientY = ev.clientY;
    if (!active) {
      const moved = Math.max(Math.abs(ev.clientX - startX), Math.abs(ev.clientY - startY));
      if (moved < ACTIVATE_THRESHOLD_PX) return;
      if (!activate()) {
        endGesture();
        return;
      }
    }
    updateSlot();
  }

  function onPointerUp(ev: PointerEvent) {
    if (ev.pointerId !== pointerId) return;
    const wasActive = active;
    const droppedId = cellId;
    const slot = dropSlot.value;
    const fromIndex = draggedIndex;
    endGesture();
    if (!wasActive || droppedId === null || slot === null) return;
    const finalIndex = finalIndexFromSlot(slot, fromIndex);
    if (finalIndex !== fromIndex) opts.onCommit(droppedId, finalIndex);
  }

  function onPointerCancel(ev: PointerEvent) {
    if (ev.pointerId !== pointerId) return;
    endGesture();
  }

  function onLostPointerCapture(ev: PointerEvent) {
    if (ev.pointerId !== pointerId) return;
    endGesture();
  }

  function onKeyDown(ev: KeyboardEvent) {
    if (ev.key === "Escape") endGesture();
  }

  function onHandlePointerDown(id: string, ev: PointerEvent) {
    if (ev.button !== 0 || opts.isDisabled?.() || pointerId !== null) return;
    // preventDefault below suppresses focus-on-click, so take focus explicitly.
    const el = ev.currentTarget as HTMLElement | null;
    if (el && typeof el.focus === "function") el.focus();
    ev.preventDefault();
    pointerId = ev.pointerId;
    cellId = id;
    startX = ev.clientX;
    startY = ev.clientY;
    lastClientY = ev.clientY;
    const target = ev.currentTarget as Element | null;
    if (target && typeof target.setPointerCapture === "function") {
      target.setPointerCapture(ev.pointerId);
      captureEl = target;
    }
    window.addEventListener("pointermove", onPointerMove);
    window.addEventListener("pointerup", onPointerUp);
    window.addEventListener("pointercancel", onPointerCancel);
    window.addEventListener("lostpointercapture", onLostPointerCapture);
    window.addEventListener("keydown", onKeyDown);
  }

  if (getCurrentInstance()) onBeforeUnmount(endGesture);

  return { onHandlePointerDown, draggingId, dropSlot, indicatorTop, cancel: endGesture };
}
