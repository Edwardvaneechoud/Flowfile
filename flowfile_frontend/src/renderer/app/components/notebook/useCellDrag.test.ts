// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  computeDropSlot,
  finalIndexFromSlot,
  findScrollParent,
  useCellDrag,
  type CellBound,
  type CellDragController,
} from "./useCellDrag";

const BOUNDS: CellBound[] = [
  { top: 0, bottom: 100 },
  { top: 100, bottom: 200 },
  { top: 200, bottom: 300 },
];

describe("computeDropSlot", () => {
  it("counts the midpoints above y", () => {
    expect(computeDropSlot(BOUNDS, 60)).toBe(1);
    expect(computeDropSlot(BOUNDS, 160)).toBe(2);
    expect(computeDropSlot(BOUNDS, 260)).toBe(3);
  });

  it("returns 0 above the first midpoint", () => {
    expect(computeDropSlot(BOUNDS, -20)).toBe(0);
    expect(computeDropSlot(BOUNDS, 10)).toBe(0);
  });

  it("returns n below the last midpoint", () => {
    expect(computeDropSlot(BOUNDS, 400)).toBe(3);
  });

  it("does not count a midpoint exactly at y", () => {
    expect(computeDropSlot(BOUNDS, 50)).toBe(0);
    expect(computeDropSlot(BOUNDS, 150)).toBe(1);
    expect(computeDropSlot(BOUNDS, 250)).toBe(2);
  });

  it("returns 0 for an empty list", () => {
    expect(computeDropSlot([], 42)).toBe(0);
  });
});

describe("finalIndexFromSlot", () => {
  it("keeps a slot at or below the dragged index", () => {
    expect(finalIndexFromSlot(0, 2)).toBe(0);
    expect(finalIndexFromSlot(2, 2)).toBe(2);
  });

  it("shifts a slot above the dragged index down by one", () => {
    expect(finalIndexFromSlot(3, 2)).toBe(2);
    expect(finalIndexFromSlot(4, 2)).toBe(3);
  });
});

describe("findScrollParent", () => {
  afterEach(() => {
    document.body.innerHTML = "";
  });

  it("finds the nearest scrollable ancestor", () => {
    const scroller = document.createElement("div");
    scroller.style.overflowY = "auto";
    const middle = document.createElement("div");
    const leaf = document.createElement("div");
    middle.appendChild(leaf);
    scroller.appendChild(middle);
    document.body.appendChild(scroller);
    expect(findScrollParent(leaf)).toBe(scroller);
  });

  it("resolves a self-scrolling element to itself, not its parent", () => {
    const outer = document.createElement("div");
    outer.style.overflowY = "auto";
    const host = document.createElement("div");
    host.style.overflowY = "auto";
    outer.appendChild(host);
    document.body.appendChild(outer);
    expect(findScrollParent(host)).toBe(host);
  });

  it("returns null without a scrollable self or ancestor, and for a null element", () => {
    const leaf = document.createElement("div");
    document.body.appendChild(leaf);
    expect(findScrollParent(leaf)).toBeNull();
    expect(findScrollParent(null)).toBeNull();
  });
});

const stubRect = (el: HTMLElement, top: number, bottom: number) => {
  el.getBoundingClientRect = () =>
    ({
      top,
      bottom,
      left: 0,
      right: 400,
      width: 400,
      height: bottom - top,
      x: 0,
      y: top,
      toJSON: () => ({}),
    }) as DOMRect;
};

describe("useCellDrag", () => {
  let host: HTMLElement;
  let handle: HTMLElement;
  let drag: CellDragController;
  let onCommit: ReturnType<typeof vi.fn>;
  let disabled: boolean;
  let setCapture: ReturnType<typeof vi.fn>;
  let releaseCapture: ReturnType<typeof vi.fn>;

  const down = (clientY = 50) =>
    handle.dispatchEvent(
      new PointerEvent("pointerdown", {
        pointerId: 1,
        button: 0,
        clientX: 10,
        clientY,
        bubbles: true,
      }),
    );

  const fire = (type: string, clientY: number, clientX = 10) =>
    window.dispatchEvent(new PointerEvent(type, { pointerId: 1, clientX, clientY }));

  const escape = () => window.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));

  beforeEach(() => {
    onCommit = vi.fn();
    disabled = false;
    host = document.createElement("div");
    stubRect(host, 0, 600);
    ["a", "b", "c"].forEach((id, index) => {
      const cell = document.createElement("div");
      cell.setAttribute("data-cell-id", id);
      stubRect(cell, index * 100, index * 100 + 100);
      host.appendChild(cell);
    });
    document.body.appendChild(host);

    handle = document.createElement("button");
    setCapture = vi.fn();
    releaseCapture = vi.fn();
    handle.setPointerCapture = setCapture;
    handle.releasePointerCapture = releaseCapture;
    host.firstElementChild!.appendChild(handle);

    drag = useCellDrag({
      getHost: () => host,
      onCommit,
      isDisabled: () => disabled,
    });
    handle.addEventListener("pointerdown", (ev) =>
      drag.onHandlePointerDown("a", ev as PointerEvent),
    );
  });

  afterEach(() => {
    drag.cancel();
    document.body.innerHTML = "";
  });

  it("focuses the handle on pointerdown, despite preventDefault", () => {
    down(50);
    expect(document.activeElement).toBe(handle);
  });

  it("stays inert below the 5px threshold", () => {
    down(50);
    fire("pointermove", 54);
    expect(drag.draggingId.value).toBeNull();
    expect(drag.dropSlot.value).toBeNull();
    expect(host.classList.contains("nb-dragging")).toBe(false);
    fire("pointerup", 54);
    expect(onCommit).not.toHaveBeenCalled();
  });

  it("activates past the threshold and reports the slot", () => {
    down(50);
    fire("pointermove", 56);
    expect(drag.draggingId.value).toBe("a");
    expect(drag.dropSlot.value).toBe(1);
    expect(drag.indicatorTop.value).toBe(100);
    expect(setCapture).toHaveBeenCalledWith(1);
    expect(host.classList.contains("nb-dragging")).toBe(true);
    expect(document.body.style.userSelect).toBe("none");
  });

  it("commits once on pointerup, with the slot shifted past the dragged cell", () => {
    down(50);
    fire("pointermove", 250);
    fire("pointerup", 250);
    expect(onCommit).toHaveBeenCalledTimes(1);
    expect(onCommit).toHaveBeenCalledWith("a", 1);
    expect(drag.draggingId.value).toBeNull();
    expect(drag.dropSlot.value).toBeNull();
    expect(document.body.style.userSelect).toBe("");
  });

  it("does not commit a drop inside the dragged cell's own span", () => {
    down(50);
    fire("pointermove", 60);
    expect(drag.dropSlot.value).toBe(1);
    fire("pointerup", 60);
    expect(onCommit).not.toHaveBeenCalled();
  });

  it("cancels on Escape without committing", () => {
    down(50);
    fire("pointermove", 250);
    escape();
    expect(drag.draggingId.value).toBeNull();
    fire("pointerup", 250);
    expect(onCommit).not.toHaveBeenCalled();
    expect(releaseCapture).toHaveBeenCalledWith(1);
  });

  it("cancels on pointercancel without committing", () => {
    down(50);
    fire("pointermove", 250);
    fire("pointercancel", 250);
    expect(drag.draggingId.value).toBeNull();
    fire("pointerup", 250);
    expect(onCommit).not.toHaveBeenCalled();
  });

  it("cancels on lostpointercapture without committing", () => {
    down(50);
    fire("pointermove", 250);
    fire("lostpointercapture", 250);
    expect(drag.draggingId.value).toBeNull();
    fire("pointerup", 250);
    expect(onCommit).not.toHaveBeenCalled();
  });

  it("ignores pointerdown while disabled", () => {
    disabled = true;
    down(50);
    expect(setCapture).not.toHaveBeenCalled();
    fire("pointermove", 250);
    expect(drag.draggingId.value).toBeNull();
    fire("pointerup", 250);
    expect(onCommit).not.toHaveBeenCalled();
  });

  it("ignores a non-primary button", () => {
    handle.dispatchEvent(
      new PointerEvent("pointerdown", { pointerId: 1, button: 2, clientX: 10, clientY: 50 }),
    );
    fire("pointermove", 250);
    expect(drag.draggingId.value).toBeNull();
  });

  it("ignores moves from another pointer", () => {
    down(50);
    window.dispatchEvent(
      new PointerEvent("pointermove", { pointerId: 2, clientX: 10, clientY: 250 }),
    );
    expect(drag.draggingId.value).toBeNull();
  });

  it("stops responding after cancel()", () => {
    down(50);
    fire("pointermove", 250);
    expect(drag.draggingId.value).toBe("a");
    drag.cancel();
    fire("pointermove", 60);
    expect(drag.draggingId.value).toBeNull();
    expect(drag.dropSlot.value).toBeNull();
    expect(drag.indicatorTop.value).toBeNull();
    fire("pointerup", 60);
    expect(onCommit).not.toHaveBeenCalled();
  });
});

describe("useCellDrag autoscroll", () => {
  const VIEWPORT = 300;
  const CONTENT = 900;

  let outer: HTMLElement;
  let host: HTMLElement;
  let handle: HTMLElement;
  let drag: CellDragController;
  let rafQueue: FrameRequestCallback[];

  const scrollable = (el: HTMLElement) => {
    let value = 0;
    Object.defineProperty(el, "scrollTop", {
      configurable: true,
      get: () => value,
      set: (next: number) => {
        value = Math.max(0, Math.min(next, CONTENT - VIEWPORT));
      },
    });
    Object.defineProperty(el, "scrollHeight", { configurable: true, get: () => CONTENT });
    Object.defineProperty(el, "clientHeight", { configurable: true, get: () => VIEWPORT });
  };

  const stepFrames = (count: number) => {
    for (let i = 0; i < count; i++) {
      const queued = rafQueue;
      rafQueue = [];
      queued.forEach((cb) => cb(0));
    }
  };

  const fire = (type: string, clientY: number) =>
    window.dispatchEvent(new PointerEvent(type, { pointerId: 1, clientX: 10, clientY }));

  beforeEach(() => {
    rafQueue = [];
    let seq = 1;
    vi.stubGlobal("requestAnimationFrame", (cb: FrameRequestCallback) => {
      rafQueue.push(cb);
      return seq++;
    });
    vi.stubGlobal("cancelAnimationFrame", () => {
      rafQueue = [];
    });

    // Mirrors the catalog notebook: `.nb-cells` is its own scroller inside a
    // `.catalog-detail` ancestor that is also `overflow-y: auto`.
    outer = document.createElement("div");
    outer.style.overflowY = "auto";
    scrollable(outer);
    host = document.createElement("div");
    host.style.overflowY = "auto";
    scrollable(host);
    stubRect(host, 0, VIEWPORT);
    outer.appendChild(host);
    document.body.appendChild(outer);

    for (let i = 0; i < CONTENT / 100; i++) {
      const cell = document.createElement("div");
      cell.setAttribute("data-cell-id", `c${i}`);
      stubRect(cell, i * 100, i * 100 + 100);
      host.appendChild(cell);
    }

    handle = document.createElement("button");
    handle.setPointerCapture = vi.fn();
    handle.releasePointerCapture = vi.fn();
    host.firstElementChild!.appendChild(handle);

    drag = useCellDrag({ getHost: () => host, onCommit: vi.fn() });
    handle.addEventListener("pointerdown", (ev) =>
      drag.onHandlePointerDown("c0", ev as PointerEvent),
    );
    handle.dispatchEvent(
      new PointerEvent("pointerdown", {
        pointerId: 1,
        button: 0,
        clientX: 10,
        clientY: 50,
        bubbles: true,
      }),
    );
    fire("pointermove", 56);
  });

  afterEach(() => {
    drag.cancel();
    vi.unstubAllGlobals();
    document.body.innerHTML = "";
  });

  it("scrolls the self-scrolling host near its bottom edge and recomputes the slot", () => {
    fire("pointermove", 296);
    expect(host.scrollTop).toBe(0);
    expect(drag.dropSlot.value).toBe(3);

    stepFrames(6);

    expect(host.scrollTop).toBeGreaterThan(0);
    expect(outer.scrollTop).toBe(0);
    expect(drag.dropSlot.value).toBe(4);
  });

  it("does not scroll while the pointer is away from the edges", () => {
    fire("pointermove", 150);
    const slot = drag.dropSlot.value;

    stepFrames(6);

    expect(host.scrollTop).toBe(0);
    expect(drag.dropSlot.value).toBe(slot);
  });
});
