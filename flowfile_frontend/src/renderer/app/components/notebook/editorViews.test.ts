// @vitest-environment happy-dom
import { afterEach, describe, expect, it, vi } from "vitest";
import type { EditorView } from "@codemirror/view";

import {
  cellSelector,
  disposeOwnerViews,
  focusCell,
  getCellView,
  ownerIdForNode,
  ownerIdForNotebook,
  registerCellView,
  unregisterCellView,
} from "./editorViews";

interface FakeView {
  dom: HTMLElement;
  focus: () => void;
  dispatch: (spec: unknown) => void;
  state: { doc: { length: number } };
}

/** A stand-in EditorView; `rendered: false` mimics a v-show-collapsed (display:none) editor. */
function fakeView(rendered: boolean): FakeView {
  const dom = document.createElement("div");
  // happy-dom has no layout, so offsetParent is spelled out for both cases.
  Object.defineProperty(dom, "offsetParent", { value: rendered ? document.body : null });
  return {
    dom,
    focus: vi.fn(),
    dispatch: vi.fn(),
    state: { doc: { length: 12 } },
  };
}

function host(cellId: string, kind: "textarea" | "none"): HTMLElement {
  const container = document.createElement("div");
  const root = document.createElement("div");
  root.setAttribute("data-cell-id", cellId);
  root.tabIndex = -1;
  if (kind === "textarea") root.appendChild(document.createElement("textarea"));
  container.appendChild(root);
  document.body.appendChild(container);
  return container;
}

afterEach(() => {
  ["owner-a", "owner-b"].forEach(disposeOwnerViews);
  document.body.innerHTML = "";
});

describe("owner ids", () => {
  it("are the tab id for notebooks and a namespaced pair for nodes", () => {
    expect(ownerIdForNotebook("tab-1")).toBe("tab-1");
    expect(ownerIdForNode(3, 7)).toBe("node:3:7");
  });
});

describe("cellSelector", () => {
  it("matches the cell root by data attribute", () => {
    const container = host("e2e-cell-0", "none");
    expect(container.querySelector(cellSelector("e2e-cell-0"))).not.toBeNull();
    expect(container.querySelector(cellSelector("e2e-cell-1"))).toBeNull();
  });
});

describe("focusCell", () => {
  it("focuses a rendered editor and parks the caret at the end", () => {
    const view = fakeView(true);
    registerCellView("owner-a", "c1", view as unknown as EditorView);

    expect(focusCell("owner-a", "c1")).toBe(true);
    expect(view.focus).toHaveBeenCalled();
    expect(view.dispatch).toHaveBeenCalledWith({ selection: { anchor: 12 } });
  });

  it("skips a collapsed editor and focuses the cell root instead", () => {
    const view = fakeView(false);
    registerCellView("owner-a", "c1", view as unknown as EditorView);
    const container = host("c1", "none");

    expect(focusCell("owner-a", "c1", container)).toBe(true);
    expect(view.focus).not.toHaveBeenCalled();
    // The caret must not be dragged to the end of a cell the user cannot see.
    expect(view.dispatch).not.toHaveBeenCalled();
    expect(document.activeElement).toBe(container.querySelector("[data-cell-id]"));
  });

  it("prefers a markdown textarea over the cell root", () => {
    const container = host("c2", "textarea");
    expect(focusCell("owner-a", "c2", container)).toBe(true);
    expect(document.activeElement).toBe(container.querySelector("textarea"));
  });

  it("reports failure when nothing can take focus", () => {
    expect(focusCell("owner-a", "missing")).toBe(false);
    expect(focusCell("owner-a", "missing", host("other", "none"))).toBe(false);
  });

  it("keeps owners apart", () => {
    const view = fakeView(true);
    registerCellView("owner-a", "c1", view as unknown as EditorView);
    expect(getCellView("owner-b", "c1")).toBeUndefined();
    expect(focusCell("owner-b", "c1")).toBe(false);
  });
});

describe("view registration", () => {
  it("returns the last registered view and reveals the previous one on unregister", () => {
    const inline = fakeView(true);
    const dialog = fakeView(true);
    registerCellView("owner-a", "c1", inline as unknown as EditorView);
    registerCellView("owner-a", "c1", dialog as unknown as EditorView);
    expect(getCellView("owner-a", "c1")).toBe(dialog);

    unregisterCellView("owner-a", "c1", dialog as unknown as EditorView);
    expect(getCellView("owner-a", "c1")).toBe(inline);
  });
});
