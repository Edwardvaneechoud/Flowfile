// @vitest-environment happy-dom
import { beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  CANVAS_PANEL_SELECTOR,
  NODE_CLIPBOARD_KEY,
  clearNodeClipboardBuffer,
  copyNodesToBuffer,
  copySingleNodeToBuffer,
  isCanvasClipboardTarget,
  isNodeBufferArmed,
  matchSentinel,
  mintSentinel,
  readNodeClipboardBuffer,
  resolvePasteIntent,
} from "./useFlowClipboard";
import type { CopyableNode } from "./useFlowClipboard";

const makeNode = (id: number, x = 0, y = 0): CopyableNode => ({
  data: {
    id,
    label: `Node ${id}`,
    nodeTemplate: { item: "formula", multi: false } as never,
    inputs: [{ id: "input-0" }] as never,
    outputs: [{ id: "output-0" }] as never,
  },
  position: { x, y },
});

beforeAll(() => {
  // vitest's happy-dom environment doesn't lift the localStorage accessor onto
  // globalThis; back the bare `localStorage` global with an in-memory Storage.
  if (typeof globalThis.localStorage === "undefined") {
    const store = new Map<string, string>();
    Object.defineProperty(globalThis, "localStorage", {
      configurable: true,
      value: {
        getItem: (key: string) => (store.has(key) ? store.get(key)! : null),
        setItem: (key: string, value: string) => void store.set(key, String(value)),
        removeItem: (key: string) => void store.delete(key),
        clear: () => store.clear(),
      },
    });
  }
});

beforeEach(() => {
  localStorage.clear();
});

describe("sentinel mint/match", () => {
  it("round-trips", () => {
    const text = mintSentinel(3, "abc-123-def");
    expect(matchSentinel(text)).toEqual({ count: 3, id: "abc-123-def" });
  });

  it("tolerates surrounding whitespace from clipboard round-trips", () => {
    expect(matchSentinel(`${mintSentinel(1, "abcdefgh")}\n`)).toEqual({
      count: 1,
      id: "abcdefgh",
    });
  });

  it.each([null, "", "hello", "Flowfile nodes", "Flowfile nodes · x · y", "col_a\tcol_b"])(
    "rejects %j",
    (text) => {
      expect(matchSentinel(text as string | null)).toBeNull();
    },
  );
});

describe("buffer lifecycle", () => {
  it("single-node copy arms the buffer and clears legacy keys", () => {
    localStorage.setItem("copiedNode", "{}");
    localStorage.setItem("copiedMultiNodes", "{}");
    localStorage.setItem("clipboardAtNodeCopy", "stale");

    const sentinel = copyNodesToBuffer([makeNode(7)], [], 42);
    expect(sentinel).not.toBeNull();
    const buffer = readNodeClipboardBuffer();
    expect(buffer?.single?.nodeIdToCopyFrom).toBe(7);
    expect(buffer?.single?.flowIdToCopyFrom).toBe(42);
    expect(buffer?.multi).toBeUndefined();
    expect(matchSentinel(sentinel)?.id).toBe(buffer?.sentinelId);
    expect(localStorage.getItem("copiedNode")).toBeNull();
    expect(localStorage.getItem("copiedMultiNodes")).toBeNull();
    expect(localStorage.getItem("clipboardAtNodeCopy")).toBeNull();
  });

  it("multi-node copy stores relative positions and intra-selection edges only", () => {
    const nodes = [makeNode(1, 100, 50), makeNode(2, 300, 250)];
    const edges = [
      { source: "1", target: "2", sourceHandle: null, targetHandle: null },
      { source: "1", target: "99", sourceHandle: "output-0", targetHandle: "input-0" },
    ];
    const sentinel = copyNodesToBuffer(nodes, edges, 5);
    expect(matchSentinel(sentinel)?.count).toBe(2);
    const buffer = readNodeClipboardBuffer();
    expect(buffer?.multi?.nodes.map((n) => [n.relativeX, n.relativeY])).toEqual([
      [0, 0],
      [200, 200],
    ]);
    expect(buffer?.multi?.edges).toEqual([
      {
        sourceNodeId: 1,
        targetNodeId: 2,
        sourceHandle: "output-0",
        targetHandle: "input-0",
      },
    ]);
  });

  it("empty selection returns null and leaves the buffer untouched", () => {
    expect(copyNodesToBuffer([], [], 1)).toBeNull();
    expect(isNodeBufferArmed()).toBe(false);
  });

  it("copySingleNodeToBuffer keeps the live description", () => {
    copySingleNodeToBuffer(makeNode(3).data, 9, "my description");
    expect(readNodeClipboardBuffer()?.single?.description).toBe("my description");
  });

  it("rejects corrupt buffers", () => {
    localStorage.setItem(NODE_CLIPBOARD_KEY, "not json");
    expect(readNodeClipboardBuffer()).toBeNull();
    localStorage.setItem(NODE_CLIPBOARD_KEY, JSON.stringify({ sentinelId: "x" }));
    expect(readNodeClipboardBuffer()).toBeNull();
  });

  it("clears", () => {
    copyNodesToBuffer([makeNode(1)], [], 1);
    clearNodeClipboardBuffer();
    expect(isNodeBufferArmed()).toBe(false);
  });
});

describe("resolvePasteIntent", () => {
  const armed = () => {
    const sentinel = copyNodesToBuffer([makeNode(1)], [], 1)!;
    return { buffer: readNodeClipboardBuffer(), sentinel };
  };

  it("non-canvas target is always none", () => {
    const { buffer, sentinel } = armed();
    for (const clipboardText of [sentinel, "a\tb\nc\td", "hello", "", null]) {
      expect(resolvePasteIntent({ buffer, clipboardText, isCanvasTarget: false })).toEqual({
        kind: "none",
      });
    }
  });

  it("matching sentinel pastes the node — repeatably", () => {
    const { buffer, sentinel } = armed();
    expect(resolvePasteIntent({ buffer, clipboardText: sentinel, isCanvasTarget: true })).toEqual({
      kind: "node",
    });
    expect(resolvePasteIntent({ buffer, clipboardText: sentinel, isCanvasTarget: true })).toEqual({
      kind: "node",
    });
  });

  it("stale sentinel (uuid mismatch) is none — a stale buffer can never win", () => {
    const { buffer } = armed();
    const staleSentinel = mintSentinel(1, "older-uuid-1234");
    expect(
      resolvePasteIntent({ buffer, clipboardText: staleSentinel, isCanvasTarget: true }),
    ).toEqual({ kind: "none" });
  });

  it("sentinel without a buffer is none", () => {
    expect(
      resolvePasteIntent({
        buffer: null,
        clipboardText: mintSentinel(1, "abcd1234"),
        isCanvasTarget: true,
      }),
    ).toEqual({ kind: "none" });
  });

  it("single-line non-tabular text with an armed buffer is none (the old fallback bug)", () => {
    const { buffer } = armed();
    expect(
      resolvePasteIntent({ buffer, clipboardText: "[price] * 1.21", isCanvasTarget: true }),
    ).toEqual({ kind: "none" });
  });

  it.each(["", null])("empty clipboard (%j) is none", (clipboardText) => {
    const { buffer } = armed();
    expect(resolvePasteIntent({ buffer, clipboardText, isCanvasTarget: true })).toEqual({
      kind: "none",
    });
  });

  it("tab-separated multi-line text is tabular even with an armed buffer", () => {
    const { buffer } = armed();
    const text = "a\tb\n1\t2";
    expect(resolvePasteIntent({ buffer, clipboardText: text, isCanvasTarget: true })).toEqual({
      kind: "tabular",
      text,
    });
  });

  it("multi-line prose without tabs is tabular (single-column manual input)", () => {
    const text = "alpha\nbeta";
    expect(resolvePasteIntent({ buffer: null, clipboardText: text, isCanvasTarget: true })).toEqual(
      { kind: "tabular", text },
    );
  });

  it("a single line with tabs (header only, no data rows) is none", () => {
    expect(
      resolvePasteIntent({ buffer: null, clipboardText: "a\tb", isCanvasTarget: true }),
    ).toEqual({ kind: "none" });
  });
});

describe("isCanvasClipboardTarget", () => {
  let pane: HTMLElement;
  let overlay: HTMLElement;
  let sidebarButton: HTMLElement;
  let tabHeader: HTMLElement;
  let resizer: HTMLElement;
  let cmContent: HTMLElement;
  let nodeButton: HTMLElement;
  let popperItem: HTMLElement;

  beforeEach(() => {
    document.body.innerHTML = `
      <main>
        <div class="vue-flow"><div class="vue-flow__pane"></div>
          <button class="node-button"></button>
        </div>
        <div data-canvas-overlay class="overlay">
          <div role="tab" tabindex="0">General Settings</div>
          <button class="cool-button">first_name</button>
          <div class="resizer"></div>
          <div class="cm-editor"><div class="cm-content" contenteditable="true"></div></div>
        </div>
      </main>
      <div class="el-popper"><span class="option">choice</span></div>
    `;
    pane = document.querySelector(".vue-flow__pane")!;
    overlay = document.querySelector("[data-canvas-overlay]")!;
    sidebarButton = document.querySelector(".cool-button")!;
    tabHeader = document.querySelector("[role=tab]")!;
    resizer = document.querySelector(".resizer")!;
    cmContent = document.querySelector(".cm-content")!;
    nodeButton = document.querySelector(".node-button")!;
    popperItem = document.querySelector(".el-popper .option")!;
  });

  it("accepts the pane, the node button, and body-after-pane-click", () => {
    expect(isCanvasClipboardTarget(pane, null)).toBe(true);
    expect(isCanvasClipboardTarget(nodeButton, null)).toBe(true);
    expect(isCanvasClipboardTarget(document.body, pane)).toBe(true);
  });

  it("rejects every element inside a canvas overlay panel", () => {
    for (const el of [sidebarButton, tabHeader, resizer, overlay]) {
      expect(isCanvasClipboardTarget(el, null)).toBe(false);
    }
  });

  it("rejects editable targets (inputs, contenteditable, CodeMirror)", () => {
    const input = document.createElement("input");
    document.body.appendChild(input);
    expect(isCanvasClipboardTarget(input, null)).toBe(false);
    expect(isCanvasClipboardTarget(cmContent, null)).toBe(false);
  });

  it("rejects teleported Element Plus poppers", () => {
    expect(isCanvasClipboardTarget(popperItem, null)).toBe(false);
  });

  it("classifies a <body> target by the last pointerdown (the non-focusable-chrome case)", () => {
    // Clicking a label/resizer/WebKit button moves focus nowhere, so the
    // ClipboardEvent targets <body>; the pointerdown is the real signal.
    expect(isCanvasClipboardTarget(document.body, sidebarButton)).toBe(false);
    expect(isCanvasClipboardTarget(document.body, resizer)).toBe(false);
    expect(isCanvasClipboardTarget(document.body, pane)).toBe(true);
  });

  it("non-Element targets resolve via the last pointerdown", () => {
    expect(isCanvasClipboardTarget(null, sidebarButton)).toBe(false);
    expect(isCanvasClipboardTarget(null, pane)).toBe(true);
  });

  it("defaults to canvas with no signal at all (sentinel still gates node paste)", () => {
    expect(isCanvasClipboardTarget(null, null)).toBe(true);
    expect(isCanvasClipboardTarget(document.body, null)).toBe(true);
  });

  it("panel selector covers the Element Plus overlay classes", () => {
    for (const cls of ["el-dialog", "el-drawer", "el-message-box", "el-overlay", "context-menu"]) {
      expect(CANVAS_PANEL_SELECTOR).toContain(cls);
    }
  });
});
