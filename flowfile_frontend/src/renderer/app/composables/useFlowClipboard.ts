// Canvas node copy/paste: the single owner of the node-copy buffer and of the
// decision "does this clipboard event belong to the canvas?".
//
// Design (replaces the old clipboard-snapshot diff heuristic):
// - Copying nodes writes a sentinel line ("Flowfile nodes · <count> · <uuid>")
//   to the OS clipboard and the node payload to localStorage. A generic paste
//   is a node paste ONLY when the clipboard text matches the buffer's sentinel
//   uuid — anything else can never resurrect a stale buffer. The context
//   menu's explicit "Paste Node" bypasses the sentinel (explicit intent needs
//   no disambiguation, and web Firefox/insecure contexts cannot read the
//   clipboard outside a paste event).
// - Clipboard events fired with focus on <body> are classified by the last
//   pointerdown target: non-focusable panel chrome (labels, resizers — and on
//   WebKit even buttons) doesn't take focus on click, so the event target
//   alone cannot distinguish "working in a settings panel" from "bare canvas".
import type { NodeHandle, NodeTemplate } from "../types/flow.types";
import type { EdgeCopyValue, MultiNodeCopyValue, NodeCopyValue } from "../types/canvas.types";
import { toSnakeCase } from "../views/DesignerView/utils";
import { parseTabularText, copyToClipboard } from "../utils/clipboardUtils";
import { DEFAULT_OUTPUT_HANDLE } from "../utils/outputHandle";
import { desktop, isDesktop } from "../../lib/desktop";

export interface NodeClipboardBuffer {
  sentinelId: string;
  flowId: number;
  copiedAt: number;
  single?: NodeCopyValue;
  multi?: MultiNodeCopyValue;
}

export type PasteIntent = { kind: "none" } | { kind: "node" } | { kind: "tabular"; text: string };

/** Structural subset of a VueFlow node the copy writer needs. */
export interface CopyableNodeData {
  id: number;
  label: string;
  nodeTemplate?: NodeTemplate;
  component?: { __name?: string };
  inputs: NodeHandle[];
  outputs: NodeHandle[];
}
export interface CopyableNode {
  data: CopyableNodeData;
  position: { x: number; y: number };
}
export interface CopyableEdge {
  source: string;
  target: string;
  sourceHandle?: string | null;
  targetHandle?: string | null;
}

export const NODE_CLIPBOARD_KEY = "flowfileNodeClipboard";
// Pre-refactor keys; cleared on every write so an old buffer can't linger.
const LEGACY_KEYS = ["copiedNode", "copiedMultiNodes", "clipboardAtNodeCopy"];

const SENTINEL_RE = /^Flowfile nodes · (\d+) · ([0-9a-zA-Z-]{8,})$/;

export const mintSentinel = (count: number, id: string): string =>
  `Flowfile nodes · ${count} · ${id}`;

export const matchSentinel = (text: string | null): { count: number; id: string } | null => {
  if (!text) return null;
  const match = SENTINEL_RE.exec(text.trim());
  if (!match) return null;
  return { count: Number(match[1]), id: match[2] };
};

const newSentinelId = (): string =>
  typeof crypto !== "undefined" && crypto.randomUUID
    ? crypto.randomUUID()
    : `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;

export const buildNodeCopyValue = (
  data: CopyableNodeData,
  flowId: number,
  description = "",
): NodeCopyValue => ({
  nodeIdToCopyFrom: data.id,
  type: data.nodeTemplate?.item || data.component?.__name || "unknown",
  label: data.label,
  description,
  numberOfInputs: data.inputs.length,
  numberOfOutputs: data.outputs.length,
  typeSnakeCase: data.nodeTemplate?.item || toSnakeCase(data.component?.__name || "unknown"),
  flowIdToCopyFrom: flowId,
  multi: data.nodeTemplate?.multi,
  nodeTemplate: data.nodeTemplate,
  // Live handle snapshots so dynamic-handle nodes paste with the same
  // inputs/outputs the source node had.
  inputHandles: data.inputs,
  outputHandles: data.outputs,
});

const writeBuffer = (buffer: NodeClipboardBuffer): void => {
  localStorage.setItem(NODE_CLIPBOARD_KEY, JSON.stringify(buffer));
  for (const key of LEGACY_KEYS) localStorage.removeItem(key);
};

export const readNodeClipboardBuffer = (): NodeClipboardBuffer | null => {
  try {
    const raw = localStorage.getItem(NODE_CLIPBOARD_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as NodeClipboardBuffer;
    if (!parsed.sentinelId || (!parsed.single && !parsed.multi)) return null;
    return parsed;
  } catch {
    return null;
  }
};

export const clearNodeClipboardBuffer = (): void => {
  localStorage.removeItem(NODE_CLIPBOARD_KEY);
};

export const isNodeBufferArmed = (): boolean => readNodeClipboardBuffer() !== null;

/**
 * Copy one node (the context-menu path, which knows the live description).
 * Returns the sentinel text the caller must place on the OS clipboard.
 */
export const copySingleNodeToBuffer = (
  data: CopyableNodeData,
  flowId: number,
  description = "",
): string => {
  const sentinelId = newSentinelId();
  writeBuffer({
    sentinelId,
    flowId,
    copiedAt: Date.now(),
    single: buildNodeCopyValue(data, flowId, description),
  });
  return mintSentinel(1, sentinelId);
};

/**
 * Copy the current selection (single or multi). Returns the sentinel text to
 * put on the OS clipboard, or null when the selection is empty — in that case
 * the caller must NOT cancel the native copy.
 */
export const copyNodesToBuffer = (
  nodes: CopyableNode[],
  edges: CopyableEdge[],
  flowId: number,
): string | null => {
  if (nodes.length === 0) return null;
  const sentinelId = newSentinelId();

  if (nodes.length === 1) {
    writeBuffer({
      sentinelId,
      flowId,
      copiedAt: Date.now(),
      single: buildNodeCopyValue(nodes[0].data, flowId),
    });
    return mintSentinel(1, sentinelId);
  }

  const selectedIds = new Set(nodes.map((n) => n.data.id));
  let minX = Infinity;
  let minY = Infinity;
  for (const node of nodes) {
    minX = Math.min(minX, node.position.x);
    minY = Math.min(minY, node.position.y);
  }
  const copiedNodes: NodeCopyValue[] = nodes.map((node) => ({
    ...buildNodeCopyValue(node.data, flowId),
    relativeX: node.position.x - minX,
    relativeY: node.position.y - minY,
  }));
  const copiedEdges: EdgeCopyValue[] = edges
    .filter(
      (edge) => selectedIds.has(parseInt(edge.source)) && selectedIds.has(parseInt(edge.target)),
    )
    .map((edge) => ({
      sourceNodeId: parseInt(edge.source),
      targetNodeId: parseInt(edge.target),
      sourceHandle: edge.sourceHandle || DEFAULT_OUTPUT_HANDLE,
      targetHandle: edge.targetHandle || "input-0",
    }));

  writeBuffer({
    sentinelId,
    flowId,
    copiedAt: Date.now(),
    multi: { nodes: copiedNodes, edges: copiedEdges, flowIdToCopyFrom: flowId },
  });
  return mintSentinel(nodes.length, sentinelId);
};

/**
 * Async sentinel write for paths outside a copy event (context-menu Copy).
 * Desktop uses the clipboard-manager plugin; web uses copyToClipboard's
 * insecure-context-safe fallback. Returns whether the write succeeded.
 */
export const writeSentinelToOsClipboard = async (text: string): Promise<boolean> => {
  if (isDesktop) {
    try {
      await desktop.writeClipboardText(text);
      return true;
    } catch {
      return false;
    }
  }
  return copyToClipboard(text);
};

/**
 * Decide what a generic (Cmd+V / menu Paste) clipboard event means for the
 * canvas. Pure — feed it the buffer and the pasted text.
 *
 * isCanvasTarget is the caller's isCanvasClipboardTarget verdict. Canvas.vue
 * early-returns on a non-canvas target before calling, so it always passes
 * true there; the false branch keeps this contract total for any future
 * caller and is pinned by the unit-test truth table.
 */
export const resolvePasteIntent = (args: {
  buffer: NodeClipboardBuffer | null;
  clipboardText: string | null;
  isCanvasTarget: boolean;
}): PasteIntent => {
  if (!args.isCanvasTarget) return { kind: "none" };
  const sentinel = matchSentinel(args.clipboardText);
  if (sentinel) {
    // A sentinel from another install/session whose uuid doesn't match the
    // local buffer must do nothing — that's the whole point of the uuid.
    return args.buffer && args.buffer.sentinelId === sentinel.id ? { kind: "node" } : { kind: "none" };
  }
  if (args.clipboardText) {
    const parsed = parseTabularText(args.clipboardText);
    if (parsed && parsed.length >= 2) return { kind: "tabular", text: args.clipboardText };
  }
  return { kind: "none" };
};

/**
 * Everything a paste can land on that is NOT the bare canvas. DraggableItem
 * (every floating panel incl. the node-settings drawer) carries
 * data-canvas-overlay; the rest are Element Plus overlays (poppers/selects
 * teleport to <body>, so closest() must see their wrapper class) and the
 * custom canvas context menu.
 */
export const CANVAS_PANEL_SELECTOR =
  "[data-canvas-overlay], .el-dialog, .el-drawer, .el-popper, .el-message-box, .el-overlay, .context-menu";

const isEditableElement = (el: Element): boolean => {
  if (el.tagName === "INPUT" || el.tagName === "TEXTAREA" || (el as HTMLElement).isContentEditable) {
    return true;
  }
  return typeof el.closest === "function" && !!el.closest(".cm-editor");
};

/**
 * Is this clipboard event the canvas's to handle? <body>/non-Element targets
 * carry no information (see module header), so they are classified by the
 * last pointerdown target instead. With no signal at all we default to true:
 * node paste is additionally gated by the sentinel, so a misclassification
 * cannot resurrect a stale buffer.
 */
export const isCanvasClipboardTarget = (
  target: EventTarget | null,
  lastPointerDownEl: Element | null,
): boolean => {
  let el: Element | null = target instanceof Element ? target : null;
  if (!el || el === document.body || el === document.documentElement) {
    el = lastPointerDownEl;
  }
  if (!el) return true;
  if (isEditableElement(el)) return false;
  if (typeof el.closest === "function" && el.closest(CANVAS_PANEL_SELECTOR)) return false;
  return true;
};
