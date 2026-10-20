/** The Data window's pure rules: a node's outputs off the flow data, and what a designer message means. */
import { deriveHandles } from "../../utils/nodeHandles";
import type { NodeHandle, VueFlowInput } from "../../types/flow.types";
import type { SelectionMessage } from "../../../lib/popoutWindow";

/** The node the window shows and the designer's send count it last saw (`null` before any message). */
export interface ShownPreview {
  nodeId: number | null;
  token: number | null;
}

/** The node's output handles as the canvas renders them, or null when the node is not on the canvas. */
export function outputsForNode(
  flowData: VueFlowInput | null,
  nodeId: number | null,
): NodeHandle[] | null {
  if (!flowData || nodeId === null) return null;
  const node = flowData.node_inputs.find((input) => input.id === nodeId);
  return node ? deriveHandles(node).outputs : null;
}

/**
 * The node to show after a designer message, and whether the same node must be read again: a null
 * node keeps the current one (a deselect), another node switches (the change reads it), the same
 * node with a send count that moved since the window last saw one is a repeated click, and the
 * first message after a mount or reload only adopts the count (the URL already gave the node).
 */
export function applySelection(
  current: ShownPreview,
  message: SelectionMessage,
): { preview: ShownPreview; refetch: boolean } {
  if (message.previewNodeId === null) return { preview: current, refetch: false };
  const preview = { nodeId: message.previewNodeId, token: message.previewToken };
  if (message.previewNodeId !== current.nodeId) return { preview, refetch: false };
  return { preview, refetch: current.token !== null && message.previewToken !== current.token };
}
