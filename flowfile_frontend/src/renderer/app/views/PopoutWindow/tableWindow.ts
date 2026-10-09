/** The Data window's pure rules: a node's outputs off the flow data, and which node a message means. */
import { deriveHandles } from "../../utils/nodeHandles";
import type { NodeHandle, VueFlowInput } from "../../types/flow.types";
import type { PopoutMessage } from "../../../lib/popoutWindow";

/** The node's output handles as the canvas renders them, or null when the node is not on the canvas. */
export function outputsForNode(
  flowData: VueFlowInput | null,
  nodeId: number | null,
): NodeHandle[] | null {
  if (!flowData || nodeId === null) return null;
  const node = flowData.node_inputs.find((input) => input.id === nodeId);
  return node ? deriveHandles(node).outputs : null;
}

/** The node to show after a designer message: a previewed node replaces the current, a deselect keeps it. */
export function nextNodeId(current: number | null, message: PopoutMessage): number | null {
  return message.previewNodeId ?? current;
}
