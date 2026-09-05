// Canvas comments — free-floating text notes (the Alteryx "Comment" tool).
// Comments are organizational only: they are not nodes, have no handles and
// never affect execution. They live in their own VueFlow id namespace so the
// node-centric code paths (settings drawer, data preview, copy) can skip them.
import { type GraphNode, type Node, type XYPosition, useVueFlow } from "@vue-flow/core";
import { nextTick } from "vue";

import { FlowApi } from "../api";
import { useFlowStore } from "../stores/flow-store";
import type { CommentBoundsUpdate, CommentInput, CommentNodeData } from "../types";

export const COMMENT_NODE_PREFIX = "comment-";
export const COMMENT_NODE_TYPE = "comment";
export const COMMENT_DEFAULT_WIDTH = 240;
export const COMMENT_DEFAULT_HEIGHT = 120;
export const COMMENT_MIN_WIDTH = 120;
export const COMMENT_MIN_HEIGHT = 40;

/** VueFlow node id for a backend comment id (separate namespace from node and group ids). */
export const commentNodeId = (commentId: number): string => `${COMMENT_NODE_PREFIX}${commentId}`;
export const isCommentNodeId = (vueId: string): boolean => vueId.startsWith(COMMENT_NODE_PREFIX);
export const commentBackendId = (vueId: string): number =>
  Number(vueId.slice(COMMENT_NODE_PREFIX.length));

/** Build the VueFlow node for a comment. Sits above group boxes and below real nodes. */
export const buildCommentNode = (
  comment: CommentInput,
  autoEdit = false,
): Node<CommentNodeData> => ({
  id: commentNodeId(comment.id),
  type: COMMENT_NODE_TYPE,
  position: { x: comment.x_position, y: comment.y_position },
  style: { width: `${comment.width}px`, height: `${comment.height}px` },
  data: { id: comment.id, text: comment.text, autoEdit },
  zIndex: -500,
  connectable: false,
});

/** Absolute bounds of a comment node as the backend persists them. */
export const commentBoundsOf = (node: GraphNode): CommentBoundsUpdate => ({
  comment_id: commentBackendId(node.id),
  x_position: node.computedPosition.x,
  y_position: node.computedPosition.y,
  width: node.dimensions.width || COMMENT_DEFAULT_WIDTH,
  height: node.dimensions.height || COMMENT_DEFAULT_HEIGHT,
});

export function useCanvasComments() {
  const { addNodes, updateNodeData, updateNodeInternals } = useVueFlow();
  const flowStore = useFlowStore();

  /** Create an empty comment at an absolute canvas position and open it for editing. */
  const addCommentAt = async (position: XYPosition): Promise<void> => {
    if (flowStore.flowId === null || flowStore.flowId <= 0) return;
    const response = await FlowApi.createComment(flowStore.flowId, {
      text: "",
      x_position: position.x,
      y_position: position.y,
      width: COMMENT_DEFAULT_WIDTH,
      height: COMMENT_DEFAULT_HEIGHT,
    });
    if (response.comment) {
      const vueId = commentNodeId(response.comment.id);
      addNodes([buildCommentNode(response.comment, true)]);
      // VueFlow's resize observer does not measure this node on its own, and an
      // unmeasured node stays invisible (and unfocusable); force the measurement.
      await nextTick();
      updateNodeInternals([vueId]);
    }
    flowStore.updateHistoryState(response.history);
  };

  /** Persist edited text (the node's data is updated optimistically). */
  const persistCommentText = async (commentId: number, text: string): Promise<void> => {
    if (flowStore.flowId === null) return;
    updateNodeData(commentNodeId(commentId), { text, autoEdit: false });
    const response = await FlowApi.updateComment(flowStore.flowId, commentId, { text });
    flowStore.updateHistoryState(response.history);
  };

  /** Persist a resize (position + size) as one undoable step. */
  const persistCommentBounds = async (bounds: CommentBoundsUpdate): Promise<void> => {
    if (flowStore.flowId === null) return;
    const response = await FlowApi.updateComment(flowStore.flowId, bounds.comment_id, {
      x_position: bounds.x_position,
      y_position: bounds.y_position,
      width: bounds.width,
      height: bounds.height,
    });
    flowStore.updateHistoryState(response.history);
  };

  return { addCommentAt, persistCommentText, persistCommentBounds };
}
