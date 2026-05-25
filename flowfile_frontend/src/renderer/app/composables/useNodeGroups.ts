// composables/useNodeGroups.ts
// Shared logic for visual node groups — labeled, resizable containers drawn around
// nodes. Groups are organizational only: they never affect execution or results.
//
// The #1 correctness concern is absolute<->relative position conversion: VueFlow
// stores a child's `position` relative to its parent group, while the backend stores
// absolute canvas coordinates. The helpers below centralize that conversion.
import { type Edge, type GraphNode, type Node, type XYPosition, useVueFlow } from "@vue-flow/core";
import { ElMessage } from "element-plus";
import { nextTick } from "vue";

import { FlowApi } from "../api";
import { useFlowStore } from "../stores/flow-store";
import type { GroupBoundsUpdate, GroupInput, GroupNodeData, NodePositionUpdate } from "../types";

export const GROUP_NODE_PREFIX = "group-";
export const CUSTOM_NODE_TYPE = "custom-node";
export const GROUP_NODE_TYPE = "group";

// Box padding around members and header allowance. Groups auto-fit their contents
// (no manual resize), so these define how tightly the box wraps its nodes.
const GROUP_PADDING = 40;
const GROUP_HEADER = 36;
// Height of a collapsed group (header bar only).
const GROUP_COLLAPSED_HEIGHT = 34;
// Width of a collapsed group — a compact pill, independent of the expanded box width.
const GROUP_COLLAPSED_WIDTH = 200;

// Handle ids on the collapsed pill that boundary "proxy" edges attach to.
export const GROUP_TARGET_HANDLE = "group-target";
export const GROUP_SOURCE_HANDLE = "group-source";
// Proxy edges are UI-only stand-ins shown while a group is collapsed: they reroute the
// edges crossing the group boundary to the pill. Ids are prefixed so Canvas.vue can skip
// backend persistence for them and so we can find/remove them on expand/ungroup.
export const GROUP_PROXY_EDGE_PREFIX = "group-proxy-";
export const GROUP_PROXY_EDGE_TYPE = "group-proxy";

/** VueFlow node id for a backend group id (kept in a separate namespace from node ids). */
export const groupNodeId = (groupId: number): string => `${GROUP_NODE_PREFIX}${groupId}`;
export const isGroupNodeId = (vueId: string): boolean => vueId.startsWith(GROUP_NODE_PREFIX);
export const groupBackendId = (vueId: string): number =>
  Number(vueId.slice(GROUP_NODE_PREFIX.length));

/** Absolute canvas position of a node, whether or not it is parented to a group. */
export const absolutePosition = (node: GraphNode): XYPosition => ({
  x: node.computedPosition.x,
  y: node.computedPosition.y,
});

/** Build the VueFlow container node for a group (rendered behind its children). */
export const buildGroupNode = (group: GroupInput): Node<GroupNodeData> => ({
  id: groupNodeId(group.id),
  type: GROUP_NODE_TYPE,
  position: { x: group.x_position, y: group.y_position },
  style: { width: `${group.width}px`, height: `${group.height}px` },
  data: {
    id: group.id,
    label: group.name,
    color: group.color ?? null,
    collapsed: group.collapsed ?? false,
  },
  zIndex: 0,
  connectable: false,
  // Removed only via the explicit "ungroup" action — never by the generic delete path.
  deletable: false,
});

export function useNodeGroups() {
  const {
    getSelectedNodes,
    getNodes,
    getEdges,
    findNode,
    addNodes,
    addEdges,
    removeNodes,
    removeEdges,
    updateNode,
    updateNodeData,
  } = useVueFlow();
  const flowStore = useFlowStore();

  const childNodesOf = (groupVueId: string): GraphNode[] =>
    getNodes.value.filter((node) => node.parentNode === groupVueId);

  /**
   * While a group is collapsed its members are hidden, so VueFlow drops every edge that
   * touches them. Re-show the connections crossing the group boundary as UI-only "proxy"
   * edges routed to the collapsed pill: external→member becomes external→pill, and
   * member→external becomes pill→external. Internal (member↔member) edges stay hidden.
   */
  const addGroupProxyEdges = (groupVueId: string): void => {
    const groupId = groupBackendId(groupVueId);
    const memberIds = new Set(childNodesOf(groupVueId).map((node) => node.id));
    if (memberIds.size === 0) return;
    const prefix = `${GROUP_PROXY_EDGE_PREFIX}${groupId}-`;
    const proxies: Edge[] = [];
    const seen = new Set<string>();
    for (const edge of getEdges.value) {
      const sourceIn = memberIds.has(edge.source);
      const targetIn = memberIds.has(edge.target);
      if (sourceIn === targetIn) continue; // internal or fully external — nothing to reroute
      const proxy: Edge = targetIn
        ? {
            id: `${prefix}in-${edge.source}-${edge.sourceHandle ?? ""}`,
            source: edge.source,
            sourceHandle: edge.sourceHandle,
            target: groupVueId,
            targetHandle: GROUP_TARGET_HANDLE,
            type: GROUP_PROXY_EDGE_TYPE,
            selectable: false,
            deletable: false,
            focusable: false,
            data: { isGroupProxy: true, groupId },
          }
        : {
            id: `${prefix}out-${edge.target}-${edge.targetHandle ?? ""}`,
            source: groupVueId,
            sourceHandle: GROUP_SOURCE_HANDLE,
            target: edge.target,
            targetHandle: edge.targetHandle,
            type: GROUP_PROXY_EDGE_TYPE,
            selectable: false,
            deletable: false,
            focusable: false,
            data: { isGroupProxy: true, groupId },
          };
      if (seen.has(proxy.id)) continue;
      seen.add(proxy.id);
      proxies.push(proxy);
    }
    if (proxies.length > 0) addEdges(proxies);
  };

  /** Remove the proxy edges created for a (now expanding/ungrouping) collapsed group. */
  const removeGroupProxyEdges = (groupId: number): void => {
    const prefix = `${GROUP_PROXY_EDGE_PREFIX}${groupId}-`;
    const ids = getEdges.value.filter((edge) => edge.id.startsWith(prefix)).map((edge) => edge.id);
    if (ids.length > 0) removeEdges(ids);
  };

  // Group nodes are deletable:false so the keyboard Delete key can't orphan their
  // children. removeNodes() honors that flag, so the explicit ungroup path must flip
  // it first to actually remove the box from the canvas.
  const removeGroupNode = (groupVueId: string): void => {
    updateNode(groupVueId, { deletable: true });
    removeNodes([groupVueId]);
  };

  /** Reparent on-canvas nodes into a group, converting positions to parent-relative. */
  const attachNodesToGroup = (group: GroupInput, nodeIds: number[]): void => {
    const parentId = groupNodeId(group.id);
    for (const nodeId of nodeIds) {
      const node = findNode(String(nodeId));
      if (!node) continue;
      const abs = absolutePosition(node);
      updateNode(String(nodeId), {
        parentNode: parentId,
        position: { x: abs.x - group.x_position, y: abs.y - group.y_position },
      });
    }
  };

  /** Restore a child to absolute coordinates, unhide it, and detach it from its group. */
  const detachNodeToAbsolute = (node: GraphNode): void => {
    const abs = absolutePosition(node);
    updateNode(node.id, { parentNode: undefined, position: { x: abs.x, y: abs.y }, hidden: false });
  };

  /**
   * Auto-fit a group's box to tightly wrap its members (+ padding/header), keeping the
   * members visually fixed. Returns the new bounds and member absolute positions to
   * persist — all computed from a single snapshot to avoid stale async dimension reads
   * after the box origin moves.
   */
  const refitGroup = (
    groupVueId: string,
  ): { bounds: GroupBoundsUpdate; positions: NodePositionUpdate[] } | null => {
    const children = childNodesOf(groupVueId);
    if (children.length === 0) return null;
    const snapshot = children.map((child) => {
      const abs = absolutePosition(child);
      return {
        id: child.id,
        x: abs.x,
        y: abs.y,
        w: child.dimensions.width || 0,
        h: child.dimensions.height || 0,
      };
    });
    const minX = Math.min(...snapshot.map((s) => s.x)) - GROUP_PADDING;
    const minY = Math.min(...snapshot.map((s) => s.y)) - GROUP_PADDING - GROUP_HEADER;
    const maxX = Math.max(...snapshot.map((s) => s.x + s.w)) + GROUP_PADDING;
    const maxY = Math.max(...snapshot.map((s) => s.y + s.h)) + GROUP_PADDING;
    const width = maxX - minX;
    const height = maxY - minY;
    // Move the box origin, then re-anchor each child relative to it (absolute unchanged).
    updateNode(groupVueId, {
      position: { x: minX, y: minY },
      style: { width: `${width}px`, height: `${height}px` },
    });
    for (const item of snapshot) {
      updateNode(item.id, { position: { x: item.x - minX, y: item.y - minY } });
    }
    return {
      bounds: {
        group_id: groupBackendId(groupVueId),
        x_position: minX,
        y_position: minY,
        width,
        height,
      },
      positions: snapshot.map((s) => ({ node_id: Number(s.id), pos_x: s.x, pos_y: s.y })),
    };
  };

  /** Group the currently selected (non-group) nodes into a new auto-fitted container. */
  const groupSelectedNodes = async (): Promise<void> => {
    if (flowStore.flowId === null) return;
    const selected = getSelectedNodes.value.filter((node) => node.type === CUSTOM_NODE_TYPE);
    if (selected.length === 0) {
      ElMessage.info("Select one or more nodes to group.");
      return;
    }
    const nodeIds = selected.map((node) => Number(node.id));
    const response = await FlowApi.createGroup(flowStore.flowId, {
      node_ids: nodeIds,
      name: "Group",
    });
    if (response.group) {
      // Parent must exist before children reference it.
      addNodes([buildGroupNode(response.group)]);
      attachNodesToGroup(response.group, nodeIds);
      // Tighten the box to the real rendered node sizes and persist those bounds.
      const fit = refitGroup(groupNodeId(response.group.id));
      if (fit) {
        await FlowApi.updateLayout(flowStore.flowId, {
          node_positions: [],
          group_bounds: [fit.bounds],
        });
      }
    }
    flowStore.updateHistoryState(response.history);
  };

  /** Ungroup: detach children back to absolute coordinates, then delete the box. */
  const ungroupNodes = async (groupId: number): Promise<void> => {
    if (flowStore.flowId === null) return;
    const parentId = groupNodeId(groupId);
    removeGroupProxyEdges(groupId);
    for (const child of childNodesOf(parentId)) {
      detachNodeToAbsolute(child);
    }
    removeGroupNode(parentId);
    const response = await FlowApi.deleteGroup(flowStore.flowId, groupId);
    flowStore.updateHistoryState(response.history);
  };

  /** Remove the currently selected grouped nodes from their group(s). */
  const removeSelectedFromGroup = async (): Promise<void> => {
    if (flowStore.flowId === null) return;
    const grouped = getSelectedNodes.value.filter(
      (node) => node.type === CUSTOM_NODE_TYPE && node.parentNode,
    );
    if (grouped.length === 0) return;
    const affectedGroups = new Set(grouped.map((node) => node.parentNode as string));
    for (const node of grouped) {
      detachNodeToAbsolute(node);
    }
    const response = await FlowApi.removeNodesFromGroup(
      flowStore.flowId,
      grouped.map((node) => Number(node.id)),
    );
    // Drop any group box left empty on the canvas (the backend already pruned it).
    for (const groupVueId of affectedGroups) {
      if (childNodesOf(groupVueId).length === 0) removeGroupNode(groupVueId);
    }
    flowStore.updateHistoryState(response.history);
  };

  /** Collapse/expand a group: hide/show members and shrink/refit the box. */
  const setGroupCollapsed = async (groupId: number, collapsed: boolean): Promise<void> => {
    if (flowStore.flowId === null) return;
    const parentId = groupNodeId(groupId);
    const group = findNode(parentId);
    if (!group) return;
    // Expanding: drop the proxy edges first so the real edges re-render once members unhide.
    if (!collapsed) removeGroupProxyEdges(groupId);
    for (const child of childNodesOf(parentId)) {
      updateNode(child.id, { hidden: collapsed });
    }
    updateNodeData(parentId, { collapsed });
    let bounds: GroupBoundsUpdate;
    if (collapsed) {
      updateNode(parentId, {
        style: { width: `${GROUP_COLLAPSED_WIDTH}px`, height: `${GROUP_COLLAPSED_HEIGHT}px` },
      });
      bounds = {
        group_id: groupId,
        x_position: group.computedPosition.x,
        y_position: group.computedPosition.y,
        width: GROUP_COLLAPSED_WIDTH,
        height: GROUP_COLLAPSED_HEIGHT,
      };
      // The pill's handles only render once `collapsed` is applied; wait a tick so the
      // proxy edges have handles to attach to.
      await nextTick();
      addGroupProxyEdges(parentId);
    } else {
      const fit = refitGroup(parentId);
      bounds = fit
        ? fit.bounds
        : {
            group_id: groupId,
            x_position: group.computedPosition.x,
            y_position: group.computedPosition.y,
            width: group.dimensions.width,
            height: group.dimensions.height,
          };
    }
    const response = await FlowApi.updateGroup(flowStore.flowId, groupId, {
      collapsed,
      x_position: bounds.x_position,
      y_position: bounds.y_position,
      width: bounds.width,
      height: bounds.height,
    });
    flowStore.updateHistoryState(response.history);
  };

  /** Persist absolute positions for a set of nodes and/or bounds for a set of groups. */
  const persistLayout = async (nodes: GraphNode[], groups: GraphNode[] = []): Promise<void> => {
    if (flowStore.flowId === null) return;
    const nodePositions: NodePositionUpdate[] = nodes.map((node) => {
      const abs = absolutePosition(node);
      return { node_id: Number(node.id), pos_x: abs.x, pos_y: abs.y };
    });
    const groupBounds: GroupBoundsUpdate[] = groups.map((group) => ({
      group_id: groupBackendId(group.id),
      x_position: group.computedPosition.x,
      y_position: group.computedPosition.y,
      width: group.dimensions.width,
      height: group.dimensions.height,
    }));
    if (nodePositions.length === 0 && groupBounds.length === 0) return;
    const response = await FlowApi.updateLayout(flowStore.flowId, {
      node_positions: nodePositions,
      group_bounds: groupBounds,
    });
    flowStore.updateHistoryState(response.history);
  };

  /**
   * Persist a drag-end:
   * - group moved → its bounds + every child's new absolute position;
   * - grouped child moved → auto-fit the box and persist box + member positions;
   * - free node moved → the dragged node, or all selected free nodes (multi-select).
   */
  const persistDrag = async (node: GraphNode): Promise<void> => {
    if (flowStore.flowId === null) return;
    if (node.type === GROUP_NODE_TYPE) {
      await persistLayout(childNodesOf(node.id), [node]);
      return;
    }
    if (node.parentNode) {
      const fit = refitGroup(node.parentNode);
      if (fit) {
        const response = await FlowApi.updateLayout(flowStore.flowId, {
          node_positions: fit.positions,
          group_bounds: [fit.bounds],
        });
        flowStore.updateHistoryState(response.history);
      }
      return;
    }
    const selected = getSelectedNodes.value.filter(
      (selectedNode) => selectedNode.type !== GROUP_NODE_TYPE && !selectedNode.parentNode,
    );
    const moved =
      selected.length > 1 && selected.some((selectedNode) => selectedNode.id === node.id)
        ? selected
        : [node];
    await persistLayout(moved);
  };

  return {
    childNodesOf,
    attachNodesToGroup,
    detachNodeToAbsolute,
    groupSelectedNodes,
    ungroupNodes,
    removeSelectedFromGroup,
    setGroupCollapsed,
    addGroupProxyEdges,
    removeGroupProxyEdges,
    persistLayout,
    persistDrag,
  };
}
