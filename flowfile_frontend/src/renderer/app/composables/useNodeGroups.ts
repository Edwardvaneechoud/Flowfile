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
const GROUP_COLLAPSED_WIDTH = 200;
// Fallback node footprint when a member isn't measured yet (matches the backend's
// nominal sizes in _recompute_group_bounds).
const NOMINAL_NODE_WIDTH = 180;
const NOMINAL_NODE_HEIGHT = 80;

export const GROUP_TARGET_HANDLE = "group-target";
export const GROUP_SOURCE_HANDLE = "group-source";
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

/** Build the VueFlow container node for a group (rendered behind its children).
 * depth = nesting level; deeper groups paint above shallower ones, all below member nodes. */
export const buildGroupNode = (group: GroupInput, depth = 0): Node<GroupNodeData> => ({
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
  zIndex: -1000 + depth,
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
    updateNodeInternals,
  } = useVueFlow();
  const flowStore = useFlowStore();

  const childNodesOf = (groupVueId: string): GraphNode[] =>
    getNodes.value.filter((node) => node.parentNode === groupVueId);

  /** All descendants (nodes and nested groups) of a group, depth-first. Cycle-guarded. */
  const descendantsOf = (groupVueId: string): GraphNode[] => {
    const out: GraphNode[] = [];
    const stack = [...childNodesOf(groupVueId)];
    const seen = new Set<string>();
    while (stack.length) {
      const node = stack.pop() as GraphNode;
      if (seen.has(node.id)) continue;
      seen.add(node.id);
      out.push(node);
      if (node.type === GROUP_NODE_TYPE) stack.push(...childNodesOf(node.id));
    }
    return out;
  };

  /** Nesting depth of a group node (0 = top level), walking its parentNode chain. */
  const vueGroupDepth = (vueId: string | undefined): number => {
    let depth = 0;
    let current = vueId ? findNode(vueId) : undefined;
    const seen = new Set<string>();
    while (current?.parentNode && !seen.has(current.id)) {
      seen.add(current.id);
      depth += 1;
      current = findNode(current.parentNode);
    }
    return depth;
  };

  /** Hide/show a group's whole subtree. Hiding cascades to all descendants; showing stops at
   * a nested collapsed group (its own members stay hidden). */
  const setSubtreeHidden = (groupVueId: string, hidden: boolean): void => {
    for (const child of childNodesOf(groupVueId)) {
      updateNode(child.id, { hidden });
      if (child.type === GROUP_NODE_TYPE && (hidden || !(child.data as GroupNodeData)?.collapsed)) {
        setSubtreeHidden(child.id, hidden);
      }
    }
  };

  /** Reroute the collapsed group's boundary edges to the pill as UI-only proxy edges. */
  const addGroupProxyEdges = (groupVueId: string): void => {
    const groupId = groupBackendId(groupVueId);
    const memberIds = new Set(descendantsOf(groupVueId).map((node) => node.id));
    if (memberIds.size === 0) return;
    const prefix = `${GROUP_PROXY_EDGE_PREFIX}${groupId}-`;
    const proxies: Edge[] = [];
    const seen = new Set<string>();
    for (const edge of getEdges.value) {
      const sourceIn = memberIds.has(edge.source);
      const targetIn = memberIds.has(edge.target);
      if (sourceIn === targetIn) continue;
      const proxy: Edge = targetIn
        ? {
            id: `${prefix}in-${edge.source}-${edge.sourceHandle ?? ""}`,
            source: edge.source,
            sourceHandle: edge.sourceHandle,
            target: groupVueId,
            targetHandle: GROUP_TARGET_HANDLE,
            type: GROUP_PROXY_EDGE_TYPE,
            selectable: false,
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
            focusable: false,
            data: { isGroupProxy: true, groupId },
          };
      if (seen.has(proxy.id)) continue;
      seen.add(proxy.id);
      proxies.push(proxy);
    }
    if (proxies.length > 0) {
      addEdges(proxies);
      // re-measure so the right-edge source handle tracks the new compact width
      updateNodeInternals([groupVueId]);
    }
  };

  /** Remove a collapsed group's proxy edges. */
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

  /** Reparent VueFlow nodes (custom or group) into a parent group, converting positions
   * to be relative to the parent's absolute origin. The parent must already exist. */
  const attachNodesToGroup = (parentVueId: string, childVueIds: string[]): void => {
    const parent = findNode(parentVueId);
    const px = parent ? parent.computedPosition.x : 0;
    const py = parent ? parent.computedPosition.y : 0;
    for (const childId of childVueIds) {
      const node = findNode(childId);
      if (!node) continue;
      const abs = absolutePosition(node);
      updateNode(childId, {
        parentNode: parentVueId,
        position: { x: abs.x - px, y: abs.y - py },
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
        w: child.dimensions.width || NOMINAL_NODE_WIDTH,
        h: child.dimensions.height || NOMINAL_NODE_HEIGHT,
      };
    });
    const minX = Math.min(...snapshot.map((s) => s.x)) - GROUP_PADDING;
    const minY = Math.min(...snapshot.map((s) => s.y)) - GROUP_PADDING - GROUP_HEADER;
    const maxX = Math.max(...snapshot.map((s) => s.x + s.w)) + GROUP_PADDING;
    const maxY = Math.max(...snapshot.map((s) => s.y + s.h)) + GROUP_PADDING;
    const width = maxX - minX;
    const height = maxY - minY;
    // Move the box origin, then re-anchor each child relative to it (absolute unchanged).
    // A nested group's VueFlow position is relative to its parent, so convert from absolute.
    const groupNode = findNode(groupVueId);
    const parent = groupNode?.parentNode ? findNode(groupNode.parentNode) : undefined;
    updateNode(groupVueId, {
      position: parent
        ? { x: minX - parent.computedPosition.x, y: minY - parent.computedPosition.y }
        : { x: minX, y: minY },
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
      positions: snapshot
        .filter((s) => !isGroupNodeId(s.id))
        .map((s) => ({ node_id: Number(s.id), pos_x: s.x, pos_y: s.y })),
    };
  };

  /** Group the selected nodes/groups into a new container, nested under their common parent. */
  const groupSelectedNodes = async (): Promise<void> => {
    if (flowStore.flowId === null) return;
    const selected = getSelectedNodes.value.filter(
      (node) => node.type === CUSTOM_NODE_TYPE || node.type === GROUP_NODE_TYPE,
    );
    if (selected.length === 0) {
      ElMessage.info("Select one or more nodes to group.");
      return;
    }
    // Items nested inside another selected group ride along inside it; only "top-level"
    // selected items become direct children of the new group.
    const selectedIds = new Set(selected.map((node) => node.id));
    const topLevel = selected.filter(
      (node) => !(node.parentNode && selectedIds.has(node.parentNode)),
    );
    // Nest the new group under the parent shared by every top-level item (undefined => top level).
    const parents = new Set(topLevel.map((node) => node.parentNode));
    const commonParentVueId = parents.size === 1 ? [...parents][0] : undefined;
    const nodeIds = topLevel.filter((n) => n.type === CUSTOM_NODE_TYPE).map((n) => Number(n.id));
    const childGroupIds = topLevel
      .filter((n) => n.type === GROUP_NODE_TYPE)
      .map((n) => groupBackendId(n.id));
    const response = await FlowApi.createGroup(flowStore.flowId, {
      node_ids: nodeIds,
      name: "Group",
      parent_group_id: commonParentVueId ? groupBackendId(commonParentVueId) : null,
      child_group_ids: childGroupIds,
    });
    if (response.group) {
      const newVueId = groupNodeId(response.group.id);
      const depth = commonParentVueId ? vueGroupDepth(commonParentVueId) + 1 : 0;
      // Parent must exist before children reference it.
      addNodes([buildGroupNode(response.group, depth)]);
      if (commonParentVueId) {
        const parent = findNode(commonParentVueId);
        const px = parent ? parent.computedPosition.x : 0;
        const py = parent ? parent.computedPosition.y : 0;
        updateNode(newVueId, {
          parentNode: commonParentVueId,
          position: { x: response.group.x_position - px, y: response.group.y_position - py },
        });
      }
      attachNodesToGroup(
        newVueId,
        topLevel.map((node) => node.id),
      );
      // Tighten the box to the real rendered sizes and persist those bounds. createGroup
      // already recorded the undo entry, so fold these bounds into it (record_history: false).
      const fit = refitGroup(newVueId);
      if (fit) {
        await FlowApi.updateLayout(flowStore.flowId, {
          node_positions: [],
          group_bounds: [fit.bounds],
          record_history: false,
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
    const grandparentVueId = findNode(parentId)?.parentNode;
    const children = childNodesOf(parentId);
    if (grandparentVueId) {
      // Lift members and sub-groups up one level into the grandparent.
      attachNodesToGroup(
        grandparentVueId,
        children.map((child) => child.id),
      );
    } else {
      for (const child of children) detachNodeToAbsolute(child);
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
    if (!collapsed) removeGroupProxyEdges(groupId);
    setSubtreeHidden(parentId, collapsed);
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
      await nextTick(); // wait for the pill's handles to render before attaching proxies
      addGroupProxyEdges(parentId);
    } else {
      // After a refresh, members loaded hidden and were never measured — force a
      // re-measure before refit so the box accounts for their sizes.
      await nextTick();
      updateNodeInternals(childNodesOf(parentId).map((child) => child.id));
      await nextTick();
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
    // This group's box changed size; re-wrap its ancestors so a nested group stays inside.
    const ancestorVueId = findNode(parentId)?.parentNode;
    if (ancestorVueId) {
      await nextTick();
      updateNodeInternals([parentId]); // measure this group's new box before the parent refits
      await nextTick();
      // updateGroup already recorded the undo entry; fold the ancestor refit into it.
      await persistGroupChainRefit(ancestorVueId, false);
    }
  };

  /** Persist absolute positions for a set of nodes and/or bounds for a set of groups. */
  const persistLayout = async (nodes: GraphNode[], groups: GraphNode[] = []): Promise<void> => {
    if (flowStore.flowId === null) return;
    // Group nodes carry bounds; only custom nodes produce a numeric node position.
    const groupNodes: GraphNode[] = [...groups];
    const nodePositions: NodePositionUpdate[] = [];
    for (const node of nodes) {
      if (node.type === GROUP_NODE_TYPE) {
        groupNodes.push(node);
      } else {
        const abs = absolutePosition(node);
        nodePositions.push({ node_id: Number(node.id), pos_x: abs.x, pos_y: abs.y });
      }
    }
    const groupBounds: GroupBoundsUpdate[] = groupNodes.map((group) => ({
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

  /** Refit a group and every ancestor group (so a moved nested group re-wraps up the tree). */
  const persistGroupChainRefit = async (
    startGroupVueId: string,
    recordHistory = true,
  ): Promise<void> => {
    if (flowStore.flowId === null) return;
    const group_bounds: GroupBoundsUpdate[] = [];
    let node_positions: NodePositionUpdate[] = [];
    let current: string | undefined = startGroupVueId;
    const seen = new Set<string>();
    while (current && !seen.has(current)) {
      seen.add(current);
      const fit = refitGroup(current);
      if (fit) {
        group_bounds.push(fit.bounds);
        if (node_positions.length === 0) node_positions = fit.positions;
      }
      const parent: string | undefined = findNode(current)?.parentNode;
      if (parent) {
        // Measure this group's resized box before refitting its parent (dimensions update async).
        await nextTick();
        updateNodeInternals([current]);
        await nextTick();
      }
      current = parent;
    }
    if (group_bounds.length === 0 && node_positions.length === 0) return;
    const response = await FlowApi.updateLayout(flowStore.flowId, {
      node_positions,
      group_bounds,
      record_history: recordHistory,
    });
    flowStore.updateHistoryState(response.history);
  };

  /**
   * Persist a drag-end:
   * - group moved → its bounds + every child's new absolute position (and refit ancestors);
   * - grouped child moved → auto-fit the box and every ancestor box;
   * - free node moved → the dragged node, or all selected free nodes (multi-select).
   */
  const persistDrag = async (node: GraphNode): Promise<void> => {
    if (flowStore.flowId === null) return;
    if (node.type === GROUP_NODE_TYPE) {
      // Persist the whole subtree — children moved with the parent.
      await persistLayout(descendantsOf(node.id), [node]);
      // persistLayout already recorded the undo entry; fold the ancestor refit into it.
      if (node.parentNode) await persistGroupChainRefit(node.parentNode, false);
      return;
    }
    if (node.parentNode) {
      await persistGroupChainRefit(node.parentNode);
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
