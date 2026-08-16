import { ref } from "vue";

import type { GraphNode, NodeMouseEvent } from "@vue-flow/core";

import { NodeApi } from "../api";
import { useFlowStore } from "../stores/flow-store";
import type { NodeTemplate } from "../types/flow.types";
import { isNodeBufferArmed } from "./useFlowClipboard";

export type ContextMenuTargetType = "node" | "edge" | "pane" | "selection";

export interface ContextMenuTarget {
  type: ContextMenuTargetType;
  id: string;
}

/**
 * Canvas context-menu state and open/close handling, extracted from Canvas.vue
 * per its TODO(refactor) plan.
 *
 * Owns which target the menu is open for, where it opens, and the lazily
 * hydrated per-node facts the menu renders (cache flag, run_flow-ness,
 * running state). Action dispatch stays in Canvas.vue, which holds the
 * VueFlow instance and the flow-level composables.
 *
 * Hydration is deliberately slot-free: it calls NodeApi.getNodeData with
 * include_inputs=false and never writes nodeStore.nodeData. The store's
 * single cache slot belongs to the node whose settings drawer is open — a
 * right-click must not clobber it, nor trigger the expensive
 * include_inputs=true path, which resolves upstream schemas and can execute
 * un-run upstream nodes.
 */
export function useContextMenu(options: {
  /** Called when a node target opens, so the node can join the selection. */
  onNodeTargeted?: (node: GraphNode) => void;
  /** Whether the node is currently executing (drives the Run Now label). */
  isNodeRunning?: (nodeId: number) => boolean;
}) {
  const flowStore = useFlowStore();

  const showContextMenu = ref(false);
  const menuPosition = ref({ x: 0, y: 0 });
  const target = ref<ContextMenuTarget>({ type: "pane", id: "" });
  const targetInGroup = ref(false);
  // Snapshot of the node-copy buffer at open time, so the template doesn't hit
  // localStorage on every re-render.
  const canPasteNode = ref(false);
  const targetIsRunFlow = ref(false);
  const targetIsRunning = ref(false);
  // null = hydration in flight; the menu shows a neutral, disabled label.
  const targetCacheResults = ref<boolean | null>(null);

  // Bumped on every open/close so a stale hydration response can't write into
  // a menu that has since closed or re-opened for another node.
  let hydrateToken = 0;

  const isEditableTarget = (event: Event): boolean => {
    const el = event.target as HTMLElement | null;
    return Boolean(
      el?.closest?.('input, textarea, .cm-editor, [contenteditable]:not([contenteditable="false"])'),
    );
  };

  const openMenu = (t: ContextMenuTarget, x: number, y: number) => {
    hydrateToken++;
    canPasteNode.value = isNodeBufferArmed();
    menuPosition.value = { x, y };
    target.value = t;
    showContextMenu.value = true;
  };

  const openForPane = (event: Event) => {
    event.preventDefault();
    const pointerEvent = event as PointerEvent;
    targetInGroup.value = false;
    openMenu({ type: "pane", id: "" }, pointerEvent.x, pointerEvent.y);
  };

  const openForNode = ({ event, node }: NodeMouseEvent) => {
    // Editable fields inside a node (the description textarea) keep the
    // native menu so right-click paste still works there.
    if (isEditableTarget(event)) return;
    event.preventDefault();
    if (node.type === "group") return;
    options.onNodeTargeted?.(node);
    const mouseEvent = event as MouseEvent;
    const nodeId = Number(node.id);
    targetInGroup.value = Boolean(node.parentNode);
    targetIsRunFlow.value =
      (node.data as { nodeTemplate?: NodeTemplate } | undefined)?.nodeTemplate?.item === "run_flow";
    targetIsRunning.value = options.isNodeRunning?.(nodeId) ?? false;
    openMenu({ type: "node", id: node.id }, mouseEvent.clientX, mouseEvent.clientY);
    hydrateNodeFacts(nodeId);
  };

  const openForSelection = ({ event }: { event: MouseEvent }) => {
    event.preventDefault();
    targetInGroup.value = false;
    openMenu({ type: "selection", id: "" }, event.clientX, event.clientY);
  };

  const closeMenu = () => {
    hydrateToken++;
    showContextMenu.value = false;
  };

  /** Slot-free settings snapshot; see the hydration note above. */
  const fetchNodeSettingsSnapshot = (nodeId: number) =>
    NodeApi.getNodeData(flowStore.flowId, nodeId, false, false);

  const hydrateNodeFacts = (nodeId: number) => {
    targetCacheResults.value = null;
    if (!Number.isFinite(nodeId)) return;
    const myToken = hydrateToken;
    fetchNodeSettingsSnapshot(nodeId)
      .then((data) => {
        if (myToken !== hydrateToken) return;
        targetCacheResults.value = Boolean(data?.setting_input?.cache_results);
      })
      .catch(() => {
        // Menu keeps the neutral label; the toggle stays disabled.
      });
  };

  return {
    showContextMenu,
    menuPosition,
    target,
    targetInGroup,
    canPasteNode,
    targetIsRunFlow,
    targetIsRunning,
    targetCacheResults,
    openForPane,
    openForNode,
    openForSelection,
    closeMenu,
    fetchNodeSettingsSnapshot,
  };
}
