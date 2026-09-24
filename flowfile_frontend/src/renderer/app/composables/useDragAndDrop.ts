// composables/useDragAndDrop.ts
//
// TODO(refactor): ~867 LOC, doing 5 jobs. Plan to split into:
//   - useNodeComponentLoader: dynamic imports + caching (~lines 148-227)
//   - useDragMechanics: drag start/over/end (~lines 298-334)
//   - useNodeCopy: single & multi-node copy logic (~lines 336-382, 637-776)
//   - useClipboardPaste: tabular data parsing (~lines 782-848)
//   - useEdgeInsertion: splice node onto edge (~lines 557-631)
// Keep this file as a thin façade re-exporting all five.
import { useVueFlow, Node, Position, Edge } from "@vue-flow/core";
import { ref, watch, markRaw, nextTick } from "vue";
import type {
  NodeTemplate,
  NodeInput,
  NodeHandle,
  VueFlowInput,
  NodeCopyInput,
  NodeCopyValue,
  NodePromise,
  MultiNodeCopyValue,
  GraphOperation,
} from "../types";
import { FlowApi } from "../api";
import { plural } from "../utils/text";
import { recoverFromFailedMutation } from "../services/mutationFailure";
import {
  addConfiguredNodeOperations,
  connection,
  insertOnEdgeOperation,
  isBackendEdge,
} from "../utils/graphOperations";
import { buildCommentNode } from "./useCanvasComments";
import {
  buildGroupNode,
  groupNodeId,
  useNodeGroups,
  GROUP_PROXY_EDGE_PREFIX,
} from "./useNodeGroups";
import { fetchNodeTemplates } from "./useNodes";
import { useEditorStore } from "../stores/editor-store";
import { useTutorialStore } from "../stores/tutorial-store";
import { parseTabularText, inferColumnDataType } from "../utils/clipboardUtils";
import { DEFAULT_OUTPUT_HANDLE, outputHandle } from "../utils/outputHandle";
import { deriveHandles } from "../utils/nodeHandles";
import {
  findAutoConnectMatch,
  preferUnusedOutputs,
  type AutoConnectDrop,
  type AutoConnectMatch,
  type AutoConnectNode,
} from "../utils/autoConnect";
import { desktop } from "../../lib/desktop";

const EDGE_DROP_CLASS = "edge-drop-target";
let hoveredEdgeId: string | null = null;

// Edges core already deleted; filled only for the synchronous span of removeCommittedEdges.
const committedEdgeRemovals = new Set<string>();

/** True while Canvas sees the removal of an edge core already deleted (so it sends nothing). */
export const isCommittedEdgeRemoval = (edgeId: string): boolean =>
  committedEdgeRemovals.has(edgeId);

/** Remove edges core has already deleted from the canvas, without deleting them again. */
export function removeCommittedEdges(removeEdges: (ids: string[]) => void, ids: string[]): void {
  ids.forEach((id) => committedEdgeRemovals.add(id));
  try {
    removeEdges(ids);
  } finally {
    ids.forEach((id) => committedEdgeRemovals.delete(id));
  }
}

function markHoveredEdge(nextId: string | null) {
  if (hoveredEdgeId === nextId) return;
  if (hoveredEdgeId) {
    document
      .querySelector(`.vue-flow__edge[data-id="${CSS.escape(hoveredEdgeId)}"]`)
      ?.classList.remove(EDGE_DROP_CLASS);
  }
  if (nextId) {
    document
      .querySelector(`.vue-flow__edge[data-id="${CSS.escape(nextId)}"]`)
      ?.classList.add(EDGE_DROP_CLASS);
  }
  hoveredEdgeId = nextId;
}

// Palette drag in snapping range of an existing node: the drop will auto-connect.
const AUTO_CONNECT_CLASS = "auto-connect-target";
let autoConnectNodeId: string | null = null;
let draggedTemplate: NodeTemplate | null = null;

function markAutoConnectNode(nextId: string | null) {
  if (autoConnectNodeId === nextId) return;
  if (autoConnectNodeId) {
    document
      .querySelector(`.vue-flow__node[data-id="${CSS.escape(autoConnectNodeId)}"]`)
      ?.classList.remove(AUTO_CONNECT_CLASS);
  }
  if (nextId) {
    document
      .querySelector(`.vue-flow__node[data-id="${CSS.escape(nextId)}"]`)
      ?.classList.add(AUTO_CONNECT_CLASS);
  }
  autoConnectNodeId = nextId;
}

function detectEdgeUnderPointer(clientX: number, clientY: number): string | null {
  // elementsFromPoint (plural) returns the full z-stack, so we can find an
  // edge even when a dragged node is painted on top and obscures it.
  const stack = document.elementsFromPoint(clientX, clientY);
  for (const el of stack) {
    const id = (el as Element).closest(".vue-flow__edge")?.getAttribute("data-id");
    // A collapsed group's proxy edge has no core connection to splice into.
    if (id && !id.startsWith(GROUP_PROXY_EDGE_PREFIX)) return id;
  }
  return null;
}

// Floating panels and the minimap sit inside the canvas drop zone.
const CANVAS_PANEL_SELECTOR = "[data-canvas-overlay], .vue-flow__panel";

/** A node released here would land on the canvas hidden underneath the panel. */
function isOverCanvasPanel(event: DragEvent): boolean {
  return !!(event.target as Element | null)?.closest?.(CANVAS_PANEL_SELECTOR);
}

/** A gesture's backend ops plus the canvas update to apply once they are committed. */
export interface GesturePlan {
  operations: GraphOperation[];
  apply: () => void;
}

// Dynamic component imports using import.meta.glob for Vite compatibility
// This creates a map of all node components that can be dynamically loaded
const nodeModules = import.meta.glob("../components/nodes/node-types/elements/**/*.vue");

// Validate that parsed JSON data is a valid NodeTemplate
// This prevents unvalidated dynamic method calls from untrusted data
function isValidNodeTemplate(data: unknown): data is NodeTemplate {
  if (typeof data !== "object" || data === null) return false;
  const obj = data as Record<string, unknown>;
  return (
    typeof obj.name === "string" &&
    typeof obj.item === "string" &&
    typeof obj.input === "number" &&
    typeof obj.output === "number" &&
    typeof obj.custom_node === "boolean"
  );
}

let id = 0;

export function getId(): number {
  return ++id;
}

const state = {
  draggedType: ref<string | null>(null),
  isDragOver: ref(false),
  isDragging: ref(false),
};

function toTitleCase(str: string): string {
  return str
    .split("_")
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join("");
}

function toCamelCase(str: string): string {
  const parts = str.split("_");
  return (
    parts[0].toLowerCase() +
    parts
      .slice(1)
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
      .join("")
  );
}

// Validate that a string only contains safe characters for module paths
function isValidModuleName(name: string): boolean {
  return /^[a-zA-Z][a-zA-Z0-9]*$/.test(name);
}

// Component cache to avoid re-importing
const componentCache: Map<string, Promise<any>> = new Map();

/**
 * Gets a specific node template by item name
 */
export async function getNodeTemplateByItem(item: string): Promise<NodeTemplate | undefined> {
  try {
    const allNodes = await fetchNodeTemplates();
    return allNodes.find((node) => node.item === item);
  } catch (error) {
    console.error("Failed to get node template for item:", item, error);
    return undefined;
  }
}

function missingNodeTemplate(node: NodeInput): NodeTemplate {
  return {
    name: node.name || node.item,
    color: node.color || "#9ca3af",
    item: node.item,
    input: node.input ?? node.input_names?.length ?? 1,
    output: node.output ?? node.output_names?.length ?? 1,
    image: node.image || "user-defined-icon.png",
    multi: node.multi ?? true,
    node_group: node.node_group || "custom",
    prod_ready: node.prod_ready ?? true,
    drawer_title: node.drawer_title || node.item,
    drawer_intro: node.drawer_intro || "This node type is not available on this machine.",
    custom_node: true,
    output_names: node.output_names,
    dynamic_inputs: node.dynamic_inputs,
  };
}

/**
 * Gets a Vue component for a node
 */
export async function getComponent(node: NodeTemplate | string): Promise<any> {
  const nodeItem = typeof node === "string" ? node : node.item;

  if (componentCache.has(nodeItem)) {
    return componentCache.get(nodeItem)!;
  }

  const nodeTemplate = typeof node === "string" ? await getNodeTemplateByItem(node) : node;

  if (!nodeTemplate) {
    throw new Error(`Node template not found for item: ${nodeItem}`);
  }

  const formattedItemName = toTitleCase(nodeTemplate.item);
  const dirName = toCamelCase(nodeTemplate.item);

  // Use CustomNode for nodes marked as custom_node, otherwise use specific component
  const modulePath = nodeTemplate.custom_node
    ? "../components/nodes/node-types/elements/customNode/CustomNode.vue"
    : `../components/nodes/node-types/elements/${dirName}/${formattedItemName}.vue`;

  // Validate module names to prevent path traversal (only needed for non-custom nodes)
  if (
    !nodeTemplate.custom_node &&
    (!isValidModuleName(formattedItemName) || !isValidModuleName(dirName))
  ) {
    throw new Error(`Invalid module name: ${formattedItemName}`);
  }

  // Use Object.hasOwn to safely check module exists (prevents prototype pollution)
  if (!Object.hasOwn(nodeModules, modulePath)) {
    const error = new Error(`Component not found: ${formattedItemName} at ${modulePath}`);
    console.error("Failed to load component:", formattedItemName, error);
    console.log("Available modules:", Object.keys(nodeModules));
    throw error;
  }

  // Safe to access after hasOwn check
  const moduleLoader = nodeModules[modulePath];

  if (typeof moduleLoader !== "function") {
    const error = new Error(`Invalid module loader for: ${formattedItemName}`);
    console.error("Failed to load component:", formattedItemName, error);
    throw error;
  }

  const componentPromise = moduleLoader()
    .then((module: any) => {
      const component = markRaw(module.default);
      return component;
    })
    .catch((error) => {
      console.error("Failed to load component:", formattedItemName, error);
      componentCache.delete(nodeItem);
      throw error;
    });

  componentCache.set(nodeItem, componentPromise);
  return componentPromise;
}

async function getComponentRaw(item: string): Promise<any> {
  const nodeTemplate = await getNodeTemplateByItem(item);

  if (!nodeTemplate) {
    throw new Error(`Node template not found for item: ${item}`);
  }

  const formattedItemName = toTitleCase(nodeTemplate.item);
  const dirName = toCamelCase(nodeTemplate.item);

  // Use CustomNode for nodes marked as custom_node, otherwise use specific component
  const modulePath = nodeTemplate.custom_node
    ? "../components/nodes/node-types/elements/customNode/CustomNode.vue"
    : `../components/nodes/node-types/elements/${dirName}/${formattedItemName}.vue`;

  console.log("Loading component:", formattedItemName, "custom_node:", nodeTemplate.custom_node);

  // Validate module names to prevent path traversal (only needed for non-custom nodes)
  if (
    !nodeTemplate.custom_node &&
    (!isValidModuleName(formattedItemName) || !isValidModuleName(dirName))
  ) {
    throw new Error(`Invalid module name: ${formattedItemName}`);
  }

  // Use Object.hasOwn to safely check module exists (prevents prototype pollution)
  if (!Object.hasOwn(nodeModules, modulePath)) {
    const error = new Error(`Component not found: ${formattedItemName} at ${modulePath}`);
    console.error("Failed to load component:", formattedItemName, error);
    console.log("Available modules:", Object.keys(nodeModules));
    throw error;
  }

  // Safe to access after hasOwn check
  const moduleLoader = nodeModules[modulePath];

  if (typeof moduleLoader !== "function") {
    const error = new Error(`Invalid module loader for: ${formattedItemName}`);
    console.error("Failed to load component:", formattedItemName, error);
    throw error;
  }

  return moduleLoader()
    .then((module: any) => markRaw(module.default))
    .catch((error) => {
      console.error("Failed to load component:", formattedItemName, error);
      throw error;
    });
}

export default function useDragAndDrop() {
  const { draggedType, isDragOver, isDragging } = state;

  const {
    addNodes,
    screenToFlowCoordinate,
    addEdges,
    removeEdges,
    findEdge,
    fromObject,
    getNodes,
    getEdges,
    viewport,
  } = useVueFlow();

  const { addGroupProxyEdges } = useNodeGroups();

  watch(isDragging, (dragging) => {
    document.body.style.userSelect = dragging ? "none" : "";
  });

  // WebKit (Tauri's WKWebView, Safari) renders no default drag preview for
  // elements with user-select: none (.node-item has it), so the palette drag
  // shows nothing in the desktop app. Snapshot a selectable clone instead —
  // explicit and identical across engines.
  function setPaletteDragImage(event: DragEvent) {
    const source = (event.target as HTMLElement | null)?.closest?.(".node-item");
    if (!(source instanceof HTMLElement) || !event.dataTransfer) return;
    const rect = source.getBoundingClientRect();
    const ghost = source.cloneNode(true) as HTMLElement;
    ghost.style.position = "absolute";
    ghost.style.top = "-1000px";
    ghost.style.left = "-1000px";
    ghost.style.width = `${rect.width}px`;
    ghost.style.margin = "0";
    ghost.style.pointerEvents = "none";
    ghost.style.backgroundColor = "var(--color-background-primary)";
    ghost.style.userSelect = "auto";
    ghost.style.webkitUserSelect = "auto";
    document.body.appendChild(ghost);
    event.dataTransfer.setDragImage(ghost, event.clientX - rect.left, event.clientY - rect.top);
    // The engine captures the image after the dragstart task completes.
    setTimeout(() => ghost.remove(), 0);
  }

  function onDragStart(event: DragEvent, nodeTemplate: NodeTemplate) {
    if (event.dataTransfer) {
      event.dataTransfer.setData("application/vueflow", JSON.stringify(nodeTemplate));
      event.dataTransfer.effectAllowed = "move";
      setPaletteDragImage(event);
    }

    draggedType.value = nodeTemplate.item;
    draggedTemplate = nodeTemplate;
    resetAutoConnectCandidates();
    isDragging.value = true;

    document.addEventListener("drop", onDragEnd);
    // A refused drop (over a panel, outside the window, Esc) fires no drop event.
    document.addEventListener("dragend", onDragEnd);
  }

  function onDragOver(event: DragEvent) {
    // Leaving dragover uncancelled makes the browser refuse the drop over a panel.
    if (draggedType.value && isOverCanvasPanel(event)) {
      isDragOver.value = false;
      markHoveredEdge(null);
      markAutoConnectNode(null);
      return;
    }
    event.preventDefault();

    if (draggedType.value) {
      isDragOver.value = true;

      if (event.dataTransfer) {
        event.dataTransfer.dropEffect = "move";
      }
      markHoveredEdge(detectEdgeUnderPointer(event.clientX, event.clientY));
      // Edge splice takes priority over snapping to a neighbour.
      let candidate: string | null = null;
      if (!hoveredEdgeId && draggedTemplate) {
        const position = screenToFlowCoordinate({ x: event.clientX, y: event.clientY });
        candidate = detectAutoConnect(draggedTemplate, position.x, position.y)?.nodeId ?? null;
      }
      markAutoConnectNode(candidate);
    }
  }

  function onDragLeave() {
    isDragOver.value = false;
    markHoveredEdge(null);
    markAutoConnectNode(null);
  }

  function onDragEnd() {
    isDragging.value = false;
    isDragOver.value = false;
    draggedType.value = null;
    draggedTemplate = null;
    markHoveredEdge(null);
    markAutoConnectNode(null);
    resetAutoConnectCandidates();
    document.removeEventListener("drop", onDragEnd);
    document.removeEventListener("dragend", onDragEnd);
  }

  // A node's rectangle in flow coordinates, read from the DOM like the edge
  // hit-test: this composable's VueFlow handle never sees measured dimensions.
  function measureNode(id: string): Pick<AutoConnectNode, "x" | "y" | "width" | "height"> | null {
    const el = document.querySelector(`.vue-flow__node[data-id="${CSS.escape(id)}"]`);
    const rect = el?.getBoundingClientRect();
    if (!rect?.width) return null;
    const zoom = viewport.value.zoom || 1;
    const { x, y } = screenToFlowCoordinate({ x: rect.left, y: rect.top });
    return { x, y, width: rect.width / zoom, height: rect.height / zoom };
  }

  // Existing nodes don't move during a drag, so they are measured once per
  // drag: reset at drag start/end, filled lazily on the first tick.
  let autoConnectCache: AutoConnectNode[] | null = null;

  function resetAutoConnectCandidates() {
    autoConnectCache = null;
  }

  function autoConnectCandidates(): AutoConnectNode[] {
    if (!autoConnectCache) autoConnectCache = collectAutoConnectCandidates();
    return autoConnectCache;
  }

  // Existing canvas nodes as auto-connect candidates: rendered, visible data
  // nodes with the side input handles that are still free.
  function collectAutoConnectCandidates(): AutoConnectNode[] {
    const edges = getEdges.value;
    return getNodes.value.flatMap((node) => {
      if (node.type !== "custom-node" || node.hidden) return [];
      const rect = measureNode(node.id);
      if (!rect) return [];
      const data = node.data as {
        inputs?: NodeHandle[];
        outputs?: NodeHandle[];
        nodeTemplate?: NodeTemplate;
      };
      // A multi node's single handle accepts any number of sources.
      const multi = Boolean(data.nodeTemplate?.multi && !data.nodeTemplate?.dynamic_inputs);
      const occupied = new Set(
        edges.filter((e) => e.target === node.id).map((e) => e.targetHandle ?? "input-0"),
      );
      const used = new Set(
        edges
          .filter((e) => e.source === node.id)
          .map((e) => e.sourceHandle ?? DEFAULT_OUTPUT_HANDLE),
      );
      return [
        {
          id: node.id,
          ...rect,
          freeInputs: (data.inputs ?? [])
            .filter((h) => h.position === Position.Left && (multi || !occupied.has(h.id)))
            .map((h) => h.id),
          outputs: preferUnusedOutputs(
            (data.outputs ?? []).map((h) => h.id),
            used,
          ),
        },
      ];
    });
  }

  // Dynamic-input nodes never auto-connect: their input-0 is the parameter handle.
  function autoConnectDrop(template: NodeTemplate, x: number, y: number): AutoConnectDrop | null {
    if (template.dynamic_inputs) return null;
    const { inputs, outputs } = deriveHandles(template);
    return {
      x,
      y,
      inputHandle: inputs.find((h) => h.position === Position.Left)?.id ?? null,
      outputHandle: outputs[0]?.id ?? null,
    };
  }

  function detectAutoConnect(
    template: NodeTemplate,
    x: number,
    y: number,
  ): AutoConnectMatch | null {
    const drop = autoConnectDrop(template, x, y);
    return drop ? findAutoConnectMatch(drop, autoConnectCandidates()) : null;
  }

  // Same rule for an existing node being dragged, using its real size and
  // handles; the node itself is never its own candidate.
  function detectAutoConnectForNode(nodeId: string): AutoConnectMatch | null {
    const node = getNodes.value.find((n) => n.id === nodeId);
    const data = node?.data as
      | { inputs?: NodeHandle[]; outputs?: NodeHandle[]; nodeTemplate?: NodeTemplate }
      | undefined;
    const rect = measureNode(nodeId);
    if (!node || !rect || !data?.nodeTemplate || data.nodeTemplate.dynamic_inputs) return null;
    const drop: AutoConnectDrop = {
      ...rect,
      inputHandle: (data.inputs ?? []).find((h) => h.position === Position.Left)?.id ?? null,
      outputHandle: data.outputs?.[0]?.id ?? null,
    };
    return findAutoConnectMatch(
      drop,
      autoConnectCandidates().filter((c) => c.id !== nodeId),
    );
  }

  /**
   * The ops that wire a dropped or dragged node to the neighbour picked by
   * findAutoConnectMatch, and the canvas edge to show once they are committed.
   */
  function planAutoConnect(newNodeId: number, match: AutoConnectMatch): GesturePlan {
    const existingId = parseInt(match.nodeId, 10);
    const upstream = match.direction === "upstream";
    const sourceId = upstream ? existingId : newNodeId;
    const sourceHandle = upstream ? match.existingHandle : match.newHandle;
    const targetId = upstream ? newNodeId : existingId;
    const targetHandle = upstream ? match.newHandle : match.existingHandle;
    return {
      operations: [
        { op: "connect", connection: connection(sourceId, sourceHandle, targetId, targetHandle) },
      ],
      apply: () => {
        addEdges([
          {
            id: `e${sourceId}-${targetId}-${sourceHandle}-${targetHandle}`,
            source: String(sourceId),
            target: String(targetId),
            sourceHandle,
            targetHandle,
          },
        ]);
        useTutorialStore().notify({ type: "edge-connected", sourceId, targetId });
      },
    };
  }

  /**
   * The op that splices a node into an existing edge (A -> B becomes A -> new -> B, B's
   * input keeping its position), and the canvas edges to show once it is committed. Null
   * when the edge is gone.
   */
  function planEdgeSplice(newNodeId: number, edgeId: string): GesturePlan | null {
    const edge = findEdge(edgeId);
    if (!edge || !isBackendEdge(edge)) return null;

    const sourceId = parseInt(edge.source, 10);
    const targetId = parseInt(edge.target, 10);
    const sourceHandle = edge.sourceHandle ?? DEFAULT_OUTPUT_HANDLE;
    const targetHandle = edge.targetHandle ?? "input-0";
    const newOutputHandle = outputHandle(0);
    const newInputHandle = "input-0";

    return {
      operations: [
        insertOnEdgeOperation(newNodeId, {
          source: edge.source,
          target: edge.target,
          sourceHandle,
          targetHandle,
        }),
      ],
      apply: () => {
        removeCommittedEdges(removeEdges, [edge.id]);
        addEdges([
          {
            id: `e${sourceId}-${newNodeId}-${sourceHandle}-${newInputHandle}`,
            source: String(sourceId),
            target: String(newNodeId),
            sourceHandle,
            targetHandle: newInputHandle,
            ...(edge.label ? { label: edge.label } : {}),
          },
          {
            id: `e${newNodeId}-${targetId}-${newOutputHandle}-${targetHandle}`,
            source: String(newNodeId),
            target: String(targetId),
            sourceHandle: newOutputHandle,
            targetHandle,
          },
        ]);
        const tutorialStore = useTutorialStore();
        tutorialStore.notify({ type: "edge-connected", sourceId, targetId: newNodeId });
        tutorialStore.notify({ type: "edge-connected", sourceId: newNodeId, targetId });
      },
    };
  }

  /**
   * Paste one copied node. The request goes out before anything is awaited, so the paste
   * keeps its place among the user's edits; the canvas shows the node once core has it.
   */
  async function createCopyNode(node: NodeCopyInput): Promise<boolean> {
    const nodeId: number = getId();
    const nodePromise: NodePromise = {
      node_id: nodeId,
      flow_id: node.flowId,
      node_type: node.typeSnakeCase,
      pos_x: node.posX,
      pos_y: node.posY,
      cache_results: true,
    };
    const copied = FlowApi.copyNode(node.nodeIdToCopyFrom, node.flowIdToCopyFrom, nodePromise);
    let newNode: Node;
    try {
      [newNode] = await Promise.all([
        buildCopiedNode(node, nodeId, { x: node.posX, y: node.posY }),
        copied,
      ]);
    } catch (error) {
      recoverFromFailedMutation(error, "Failed to paste node");
      return false;
    }
    addNodes(newNode);
    return true;
  }

  /** A pasted node's canvas node; handle snapshots from copy time win over the template's counts. */
  async function buildCopiedNode(
    node: NodeCopyValue,
    newNodeId: number,
    position: { x: number; y: number },
  ): Promise<Node> {
    const component = await getComponentRaw(node.type);
    const derived = deriveHandles({
      input: node.numberOfInputs,
      output: node.numberOfOutputs,
      dynamic_inputs: node.nodeTemplate?.dynamic_inputs,
      output_names: node.nodeTemplate?.output_names,
      input_labels: node.nodeTemplate?.input_labels,
    });
    return {
      id: String(newNodeId),
      type: "custom-node",
      position,
      data: {
        id: newNodeId,
        label: node.label,
        component: markRaw(component),
        inputs: node.inputHandles ?? derived.inputs,
        outputs: node.outputHandles ?? derived.outputs,
        nodeTemplate: node.nodeTemplate,
      },
    };
  }

  const getMaxDataId = (nodes: NodeInput[]): number => {
    return nodes.reduce((maxId, node) => {
      return node.id > maxId ? node.id : maxId;
    }, 0);
  };

  async function getNodeToAdd(node: NodeInput): Promise<Node> {
    let nodeTemplate = await getNodeTemplateByItem(node.item);
    let component: any;
    try {
      component = await getComponent(nodeTemplate || node.item);
    } catch (error) {
      // Unavailable node type: show the not-installed placeholder so the rest of the flow still loads.
      console.warn(
        `Node type "${node.item}" is unavailable; rendering a not-installed placeholder.`,
        error,
      );
      nodeTemplate = missingNodeTemplate(node);
      component = await getComponent(nodeTemplate);
    }

    // The per-instance NodeInput carries input_names/output_names; the template
    // flag covers dynamic nodes saved before a subflow was picked.
    const { inputs, outputs } = deriveHandles({
      ...node,
      dynamic_inputs: node.dynamic_inputs || nodeTemplate?.dynamic_inputs,
    });

    const newNode: Node = {
      id: String(node.id),
      type: "custom-node",
      position: {
        x: node.pos_x,
        y: node.pos_y,
      },
      data: {
        id: node.id,
        label: node.name,
        component: markRaw(component),
        nodeReference: node.node_reference,
        inputs,
        outputs,
        nodeTemplate: nodeTemplate,
      },
    };
    return newNode;
  }

  async function createEmptyFlow() {
    const emptyFlow = {
      nodes: [],
      edges: [],
      position: [0, 0] as [number, number],
      zoom: 1,
      viewport: { x: 0, y: 0, zoom: 1 },
    };
    await fromObject(emptyFlow);
    await nextTick();
  }

  async function importFlow(flowData: VueFlowInput) {
    await createEmptyFlow();
    const childNodes = await Promise.all(flowData.node_inputs.map((node) => getNodeToAdd(node)));

    // Build group container nodes, then reparent member nodes — converting each
    // member's absolute position to be relative to its group origin (VueFlow stores
    // child positions relative to the parent).
    const groups = flowData.groups ?? [];
    const groupById = new Map(groups.map((group) => [group.id, group]));
    const groupDepth = (group: (typeof groups)[number]): number => {
      let depth = 0;
      let current: (typeof groups)[number] | undefined = group;
      const seen = new Set<number>();
      while (current?.parent_group_id != null && !seen.has(current.id)) {
        seen.add(current.id);
        depth += 1;
        current = groupById.get(current.parent_group_id);
      }
      return depth;
    };
    // Hidden if the group itself or any ancestor is collapsed.
    const anyCollapsedFrom = (groupId: number): boolean => {
      let current = groupById.get(groupId);
      const seen = new Set<number>();
      while (current && !seen.has(current.id)) {
        if (current.collapsed) return true;
        seen.add(current.id);
        current =
          current.parent_group_id != null ? groupById.get(current.parent_group_id) : undefined;
      }
      return false;
    };

    // Parents before children so a nested group's parent exists first; nested groups get a
    // parent-relative position and a depth-based z-index (deeper above shallower, below nodes).
    const groupNodes: Node[] = [...groups]
      .sort((a, b) => groupDepth(a) - groupDepth(b))
      .map((group) => {
        const node = buildGroupNode(group, groupDepth(group));
        if (group.parent_group_id != null) {
          const parent = groupById.get(group.parent_group_id);
          if (parent) {
            node.parentNode = groupNodeId(group.parent_group_id);
            node.position = {
              x: group.x_position - parent.x_position,
              y: group.y_position - parent.y_position,
            };
          }
          if (anyCollapsedFrom(group.parent_group_id)) node.hidden = true;
        }
        return node;
      });
    flowData.node_inputs.forEach((input, index) => {
      if (input.group_id == null) return;
      const group = groupById.get(input.group_id);
      if (!group) return;
      childNodes[index].parentNode = groupNodeId(group.id);
      childNodes[index].position = {
        x: input.pos_x - group.x_position,
        y: input.pos_y - group.y_position,
      };
      if (anyCollapsedFrom(group.id)) childNodes[index].hidden = true;
    });

    const commentNodes: Node[] = (flowData.comments ?? []).map((comment) =>
      buildCommentNode(comment),
    );
    // Groups first so a parent exists before its children reference it.
    addNodes([...groupNodes, ...childNodes, ...commentNodes]);
    // Never lower the counter: an undone node's id may still be referenced by a redo.
    id = Math.max(id, getMaxDataId(flowData.node_inputs));

    // Add labels to edges from source node output handles, node_reference, or df_{nodeId} default
    const editorStore = useEditorStore();
    const edgesWithLabels = flowData.node_edges.map((edge) => {
      if (!editorStore.showEdgeLabels) return edge;
      const sourceNode = childNodes.find((n) => n.id === edge.source);
      if (sourceNode?.data?.outputs) {
        const output = (sourceNode.data.outputs as NodeHandle[]).find(
          (o) => o.id === edge.sourceHandle,
        );
        if (output?.label) {
          return { ...edge, label: output.label };
        }
      }
      if (sourceNode?.data?.nodeReference) {
        return { ...edge, label: sourceNode.data.nodeReference };
      }
      return { ...edge, label: `df_${sourceNode?.data?.id ?? edge.source}` };
    });

    addEdges(edgesWithLabels);

    // Re-create proxy edges for groups that load collapsed and are not themselves hidden
    // inside a collapsed ancestor.
    const collapsedGroups = groups.filter(
      (group) =>
        group.collapsed &&
        (group.parent_group_id == null || !anyCollapsedFrom(group.parent_group_id)),
    );
    if (collapsedGroups.length > 0) {
      await nextTick();
      for (const group of collapsedGroups) {
        addGroupProxyEdges(groupNodeId(group.id));
      }
    }
  }

  async function onDrop(event: DragEvent, flowId: number): Promise<void> {
    if (isOverCanvasPanel(event)) return;
    const position = screenToFlowCoordinate({
      x: event.clientX,
      y: event.clientY,
    });
    if (!event.dataTransfer) return;

    // Parse and validate the drag data to prevent unvalidated dynamic method calls
    const rawData = event.dataTransfer.getData("application/vueflow");
    if (!rawData) return;

    let parsedData: unknown;
    try {
      parsedData = JSON.parse(rawData);
    } catch {
      console.error("Invalid JSON in drag data");
      return;
    }

    if (!isValidNodeTemplate(parsedData)) {
      console.error("Invalid node template data in drag event");
      return;
    }

    const nodeData: NodeTemplate = parsedData;
    const nodeId = getId();

    // Snapshot which edge (if any) the user was hovering at drop time, then clear
    // the hover cue — we'll consume the snapshot below.
    const droppedOnEdgeId = hoveredEdgeId;
    markHoveredEdge(null);
    // Resolve the neighbour now, before the new node joins the candidate pool.
    const autoConnectMatch = droppedOnEdgeId
      ? null
      : detectAutoConnect(nodeData, position.x, position.y);
    markAutoConnectNode(null);
    resetAutoConnectCandidates();

    // Core stores integer positions, so the canvas shows the node where core puts it.
    const placed = { x: Math.round(position.x), y: Math.round(position.y) };

    // A `multi` node has one input handle whatever its backend input count; dynamic-input
    // nodes never splice (their input-0 is the parameter handle).
    const effectiveInputCount = nodeData.multi ? 1 : nodeData.input;
    const splice =
      droppedOnEdgeId &&
      !nodeData.dynamic_inputs &&
      effectiveInputCount === 1 &&
      nodeData.output >= 1
        ? planEdgeSplice(nodeId, droppedOnEdgeId)
        : null;
    const plan = splice ?? (autoConnectMatch ? planAutoConnect(nodeId, autoConnectMatch) : null);

    // Sent before any await so the drop keeps its place; the canvas shows it once core has it.
    const added = plan
      ? FlowApi.applyOperations(flowId, `${splice ? "Insert" : "Add"} ${nodeData.item} node`, [
          {
            op: "add_node",
            node_id: nodeId,
            node_type: nodeData.item,
            pos_x: placed.x,
            pos_y: placed.y,
          },
          ...plan.operations,
        ])
      : FlowApi.insertNode(flowId, nodeId, nodeData.item, placed.x, placed.y);
    let component: any;
    try {
      [component] = await Promise.all([getComponent(nodeData), added]);
    } catch (error) {
      recoverFromFailedMutation(error, `Could not add ${nodeData.name}`);
      return;
    }
    const { inputs, outputs } = deriveHandles(nodeData);
    addNodes({
      id: String(nodeId),
      type: "custom-node",
      position: placed,
      data: {
        id: nodeId,
        label: nodeData.name,
        component: markRaw(component),
        inputs,
        outputs,
        nodeTemplate: nodeData,
      },
    });
    useTutorialStore().notify({ type: "node-added", nodeItem: nodeData.item, nodeId });
    if (plan) {
      await nextTick();
      plan.apply();
    }
  }

  /** Paste several copied nodes plus the edges between them as one step. */
  async function createMultiCopyNodes(
    multiCopyValue: MultiNodeCopyValue,
    baseX: number,
    baseY: number,
    flowId: number,
  ): Promise<boolean> {
    const nodeIdMapping: Map<number, number> = new Map();
    const nodeInfos = multiCopyValue.nodes.map((node, i) => {
      const newNodeId = getId();
      nodeIdMapping.set(node.nodeIdToCopyFrom, newNodeId);
      return {
        node,
        newNodeId,
        offsetX: Math.round(baseX + (node.relativeX ?? (i % 3) * 200)),
        offsetY: Math.round(baseY + (node.relativeY ?? Math.floor(i / 3) * 150)),
      };
    });

    const operations: GraphOperation[] = nodeInfos.map(
      ({ node, newNodeId, offsetX, offsetY }): GraphOperation => ({
        op: "copy_node",
        node_id_to_copy_from: node.nodeIdToCopyFrom,
        flow_id_to_copy_from: multiCopyValue.flowIdToCopyFrom,
        node_promise: {
          node_id: newNodeId,
          flow_id: flowId,
          node_type: node.typeSnakeCase,
          pos_x: offsetX,
          pos_y: offsetY,
          cache_results: true,
        },
      }),
    );
    const newEdges: Edge[] = [];
    for (const edge of multiCopyValue.edges) {
      const newSourceId = nodeIdMapping.get(edge.sourceNodeId);
      const newTargetId = nodeIdMapping.get(edge.targetNodeId);
      if (newSourceId === undefined || newTargetId === undefined) continue;
      const sourceNodeInfo = multiCopyValue.nodes.find(
        (n) => n.nodeIdToCopyFrom === edge.sourceNodeId,
      );
      const outputIndex = parseInt(edge.sourceHandle.replace("output-", ""), 10);
      const snapshotHandles = sourceNodeInfo?.outputHandles;
      const snapshotLabel =
        snapshotHandles && snapshotHandles.length > 1
          ? snapshotHandles[outputIndex]?.label
          : undefined;
      const outputLabel =
        snapshotLabel ??
        (sourceNodeInfo?.nodeTemplate?.output_names &&
        sourceNodeInfo.nodeTemplate.output_names.length > 1
          ? sourceNodeInfo.nodeTemplate.output_names[outputIndex]
          : undefined);
      newEdges.push({
        id: `e${newSourceId}-${newTargetId}-${edge.sourceHandle}-${edge.targetHandle}`,
        source: String(newSourceId),
        target: String(newTargetId),
        sourceHandle: edge.sourceHandle,
        targetHandle: edge.targetHandle,
        ...(outputLabel ? { label: outputLabel } : {}),
      });
      operations.push({
        op: "connect",
        connection: connection(newSourceId, edge.sourceHandle, newTargetId, edge.targetHandle),
      });
    }

    // Sent before anything is awaited; the canvas shows the nodes once core has them.
    const pasted = FlowApi.applyOperations(
      flowId,
      `Paste ${plural(nodeInfos.length, "node")}`,
      operations,
    );
    const building = Promise.all(
      nodeInfos.map(({ node, newNodeId, offsetX, offsetY }) =>
        buildCopiedNode(node, newNodeId, { x: offsetX, y: offsetY }),
      ),
    );
    let newNodes: Node[];
    try {
      [newNodes] = await Promise.all([building, pasted]);
    } catch (error) {
      recoverFromFailedMutation(error, "Failed to paste nodes");
      return false;
    }
    addNodes(newNodes);
    await nextTick();
    addEdges(newEdges);
    return true;
  }

  /**
   * Creates a manual_input node from clipboard tabular data pasted on the canvas.
   * Returns true when a node was added.
   */
  async function createManualInputFromClipboard(
    flowId: number,
    x: number,
    y: number,
    clipboardText?: string | null,
  ): Promise<boolean> {
    // Callers on the ClipboardEvent path pass the already-read text. Otherwise
    // read the OS clipboard via desktop.readClipboardText() (native plugin on
    // desktop, navigator.clipboard on web) — bail if it rejects.
    let text = clipboardText ?? null;
    if (text === null) {
      try {
        // Desktop reads go through the native clipboard-manager plugin (no macOS
        // "Paste" pill); web mode falls back to navigator.clipboard.
        text = await desktop.readClipboardText();
      } catch {
        return false;
      }
    }

    const parsed = parseTabularText(text);
    if (!parsed || parsed.length < 2) return false;

    const headers = parsed[0];
    const dataRows = parsed.slice(1);

    const data: unknown[][] = headers.map((_, colIdx) => dataRows.map((row) => row[colIdx] ?? ""));
    const columns = headers.map((name, colIdx) => ({
      name: name || `Column ${colIdx + 1}`,
      data_type: inferColumnDataType(data[colIdx]),
    }));

    const nodeId = getId();

    // Node and data in one step, sent before any await; the canvas shows it once core has it.
    const pasted = FlowApi.applyOperations(
      flowId,
      "Paste table",
      addConfiguredNodeOperations(
        flowId,
        nodeId,
        "manual_input",
        { x, y },
        { cache_results: false, is_setup: true, raw_data_format: { columns, data } },
      ),
    );
    let component: any;
    let nodeTemplate: NodeTemplate | undefined;
    try {
      [component, nodeTemplate] = await Promise.all([
        getComponent("manual_input"),
        getNodeTemplateByItem("manual_input"),
        pasted,
      ]);
    } catch (error) {
      recoverFromFailedMutation(error, "Could not paste the table");
      return false;
    }

    addNodes({
      id: String(nodeId),
      type: "custom-node",
      position: { x, y },
      data: {
        id: nodeId,
        label: "Manual Input",
        component: markRaw(component),
        inputs: [],
        outputs: [{ id: DEFAULT_OUTPUT_HANDLE, position: Position.Right }],
        nodeTemplate,
      },
    });
    useTutorialStore().notify({ type: "node-added", nodeItem: "manual_input", nodeId });
    return true;
  }

  return {
    draggedType,
    isDragOver,
    isDragging,
    onDragStart,
    onDragLeave,
    onDragOver,
    onDrop,
    createCopyNode,
    createMultiCopyNodes,
    createManualInputFromClipboard,
    importFlow,
    createEmptyFlow,
    planEdgeSplice,
    planAutoConnect,
    detectAutoConnectForNode,
    resetAutoConnectCandidates,
  };
}

export { markHoveredEdge, detectEdgeUnderPointer, markAutoConnectNode };
