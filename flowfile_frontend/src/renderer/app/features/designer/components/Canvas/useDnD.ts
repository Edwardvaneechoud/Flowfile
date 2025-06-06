//useDnD.ts
import { useVueFlow, Node, Position, } from "@vue-flow/core";
import { ref, watch, markRaw, nextTick } from "vue";
import { NodeTemplate, NodeInput, VueFlowInput} from "../../types";
import { NodeCopyInput, NodePromise } from "./types";
import { getComponent, getComponentRaw } from "./componentLoader";
import { insertNode, copyNode } from './backendInterface'


let id = 0;

function getId(): number {
  return ++id;
}

const state = {
  draggedType: ref<string | null>(null),
  isDragOver: ref(false),
  isDragging: ref(false),
};

export default function useDragAndDrop() {
  const { draggedType, isDragOver, isDragging } = state;

  const { addNodes, screenToFlowCoordinate, onNodesInitialized, updateNode, addEdges, fromObject } = useVueFlow();

  watch(isDragging, (dragging) => {
    document.body.style.userSelect = dragging ? "none" : "";
  });

  function onDragStart(event: DragEvent, nodeTemplate: NodeTemplate) {
    if (event.dataTransfer) {
      event.dataTransfer.setData(
        "application/vueflow",
        JSON.stringify(nodeTemplate),
      );
      event.dataTransfer.effectAllowed = "move";
    }

    draggedType.value = nodeTemplate.item;
    isDragging.value = true;

    document.addEventListener("drop", onDragEnd);
  }

  function onDragOver(event: DragEvent) {
    event.preventDefault();

    if (draggedType.value) {
      isDragOver.value = true;

      if (event.dataTransfer) {
        event.dataTransfer.dropEffect = "move";
      }
    }
  }

  function onDragLeave() {
    isDragOver.value = false;
  }

  function onDragEnd() {
    isDragging.value = false;
    isDragOver.value = false;
    draggedType.value = null;
    document.removeEventListener("drop", onDragEnd);
  }

  function createCopyNode(node: NodeCopyInput) {
    getComponentRaw(node.type).then((component) => {
      let nodeId: number = getId()
      const newNode: Node = {
        id: String(nodeId),
        type:  "custom-node",
        position: {
          x: node.posX,
          y: node.posY,
        },
        data: {
          id: nodeId,
          label: node.label,
          component: markRaw(component),
          inputs: Array.from({ length: node.numberOfInputs }, (_, i) => ({
            id: `input-${i}`,
            position: Position.Left,
          })),
          outputs: Array.from({ length: node.numberOfOutputs }, (_, i) => ({
            id: `output-${i}`,
            position: Position.Right,
          }))
        },
      };
      const nodePromise: NodePromise = {
        node_id: nodeId,
        flow_id: node.flowId,
        node_type: node.typeSnakeCase,
        pos_x: node.posX,
        pos_y: node.posY,
        cache_results: true
      }
      copyNode(node.nodeIdToCopyFrom, node.flowIdToCopyFrom, nodePromise)

      addNodes(newNode);
    }
  );
  }

  const getMaxDataId = (nodes: NodeInput[]): number => {
    return nodes.reduce((maxId, node) => {
      return node.id > maxId ? node.id : maxId;
    }, 0); // Initial value should be -Infinity to ensure it works with any positive number
  };


  async function getNodeToAdd(node: NodeInput): Promise<Node> {
    const numberOfInputs: number = (node.multi) ? 1 : node.input;

    const component = await getComponent(node.item);
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
        inputs: Array.from({ length: numberOfInputs }, (_, i) => ({
          id: `input-${i}`,
          position: Position.Left,
        })),
        outputs: Array.from({ length: node.output }, (_, i) => ({
          id: `output-${i}`,
          position: Position.Right,
        })),
      },
    };
    return newNode;
  }

  async function createEmptyFlow() {
    const emptyFlow = {"nodes":[],"edges":[],"position":[0,0] as [number, number],"zoom":1,"viewport":{"x":0,"y":0,"zoom":1}}
    await fromObject(emptyFlow)
    await nextTick()
  }
  
  async function importFlow(flowData: VueFlowInput) {
    await createEmptyFlow();
    const allNodes = await Promise.all(
      flowData.node_inputs.map((node) => getNodeToAdd(node))
    );
    
    addNodes(allNodes);
    id = getMaxDataId(flowData.node_inputs);
    addEdges(flowData.node_edges)
  }

  function onDrop(event: DragEvent, flowId: number): Node|undefined {
    const position = screenToFlowCoordinate({
      x: event.clientX,
      y: event.clientY,
    });
    if (!event.dataTransfer) return;
    const nodeData: NodeTemplate = JSON.parse(
      event.dataTransfer.getData("application/vueflow"),
    );
    console.log('nodeData:', nodeData)
    const nodeId = getId();

    getComponent(nodeData.item)
      .then((component) => {
        const numberOfInputs: number = (nodeData.multi) ? 1 : nodeData.input;
        console.log('logging' , numberOfInputs)
        const newNode: Node = {
          id: String(nodeId),
          type: "custom-node",
          position,
          data: {
            id: nodeId, // Pass nodeId here
            label: nodeData.name,
            component: markRaw(component), // Pass component directly
            inputs: Array.from({ length: numberOfInputs }, (_, i) => ({
              id: `input-${i}`,
              position: Position.Left,
            })),
            outputs: Array.from({ length: nodeData.output }, (_, i) => ({
              id: `output-${i}`,
              position: Position.Right,
            })),
            nodeItem: nodeData.item
          },
        };
        const { off } = onNodesInitialized(() => {
          updateNode(String(nodeId), (node) => ({
            position: {
              x: node.position.x - (node.dimensions?.width || 0) / 55,
              y: node.position.y - (node.dimensions?.height || 0) / 55,
            },
          }));

          off();
        });
        console.log("nodeData", nodeData)
        insertNode(flowId, nodeId, nodeData.item)
        console.log(newNode)
        addNodes(newNode);
      })
      .catch((error) => {
        console.error(`Error importing component for ${nodeData.item}`, error);
      });
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
    importFlow,
  };
}
