import { ref, onMounted, DefineComponent, Component, markRaw } from "vue";
import axios from "axios";
import { NodeTemplate } from "../../types";
import { NodeCopyInput } from "./types";
import { toTitleCase } from "./utils";
import { ENV } from "../../../../../config/environment";
import { Node, Position } from '@vue-flow/core';


export async function getComponent(
  node: NodeTemplate,
): Promise<DefineComponent> {
  const formattedItemName = toTitleCase(node.item);
  console.log(formattedItemName);
  // Dynamically import the corresponding Vue component based on the item name
  const { default: component } = await import(
    `../../nodes/${formattedItemName}.vue`
  );
  return component;
}

export const useNodes = () => {
  const nodes = ref<NodeTemplate[]>([]);

  const fetchNodes = async () => {
    const response = await axios.get("/node_list");
    const allNodes = response.data as NodeTemplate[];
    nodes.value = ENV.isProduction 
      ? allNodes.filter(node => node.prod_ready)
      : allNodes;
  };

  onMounted(fetchNodes);

  return { nodes };
};

