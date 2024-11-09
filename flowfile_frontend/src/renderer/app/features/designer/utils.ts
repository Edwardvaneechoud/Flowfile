import axios from "axios";
import { NodeTemplate } from "./types";

export const getImageUrl = (name: string): string => {
  return new URL(`./assets/icons/${name}`, import.meta.url).href;
};

export const fetchNodes = async (): Promise<NodeTemplate[]> => {
  const response = await axios.get("/node_list");
  const listNodes = response.data as NodeTemplate[];
  return listNodes;
};
