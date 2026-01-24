export const getNodeDataFromDf = (df: any, nodeId: number) => {
  const nodeData = df.drawflow.drawflow["Home"]["data"][nodeId];
  return nodeData || null;
};

interface NodeConnection {
  node: string;
  input: string;
}

interface NodePort {
  connections: NodeConnection[];
}

export interface Node {
  id: number;
  name: string;
  data: Record<string, any>; // Using a generic object type, but you might want to make this more specific if possible
  class: string;
  html: string;
  typenode: string;
  inputs: Record<string, NodePort>;
  outputs: Record<string, NodePort>;
  pos_x: number;
  pos_y: number;
}
