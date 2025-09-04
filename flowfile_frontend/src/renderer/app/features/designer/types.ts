
export interface NodeTemplate {
  name: string;
  color: string;
  item: string;
  input: number;
  output: number;
  image: string;
  multi: boolean;
  node_group: string;
  prod_ready: boolean;
  drawer_title: string; 
  drawer_intro: string;
}

export interface NodeInput extends NodeTemplate {
  id: number;
  pos_x: number;
  pos_y: number;
}

export interface EdgeInput {
  id: string;
  source: string;
  target: string;
  sourceHandle: string;
  targetHandle: string;
}

export interface VueFlowInput {
  node_edges: EdgeInput[];
  node_inputs: NodeInput[];
}
