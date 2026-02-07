import type { NodePythonScript, PythonScriptInput } from "../../../../../types/node.types";

export const DEFAULT_PYTHON_SCRIPT_CODE = `import polars as pl

df = ff_kernel.read_input()

# Your transformation here

ff_kernel.publish_output(df)
`;

export const createPythonScriptNode = (
  flowId: number,
  nodeId: number,
): NodePythonScript => {
  const pythonScriptInput: PythonScriptInput = {
    code: DEFAULT_PYTHON_SCRIPT_CODE,
    kernel_id: null,
    cells: [
      { id: crypto.randomUUID(), code: DEFAULT_PYTHON_SCRIPT_CODE },
    ],
  };

  return {
    flow_id: flowId,
    node_id: nodeId,
    pos_x: 0,
    pos_y: 0,
    depending_on_ids: null,
    python_script_input: pythonScriptInput,
    cache_results: false,
  };
};
