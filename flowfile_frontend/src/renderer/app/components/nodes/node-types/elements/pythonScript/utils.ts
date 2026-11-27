import type { ImageFlavour, KernelInfo, KernelState } from "../../../../../types/kernel.types";
import type { NodePythonScript, PythonScriptInput } from "../../../../../types/node.types";
import { newCellId } from "../../../../notebook/cellOperations";

export const DEFAULT_PYTHON_SCRIPT_CODE = `import polars as pl

df = flowfile_ctx.read_input()

# Your transformation here

flowfile_ctx.publish_output(df)
`;

export const createPythonScriptNode = (flowId: number, nodeId: number): NodePythonScript => {
  const pythonScriptInput: PythonScriptInput = {
    code: DEFAULT_PYTHON_SCRIPT_CODE,
    kernel_id: null,
    cells: [{ id: newCellId(), code: DEFAULT_PYTHON_SCRIPT_CODE }],
  };

  return {
    flow_id: flowId,
    node_id: nodeId,
    pos_x: 0,
    pos_y: 0,
    depending_on_ids: null,
    python_script_input: pythonScriptInput,
    cache_results: false,
    output_names: ["main"],
  };
};

const DEFAULT_KERNEL_FLAVOURS: ImageFlavour[] = ["base", "lite"];
const DEFAULT_KERNEL_STATES: KernelState[] = ["idle", "executing", "starting", "stopped"];

/**
 * The kernel a Python Script node without one starts on: a base kernel, else a lite one,
 * preferring a running kernel within each flavour. Kernels in error or still being created
 * are never picked, and neither are ml, notebook or custom ones.
 */
export const pickDefaultKernel = (kernels: KernelInfo[]): KernelInfo | null => {
  for (const flavour of DEFAULT_KERNEL_FLAVOURS) {
    for (const state of DEFAULT_KERNEL_STATES) {
      const kernel = kernels.find((k) => k.image_flavour === flavour && k.state === state);
      if (kernel) return kernel;
    }
  }
  return null;
};
