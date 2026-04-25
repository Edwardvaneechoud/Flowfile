import type { DynamicRenameInput, NodeDynamicRename } from "../../../../../types/node.types";

export const createDynamicRenameInput = (): DynamicRenameInput => ({
  rename_mode: "prefix",
  prefix: "",
  suffix: "",
  formula: "",
  selection_mode: "all",
  selected_columns: [],
  selected_data_type: null,
});

export const createDynamicRenameNode = (
  flowId = -1,
  nodeId = -1,
  pos_x = 0,
  pos_y = 0,
): NodeDynamicRename => ({
  flow_id: flowId,
  node_id: nodeId,
  pos_x,
  pos_y,
  cache_results: false,
  dynamic_rename_input: createDynamicRenameInput(),
});
