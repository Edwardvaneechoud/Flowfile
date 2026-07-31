import { ref, Ref } from "vue";
import { FileColumn, NodeSelect, SelectInput, TableExample } from "../../../../types/node.types";
import axios from "axios";

export const createSelectInput = (
  column_name: string,
  data_type: string | undefined = undefined,
  position: number | undefined = undefined,
  original_position: number | undefined = undefined,
): SelectInput => {
  const selectInput: SelectInput = {
    old_name: column_name,
    new_name: column_name,
    data_type: data_type,
    keep: true,
    join_key: false,
    is_altered: false,
    data_type_change: false,
    is_available: true,
    position: position ?? 0,
    original_position: original_position ?? 0,
  };
  return selectInput;
};

export const updateNodeSelect = (
  nodeTable: TableExample,
  nodeSelectRef: Ref<NodeSelect | null>,
): void => {
  if (!nodeTable?.table_schema || !nodeSelectRef.value) return;

  // Create a map for fast lookups instead of using find() in a loop
  const existingInputMap = new Map<string, SelectInput>();

  nodeSelectRef.value.select_input.forEach((input) => {
    existingInputMap.set(input.old_name, input);
  });

  nodeTable.table_schema.forEach((schema, index) => {
    const existingInput = existingInputMap.get(schema.name);

    if (existingInput) {
      if (!existingInput.is_altered) {
        existingInput.data_type = schema.data_type;
      }
      if (!existingInput.original_position) {
        existingInput.original_position = index;
      }
    } else {
      const newInput = createSelectInput(schema.name, schema.data_type, index, index);
      nodeSelectRef.value?.select_input.push(newInput);
      existingInputMap.set(schema.name, newInput);
    }
  });
};
/**
 * Normalise a select node's inputs just before saving: order by the on-screen
 * `position`, then re-stamp `position`, `original_position` and the
 * data-type-change flags from the upstream schema.
 *
 * Mutates and returns the same array holding the same object references — core
 * re-sorts `renames` by `position` on the wire, and Join/CrossJoin/FuzzyMatch
 * never re-emit on reorder, so the in-place write is the contract.
 */
export const applySelectPositions = (
  selectInputs: SelectInput[],
  tableSchema: FileColumn[] | undefined,
): SelectInput[] => {
  selectInputs.sort((a, b) => a.position - b.position);
  selectInputs.forEach((selectInput, index) => {
    selectInput.position = index;
    if (!tableSchema) return;
    const originalIndex = tableSchema.findIndex((column) => column.name === selectInput.old_name);
    if (originalIndex === -1) return;
    selectInput.original_position = originalIndex;
    // Mirrors the backend contract (transform_schema.SelectInput): data_type_change
    // is type-only, is_altered also covers a rename.
    selectInput.data_type_change = tableSchema[originalIndex].data_type !== selectInput.data_type;
    selectInput.is_altered =
      selectInput.data_type_change || selectInput.old_name !== selectInput.new_name;
  });
  return selectInputs;
};

export const createNodeSelect = (
  flowId = -1,
  nodeId = -1,
  pos_x = 0,
  pos_y = 0,
  cache_input = false,
  keep_missing = false,
): Ref<NodeSelect> => {
  const selectInputData: SelectInput[] = [];
  const nodeSelectRef: Ref<NodeSelect> = ref({
    flow_id: flowId,
    node_id: nodeId,
    pos_x: pos_x,
    pos_y: pos_y,
    cache_input: cache_input,
    keep_missing: keep_missing,
    select_input: selectInputData,
    cache_results: false,
    sorted_by: "none",
  });
  return nodeSelectRef;
};

export const createNewSelect = (org_node_select: NodeSelect, node_id: number): NodeSelect => {
  const newNodeSelect = createNodeSelect();
  newNodeSelect.value.select_input = org_node_select.select_input;
  newNodeSelect.value.flow_id = org_node_select.flow_id;
  newNodeSelect.value.depending_on_id = org_node_select.node_id;
  newNodeSelect.value.node_id = node_id;
  return newNodeSelect.value;
};

export const insertSelect = async (select_input: NodeSelect): Promise<NodeSelect> => {
  await axios.post("/transform/select", select_input, {
    headers: {
      "Content-Type": "application/json",
    },
  });
  return select_input;
};
