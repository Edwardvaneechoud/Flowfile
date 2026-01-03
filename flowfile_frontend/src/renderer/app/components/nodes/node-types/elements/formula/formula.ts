import { NodeFormula, FormulaInput, FieldInput } from '../../../baseNode/nodeInput'

export const createFormulaInput = (
  field_name = '',
  data_type = 'Auto',
  function_def = ''
): FormulaInput => {
  const fieldInput: FieldInput = {
    name: field_name,
    data_type: data_type
  }

  const functionInput: FormulaInput = {
    field: fieldInput,
    function: function_def,
  }

  return functionInput
}

export const createFormulaNode = (
  flowId = -1,
  nodeId = -1,
  pos_x = 0,
  pos_y = 0,
  field_name = 'output_field',
  data_type = 'Auto',
  function_def = '',
): NodeFormula => {
  const func_info = createFormulaInput(field_name, data_type, function_def)

  const nodeFunction: NodeFormula = {
    flow_id: flowId,
    node_id: nodeId,
    pos_x: pos_x,
    pos_y: pos_y,
    function: func_info,
    cache_results: false,
  }
  return nodeFunction
}
