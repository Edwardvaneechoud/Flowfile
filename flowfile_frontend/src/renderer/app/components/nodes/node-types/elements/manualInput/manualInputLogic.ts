import { ref, Ref } from "vue";
import { NodeManualInput, RawDataFormat } from "../../../baseNode/nodeInput";

export const createManualInput = (
  flowId = -1,
  nodeId = -1,
  pos_x = 0,
  pos_y = 0,
): Ref<NodeManualInput> => {
  const nodeManualInput: Ref<NodeManualInput> = ref({
    flow_id: flowId,
    node_id: nodeId,
    pos_x: pos_x,
    pos_y: pos_y,
    cache_input: false,
    raw_data_format: { columns: [], data: [] } as RawDataFormat,
    cache_results: false,
  });
  return nodeManualInput;
};

/**
 * Infer the best data type for a column based on its values
 */
export const inferDataType = (values: unknown[]): string => {
  const validValues = values.filter((v) => v !== null && v !== undefined && v !== "");

  if (validValues.length === 0) {
    return "String";
  }

  const allBooleans = validValues.every(
    (v) => typeof v === "boolean" || v === "true" || v === "false",
  );
  if (allBooleans) {
    return "Boolean";
  }

  const allNumeric = validValues.every((v) => {
    if (typeof v === "number") return true;
    if (typeof v === "string") {
      const parsed = Number(v);
      return !isNaN(parsed) && v.trim() !== "";
    }
    return false;
  });

  if (allNumeric) {
    const allIntegers = validValues.every((v) => {
      const num = typeof v === "number" ? v : Number(v);
      return Number.isInteger(num);
    });

    return allIntegers ? "Int64" : "Float64";
  }

  return "String";
};
