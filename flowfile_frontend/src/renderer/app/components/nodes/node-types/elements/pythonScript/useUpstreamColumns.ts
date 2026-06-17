import { ref, watch, type Ref } from "vue";
import { NodeApi } from "../../../../../api/node.api";
import type { InputNameInfo } from "../../../../../types/flow.types";

export interface UpstreamColumn {
  name: string;
  data_type: string;
  source_input: string;
}

/**
 * Fetches column schemas for each upstream input by calling NodeApi.getTableExample
 * on the source node. Returns an empty list if no upstream node has run yet —
 * schemas only exist post-execution, matching the behaviour of Sort/Pivot/GroupBy nodes.
 */
export function useUpstreamColumns(
  flowId: Ref<number | null>,
  inputs: Ref<InputNameInfo[]>,
): {
  columns: Ref<UpstreamColumn[]>;
  reload: () => Promise<void>;
} {
  const columns = ref<UpstreamColumn[]>([]);

  const load = async () => {
    const fid = flowId.value;
    const ins = inputs.value;
    if (fid == null || ins.length === 0) {
      columns.value = [];
      return;
    }
    const results = await Promise.allSettled(
      ins.map(async (input) => {
        const example = await NodeApi.getTableExample(fid, input.source_node_id);
        return (example?.table_schema ?? []).map((col) => ({
          name: col.name,
          data_type: col.data_type,
          source_input: input.name,
        }));
      }),
    );
    columns.value = results.flatMap((r) => (r.status === "fulfilled" ? r.value : []));
  };

  watch([flowId, inputs], load, { immediate: true, deep: true });

  return { columns, reload: load };
}
