import { ref, type Ref } from "vue";
import type { IRow } from "../components/nodes/node-types/elements/exploreData/vueGraphicWalker/interfaces";

export interface ComputeResult {
  rows: IRow[];
  error: string | null;
}

export interface UseGraphicWalkerComputeReturn {
  computation: (payload: any) => Promise<IRow[]>;
  lastError: Ref<string | null>;
}

// Wraps a fetcher into the (payload) => Promise<IRow[]> signature
// VueGraphicWalker expects, while exposing the latest error message via
// lastError so the UI can surface it instead of swallowing it in console.
export function useGraphicWalkerCompute(
  fetcher: (payload: any) => Promise<ComputeResult>,
  label = "viz",
): UseGraphicWalkerComputeReturn {
  const lastError = ref<string | null>(null);

  const computation = async (payload: any): Promise<IRow[]> => {
    try {
      const resp = await fetcher(payload);
      if (resp.error) {
        lastError.value = resp.error;
        console.error(`[${label}] compute failed:`, resp.error);
        return [];
      }
      lastError.value = null;
      return resp.rows;
    } catch (err: any) {
      const msg = err?.message ?? String(err);
      lastError.value = msg;
      console.error(`[${label}] compute threw:`, err);
      return [];
    }
  };

  return { computation, lastError };
}
