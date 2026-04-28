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

const RETRY_DELAY_MS = 500;
const RETRYING_MESSAGE = "Worker unavailable. Retrying...";

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const isRetriable = (err: any): boolean => {
  const status = err?.response?.status;
  return status === undefined || status >= 500;
};

const messageFor4xx = (err: any): string => {
  const data = err?.response?.data;
  return data?.error ?? data?.detail ?? err?.message ?? String(err);
};

const messageFor5xx = (err: any): string => {
  const data = err?.response?.data;
  return data?.detail ?? err?.message ?? String(err);
};

// Wraps a fetcher into the (payload) => Promise<IRow[]> signature
// VueGraphicWalker expects, while exposing the latest error message via
// lastError so the UI can surface it instead of swallowing it in console.
//
// Retries once with a short delay on 5xx / network errors; 4xx surfaces
// the server-provided error string immediately without a retry.
export function useGraphicWalkerCompute(
  fetcher: (payload: any) => Promise<ComputeResult>,
  label = "viz",
): UseGraphicWalkerComputeReturn {
  const lastError = ref<string | null>(null);

  const runOnce = async (payload: any): Promise<IRow[] | null> => {
    const resp = await fetcher(payload);
    if (resp.error) {
      lastError.value = resp.error;
      console.error(`[${label}] compute failed:`, resp.error);
      return null;
    }
    lastError.value = null;
    return resp.rows;
  };

  const computation = async (payload: any): Promise<IRow[]> => {
    try {
      const rows = await runOnce(payload);
      return rows ?? [];
    } catch (err: any) {
      if (!isRetriable(err)) {
        lastError.value = messageFor4xx(err);
        console.error(`[${label}] compute threw (4xx):`, err);
        return [];
      }
      lastError.value = RETRYING_MESSAGE;
      console.warn(`[${label}] compute failed, retrying once:`, err);
      await sleep(RETRY_DELAY_MS);
      try {
        const rows = await runOnce(payload);
        return rows ?? [];
      } catch (retryErr: any) {
        lastError.value = messageFor5xx(retryErr);
        console.error(`[${label}] compute threw after retry:`, retryErr);
        return [];
      }
    }
  };

  return { computation, lastError };
}
