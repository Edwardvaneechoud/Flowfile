// Module-singleton cache of kernel-readiness summaries for custom-node lists,
// keyed by readinessKey(deps): one batched /kernels/match/batch call shared by
// every list surface. On failure the badges simply don't render (unavailable),
// retried after the short TTL.
import { ref, type Ref } from "vue";

import { KernelApi } from "@/api/kernel.api";
import type { KernelMatchBatchSummary } from "@/types";

const readiness: Ref<Record<string, KernelMatchBatchSummary>> = ref({});
const unavailable = ref(false);

const TTL_MS = 30_000;
let lastLoaded = 0;
let inFlight: Promise<void> | null = null;

export function readinessKey(dependencies: string[]): string {
  return JSON.stringify([...dependencies].sort());
}

export function invalidateReadiness(): void {
  readiness.value = {};
  unavailable.value = false;
  lastLoaded = 0;
}

async function fetchMissing(items: Record<string, string[]>): Promise<void> {
  const missing: Record<string, string[]> = {};
  for (const [key, deps] of Object.entries(items)) {
    if (!(key in readiness.value)) missing[key] = deps;
  }
  if (Object.keys(missing).length === 0) return;
  try {
    const response = await KernelApi.matchKernelsBatch(missing);
    readiness.value = { ...readiness.value, ...response.results };
    unavailable.value = false;
  } catch {
    unavailable.value = true;
  }
  lastLoaded = Date.now();
}

export function useKernelReadiness() {
  async function ensureReadiness(items: Record<string, string[]>): Promise<void> {
    if (lastLoaded && Date.now() - lastLoaded >= TTL_MS) invalidateReadiness();
    // After a failure, stay silent until the TTL expires instead of hammering.
    if (unavailable.value) return;
    if (inFlight) await inFlight;
    inFlight = fetchMissing(items).finally(() => {
      inFlight = null;
    });
    return inFlight;
  }

  return { readiness, unavailable, ensureReadiness };
}
