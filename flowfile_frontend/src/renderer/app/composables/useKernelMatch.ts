// Per-instance kernel-match state for a node drawer or dialog. Backend does
// the actual matching (POST /kernels/match); this only guards staleness and
// degrades to "unavailable" so callers can fall back to a plain kernel list.
import { ref } from "vue";

import { KernelApi } from "@/api/kernel.api";
import type { KernelMatch, KernelSuggestion } from "@/types";

export function useKernelMatch() {
  const matches = ref<KernelMatch[]>([]);
  const suggestion = ref<KernelSuggestion | null>(null);
  const matchLoading = ref(false);
  const matchUnavailable = ref(false);
  let matchSeq = 0;

  const matchFor = (kernelId: string | null | undefined): KernelMatch | undefined =>
    kernelId ? matches.value.find((m) => m.kernel_id === kernelId) : undefined;

  async function refreshMatch(dependencies: string[], nodeName?: string): Promise<void> {
    const seq = ++matchSeq;
    if (!dependencies.length) {
      matches.value = [];
      suggestion.value = null;
      matchUnavailable.value = false;
      matchLoading.value = false; // an in-flight older call must not leave this stuck
      return;
    }
    matchLoading.value = true;
    try {
      const response = await KernelApi.matchKernels(dependencies, nodeName);
      if (seq !== matchSeq) return;
      matches.value = response.matches;
      suggestion.value = response.suggestion;
      matchUnavailable.value = false;
    } catch {
      if (seq !== matchSeq) return;
      matches.value = [];
      suggestion.value = null;
      matchUnavailable.value = true;
    } finally {
      if (seq === matchSeq) matchLoading.value = false;
    }
  }

  return { matches, suggestion, matchLoading, matchUnavailable, matchFor, refreshMatch };
}
