// Module-singleton cache for docker status + flavour metadata, so kernel
// dialogs outside KernelManagerView can reuse one fetch. useKernelManager
// keeps its own polling copy — a duplicate fetch when both are open is fine.
import { computed, ref } from "vue";

import { KernelApi } from "@/api/kernel.api";
import type { DockerStatus, FlavourInfo, ImageFlavour, KernelImageStatus } from "@/types";

const dockerStatus = ref<DockerStatus | null>(null);
const flavourInfo = ref<Map<ImageFlavour, FlavourInfo>>(new Map());
const loading = ref(false);

const TTL_MS = 30_000;
let lastLoaded = 0;
let inFlight: Promise<void> | null = null;

async function fetchResources(): Promise<void> {
  loading.value = true;
  try {
    const [status, flavours] = await Promise.all([
      KernelApi.getDockerStatus(),
      KernelApi.listFlavours(),
    ]);
    dockerStatus.value = status;
    flavourInfo.value = new Map(flavours.map((f) => [f.flavour, f]));
    lastLoaded = Date.now();
  } finally {
    loading.value = false;
  }
}

export function useKernelResources() {
  const imageStatuses = computed<KernelImageStatus[]>(() => dockerStatus.value?.images ?? []);

  async function ensureLoaded(force = false): Promise<void> {
    // Only a usable (Docker-up) result is worth serving from cache: the UI
    // tells users to retry after starting Docker, so down-status must refetch.
    const usable = dockerStatus.value?.available === true;
    if (!force && usable && lastLoaded && Date.now() - lastLoaded < TTL_MS) return;
    if (!inFlight) {
      inFlight = fetchResources().finally(() => {
        inFlight = null;
      });
    }
    return inFlight;
  }

  return { dockerStatus, flavourInfo, imageStatuses, loading, ensureLoaded };
}
