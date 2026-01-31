import { ref, onMounted, onUnmounted } from "vue";
import type { Ref } from "vue";
import { KernelApi } from "../../api/kernel.api";
import type { KernelInfo, KernelConfig } from "../../types";

const POLL_INTERVAL_MS = 5000;

export function useKernelManager() {
  const kernels: Ref<KernelInfo[]> = ref([]);
  const isLoading = ref(true);
  const errorMessage: Ref<string | null> = ref(null);
  const actionInProgress: Ref<Record<string, boolean>> = ref({});
  let pollTimer: ReturnType<typeof setInterval> | null = null;

  const loadKernels = async () => {
    try {
      kernels.value = await KernelApi.getAll();
      errorMessage.value = null;
    } catch (error: any) {
      console.error("Failed to load kernels:", error);
      errorMessage.value = error.message || "Failed to load kernels";
      throw error;
    } finally {
      isLoading.value = false;
    }
  };

  const createKernel = async (config: KernelConfig): Promise<KernelInfo> => {
    const kernel = await KernelApi.create(config);
    await loadKernels();
    return kernel;
  };

  const startKernel = async (kernelId: string) => {
    actionInProgress.value[kernelId] = true;
    try {
      await KernelApi.start(kernelId);
      await loadKernels();
    } finally {
      actionInProgress.value[kernelId] = false;
    }
  };

  const stopKernel = async (kernelId: string) => {
    actionInProgress.value[kernelId] = true;
    try {
      await KernelApi.stop(kernelId);
      await loadKernels();
    } finally {
      actionInProgress.value[kernelId] = false;
    }
  };

  const deleteKernel = async (kernelId: string) => {
    actionInProgress.value[kernelId] = true;
    try {
      await KernelApi.delete(kernelId);
      await loadKernels();
    } finally {
      delete actionInProgress.value[kernelId];
    }
  };

  const isActionInProgress = (kernelId: string): boolean => {
    return !!actionInProgress.value[kernelId];
  };

  const startPolling = () => {
    stopPolling();
    pollTimer = setInterval(async () => {
      try {
        kernels.value = await KernelApi.getAll();
      } catch {
        // Silently ignore poll errors to avoid spamming the user
      }
    }, POLL_INTERVAL_MS);
  };

  const stopPolling = () => {
    if (pollTimer !== null) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  };

  onMounted(async () => {
    await loadKernels();
    startPolling();
  });

  onUnmounted(() => {
    stopPolling();
  });

  return {
    kernels,
    isLoading,
    errorMessage,
    actionInProgress,
    loadKernels,
    createKernel,
    startKernel,
    stopKernel,
    deleteKernel,
    isActionInProgress,
  };
}
