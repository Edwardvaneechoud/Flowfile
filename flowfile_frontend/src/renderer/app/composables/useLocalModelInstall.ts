import { computed, onBeforeUnmount, ref } from "vue";
import { useAiStore } from "../stores/ai-store";
import {
  fetchLocalModelStatus,
  streamLocalModelInstall,
  type LocalModelStatus,
} from "../views/AiSettingsView/localModelApi";

const INSTALL_POLL_MS = 1000;

export const formatDownloadMb = (mb: number | null | undefined): string => {
  if (!mb || mb <= 0) return "";
  return mb >= 1000 ? `${(mb / 1000).toFixed(1)} GB` : `${mb} MB`;
};

const pctOf = (received?: number | null, total?: number | null): number | null =>
  typeof received === "number" && typeof total === "number" && total > 0
    ? Math.min(100, Math.round((received / total) * 100))
    : null;

export interface LocalModelInstallOptions {
  onInstalled?: () => void;
}

/** Install, progress and re-attach logic for the on-device model, writing through the AI store. */
export function useLocalModelInstall(options: LocalModelInstallOptions = {}) {
  const aiStore = useAiStore();
  const status = computed(() => aiStore.localModelStatus);
  const loading = ref(false);
  const loadError = ref<string | null>(null);
  const installing = ref(false);
  const installPhase = ref("");
  const installPct = ref<number | null>(null);
  const installingModelId = ref<string | null>(null);
  const installError = ref<string | null>(null);
  let pollTimer: ReturnType<typeof setTimeout> | null = null;
  let disposed = false;

  const recommendedModel = computed(
    () =>
      status.value?.models.find((m) => m.id === status.value?.selectedModelId && !m.legacy) ??
      status.value?.models.find((m) => !m.legacy) ??
      null,
  );
  const downloadLabel = computed(() => formatDownloadMb(recommendedModel.value?.approxDownloadMb));
  const available = computed(() => status.value?.available === true);
  const installed = computed(() => status.value?.anyModelInstalled === true);
  const installingModel = computed(
    () => status.value?.models.find((m) => m.id === installingModelId.value) ?? null,
  );

  const phaseLabel = computed(() => {
    switch (installPhase.value) {
      case "downloading_binary":
        return "Downloading runtime…";
      case "extracting":
        return "Extracting…";
      case "downloading_model": {
        const size = formatDownloadMb(installingModel.value?.approxDownloadMb);
        return size ? `Downloading model (~${size})…` : "Downloading model…";
      }
      case "verifying":
        return "Verifying…";
      case "done":
        return "Done";
      default:
        return "Installing…";
    }
  });

  const stopPolling = (): void => {
    if (pollTimer !== null) {
      clearTimeout(pollTimer);
      pollTimer = null;
    }
  };

  const finish = (error: string | null): void => {
    installing.value = false;
    installingModelId.value = null;
    installError.value = error;
    if (!error && installed.value) options.onInstalled?.();
  };

  const applyProgress = (st: LocalModelStatus): void => {
    const inst = st.install;
    if (!inst?.active) return;
    installingModelId.value = inst.modelId;
    installPhase.value = inst.phase;
    installPct.value = pctOf(inst.received, inst.total);
  };

  const attach = (st: LocalModelStatus | null): void => {
    if (!st?.install?.active || installing.value) return;
    installing.value = true;
    installError.value = null;
    applyProgress(st);
    const poll = async (): Promise<void> => {
      if (disposed) return;
      let latest: LocalModelStatus;
      try {
        latest = await fetchLocalModelStatus();
      } catch {
        pollTimer = setTimeout(poll, INSTALL_POLL_MS);
        return;
      }
      aiStore.applyLocalModelStatus(latest);
      if (latest.install?.active) {
        applyProgress(latest);
        pollTimer = setTimeout(poll, INSTALL_POLL_MS);
        return;
      }
      pollTimer = null;
      finish(latest.install?.error ?? null);
    };
    pollTimer = setTimeout(poll, INSTALL_POLL_MS);
  };

  const load = async (): Promise<void> => {
    loading.value = true;
    loadError.value = null;
    try {
      const st = await fetchLocalModelStatus();
      aiStore.applyLocalModelStatus(st);
      attach(st);
    } catch (error) {
      aiStore.applyLocalModelStatus(null);
      loadError.value = (error as Error)?.message || "Couldn't reach the on-device AI service.";
    } finally {
      loading.value = false;
    }
  };

  const install = async (modelId?: string): Promise<void> => {
    if (status.value?.install?.active) {
      attach(status.value);
      return;
    }
    const id = modelId ?? recommendedModel.value?.id ?? status.value?.selectedModelId;
    if (!id || installing.value) return;
    installing.value = true;
    installError.value = null;
    installingModelId.value = id;
    installPhase.value = "";
    installPct.value = null;
    let failed: string | null = null;
    try {
      await streamLocalModelInstall(
        {
          onProgress: (ev) => {
            installPhase.value = ev.phase;
            installPct.value = pctOf(ev.received, ev.total);
          },
          onError: (msg) => {
            failed = msg;
          },
        },
        undefined,
        id,
      );
    } catch (error) {
      failed = (error as Error).message || "Install failed";
    }
    try {
      aiStore.applyLocalModelStatus(await fetchLocalModelStatus());
    } catch {
      /* the stream result below still decides success */
    }
    finish(failed);
  };

  onBeforeUnmount(() => {
    disposed = true;
    stopPolling();
  });

  return {
    status,
    loading,
    loadError,
    installing,
    installPhase,
    installPct,
    installError,
    phaseLabel,
    available,
    installed,
    recommendedModel,
    downloadLabel,
    load,
    install,
  };
}
