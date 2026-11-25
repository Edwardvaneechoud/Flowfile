import { computed, onBeforeUnmount, ref } from "vue";
import { useAiStore } from "../stores/ai-store";
import {
  fetchLocalModelStatus,
  streamLocalModelInstall,
  type LocalModelStatus,
} from "../views/AiSettingsView/localModelApi";

const INSTALL_POLL_MS = 1000;

const formatDownloadMb = (mb: number | null | undefined): string => {
  if (!mb || mb <= 0) return "";
  return mb >= 1000 ? `${(mb / 1000).toFixed(1)} GB` : `${mb} MB`;
};

const pctOf = (received?: number | null, total?: number | null): number | null =>
  typeof received === "number" && typeof total === "number" && total > 0
    ? Math.min(100, Math.round((received / total) * 100))
    : null;

/** Install, progress and re-attach logic for the on-device model, writing through the AI store. */
export function useLocalModelInstall(onInstalled: () => void) {
  const aiStore = useAiStore();
  const status = computed(() => aiStore.localModelStatus);
  const loading = ref(false);
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
  const installed = computed(() => status.value?.anyModelInstalled === true);
  const installingModel = computed(
    () => status.value?.models.find((m) => m.id === installingModelId.value) ?? null,
  );

  const phaseLabel = computed(() => {
    const size = formatDownloadMb(installingModel.value?.approxDownloadMb);
    const labels: Record<string, string> = {
      downloading_binary: "Downloading runtime…",
      extracting: "Extracting…",
      downloading_model: size ? `Downloading model (~${size})…` : "Downloading model…",
      verifying: "Verifying…",
      done: "Done",
    };
    return labels[installPhase.value] ?? "Installing…";
  });

  const finish = (error: string | null): void => {
    installing.value = false;
    installingModelId.value = null;
    installError.value = error;
    if (!error && installed.value) onInstalled();
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
    try {
      const st = await fetchLocalModelStatus();
      aiStore.applyLocalModelStatus(st);
      attach(st);
    } catch {
      aiStore.applyLocalModelStatus(null);
    } finally {
      loading.value = false;
    }
  };

  const install = async (): Promise<void> => {
    if (status.value?.install?.active) {
      attach(status.value);
      return;
    }
    const id = recommendedModel.value?.id ?? status.value?.selectedModelId;
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
    if (pollTimer !== null) clearTimeout(pollTimer);
  });

  return {
    status,
    loading,
    installing,
    installPct,
    installError,
    phaseLabel,
    installed,
    downloadLabel,
    load,
    install,
  };
}
