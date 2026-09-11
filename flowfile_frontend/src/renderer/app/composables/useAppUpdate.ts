// Shared "which version am I on / is there a newer one" behaviour for the
// About dialog, the home footer and the sidebar help menu. Desktop checks go
// through the Tauri updater (and its install prompt); browser-served installs
// ask PyPI and are pointed at the release notes instead.
import { computed, ref } from "vue";

import { desktop, isDesktop } from "../../lib/desktop";
import { releasePageUrl } from "../components/settings/updatePrompt";
import { fetchLatestVersion, isNewerVersion } from "../lib/latestVersion";
import { useUpdateStore } from "../stores/update-store";

const RELEASES_URL = "https://github.com/edwardvaneechoud/Flowfile/releases";

/** Module-level: the running version never changes, so resolve it once per session. */
const version = ref("");
let versionRequest: Promise<void> | null = null;

function loadVersion(): Promise<void> {
  versionRequest ??= (async () => {
    try {
      version.value = (isDesktop ? await desktop.getAppVersion() : "") || __APP_VERSION__;
    } catch {
      version.value = __APP_VERSION__;
    }
  })();
  return versionRequest;
}

export type UpdateCheckOutcome = "none" | "current" | "newer" | "failed";

export function useAppUpdate() {
  const updateStore = useUpdateStore();
  const outcome = ref<UpdateCheckOutcome>("none");
  const webChecking = ref(false);
  /** Set by a browser-mode check that found a newer PyPI release. */
  const newerVersion = ref<string | null>(null);

  void loadVersion();

  const checking = computed(() => updateStore.checking || webChecking.value);

  const statusText = computed(() => {
    if (checking.value) return "Checking…";
    if (outcome.value === "newer") return `Flowfile ${newerVersion.value} is available`;
    if (outcome.value === "current") return "You're up to date";
    if (outcome.value === "failed") return "Couldn't check for updates";
    return "";
  });

  /** True when the desktop updater offered a release — the prompt modal in AppLayout takes over. */
  async function checkForUpdates(): Promise<boolean> {
    outcome.value = "none";
    newerVersion.value = null;
    if (isDesktop) {
      const info = await updateStore.checkNow();
      if (info) return true;
      outcome.value = updateStore.checkError ? "failed" : "current";
      return false;
    }
    webChecking.value = true;
    try {
      await loadVersion();
      const latest = await fetchLatestVersion();
      if (isNewerVersion(latest, version.value)) {
        newerVersion.value = latest;
        outcome.value = "newer";
      } else {
        outcome.value = "current";
      }
    } catch (error) {
      console.warn("[update] check failed:", error);
      outcome.value = "failed";
    } finally {
      webChecking.value = false;
    }
    return false;
  }

  function resetStatus(): void {
    outcome.value = "none";
    newerVersion.value = null;
  }

  /** The newer release once a check found one, else the running version's page. */
  function openReleaseNotes(): void {
    const target = newerVersion.value ?? version.value;
    void desktop.openExternal(target ? releasePageUrl(target) : RELEASES_URL);
  }

  return {
    version,
    checking,
    outcome,
    newerVersion,
    statusText,
    checkForUpdates,
    resetStatus,
    openReleaseNotes,
  };
}
