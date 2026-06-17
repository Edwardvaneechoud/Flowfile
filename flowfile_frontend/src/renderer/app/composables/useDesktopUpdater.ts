import { isDesktop } from "../../lib/desktop";

/**
 * On startup, ask the Tauri updater if a newer release exists. If so, prompt
 * the user with a native confirm dialog and (on accept) download + install.
 *
 * Skipped in:
 *   - Web/Docker mode (no Tauri runtime)
 *   - Dev mode (we don't ship dev builds via the updater feed)
 *   - When `VITE_FLOWFILE_UPDATER_ENABLED` is not set — until a real
 *     `latest.json` is published the plugin logs a noisy ERROR on every
 *     startup. Flip the env var (e.g. in `.env.production` or via the
 *     release CI) once the endpoint is wired up.
 */
export async function checkForUpdatesOnStartup(): Promise<void> {
  if (!isDesktop) return;
  if (import.meta.env.DEV) return;
  if (!import.meta.env.VITE_FLOWFILE_UPDATER_ENABLED) return;

  try {
    const { check } = await import("@tauri-apps/plugin-updater");
    const update = await check();
    if (!update) return;

    const accept = window.confirm(
      `A new version of Flowfile is available (${update.version}).\n\n` +
        "Install now? The app will restart after the download completes.",
    );
    if (!accept) return;

    await update.downloadAndInstall();

    const { relaunch } = await import("@tauri-apps/plugin-process");
    await relaunch();
  } catch (err) {
    console.warn("Update check failed:", err);
  }
}
