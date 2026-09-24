export type SettingsLoadView = "form" | "error" | "loading";

/**
 * Which body a node settings drawer shows: its form once the settings loaded, the load error
 * with a Retry button after a failed load, else the loading skeleton. A failed load must never
 * leave the skeleton up forever.
 */
export function settingsLoadView(
  settingsLoaded: boolean,
  loadError: string | null,
): SettingsLoadView {
  if (settingsLoaded) return "form";
  return loadError ? "error" : "loading";
}
