export type SettingsLoadView = "form" | "error" | "loading";

/**
 * Which body a node settings drawer shows: the form once loaded, the load error with Retry after a
 * failed load, else the skeleton.
 */
export function settingsLoadView(
  settingsLoaded: boolean,
  loadError: string | null,
): SettingsLoadView {
  if (settingsLoaded) return "form";
  return loadError ? "error" : "loading";
}
