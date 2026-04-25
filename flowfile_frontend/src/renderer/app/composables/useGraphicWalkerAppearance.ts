import { computed } from "vue";
import { useThemeStore } from "../stores/theme-store";

/**
 * Map the application theme store value to a Graphic Walker `appearance` prop.
 *
 * GW's `appearance` accepts `"light" | "dark" | "media"`, where `"media"` falls
 * back to the OS preference. The Flowfile theme store can be `"system"`, which
 * we map to `"media"`; everything else passes through unchanged.
 */
export function useGraphicWalkerAppearance() {
  const themeStore = useThemeStore();
  return computed(() => {
    const mode = themeStore.mode;
    if (mode === "system") return "media";
    return mode;
  });
}
