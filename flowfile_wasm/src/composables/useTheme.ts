import { computed } from "vue";
import { useThemeStore, type ThemeMode } from "../stores/theme-store";
import { storeToRefs } from "pinia";

/**
 * Composable for easy access to theme state and actions
 */
export function useTheme() {
  const themeStore = useThemeStore();
  const { mode, systemPreference } = storeToRefs(themeStore);

  const effectiveTheme = computed(() => themeStore.effectiveTheme);
  const isDark = computed(() => themeStore.isDark);

  const setTheme = (newMode: ThemeMode) => {
    themeStore.setTheme(newMode);
  };

  const toggleTheme = () => {
    themeStore.toggleTheme();
  };

  return {
    // State (reactive refs)
    mode,
    systemPreference,
    // Computed
    effectiveTheme,
    isDark,
    // Actions
    setTheme,
    toggleTheme,
  };
}
