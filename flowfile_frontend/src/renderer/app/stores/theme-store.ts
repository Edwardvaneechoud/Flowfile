import { defineStore } from "pinia";

export type ThemeMode = "light" | "dark" | "system";

interface ThemeState {
  mode: ThemeMode;
  systemPreference: "light" | "dark";
}

const THEME_STORAGE_KEY = "flowfile-theme-preference";

function getSystemPreference(): "light" | "dark" {
  if (typeof window !== "undefined" && window.matchMedia) {
    return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  }
  return "light";
}

function getSavedTheme(): ThemeMode {
  if (typeof localStorage !== "undefined") {
    const saved = localStorage.getItem(THEME_STORAGE_KEY);
    if (saved === "light" || saved === "dark" || saved === "system") {
      return saved;
    }
  }
  return "light";
}

export const useThemeStore = defineStore("theme", {
  state: (): ThemeState => ({
    mode: getSavedTheme(),
    systemPreference: getSystemPreference(),
  }),

  getters: {
    /**
     * Returns the effective theme based on mode and system preference
     */
    effectiveTheme: (state): "light" | "dark" => {
      if (state.mode === "system") {
        return state.systemPreference;
      }
      return state.mode;
    },

    /**
     * Returns true if dark mode is active
     */
    isDark(): boolean {
      return this.effectiveTheme === "dark";
    },
  },

  actions: {
    /**
     * Sets the theme mode and persists to localStorage
     */
    setTheme(mode: ThemeMode) {
      this.mode = mode;
      localStorage.setItem(THEME_STORAGE_KEY, mode);
      this.applyTheme();
    },

    /**
     * Toggles between light and dark mode
     */
    toggleTheme() {
      const newTheme = this.effectiveTheme === "light" ? "dark" : "light";
      this.setTheme(newTheme);
    },

    /**
     * Applies the current theme to the document
     */
    applyTheme() {
      const theme = this.effectiveTheme;
      document.documentElement.setAttribute("data-theme", theme);
    },

    /**
     * Updates the system preference (called when OS theme changes)
     */
    updateSystemPreference() {
      this.systemPreference = getSystemPreference();
      if (this.mode === "system") {
        this.applyTheme();
      }
    },

    /**
     * Initialize theme on app startup
     */
    initialize() {
      // Apply initial theme
      this.applyTheme();

      // Listen for system theme changes
      if (typeof window !== "undefined" && window.matchMedia) {
        const mediaQuery = window.matchMedia("(prefers-color-scheme: dark)");
        mediaQuery.addEventListener("change", () => {
          this.updateSystemPreference();
        });
      }
    },
  },
});
