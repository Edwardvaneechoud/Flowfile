import { defineStore } from "pinia";

export type ThemeMode = "light" | "dark" | "system";

interface ThemeState {
  mode: ThemeMode;
  systemPreference: "light" | "dark";
  /** When true, theme is applied only to store state (not to document.documentElement) */
  embedded: boolean;
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
  return "dark";
}

export const useThemeStore = defineStore("theme", {
  state: (): ThemeState => ({
    mode: getSavedTheme(),
    systemPreference: getSystemPreference(),
    embedded: false,
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
     * Mark this store as operating in embedded mode.
     * In embedded mode, applyTheme only updates store state
     * and does NOT modify document.documentElement.
     */
    setEmbedded(value: boolean) {
      this.embedded = value;
    },

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
     * Applies the current theme to the document (or just updates state in embedded mode)
     */
    applyTheme() {
      const theme = this.effectiveTheme;
      // In embedded mode, the FlowfileEditor wrapper component
      // reads effectiveTheme and applies data-theme to its own root div.
      // We skip modifying the global document element.
      if (!this.embedded) {
        document.documentElement.setAttribute("data-theme", theme);
      }
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
