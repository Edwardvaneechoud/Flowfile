import { defineStore } from "pinia";

export type SortByType = "name" | "size" | "last_modified" | "created_date";
export type SortDirectionType = "asc" | "desc";

/**
 * Context types for different file browser use cases.
 * Each context maintains its own independent path state.
 */
export type FileBrowserContext = "flows" | "dataFiles";

/**
 * State for a single file browser context
 */
interface ContextState {
  currentPath: string;
  lastUsedPath: string;
}

/**
 * Get default path for a context.
 * Returns empty string to let the backend determine the initial path.
 */
function getDefaultPath(): string {
  return "";
}

/**
 * Load context state from localStorage
 */
function loadContextState(context: FileBrowserContext): ContextState {
  const storedPath = localStorage.getItem(`fileBrowser_${context}_lastPath`);
  return {
    currentPath: storedPath || getDefaultPath(),
    lastUsedPath: storedPath || getDefaultPath(),
  };
}

/**
 * Save context state to localStorage
 */
function saveContextPath(context: FileBrowserContext, path: string): void {
  if (path) {
    localStorage.setItem(`fileBrowser_${context}_lastPath`, path);
  }
}

export const useFileBrowserStore = defineStore("fileBrowser", {
  state: () => ({
    // Initialize sort state from localStorage if available, otherwise use defaults.
    sortBy: (localStorage.getItem("fileBrowser_sortBy") as SortByType) || "name",
    sortDirection:
      (localStorage.getItem("fileBrowser_sortDirection") as SortDirectionType) || "asc",

    // Context-specific path states
    contexts: {
      flows: loadContextState("flows"),
      dataFiles: loadContextState("dataFiles"),
    } as Record<FileBrowserContext, ContextState>,
  }),

  getters: {
    /**
     * Get the current path for a specific context
     */
    getCurrentPath:
      (state) =>
      (context: FileBrowserContext): string => {
        return state.contexts[context]?.currentPath || "";
      },

    /**
     * Get the last used path for a specific context
     */
    getLastUsedPath:
      (state) =>
      (context: FileBrowserContext): string => {
        return state.contexts[context]?.lastUsedPath || "";
      },
  },

  actions: {
    setSortBy(newSortBy: SortByType) {
      this.sortBy = newSortBy;
      localStorage.setItem("fileBrowser_sortBy", newSortBy);
    },

    setSortDirection(newSortDirection: SortDirectionType) {
      this.sortDirection = newSortDirection;
      localStorage.setItem("fileBrowser_sortDirection", newSortDirection);
    },

    toggleSortDirection() {
      this.sortDirection = this.sortDirection === "asc" ? "desc" : "asc";
      localStorage.setItem("fileBrowser_sortDirection", this.sortDirection);
    },

    /**
     * Set the current path for a specific context
     */
    setCurrentPath(context: FileBrowserContext, path: string) {
      if (!this.contexts[context]) {
        this.contexts[context] = { currentPath: "", lastUsedPath: "" };
      }
      this.contexts[context].currentPath = path;
      this.contexts[context].lastUsedPath = path;
      saveContextPath(context, path);
    },

    /**
     * Initialize a context with a path if it doesn't have one yet
     */
    initializeContext(context: FileBrowserContext, defaultPath: string) {
      if (!this.contexts[context]) {
        this.contexts[context] = { currentPath: "", lastUsedPath: "" };
      }
      // Only set if no path is stored yet
      if (!this.contexts[context].currentPath && defaultPath) {
        this.contexts[context].currentPath = defaultPath;
      }
    },

    /**
     * Reset a context to its default state
     */
    resetContext(context: FileBrowserContext) {
      const defaultPath = getDefaultPath();
      this.contexts[context] = {
        currentPath: defaultPath,
        lastUsedPath: defaultPath,
      };
      localStorage.removeItem(`fileBrowser_${context}_lastPath`);
    },
  },
});
