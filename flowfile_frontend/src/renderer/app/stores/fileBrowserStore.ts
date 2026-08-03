import { defineStore } from "pinia";
import { isInternalFlowfilePath } from "../components/layout/Header/utils";
import { isCloudUri } from "../utils/storagePath";

export type SortByType = "name" | "size" | "last_modified" | "created_date";
export type SortDirectionType = "asc" | "desc";

/**
 * Context types for different file browser use cases.
 * Each context maintains its own independent path state.
 *
 * Cloud contexts are keyed by node role rather than by connection, so the number of
 * localStorage entries stays fixed however many connections a user cycles through.
 * Which connection a remembered path belongs to is tracked separately as its scope.
 */
export type FileBrowserContext = "flows" | "dataFiles" | "output" | "cloudRead" | "cloudWrite";

/**
 * State for a single file browser context
 */
interface ContextState {
  currentPath: string;
  lastUsedPath: string;
  /** Which storage the remembered path belongs to; null for the local filesystem. */
  scope: string | null;
}

/**
 * Get default path for a context.
 * Returns empty string to let the backend determine the initial path.
 */
function getDefaultPath(): string {
  return "";
}

/**
 * Load context state from localStorage.
 * Paths inside Flowfile's internal storage (``~/.flowfile/``) are discarded
 * so the file browser never reopens at an internal location the user didn't
 * choose — e.g. after quick-creating a flow.
 */
function loadContextState(context: FileBrowserContext): ContextState {
  const storedPath = localStorage.getItem(`fileBrowser_${context}_lastPath`);
  const safePath = storedPath && !isDiscardedPath(storedPath) ? storedPath : "";
  return {
    currentPath: safePath || getDefaultPath(),
    lastUsedPath: safePath || getDefaultPath(),
    scope: localStorage.getItem(`fileBrowser_${context}_lastScope`),
  };
}

/** Internal Flowfile paths are local-only; the check must not run on object-store URIs. */
function isDiscardedPath(path: string): boolean {
  return !isCloudUri(path) && isInternalFlowfilePath(path);
}

/**
 * Save context state to localStorage.  Internal Flowfile paths are not
 * persisted so they don't become the starting point next session.
 */
function saveContextPath(context: FileBrowserContext, path: string, scope: string | null): void {
  if (!path || isDiscardedPath(path)) return;
  localStorage.setItem(`fileBrowser_${context}_lastPath`, path);
  if (scope) {
    localStorage.setItem(`fileBrowser_${context}_lastScope`, scope);
  } else {
    localStorage.removeItem(`fileBrowser_${context}_lastScope`);
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
      // Output nodes keep their own remembered directory, independent from the
      // read-file browser, so "where I last wrote" doesn't follow "where I last read".
      output: loadContextState("output"),
      cloudRead: loadContextState("cloudRead"),
      cloudWrite: loadContextState("cloudWrite"),
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

    /**
     * Which storage the remembered path belongs to.
     * A caller whose scope differs must not reuse the path.
     */
    getScope:
      (state) =>
      (context: FileBrowserContext): string | null => {
        return state.contexts[context]?.scope ?? null;
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
    setCurrentPath(context: FileBrowserContext, path: string, scope: string | null = null) {
      if (!this.contexts[context]) {
        this.contexts[context] = { currentPath: "", lastUsedPath: "", scope: null };
      }
      this.contexts[context].currentPath = path;
      this.contexts[context].lastUsedPath = path;
      this.contexts[context].scope = scope;
      saveContextPath(context, path, scope);
    },

    /**
     * Initialize a context with a path if it doesn't have one yet
     */
    initializeContext(context: FileBrowserContext, defaultPath: string) {
      if (!this.contexts[context]) {
        this.contexts[context] = { currentPath: "", lastUsedPath: "", scope: null };
      }
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
        scope: null,
      };
      localStorage.removeItem(`fileBrowser_${context}_lastPath`);
      localStorage.removeItem(`fileBrowser_${context}_lastScope`);
    },
  },
});
