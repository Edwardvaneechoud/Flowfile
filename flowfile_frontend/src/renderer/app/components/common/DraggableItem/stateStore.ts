import { defineStore } from "pinia";
import { ref, computed } from "vue";
import { Z_INDEX } from "./zIndex";

// Bump when the localStorage shape or coordinate system changes — old keys are
// purged on next load. Last bump: right-side panels merged into one tabbed
// `rightDrawer`, so the old per-panel ids (nodeSettings/aiAssistant/...) are gone.
const STORAGE_VERSION = 3;
const itemStorageKey = (id: string) => `overlayPositionAndSize.v${STORAGE_VERSION}_${id}`;
const groupsStorageKey = `overlayGroups.v${STORAGE_VERSION}`;

const purgeLegacyKeys = () => {
  try {
    for (const key of Object.keys(localStorage)) {
      // Drop any panel-position / group key from an older version (underscore
      // pre-v2 legacy AND dotted `.v2_` etc.) — keep only the current version.
      if (
        key.startsWith("overlayPositionAndSize") &&
        !key.includes(`.v${STORAGE_VERSION}_`)
      ) {
        localStorage.removeItem(key);
      }
      if (key.startsWith("overlayGroups") && key !== groupsStorageKey) {
        localStorage.removeItem(key);
      }
    }
  } catch {
    // localStorage can throw in private mode / quota exhaustion — ignore.
  }
};

// Looks up the canvas <main> element so fullscreen panels fill the canvas
// region (not the viewport, which would overlap the page header).
const getCanvasBounds = (): { width: number; height: number } => {
  if (typeof document === "undefined") {
    return { width: 0, height: 0 };
  }
  const main = document.querySelector("main");
  if (main) {
    const rect = main.getBoundingClientRect();
    return { width: rect.width, height: rect.height };
  }
  return { width: window.innerWidth, height: window.innerHeight };
};

// Per-axis resize response: "scale" keeps a constant gap to the edge, "fill"
// stretches to the container, "fixed" stays put.
export type AxisBehaviour = "scale" | "fixed" | "fill";

export interface ItemLayout {
  width: number;
  height: number;
  left: number;
  top: number;
  stickynessPosition: "top" | "bottom" | "left" | "right" | "free" | "bottom-center";
  fullWidth: boolean;
  fullHeight: boolean;
  zIndex: number;
  fullScreen: boolean;
  prevWidth?: number;
  prevHeight?: number;
  prevLeft?: number;
  prevTop?: number;
  clicked: boolean;
  group?: string;
  syncDimensions?: boolean;
}

export interface ItemInitialState {
  width?: number;
  height?: number;
  left?: number;
  top?: number;
  stickynessPosition?: "top" | "bottom" | "left" | "right" | "free";
  group?: string;
  syncDimensions?: boolean;
  fullWidth?: boolean;
  fullHeight?: boolean;
}

export const useItemStore = defineStore("itemStore", () => {
  // Run-once cleanup of pre-v2 localStorage keys on first store instantiation.
  purgeLegacyKeys();

  const items = ref<Record<string, ItemLayout>>({});
  const initialItemStates = ref<Record<string, ItemInitialState>>({});
  const groups = ref<Record<string, string[]>>({});
  const inResizing = ref(false);
  const idItemClicked = ref<string | null>(null);
  const idItemVisible = ref<string | null>(null);

  // Z-index constants (see zIndex.ts for the full hierarchy).
  const BASE_Z_INDEX = Z_INDEX.PANEL_BASE;
  const MAX_Z_INDEX = Z_INDEX.PANEL_MAX;
  const FULLSCREEN_Z_INDEX = Z_INDEX.FULLSCREEN;

  // Per-id debounce timers for localStorage writes. Drag/resize fires at every
  // mousemove (~60 Hz), so without throttling we'd hammer localStorage with
  // hundreds of writes per gesture. 250 ms trailing-edge is invisible to the
  // user and survives reload because stop handlers flush immediately.
  const writeTimers = new Map<string, ReturnType<typeof setTimeout>>();
  const SAVE_DEBOUNCE_MS = 250;

  const persistItem = (id: string) => {
    const state = items.value[id];
    if (!state) return;
    try {
      localStorage.setItem(itemStorageKey(id), JSON.stringify(state));
      if (state.group) {
        localStorage.setItem(groupsStorageKey, JSON.stringify({ groups: groups.value }));
      }
    } catch {
      // Ignore quota / private-mode failures.
    }
  };

  const layoutPresets = {
    sidePanel: { width: 400, height: "100%" },
    bottomPanel: { width: "100%", height: 300 },
    dataView: { width: 600, height: 400 },
    logView: { width: 600, height: 400 },
  };

  const getGroupItems = computed(() => (groupName: string) => {
    if (!groups.value[groupName]) return [];
    return groups.value[groupName].map((id) => items.value[id]).filter(Boolean);
  });

  const registerInitialState = (id: string, initialState: ItemInitialState) => {
    // Only register if not already registered (preserve the true initial state)
    if (!initialItemStates.value[id]) {
      initialItemStates.value[id] = { ...initialState };
    }
  };

  const normalizeZIndices = () => {
    const nonFullscreenEntries = Object.entries(items.value)
      .filter(([, item]) => !item.fullScreen)
      .sort((a, b) => a[1].zIndex - b[1].zIndex);

    nonFullscreenEntries.forEach(([entryId, item], index) => {
      item.zIndex = BASE_Z_INDEX + index;
      saveItemState(entryId);
    });
  };

  const bringToFront = (id: string) => {
    if (!items.value[id]) {
      console.warn(`Item ${id} not found`);
      return;
    }

    if (items.value[id].fullScreen) return;

    let maxZIndex = BASE_Z_INDEX - 1;
    Object.entries(items.value).forEach(([itemId, item]) => {
      if (!item.fullScreen && itemId !== id) {
        maxZIndex = Math.max(maxZIndex, item.zIndex);
      }
    });

    if (items.value[id].zIndex > maxZIndex) return;

    items.value[id].zIndex = maxZIndex + 1;

    // Normalize if z-indices are getting too high to prevent unbounded growth
    if (items.value[id].zIndex > MAX_Z_INDEX) {
      normalizeZIndices();
    } else {
      saveItemState(id);
    }
  };

  const setItemState = (id: string, state: Partial<ItemLayout>) => {
    if (!items.value[id]) {
      items.value[id] = {
        width: 400,
        height: 300,
        left: 100,
        top: 100,
        stickynessPosition: "free",
        fullWidth: false,
        fullHeight: false,
        zIndex: BASE_Z_INDEX,
        fullScreen: false,
        clicked: false,
      };
    }

    const oldGroup = items.value[id].group;
    Object.assign(items.value[id], state);

    if (state.group !== undefined) {
      if (oldGroup && groups.value[oldGroup]) {
        groups.value[oldGroup] = groups.value[oldGroup].filter((itemId) => itemId !== id);
      }

      if (state.group) {
        if (!groups.value[state.group]) {
          groups.value[state.group] = [];
        }
        if (!groups.value[state.group].includes(id)) {
          groups.value[state.group].push(id);
        }

        if (state.syncDimensions) {
          syncGroupDimensions(state.group, id);
        }
      }
    }
  };

  const syncGroupDimensions = (groupName: string, sourceId?: string) => {
    const groupItems = groups.value[groupName];
    if (!groupItems || groupItems.length < 2) return;

    const referenceId = sourceId || groupItems[0];
    const reference = items.value[referenceId];
    if (!reference) return;

    groupItems.forEach((id) => {
      if (id !== referenceId && items.value[id]?.syncDimensions) {
        items.value[id].width = reference.width;
        items.value[id].height = reference.height;
        saveItemState(id);
      }
    });
  };

  const arrangeItems = (arrangement: "cascade" | "tile" | "stack") => {
    const visibleItems = Object.entries(items.value)
      .filter(([, item]) => !item.fullScreen)
      .sort((a, b) => a[1].zIndex - b[1].zIndex);

    switch (arrangement) {
      case "cascade": {
        let offset = 0;
        visibleItems.forEach(([id, item]) => {
          item.left = 100 + offset;
          item.top = 100 + offset;
          item.stickynessPosition = "free";
          offset += 30;
          saveItemState(id);
        });
        break;
      }

      case "tile": {
        const screenWidth = window.innerWidth;
        const screenHeight = window.innerHeight;
        const itemCount = visibleItems.length;
        const cols = Math.ceil(Math.sqrt(itemCount));
        const rows = Math.ceil(itemCount / cols);
        const itemWidth = Math.floor(screenWidth / cols) - 20;
        const itemHeight = Math.floor(screenHeight / rows) - 20;

        visibleItems.forEach(([id, item], index) => {
          const col = index % cols;
          const row = Math.floor(index / cols);
          item.left = col * (itemWidth + 10) + 10;
          item.top = row * (itemHeight + 10) + 10;
          item.width = itemWidth;
          item.height = itemHeight;
          item.stickynessPosition = "free";
          saveItemState(id);
        });
        break;
      }

      case "stack": {
        visibleItems.forEach(([id, item]) => {
          item.left = 100;
          item.top = 100;
          item.stickynessPosition = "free";
          saveItemState(id);
        });
        break;
      }
    }
  };

  const saveItemState = (id: string) => {
    const existing = writeTimers.get(id);
    if (existing) clearTimeout(existing);
    writeTimers.set(
      id,
      setTimeout(() => {
        writeTimers.delete(id);
        persistItem(id);
      }, SAVE_DEBOUNCE_MS),
    );
  };

  // Flush a pending write immediately. Call from drag-stop / resize-stop so
  // the final state is durable even if the user closes the tab in <250 ms.
  const flushItemState = (id: string) => {
    const existing = writeTimers.get(id);
    if (existing) {
      clearTimeout(existing);
      writeTimers.delete(id);
    }
    persistItem(id);
  };

  const loadItemState = (id: string) => {
    const savedState = localStorage.getItem(itemStorageKey(id));
    if (savedState) {
      try {
        const state = JSON.parse(savedState);
        // Clamp restored z-index to prevent inflated values from localStorage.
        if (state.zIndex !== undefined && state.zIndex > MAX_Z_INDEX) {
          state.zIndex = BASE_Z_INDEX;
        }
        setItemState(id, state);
      } catch {
        // Corrupted entry — drop it so next save overwrites cleanly.
        localStorage.removeItem(itemStorageKey(id));
      }
    }

    const savedGroups = localStorage.getItem(groupsStorageKey);
    if (savedGroups) {
      try {
        const groupData = JSON.parse(savedGroups);
        groups.value = groupData.groups || {};
      } catch {
        localStorage.removeItem(groupsStorageKey);
      }
    }
  };

  const applyPreset = (id: string, presetName: keyof typeof layoutPresets) => {
    const preset = layoutPresets[presetName];
    const updates: Partial<ItemLayout> = {};

    if (preset.width === "100%") {
      updates.width = window.innerWidth;
      updates.fullWidth = true;
    } else {
      updates.width = preset.width as number;
      updates.fullWidth = false;
    }

    if (preset.height === "100%") {
      updates.height = window.innerHeight;
      updates.fullHeight = true;
    } else {
      updates.height = preset.height as number;
      updates.fullHeight = false;
    }

    setItemState(id, updates);
    saveItemState(id);
  };

  const toggleFullScreen = (id: string) => {
    if (!items.value[id]) return;
    setFullScreen(id, !items.value[id].fullScreen);
  };

  const setFullScreen = (id: string, fullScreen: boolean) => {
    if (!items.value[id]) return;

    if (items.value[id].fullScreen !== fullScreen) {
      if (fullScreen) {
        Object.keys(items.value).forEach((otherId) => {
          if (otherId !== id) {
            items.value[otherId].zIndex = 1;
          }
        });

        items.value[id].fullScreen = true;
        items.value[id].prevWidth = items.value[id].width;
        items.value[id].prevHeight = items.value[id].height;
        items.value[id].prevLeft = items.value[id].left;
        items.value[id].prevTop = items.value[id].top;

        // Fill the canvas region (main element), not the viewport — panels
        // are positioned inside <main> so width/height are container-relative.
        const bounds = getCanvasBounds();
        items.value[id].width = bounds.width;
        items.value[id].height = bounds.height;
        items.value[id].left = 0;
        items.value[id].top = 0;
        items.value[id].zIndex = FULLSCREEN_Z_INDEX;
      } else {
        items.value[id].fullScreen = false;
        items.value[id].width = items.value[id].prevWidth || 400;
        items.value[id].height = items.value[id].prevHeight || 300;
        items.value[id].left = items.value[id].prevLeft || 100;
        items.value[id].top = items.value[id].prevTop || 100;

        Object.keys(items.value).forEach((otherId) => {
          items.value[otherId].zIndex = BASE_Z_INDEX;
        });
      }

      flushItemState(id);
      clickOnItem(id);
    }
  };

  const resetLayout = () => {
    // Cancel any pending debounced writes — we're about to overwrite state.
    writeTimers.forEach((timer) => clearTimeout(timer));
    writeTimers.clear();

    Object.keys(items.value).forEach((id) => {
      localStorage.removeItem(itemStorageKey(id));
    });

    localStorage.removeItem(groupsStorageKey);

    groups.value = {};

    Object.keys(initialItemStates.value).forEach((id) => {
      const initialState = initialItemStates.value[id];
      if (!initialState) return;

      const resetState: ItemLayout = {
        width: initialState.width || 400,
        height: initialState.height || 300,
        left: initialState.left || 100,
        top: initialState.top || 100,
        stickynessPosition: initialState.stickynessPosition || "free",
        fullWidth: initialState.fullWidth || false,
        fullHeight: initialState.fullHeight || false,
        zIndex: BASE_Z_INDEX,
        fullScreen: false,
        clicked: false,
        group: initialState.group,
        syncDimensions: initialState.syncDimensions,
      };

      items.value[id] = resetState;

      if (resetState.group) {
        if (!groups.value[resetState.group]) {
          groups.value[resetState.group] = [];
        }
        if (!groups.value[resetState.group].includes(id)) {
          groups.value[resetState.group].push(id);
        }
      }
    });

    // Emit a custom event to notify components to re-apply sticky positions
    // This event should trigger each DraggableItem to call its applyStickyPosition method
    setTimeout(() => {
      window.dispatchEvent(
        new CustomEvent("layout-reset", {
          detail: { initialStates: initialItemStates.value },
        }),
      );
    }, 0);
  };

  const resetSingleItem = (id: string) => {
    const initialState = initialItemStates.value[id];
    if (!initialState) {
      console.warn(`No initial state found for item ${id}`);
      return;
    }

    const pending = writeTimers.get(id);
    if (pending) {
      clearTimeout(pending);
      writeTimers.delete(id);
    }

    localStorage.removeItem(itemStorageKey(id));

    const resetState: ItemLayout = {
      width: initialState.width || 400,
      height: initialState.height || 300,
      left: initialState.left || 100,
      top: initialState.top || 100,
      stickynessPosition: initialState.stickynessPosition || "free",
      fullWidth: initialState.fullWidth || false,
      fullHeight: initialState.fullHeight || false,
      zIndex: items.value[id]?.zIndex || BASE_Z_INDEX,
      fullScreen: false,
      clicked: false,
      group: initialState.group,
      syncDimensions: initialState.syncDimensions,
    };

    items.value[id] = resetState;
  };

  const clickOnItem = (id: string) => {
    if (!items.value[id] || items.value[id].fullScreen) return;

    bringToFront(id);
    idItemClicked.value = id;
  };

  const setResizing = (resizing: boolean) => {
    inResizing.value = resizing;
  };

  const getResizing = () => {
    return inResizing.value;
  };

  const scrollOnItem = (id: string) => {
    const itemElement = document.getElementById(id);
    if (!itemElement) return;

    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            idItemVisible.value = id;
            bringToFront(id);
          } else if (idItemVisible.value === id) {
            items.value[id].zIndex = BASE_Z_INDEX;
            idItemVisible.value = null;
          }
        });
      },
      {
        threshold: 0.5,
      },
    );

    observer.observe(itemElement);
  };

  const hasSavedState = (id: string): boolean => {
    try {
      return localStorage.getItem(itemStorageKey(id)) !== null;
    } catch {
      return false;
    }
  };

  return {
    inResizing,
    items,
    groups,
    layoutPresets,
    initialItemStates,
    registerInitialState,
    setItemState,
    saveItemState,
    flushItemState,
    loadItemState,
    hasSavedState,
    setResizing,
    getResizing,
    clickOnItem,
    scrollOnItem,
    idItemVisible,
    toggleFullScreen,
    setFullScreen,
    arrangeItems,
    syncGroupDimensions,
    applyPreset,
    getGroupItems,
    resetLayout,
    resetSingleItem,
    bringToFront,
  };
});
