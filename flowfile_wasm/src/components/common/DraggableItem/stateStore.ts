import { defineStore } from 'pinia'
import { ref } from 'vue'
import { Z_INDEX } from './zIndex'

// Bump when the localStorage shape or coordinate system changes — old keys are
// purged on next load.
const STORAGE_VERSION = 1
const itemStorageKey = (id: string) => `overlayPositionAndSize.v${STORAGE_VERSION}_${id}`
const groupsStorageKey = `overlayGroups.v${STORAGE_VERSION}`
// Key written by the removed pre-port panel system (DraggablePanel/panel-store).
const LEGACY_PANEL_STORE_KEY = 'flowfile-panel-state-v2'

const purgeLegacyKeys = () => {
  try {
    for (const key of Object.keys(localStorage)) {
      // Drop any panel-position / group key from an older version — keep only
      // the current version.
      if (key.startsWith('overlayPositionAndSize') && !key.includes(`.v${STORAGE_VERSION}_`)) {
        localStorage.removeItem(key)
      }
      if (key.startsWith('overlayGroups') && key !== groupsStorageKey) {
        localStorage.removeItem(key)
      }
    }
    // Drop the orphaned key from the removed DraggablePanel system.
    localStorage.removeItem(LEGACY_PANEL_STORE_KEY)
  } catch {
    // localStorage can throw in private mode / quota exhaustion — ignore.
  }
}

// Looks up the canvas container so fullscreen panels fill the canvas region
// (not the viewport, which would overlap the app header/toolbar). Uses
// clientWidth/Height (untransformed layout box) to match getStickyContainer so
// a host CSS transform:scale keeps fullscreen and docking consistent.
const getCanvasBounds = (): { width: number; height: number } => {
  if (typeof document === 'undefined') {
    return { width: 0, height: 0 }
  }
  const container = document.querySelector('.canvas-container')
  if (container) {
    return { width: container.clientWidth, height: container.clientHeight }
  }
  return { width: window.innerWidth, height: window.innerHeight }
}

// Per-axis resize response: "scale" keeps a constant gap to the edge, "fill"
// stretches to the container, "fixed" stays put.
export type AxisBehaviour = 'scale' | 'fixed' | 'fill'

export interface ItemLayout {
  width: number
  height: number
  left: number
  top: number
  stickynessPosition: 'top' | 'bottom' | 'left' | 'right' | 'free' | 'bottom-center'
  fullWidth: boolean
  fullHeight: boolean
  zIndex: number
  fullScreen: boolean
  prevWidth?: number
  prevHeight?: number
  prevLeft?: number
  prevTop?: number
  clicked: boolean
  // Collapsed-to-header state, persisted so it survives a reload.
  minimized?: boolean
  group?: string
  syncDimensions?: boolean
}

export interface ItemInitialState {
  width?: number
  height?: number
  left?: number
  top?: number
  stickynessPosition?: 'top' | 'bottom' | 'left' | 'right' | 'free'
  group?: string
  syncDimensions?: boolean
  fullWidth?: boolean
  fullHeight?: boolean
}

export const useItemStore = defineStore('itemStore', () => {
  // Run-once cleanup of legacy localStorage keys on first store instantiation.
  purgeLegacyKeys()

  const items = ref<Record<string, ItemLayout>>({})
  const initialItemStates = ref<Record<string, ItemInitialState>>({})
  const groups = ref<Record<string, string[]>>({})
  const inResizing = ref(false)
  const idItemClicked = ref<string | null>(null)

  // Z-index constants (see zIndex.ts for the full hierarchy).
  const BASE_Z_INDEX = Z_INDEX.PANEL_BASE
  const MAX_Z_INDEX = Z_INDEX.PANEL_MAX
  const FULLSCREEN_Z_INDEX = Z_INDEX.FULLSCREEN

  // Per-id debounce timers for localStorage writes. Drag/resize fires at every
  // mousemove (~60 Hz), so without throttling we'd hammer localStorage with
  // hundreds of writes per gesture. 250 ms trailing-edge is invisible to the
  // user and survives reload because stop handlers flush immediately.
  const writeTimers = new Map<string, ReturnType<typeof setTimeout>>()
  const SAVE_DEBOUNCE_MS = 250

  const persistItem = (id: string) => {
    const state = items.value[id]
    if (!state) return
    try {
      localStorage.setItem(itemStorageKey(id), JSON.stringify(state))
      if (state.group) {
        localStorage.setItem(groupsStorageKey, JSON.stringify({ groups: groups.value }))
      }
    } catch {
      // Ignore quota / private-mode failures.
    }
  }

  const registerInitialState = (id: string, initialState: ItemInitialState) => {
    // Only register if not already registered (preserve the true initial state).
    if (!initialItemStates.value[id]) {
      initialItemStates.value[id] = { ...initialState }
    }
  }

  const normalizeZIndices = () => {
    const nonFullscreenEntries = Object.entries(items.value)
      .filter(([, item]) => !item.fullScreen)
      .sort((a, b) => a[1].zIndex - b[1].zIndex)

    nonFullscreenEntries.forEach(([entryId, item], index) => {
      item.zIndex = BASE_Z_INDEX + index
      saveItemState(entryId)
    })
  }

  const bringToFront = (id: string) => {
    if (!items.value[id]) {
      return
    }

    if (items.value[id].fullScreen) return

    let maxZIndex = BASE_Z_INDEX - 1
    Object.entries(items.value).forEach(([itemId, item]) => {
      if (!item.fullScreen && itemId !== id) {
        maxZIndex = Math.max(maxZIndex, item.zIndex)
      }
    })

    if (items.value[id].zIndex > maxZIndex) return

    items.value[id].zIndex = maxZIndex + 1

    // Normalize if z-indices are getting too high to prevent unbounded growth.
    if (items.value[id].zIndex > MAX_Z_INDEX) {
      normalizeZIndices()
    } else {
      saveItemState(id)
    }
  }

  const setItemState = (id: string, state: Partial<ItemLayout>) => {
    if (!items.value[id]) {
      items.value[id] = {
        width: 400,
        height: 300,
        left: 100,
        top: 100,
        stickynessPosition: 'free',
        fullWidth: false,
        fullHeight: false,
        zIndex: BASE_Z_INDEX,
        fullScreen: false,
        clicked: false,
      }
    }

    const oldGroup = items.value[id].group
    Object.assign(items.value[id], state)

    if (state.group !== undefined) {
      if (oldGroup && groups.value[oldGroup]) {
        groups.value[oldGroup] = groups.value[oldGroup].filter((itemId) => itemId !== id)
      }

      if (state.group) {
        if (!groups.value[state.group]) {
          groups.value[state.group] = []
        }
        if (!groups.value[state.group].includes(id)) {
          groups.value[state.group].push(id)
        }

        if (state.syncDimensions) {
          syncGroupDimensions(state.group, id)
        }
      }
    }
  }

  const syncGroupDimensions = (groupName: string, sourceId?: string) => {
    const groupItems = groups.value[groupName]
    if (!groupItems || groupItems.length < 2) return

    const referenceId = sourceId || groupItems[0]
    const reference = items.value[referenceId]
    if (!reference) return

    groupItems.forEach((id) => {
      if (id !== referenceId && items.value[id]?.syncDimensions) {
        items.value[id].width = reference.width
        items.value[id].height = reference.height
        saveItemState(id)
      }
    })
  }

  const saveItemState = (id: string) => {
    const existing = writeTimers.get(id)
    if (existing) clearTimeout(existing)
    writeTimers.set(
      id,
      setTimeout(() => {
        writeTimers.delete(id)
        persistItem(id)
      }, SAVE_DEBOUNCE_MS),
    )
  }

  // Flush a pending write immediately. Call from drag-stop / resize-stop so
  // the final state is durable even if the user closes the tab in <250 ms.
  const flushItemState = (id: string) => {
    const existing = writeTimers.get(id)
    if (existing) {
      clearTimeout(existing)
      writeTimers.delete(id)
    }
    persistItem(id)
  }

  const loadItemState = (id: string) => {
    const savedState = localStorage.getItem(itemStorageKey(id))
    if (savedState) {
      try {
        const state = JSON.parse(savedState)
        // Clamp restored z-index to prevent inflated values from localStorage.
        if (state.zIndex !== undefined && state.zIndex > MAX_Z_INDEX) {
          state.zIndex = BASE_Z_INDEX
        }
        setItemState(id, state)
      } catch {
        // Corrupted entry — drop it so next save overwrites cleanly.
        localStorage.removeItem(itemStorageKey(id))
      }
    }

    const savedGroups = localStorage.getItem(groupsStorageKey)
    if (savedGroups) {
      try {
        const groupData = JSON.parse(savedGroups)
        groups.value = groupData.groups || {}
      } catch {
        localStorage.removeItem(groupsStorageKey)
      }
    }
  }

  const toggleFullScreen = (id: string) => {
    if (!items.value[id]) return
    setFullScreen(id, !items.value[id].fullScreen)
  }

  const setFullScreen = (id: string, fullScreen: boolean) => {
    if (!items.value[id]) return

    if (items.value[id].fullScreen !== fullScreen) {
      if (fullScreen) {
        Object.keys(items.value).forEach((otherId) => {
          if (otherId !== id) {
            items.value[otherId].zIndex = 1
          }
        })

        items.value[id].fullScreen = true
        items.value[id].prevWidth = items.value[id].width
        items.value[id].prevHeight = items.value[id].height
        items.value[id].prevLeft = items.value[id].left
        items.value[id].prevTop = items.value[id].top

        // Fill the canvas region (container), not the viewport — panels are
        // positioned inside the container so width/height are container-relative.
        const bounds = getCanvasBounds()
        items.value[id].width = bounds.width
        items.value[id].height = bounds.height
        items.value[id].left = 0
        items.value[id].top = 0
        items.value[id].zIndex = FULLSCREEN_Z_INDEX
      } else {
        items.value[id].fullScreen = false
        // ?? not || — a legitimately-saved 0 (panel flush at top/left) must not
        // fall through to the default and shift the panel on restore.
        items.value[id].width = items.value[id].prevWidth ?? 400
        items.value[id].height = items.value[id].prevHeight ?? 300
        items.value[id].left = items.value[id].prevLeft ?? 100
        items.value[id].top = items.value[id].prevTop ?? 100

        Object.keys(items.value).forEach((otherId) => {
          items.value[otherId].zIndex = BASE_Z_INDEX
        })
      }

      flushItemState(id)
      clickOnItem(id)
    }
  }

  const resetLayout = () => {
    // Cancel any pending debounced writes — we're about to overwrite state.
    writeTimers.forEach((timer) => clearTimeout(timer))
    writeTimers.clear()

    try {
      Object.keys(items.value).forEach((id) => {
        localStorage.removeItem(itemStorageKey(id))
      })
      localStorage.removeItem(groupsStorageKey)
    } catch {
      // localStorage can throw in blocked-storage / private-mode contexts — the
      // in-memory reset below still runs so the layout resets this session.
    }

    groups.value = {}

    Object.keys(initialItemStates.value).forEach((id) => {
      const initialState = initialItemStates.value[id]
      if (!initialState) return

      const resetState: ItemLayout = {
        width: initialState.width || 400,
        height: initialState.height || 300,
        left: initialState.left || 100,
        top: initialState.top || 100,
        stickynessPosition: initialState.stickynessPosition || 'free',
        fullWidth: initialState.fullWidth || false,
        fullHeight: initialState.fullHeight || false,
        zIndex: BASE_Z_INDEX,
        fullScreen: false,
        clicked: false,
        minimized: false,
        group: initialState.group,
        syncDimensions: initialState.syncDimensions,
      }

      items.value[id] = resetState

      if (resetState.group) {
        if (!groups.value[resetState.group]) {
          groups.value[resetState.group] = []
        }
        if (!groups.value[resetState.group].includes(id)) {
          groups.value[resetState.group].push(id)
        }
      }
    })

    // Notify each DraggableItem to re-apply its sticky position.
    setTimeout(() => {
      window.dispatchEvent(
        new CustomEvent('layout-reset', {
          detail: { initialStates: initialItemStates.value },
        }),
      )
    }, 0)
  }

  const clickOnItem = (id: string) => {
    if (!items.value[id] || items.value[id].fullScreen) return

    bringToFront(id)
    idItemClicked.value = id
  }

  const hasSavedState = (id: string): boolean => {
    try {
      return localStorage.getItem(itemStorageKey(id)) !== null
    } catch {
      return false
    }
  }

  return {
    inResizing,
    items,
    groups,
    initialItemStates,
    registerInitialState,
    setItemState,
    saveItemState,
    flushItemState,
    loadItemState,
    hasSavedState,
    clickOnItem,
    toggleFullScreen,
    setFullScreen,
    syncGroupDimensions,
    resetLayout,
    bringToFront,
  }
})
