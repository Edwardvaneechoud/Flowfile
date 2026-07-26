import { defineStore } from 'pinia'
import { ref } from 'vue'

import { defaultIntent, type Bounds, type PanelConfig, type PanelIntent } from './layoutGeometry'
import {
  STORAGE_VERSION,
  intentStorageKey,
  legacyV3StorageKey,
  migrateV3,
  sanitizeIntent,
} from './layoutIntent'
import { Z_INDEX } from './zIndex'

// The store holds per-panel USER INTENT (see layoutGeometry.ts for the
// intent-vs-derived contract) plus session state: z-order, minimized flags,
// and the shared canvas container bounds. commitIntent is the ONLY intent
// localStorage writer, and it only runs on explicit user gestures — derived
// geometry is never saved, so a mis-measured container can no longer corrupt
// a stored layout. wasm keeps one extra persisted bit the desktop app treats
// as transient: the collapsed-to-header (minimized) state, under a parallel
// per-panel key so the intent records stay byte-compatible with the app's.

// Collapsed-to-header state, persisted so it survives a reload (wasm-only).
const minimizedStorageKey = (id: string): string => `overlayMinimized.v${STORAGE_VERSION}_${id}`

// Key written by the removed pre-port panel system (DraggablePanel/panel-store).
const LEGACY_PANEL_STORE_KEY = 'flowfile-panel-state-v2'

const safeParse = (raw: string): unknown => {
  try {
    return JSON.parse(raw)
  } catch {
    return null
  }
}

const purgeLegacyKeys = () => {
  try {
    for (const key of Object.keys(localStorage)) {
      // Keep current-version intent records and v3 records (consumed by the
      // one-time migration at panel registration); drop anything older.
      if (
        key.startsWith('overlayPositionAndSize') &&
        !key.includes(`.v${STORAGE_VERSION}_`) &&
        !key.includes('.v3_')
      ) {
        localStorage.removeItem(key)
      }
      if (key.startsWith('overlayMinimized') && !key.includes(`.v${STORAGE_VERSION}_`)) {
        localStorage.removeItem(key)
      }
      // Panel grouping was removed (it never had live consumers).
      if (key.startsWith('overlayGroups')) {
        localStorage.removeItem(key)
      }
    }
    // Drop the orphaned key from the removed DraggablePanel system.
    localStorage.removeItem(LEGACY_PANEL_STORE_KEY)
  } catch {
    // localStorage can throw in private mode / quota exhaustion — ignore.
  }
}

export const useItemStore = defineStore('itemStore', () => {
  purgeLegacyKeys()

  const items = ref<Record<string, PanelIntent>>({})
  const panelConfigs = ref<Record<string, PanelConfig>>({})
  const zIndices = ref<Record<string, number>>({})
  const minimized = ref<Record<string, boolean>>({})
  const containerBounds = ref<Bounds>({ width: 0, height: 0 })
  const inResizing = ref(false)

  const BASE_Z_INDEX = Z_INDEX.PANEL_BASE
  const MAX_Z_INDEX = Z_INDEX.PANEL_MAX
  const FULLSCREEN_Z_INDEX = Z_INDEX.FULLSCREEN

  // --- container -----------------------------------------------------------

  let containerObserver: ResizeObserver | null = null

  // One shared measurement of the canvas container — panels derive geometry
  // from this instead of each observing (and mis-resolving) their own
  // offsetParent.
  const registerContainer = (el: HTMLElement): (() => void) => {
    containerObserver?.disconnect()
    containerBounds.value = { width: el.clientWidth, height: el.clientHeight }
    containerObserver = new ResizeObserver((entries) => {
      const entry = entries[0]
      if (entry) {
        containerBounds.value = {
          width: entry.contentRect.width,
          height: entry.contentRect.height,
        }
      }
    })
    containerObserver.observe(el)
    return () => {
      containerObserver?.disconnect()
      containerObserver = null
      // Keep the last bounds: zeroing them would blank every panel rect
      // during a route transition.
    }
  }

  // --- intent load / persist ----------------------------------------------

  const loadIntent = (id: string, cfg: PanelConfig): PanelIntent => {
    const v4Key = intentStorageKey(id)
    try {
      const rawV4 = localStorage.getItem(v4Key)
      if (rawV4 !== null) {
        const intent = sanitizeIntent(safeParse(rawV4))
        if (intent) return intent
        // Degenerate record — drop it so the panel heals to defaults.
        localStorage.removeItem(v4Key)
      }
      const v3Key = legacyV3StorageKey(id)
      const rawV3 = localStorage.getItem(v3Key)
      if (rawV3 !== null) {
        localStorage.removeItem(v3Key)
        const parsed = safeParse(rawV3)
        // wasm's v3 records carried the collapsed state inline — carry it over.
        if ((parsed as { minimized?: unknown } | null)?.minimized === true) {
          localStorage.setItem(minimizedStorageKey(id), '1')
        }
        const migrated = migrateV3(parsed, cfg)
        if (migrated) {
          localStorage.setItem(v4Key, JSON.stringify(migrated))
          return migrated
        }
      }
    } catch {
      // localStorage unavailable — session-only layout.
    }
    return defaultIntent(cfg)
  }

  const loadMinimized = (id: string): boolean => {
    try {
      return localStorage.getItem(minimizedStorageKey(id)) === '1'
    } catch {
      return false
    }
  }

  const registerPanel = (id: string, cfg: PanelConfig): PanelIntent => {
    panelConfigs.value[id] = cfg
    if (zIndices.value[id] === undefined) {
      zIndices.value[id] = BASE_Z_INDEX + Object.keys(zIndices.value).length
    }
    // A remounting panel (drawer close/open) keeps its in-session intent.
    if (!items.value[id]) {
      items.value[id] = loadIntent(id, cfg)
      minimized.value[id] = loadMinimized(id)
    }
    return items.value[id]
  }

  // The only intent localStorage writer. Called on gesture end (resize-stop,
  // drag-stop, dock, fullscreen, arrange, reset) — never from derived
  // geometry, which is what made the old store corruptible.
  const commitIntent = (id: string, intent: PanelIntent) => {
    items.value[id] = intent
    try {
      localStorage.setItem(intentStorageKey(id), JSON.stringify(intent))
    } catch {
      // Ignore quota / private-mode failures.
    }
  }

  // --- minimized (persisted, wasm-only) ------------------------------------

  const isMinimized = (id: string): boolean => minimized.value[id] ?? false

  const setMinimized = (id: string, value: boolean) => {
    minimized.value[id] = value
    try {
      if (value) {
        localStorage.setItem(minimizedStorageKey(id), '1')
      } else {
        localStorage.removeItem(minimizedStorageKey(id))
      }
    } catch {
      // Ignore quota / private-mode failures.
    }
  }

  // --- z-order (session only) ---------------------------------------------

  const normalizeZIndices = () => {
    Object.entries(zIndices.value)
      .filter(([id]) => !items.value[id]?.fullScreen)
      .sort((a, b) => a[1] - b[1])
      .forEach(([id], index) => {
        zIndices.value[id] = BASE_Z_INDEX + index
      })
  }

  const bringToFront = (id: string) => {
    if (zIndices.value[id] === undefined) {
      zIndices.value[id] = BASE_Z_INDEX + Object.keys(zIndices.value).length
    }
    if (items.value[id]?.fullScreen) return

    let maxZIndex = BASE_Z_INDEX - 1
    for (const [otherId, z] of Object.entries(zIndices.value)) {
      if (otherId !== id && !items.value[otherId]?.fullScreen) {
        maxZIndex = Math.max(maxZIndex, z)
      }
    }
    if (zIndices.value[id] > maxZIndex) return
    zIndices.value[id] = maxZIndex + 1
    if (zIndices.value[id] > MAX_Z_INDEX) normalizeZIndices()
  }

  const zIndexFor = (id: string): number =>
    items.value[id]?.fullScreen ? FULLSCREEN_Z_INDEX : (zIndices.value[id] ?? BASE_Z_INDEX)

  const clickOnItem = (id: string) => {
    if (items.value[id]?.fullScreen) return
    bringToFront(id)
  }

  // --- fullscreen ----------------------------------------------------------

  const setFullScreen = (id: string, fullScreen: boolean) => {
    const intent = items.value[id]
    if (!intent || intent.fullScreen === fullScreen) return
    // Persisted, but sanitizeIntent resets it on load: fullscreen is transient
    // view state whose driver re-establishes it per session.
    commitIntent(id, { ...intent, fullScreen })
  }

  const toggleFullScreen = (id: string) => {
    const intent = items.value[id]
    if (!intent) return
    setFullScreen(id, !intent.fullScreen)
  }

  // --- layout actions ------------------------------------------------------

  const arrangeItems = (arrangement: 'cascade' | 'tile' | 'stack') => {
    const c = containerBounds.value
    const ids = Object.keys(items.value)
      .filter((id) => !items.value[id].fullScreen)
      .sort((a, b) => (zIndices.value[a] ?? 0) - (zIndices.value[b] ?? 0))

    switch (arrangement) {
      case 'cascade': {
        ids.forEach((id, index) => {
          const intent = items.value[id]
          commitIntent(id, {
            dock: 'free',
            h: { ...intent.h, offset: 100 + index * 30, gap: null },
            v: { ...intent.v, offset: 100 + index * 30, gap: null },
            fullScreen: false,
          })
        })
        break
      }
      case 'tile': {
        const cols = Math.ceil(Math.sqrt(ids.length))
        const rows = Math.ceil(ids.length / cols)
        const itemWidth = Math.max(150, Math.floor(c.width / cols) - 20)
        const itemHeight = Math.max(100, Math.floor(c.height / rows) - 20)
        ids.forEach((id, index) => {
          const col = index % cols
          const row = Math.floor(index / cols)
          commitIntent(id, {
            dock: 'free',
            h: { size: itemWidth, offset: col * (itemWidth + 10) + 10, gap: null },
            v: { size: itemHeight, offset: row * (itemHeight + 10) + 10, gap: null },
            fullScreen: false,
          })
        })
        break
      }
      case 'stack': {
        ids.forEach((id) => {
          const intent = items.value[id]
          commitIntent(id, {
            dock: 'free',
            h: { ...intent.h, offset: 100, gap: null },
            v: { ...intent.v, offset: 100, gap: null },
            fullScreen: false,
          })
        })
        break
      }
    }
  }

  const resetSingleItem = (id: string) => {
    const cfg = panelConfigs.value[id]
    if (!cfg) return
    try {
      localStorage.removeItem(intentStorageKey(id))
      localStorage.removeItem(minimizedStorageKey(id))
    } catch {
      // Ignore.
    }
    items.value[id] = defaultIntent(cfg)
    minimized.value[id] = false
  }

  const resetLayout = () => {
    for (const id of Object.keys(panelConfigs.value)) {
      resetSingleItem(id)
    }
    normalizeZIndices()
  }

  return {
    items,
    panelConfigs,
    zIndices,
    minimized,
    containerBounds,
    inResizing,
    registerContainer,
    registerPanel,
    commitIntent,
    isMinimized,
    setMinimized,
    bringToFront,
    zIndexFor,
    clickOnItem,
    setFullScreen,
    toggleFullScreen,
    arrangeItems,
    resetLayout,
    resetSingleItem,
  }
})
