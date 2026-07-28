/**
 * DraggableItem intent store (useItemStore) unit tests.
 *
 * Ported from the desktop frontend's stateStore.test.ts (v3→v4 migration at
 * panel registration, gesture-only persistence, reset, legacy-key purge) plus
 * the wasm-specific coverage: z-index bring-to-front/normalization, fullscreen
 * enter/exit via zIndexFor, the pre-port panel-store key purge, and the
 * wasm-only persisted minimized state.
 *
 * A fake localStorage is stubbed per-test: the shared setup.ts mock keeps its
 * data in a closure, so `Object.keys(localStorage)` (used by the legacy-key
 * purge) would not see stored keys. The fake stores data as enumerable
 * own-properties, behaving like a real Storage.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

import {
  defaultIntent,
  type PanelConfig,
} from '../../src/components/common/DraggableItem/layoutGeometry'
import { useItemStore } from '../../src/components/common/DraggableItem/stateStore'
import { Z_INDEX } from '../../src/components/common/DraggableItem/zIndex'

function fakeLocalStorage(): Storage {
  const ls = {} as Record<string, string> & Storage
  Object.defineProperties(ls, {
    getItem: {
      value: (k: string) => (Object.prototype.hasOwnProperty.call(ls, k) ? ls[k] : null),
      enumerable: false,
      configurable: true,
    },
    setItem: {
      value: (k: string, v: string) => {
        ls[k] = String(v)
      },
      enumerable: false,
      configurable: true,
    },
    removeItem: {
      value: (k: string) => {
        delete ls[k]
      },
      enumerable: false,
      configurable: true,
    },
    clear: {
      value: () => {
        for (const k of Object.keys(ls)) delete ls[k]
      },
      enumerable: false,
      configurable: true,
    },
    key: { value: (i: number) => Object.keys(ls)[i] ?? null, enumerable: false, configurable: true },
    length: { get: () => Object.keys(ls).length, enumerable: false, configurable: true },
  })
  return ls
}

const v3Key = (id: string) => `overlayPositionAndSize.v3_${id}`
const v4Key = (id: string) => `overlayPositionAndSize.v4_${id}`
const minimizedKey = (id: string) => `overlayMinimized.v4_${id}`

const v3Layout = (width: number, height: number, patch: Record<string, unknown> = {}) =>
  JSON.stringify({
    width,
    height,
    left: 20,
    top: 8,
    stickynessPosition: 'right',
    fullWidth: false,
    fullHeight: false,
    zIndex: 100,
    fullScreen: false,
    clicked: false,
    ...patch,
  })

const rightDrawerCfg: PanelConfig = {
  defaultDock: 'right',
  h: { behaviour: 'fixed', defaultSize: 600, defaultOffset: 0 },
  v: { behaviour: 'scale', defaultSize: 700, defaultOffset: 8 },
}

beforeEach(() => {
  vi.stubGlobal('localStorage', fakeLocalStorage())
  setActivePinia(createPinia())
})

afterEach(() => {
  vi.unstubAllGlobals()
  vi.restoreAllMocks()
})

describe('v3 migration at panel registration', () => {
  it('heals the exact collapse fingerprint (150x100) to defaults', () => {
    localStorage.setItem(v3Key('rightDrawer'), v3Layout(150, 100, { left: 464, top: 0 }))
    const store = useItemStore()
    const intent = store.registerPanel('rightDrawer', rightDrawerCfg)
    expect(intent).toEqual(defaultIntent(rightDrawerCfg))
    expect(localStorage.getItem(v3Key('rightDrawer'))).toBeNull()
    expect(localStorage.getItem(v4Key('rightDrawer'))).toBeNull()
  })

  it('heals one-axis crushes the old sweep missed', () => {
    localStorage.setItem(v3Key('rightDrawer'), v3Layout(600, 100))
    const store = useItemStore()
    expect(store.registerPanel('rightDrawer', rightDrawerCfg)).toEqual(
      defaultIntent(rightDrawerCfg),
    )
    expect(localStorage.getItem(v3Key('rightDrawer'))).toBeNull()
  })

  it('converts a healthy v3 record to a v4 intent record', () => {
    localStorage.setItem(v3Key('rightDrawer'), v3Layout(640, 800))
    const store = useItemStore()
    const intent = store.registerPanel('rightDrawer', rightDrawerCfg)
    expect(intent).toEqual({
      dock: 'right',
      h: { size: 640, offset: null, gap: null },
      v: { size: null, offset: null, gap: null },
      fullScreen: false,
    })
    expect(localStorage.getItem(v3Key('rightDrawer'))).toBeNull()
    expect(JSON.parse(localStorage.getItem(v4Key('rightDrawer'))!)).toEqual(intent)
  })

  it('migrates a v3 record under a wasm panel id (unchanged ids map 1:1)', () => {
    localStorage.setItem(v3Key('node-settings-panel'), v3Layout(450, 640))
    const store = useItemStore()
    const intent = store.registerPanel('node-settings-panel', rightDrawerCfg)
    expect(intent.dock).toBe('right')
    expect(intent.h.size).toBe(450)
    expect(localStorage.getItem(v4Key('node-settings-panel'))).not.toBeNull()
  })

  it('drops unparseable v3 JSON and falls back to defaults', () => {
    localStorage.setItem(v3Key('broken'), '{ not json')
    const store = useItemStore()
    expect(store.registerPanel('broken', rightDrawerCfg)).toEqual(defaultIntent(rightDrawerCfg))
    expect(localStorage.getItem(v3Key('broken'))).toBeNull()
  })
})

describe('v4 load', () => {
  it('loads a healthy v4 record', () => {
    const saved = {
      dock: 'right',
      h: { size: 500, offset: null, gap: null },
      v: { size: null, offset: 8, gap: 250 },
      fullScreen: false,
    }
    localStorage.setItem(v4Key('rightDrawer'), JSON.stringify(saved))
    const store = useItemStore()
    expect(store.registerPanel('rightDrawer', rightDrawerCfg)).toEqual(saved)
  })

  it('drops a degenerate v4 record and heals to defaults', () => {
    localStorage.setItem(
      v4Key('rightDrawer'),
      JSON.stringify({ dock: 'right', h: { size: 20 }, v: {} }),
    )
    const store = useItemStore()
    expect(store.registerPanel('rightDrawer', rightDrawerCfg)).toEqual(
      defaultIntent(rightDrawerCfg),
    )
    expect(localStorage.getItem(v4Key('rightDrawer'))).toBeNull()
  })

  it('keeps the in-session intent when a panel remounts', () => {
    const store = useItemStore()
    store.registerPanel('rightDrawer', rightDrawerCfg)
    const resized = {
      ...defaultIntent(rightDrawerCfg),
      h: { size: 480, offset: null, gap: null },
    }
    store.commitIntent('rightDrawer', resized)
    // Drawer close/open remounts the component; the session intent must win.
    expect(store.registerPanel('rightDrawer', rightDrawerCfg)).toEqual(resized)
  })

  it('does not throw when localStorage access is blocked', () => {
    const store = useItemStore()
    vi.spyOn(localStorage, 'getItem').mockImplementation(() => {
      throw new Error('The operation is insecure.')
    })
    expect(() => store.registerPanel('anything', rightDrawerCfg)).not.toThrow()
    expect(store.items['anything']).toEqual(defaultIntent(rightDrawerCfg))
  })
})

describe('persistence discipline', () => {
  it('commitIntent writes the v4 record', () => {
    const store = useItemStore()
    store.registerPanel('rightDrawer', rightDrawerCfg)
    const intent = {
      ...defaultIntent(rightDrawerCfg),
      h: { size: 480, offset: null, gap: null },
    }
    store.commitIntent('rightDrawer', intent)
    expect(JSON.parse(localStorage.getItem(v4Key('rightDrawer'))!)).toEqual(intent)
  })

  it('registering a panel with no stored record writes nothing', () => {
    const store = useItemStore()
    store.registerPanel('rightDrawer', rightDrawerCfg)
    expect(localStorage.getItem(v4Key('rightDrawer'))).toBeNull()
  })

  it('resetLayout removes stored records and restores defaults', () => {
    const store = useItemStore()
    store.registerPanel('rightDrawer', rightDrawerCfg)
    store.commitIntent('rightDrawer', {
      ...defaultIntent(rightDrawerCfg),
      h: { size: 480, offset: null, gap: null },
    })
    store.resetLayout()
    expect(localStorage.getItem(v4Key('rightDrawer'))).toBeNull()
    expect(store.items['rightDrawer']).toEqual(defaultIntent(rightDrawerCfg))
  })

  it('resetLayout still resets in-memory when localStorage removeItem is blocked', () => {
    const store = useItemStore()
    store.registerPanel('rightDrawer', rightDrawerCfg)
    store.commitIntent('rightDrawer', {
      ...defaultIntent(rightDrawerCfg),
      h: { size: 480, offset: null, gap: null },
    })
    vi.spyOn(localStorage, 'removeItem').mockImplementation(() => {
      throw new Error('The operation is insecure.')
    })
    expect(() => store.resetLayout()).not.toThrow()
    expect(store.items['rightDrawer']).toEqual(defaultIntent(rightDrawerCfg))
  })
})

describe('legacy key handling at store init', () => {
  it('purges pre-v3 keys and group keys, keeps v3 and v4 and unrelated keys', () => {
    localStorage.setItem('overlayPositionAndSize_old', '{}')
    localStorage.setItem('overlayPositionAndSize.v2_x', '{}')
    localStorage.setItem('overlayGroups.v3', '{}')
    localStorage.setItem(v3Key('rightDrawer'), v3Layout(640, 800))
    localStorage.setItem(v4Key('bottomDock'), '{}')
    localStorage.setItem('someOtherKey', 'value')
    localStorage.setItem('layoutControlsPosition', '{"x":10,"y":20}')
    useItemStore()
    expect(localStorage.getItem('overlayPositionAndSize_old')).toBeNull()
    expect(localStorage.getItem('overlayPositionAndSize.v2_x')).toBeNull()
    expect(localStorage.getItem('overlayGroups.v3')).toBeNull()
    expect(localStorage.getItem(v3Key('rightDrawer'))).not.toBeNull()
    expect(localStorage.getItem(v4Key('bottomDock'))).not.toBeNull()
    expect(localStorage.getItem('someOtherKey')).toBe('value')
    expect(localStorage.getItem('layoutControlsPosition')).toBe('{"x":10,"y":20}')
  })

  it('purges the orphaned pre-port panel-store key on init', () => {
    localStorage.setItem('flowfile-panel-state-v2', '{"x":1}')
    // First useItemStore() in this fresh pinia runs purgeLegacyKeys().
    useItemStore()
    expect(localStorage.getItem('flowfile-panel-state-v2')).toBeNull()
  })
})

describe('z-order', () => {
  it('bringToFront raises a panel above its peers', () => {
    const store = useItemStore()
    store.registerPanel('a', rightDrawerCfg)
    store.registerPanel('b', rightDrawerCfg)
    store.bringToFront('a')
    expect(store.zIndexFor('a')).toBeGreaterThan(store.zIndexFor('b'))
  })

  it('bringToFront on an unregistered id does not throw or create an intent', () => {
    const store = useItemStore()
    expect(() => store.bringToFront('ghost')).not.toThrow()
    expect(store.items['ghost']).toBeUndefined()
  })

  it('normalizes z-indices back into range past the ceiling, preserving order', () => {
    const store = useItemStore()
    store.registerPanel('a', rightDrawerCfg)
    store.registerPanel('b', rightDrawerCfg)
    store.zIndices['a'] = Z_INDEX.PANEL_MAX
    store.zIndices['b'] = Z_INDEX.PANEL_MAX - 1

    // Fronting b would push it to MAX+1, which trips normalizeZIndices().
    store.bringToFront('b')

    expect(store.zIndexFor('a')).toBeLessThanOrEqual(Z_INDEX.PANEL_MAX)
    expect(store.zIndexFor('b')).toBeLessThanOrEqual(Z_INDEX.PANEL_MAX)
    // Relative order is preserved: b was fronted, so it stays above a.
    expect(store.zIndexFor('b')).toBeGreaterThan(store.zIndexFor('a'))
    expect(store.zIndexFor('a')).toBe(Z_INDEX.PANEL_BASE)
  })
})

describe('fullscreen', () => {
  it('entering fullscreen lifts the panel to the fullscreen layer without flattening others', () => {
    const store = useItemStore()
    store.registerPanel('a', rightDrawerCfg)
    store.registerPanel('b', rightDrawerCfg)
    store.zIndices['b'] = Z_INDEX.PANEL_BASE + 50

    store.setFullScreen('a', true)

    expect(store.items['a'].fullScreen).toBe(true)
    expect(store.zIndexFor('a')).toBe(Z_INDEX.FULLSCREEN)
    // The other panel keeps its z-index.
    expect(store.zIndexFor('b')).toBe(Z_INDEX.PANEL_BASE + 50)
  })

  it('exiting fullscreen restores layout intent and z-order untouched', () => {
    const store = useItemStore()
    store.registerPanel('a', rightDrawerCfg)
    store.registerPanel('b', rightDrawerCfg)
    const resized = {
      ...defaultIntent(rightDrawerCfg),
      h: { size: 480, offset: null, gap: null },
    }
    store.commitIntent('a', resized)
    const zBefore = store.zIndexFor('a')

    store.setFullScreen('a', true)
    store.setFullScreen('a', false)

    expect(store.items['a'].fullScreen).toBe(false)
    // Layout intent survives the round-trip (the rect is derived, not stored).
    expect(store.items['a'].h).toEqual(resized.h)
    expect(store.zIndexFor('a')).toBe(zBefore)
  })

  it('toggleFullScreen flips the flag both ways', () => {
    const store = useItemStore()
    store.registerPanel('a', rightDrawerCfg)

    store.toggleFullScreen('a')
    expect(store.items['a'].fullScreen).toBe(true)

    store.toggleFullScreen('a')
    expect(store.items['a'].fullScreen).toBe(false)
  })
})

describe('minimized persistence (wasm-only)', () => {
  it('persists the collapsed state and restores it across store instances', () => {
    const store = useItemStore()
    store.registerPanel('data-preview-panel', rightDrawerCfg)
    store.setMinimized('data-preview-panel', true)
    expect(localStorage.getItem(minimizedKey('data-preview-panel'))).toBe('1')

    // Fresh pinia + store simulates a reload.
    setActivePinia(createPinia())
    const store2 = useItemStore()
    store2.registerPanel('data-preview-panel', rightDrawerCfg)
    expect(store2.isMinimized('data-preview-panel')).toBe(true)

    store2.setMinimized('data-preview-panel', false)
    expect(localStorage.getItem(minimizedKey('data-preview-panel'))).toBeNull()
  })

  it('carries the minimized flag over from a wasm v3 record', () => {
    localStorage.setItem(v3Key('data-preview-panel'), v3Layout(640, 800, { minimized: true }))
    const store = useItemStore()
    store.registerPanel('data-preview-panel', rightDrawerCfg)
    expect(store.isMinimized('data-preview-panel')).toBe(true)
    expect(localStorage.getItem(minimizedKey('data-preview-panel'))).toBe('1')
  })

  it('resetLayout clears the collapsed state', () => {
    const store = useItemStore()
    store.registerPanel('data-preview-panel', rightDrawerCfg)
    store.setMinimized('data-preview-panel', true)
    store.resetLayout()
    expect(store.isMinimized('data-preview-panel')).toBe(false)
    expect(localStorage.getItem(minimizedKey('data-preview-panel'))).toBeNull()
  })
})
