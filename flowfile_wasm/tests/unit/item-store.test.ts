/**
 * DraggableItem state store (useItemStore) unit tests.
 *
 * Covers the panel z-index management, fullscreen enter/exit, localStorage
 * persistence, and reset-layout logic that backs the draggable dynamic views —
 * the parts most prone to silent regression (relative stacking order, blocked
 * storage, initial-state restore).
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { useItemStore } from '../../src/components/common/DraggableItem/stateStore'
import { Z_INDEX } from '../../src/components/common/DraggableItem/zIndex'

// STORAGE_VERSION is 3 → this is the persisted key format (kept in sync manually).
const key = (id: string) => `overlayPositionAndSize.v3_${id}`

describe('DraggableItem item store', () => {
  beforeEach(() => {
    // setup.ts clears sessionStorage but not localStorage; do it here so saved
    // geometry from one test can't leak into the next.
    localStorage.clear()
    setActivePinia(createPinia())
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe('z-index', () => {
    it('bringToFront raises a panel above its peers', () => {
      const store = useItemStore()
      store.setItemState('a', { zIndex: Z_INDEX.PANEL_BASE })
      store.setItemState('b', { zIndex: Z_INDEX.PANEL_BASE + 1 })

      store.bringToFront('a')

      expect(store.items['a'].zIndex).toBeGreaterThan(store.items['b'].zIndex)
    })

    it('bringToFront is a no-op for an unregistered id (does not throw or create it)', () => {
      const store = useItemStore()
      expect(() => store.bringToFront('ghost')).not.toThrow()
      expect(store.items['ghost']).toBeUndefined()
    })

    it('normalizes z-indices back into range when they exceed the ceiling, preserving order', () => {
      const store = useItemStore()
      store.setItemState('a', { zIndex: Z_INDEX.PANEL_MAX })
      store.setItemState('b', { zIndex: Z_INDEX.PANEL_MAX - 1 })

      // Fronting b would push it to MAX+1, which trips normalizeZIndices().
      store.bringToFront('b')

      expect(store.items['a'].zIndex).toBeLessThanOrEqual(Z_INDEX.PANEL_MAX)
      expect(store.items['b'].zIndex).toBeLessThanOrEqual(Z_INDEX.PANEL_MAX)
      // Relative order is preserved: b was fronted, so it stays above a.
      expect(store.items['b'].zIndex).toBeGreaterThan(store.items['a'].zIndex)
      expect(store.items['a'].zIndex).toBe(Z_INDEX.PANEL_BASE)
    })
  })

  describe('fullscreen', () => {
    it('entering fullscreen lifts the panel above the fullscreen line without flattening others', () => {
      const store = useItemStore()
      store.setItemState('a', {
        zIndex: Z_INDEX.PANEL_BASE,
        width: 300,
        height: 200,
        left: 40,
        top: 60,
      })
      store.setItemState('b', { zIndex: Z_INDEX.PANEL_BASE + 50 })

      store.setFullScreen('a', true)

      expect(store.items['a'].fullScreen).toBe(true)
      expect(store.items['a'].zIndex).toBe(Z_INDEX.FULLSCREEN)
      // The pre-fullscreen z-index is stashed for restore on exit.
      expect(store.items['a'].prevZIndex).toBe(Z_INDEX.PANEL_BASE)
      // The other panel keeps its z-index (the bug reset every peer to a bare 1).
      expect(store.items['b'].zIndex).toBe(Z_INDEX.PANEL_BASE + 50)
    })

    it('exiting fullscreen restores geometry and re-fronts without resetting other panels', () => {
      const store = useItemStore()
      store.setItemState('a', {
        zIndex: Z_INDEX.PANEL_BASE,
        width: 300,
        height: 200,
        left: 40,
        top: 60,
      })
      store.setItemState('b', { zIndex: Z_INDEX.PANEL_BASE + 50 })

      store.setFullScreen('a', true)
      store.setFullScreen('a', false)

      expect(store.items['a'].fullScreen).toBe(false)
      // Geometry restored to the pre-fullscreen values.
      expect(store.items['a'].width).toBe(300)
      expect(store.items['a'].height).toBe(200)
      expect(store.items['a'].left).toBe(40)
      expect(store.items['a'].top).toBe(60)
      // b keeps its relative z (the bug reset every panel to BASE on exit).
      expect(store.items['b'].zIndex).toBe(Z_INDEX.PANEL_BASE + 50)
      // a is re-fronted on exit, so it sits above b again.
      expect(store.items['a'].zIndex).toBeGreaterThan(store.items['b'].zIndex)
    })

    it('toggleFullScreen flips the flag both ways', () => {
      const store = useItemStore()
      store.setItemState('a', {})

      store.toggleFullScreen('a')
      expect(store.items['a'].fullScreen).toBe(true)

      store.toggleFullScreen('a')
      expect(store.items['a'].fullScreen).toBe(false)
    })
  })

  describe('persistence', () => {
    it('persists and restores panel geometry across store instances', () => {
      const store = useItemStore()
      store.setItemState('p1', {
        width: 333,
        height: 222,
        left: 10,
        top: 20,
        stickynessPosition: 'free',
        zIndex: 120,
      })
      store.flushItemState('p1')

      // Fresh pinia + store simulates a reload.
      setActivePinia(createPinia())
      const store2 = useItemStore()
      expect(store2.hasSavedState('p1')).toBe(true)

      store2.loadItemState('p1')
      expect(store2.items['p1'].width).toBe(333)
      expect(store2.items['p1'].height).toBe(222)
      expect(store2.items['p1'].zIndex).toBe(120)
    })

    it('clamps a restored z-index that exceeds the ceiling', () => {
      const store = useItemStore()
      localStorage.setItem(
        key('big'),
        JSON.stringify({ width: 200, height: 200, left: 0, top: 0, zIndex: 99999 }),
      )
      store.loadItemState('big')
      expect(store.items['big'].zIndex).toBe(Z_INDEX.PANEL_BASE)
    })

    it('drops a corrupted saved entry instead of throwing', () => {
      const store = useItemStore()
      localStorage.setItem(key('bad'), '{not valid json')

      expect(() => store.loadItemState('bad')).not.toThrow()
      expect(localStorage.getItem(key('bad'))).toBeNull()
    })

    it('loadItemState does not throw when localStorage access is blocked', () => {
      const store = useItemStore()
      vi.spyOn(localStorage, 'getItem').mockImplementation(() => {
        throw new Error('The operation is insecure.')
      })
      expect(() => store.loadItemState('anything')).not.toThrow()
    })

    it('hasSavedState returns false when localStorage access is blocked', () => {
      const store = useItemStore()
      vi.spyOn(localStorage, 'getItem').mockImplementation(() => {
        throw new Error('The operation is insecure.')
      })
      expect(store.hasSavedState('anything')).toBe(false)
    })

    it('purges the orphaned pre-port panel-store key on init', () => {
      localStorage.setItem('flowfile-panel-state-v2', '{"x":1}')
      // First useItemStore() in this fresh pinia runs purgeLegacyKeys().
      useItemStore()
      expect(localStorage.getItem('flowfile-panel-state-v2')).toBeNull()
    })
  })

  describe('reset & initial state', () => {
    it('registerInitialState keeps the first registered state', () => {
      const store = useItemStore()
      store.registerInitialState('a', { width: 100 })
      store.registerInitialState('a', { width: 200 })
      expect(store.initialItemStates['a'].width).toBe(100)
    })

    it('resetLayout restores each panel to its registered initial state', () => {
      const store = useItemStore()
      store.registerInitialState('a', {
        width: 400,
        height: 300,
        left: 10,
        top: 20,
        stickynessPosition: 'right',
      })
      store.setItemState('a', { width: 999, left: 500 })
      store.flushItemState('a')

      store.resetLayout()

      expect(store.items['a'].width).toBe(400)
      expect(store.items['a'].left).toBe(10)
      expect(store.items['a'].stickynessPosition).toBe('right')
      // Saved geometry is cleared so the reset survives a reload.
      expect(localStorage.getItem(key('a'))).toBeNull()
    })

    it('resetLayout still resets in-memory when localStorage removeItem is blocked', () => {
      const store = useItemStore()
      store.registerInitialState('a', { width: 400, left: 10 })
      store.setItemState('a', { width: 999 })
      vi.spyOn(localStorage, 'removeItem').mockImplementation(() => {
        throw new Error('The operation is insecure.')
      })

      expect(() => store.resetLayout()).not.toThrow()
      expect(store.items['a'].width).toBe(400)
    })
  })
})
