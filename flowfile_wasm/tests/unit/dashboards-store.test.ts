/**
 * Dashboards Store Unit Tests
 * A localStorage-backed library of tile layouts. `current` is a detached working
 * copy; `save()` commits it into `library`. Verifies the new/edit/save/delete
 * lifecycle, the persistence round-trip, and that loaded copies are cloned.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { useDashboardsStore } from '../../src/stores/dashboards-store'
import type { DashboardLayout } from '../../src/types/visuals'

function layoutWithTextTile(): DashboardLayout {
  return {
    tiles: [
      { id: 't1', type: 'text', viz_id: null, chart_index: 0, text_md: 'hi', x: 0, y: 0, w: 12, h: 3 },
    ],
    grid: { cols: 12, row_height: 40, version: 1 },
    filters: [],
  }
}

describe('Dashboards Store', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    localStorage.clear()
  })

  it('newBlankDashboard sets current without touching the library', () => {
    const store = useDashboardsStore()
    store.newBlankDashboard()
    expect(store.current).not.toBeNull()
    expect(store.current!.id).toBe('')
    expect(store.library.length).toBe(0)
  })

  it('save() inserts current into the library with a generated id', () => {
    const store = useDashboardsStore()
    store.newBlankDashboard()
    store.setName('Q1')
    const saved = store.save()
    expect(saved?.id).toBeTruthy()
    expect(store.count).toBe(1)
    expect(store.library[0].name).toBe('Q1')
  })

  it('setLayout is captured and saved', () => {
    const store = useDashboardsStore()
    store.newBlankDashboard()
    store.setLayout(layoutWithTextTile())
    store.setName('D')
    const saved = store.save()
    expect(saved?.layout.tiles.length).toBe(1)
    expect(saved?.layout.tiles[0].text_md).toBe('hi')
  })

  it('persists, reloads, and loadDashboard returns a detached clone', () => {
    const store = useDashboardsStore()
    store.newBlankDashboard()
    store.setName('D')
    const id = store.save()!.id

    setActivePinia(createPinia())
    const store2 = useDashboardsStore()
    expect(store2.library.length).toBe(1)
    expect(store2.loadDashboard(id)).toBe(true)
    expect(store2.current!.name).toBe('D')

    // Editing the working copy must not mutate the stored copy until save().
    store2.setName('Changed')
    expect(store2.get(id)!.name).toBe('D')
  })

  it('save() on an existing id replaces in place (no duplicate)', () => {
    const store = useDashboardsStore()
    store.newBlankDashboard()
    store.setName('D')
    store.save()
    store.setName('D2')
    store.save()
    expect(store.library.length).toBe(1)
    expect(store.library[0].name).toBe('D2')
  })

  it('loadDashboard returns false and clears current for an unknown id', () => {
    const store = useDashboardsStore()
    expect(store.loadDashboard('nope')).toBe(false)
    expect(store.current).toBeNull()
  })

  it('deleteDashboard removes from the library and clears current', () => {
    const store = useDashboardsStore()
    store.newBlankDashboard()
    store.setName('D')
    const id = store.save()!.id
    store.deleteDashboard(id)
    expect(store.library.length).toBe(0)
    expect(store.current).toBeNull()
  })
})
