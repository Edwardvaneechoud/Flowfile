/**
 * Visuals Store Unit Tests
 * A localStorage-backed library of GraphicWalker chart specs. Verifies CRUD +
 * persistence round-trip (the setup mock clears sessionStorage only, so we clear
 * localStorage explicitly).
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { useVisualsStore } from '../../src/stores/visuals-store'
import type { VisualizationCreatePayload } from '../../src/types/visuals'

function payload(over: Partial<VisualizationCreatePayload> = {}): VisualizationCreatePayload {
  return {
    name: 'Sales',
    spec: [{ mark: 'bar' }],
    source_type: 'table',
    dataset_name: 'sales',
    source_kind: 'catalog',
    ...over,
  }
}

describe('Visuals Store', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    localStorage.clear()
  })

  it('creates a visual with a generated id, timestamps, and normalized nulls', () => {
    const store = useVisualsStore()
    const v = store.create(payload())
    expect(v.id).toBeTruthy()
    expect(v.name).toBe('Sales')
    expect(v.dataset_name).toBe('sales')
    expect(v.source_kind).toBe('catalog')
    expect(v.description).toBeNull()
    expect(v.thumbnail_data_url).toBeNull()
    expect(v.createdAt).toBeGreaterThan(0)
    expect(store.count).toBe(1)
  })

  it('persists to localStorage and reloads on a fresh store', () => {
    const store = useVisualsStore()
    store.create(payload({ name: 'A' }))
    // Simulate a reload: brand-new pinia, same localStorage.
    setActivePinia(createPinia())
    const store2 = useVisualsStore()
    expect(store2.visuals.length).toBe(1)
    expect(store2.visuals[0].name).toBe('A')
  })

  it('updates name + spec in place and leaves other fields untouched', () => {
    const store = useVisualsStore()
    const v = store.create(payload({ name: 'A' }))
    const updated = store.update(v.id, { name: 'B', spec: [{ mark: 'line' }, { mark: 'area' }] })
    expect(updated?.name).toBe('B')
    expect(updated?.spec.length).toBe(2)
    expect(updated?.dataset_name).toBe('sales') // unchanged
    expect(updated!.updatedAt).toBeGreaterThanOrEqual(v.updatedAt)
    expect(store.get(v.id)?.name).toBe('B')
  })

  it('returns undefined when updating an unknown id', () => {
    const store = useVisualsStore()
    expect(store.update('does-not-exist', { name: 'x' })).toBeUndefined()
  })

  it('removes a visual', () => {
    const store = useVisualsStore()
    const v = store.create(payload())
    store.remove(v.id)
    expect(store.visuals.length).toBe(0)
    expect(store.get(v.id)).toBeUndefined()
  })

  it('keeps removal across a reload', () => {
    const store = useVisualsStore()
    const a = store.create(payload({ name: 'A' }))
    store.create(payload({ name: 'B' }))
    store.remove(a.id)
    setActivePinia(createPinia())
    const store2 = useVisualsStore()
    expect(store2.visuals.map((v) => v.name)).toEqual(['B'])
  })
})
