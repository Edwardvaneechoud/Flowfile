/**
 * Saved Flows Store Unit Tests
 * The persistent flow library: list/recent, rename (non-lossy), updateDescription,
 * duplicate (new id), remove. Backed by an in-memory IndexedDB mock.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

vi.mock('../../src/stores/pyodide-store', () => ({
  usePyodideStore: () => ({
    isReady: false,
    runPython: vi.fn(),
    runPythonWithResult: vi.fn(),
    setGlobal: vi.fn(),
    deleteGlobal: vi.fn()
  })
}))

const backing = new Map<string, any>()

vi.mock('../../src/stores/file-storage', () => ({
  SIZE_THRESHOLD: 5 * 1024 * 1024,
  fileStorage: {
    // Saved-flows surface used by the store.
    getAllSavedFlows: vi.fn(async () => [...backing.values()].sort((a, b) => b.updatedAt - a.updatedAt)),
    getSavedFlow: vi.fn(async (id: string) => backing.get(id) ?? null),
    putSavedFlow: vi.fn(async (e: any) => { backing.set(e.id, e) }),
    deleteSavedFlow: vi.fn(async (id: string) => { backing.delete(id) }),
    duplicateSavedFlow: vi.fn(async (id: string, newId: string, newName: string) => {
      const src = backing.get(id)
      if (!src) return null
      const clone = { ...src, id: newId, name: newName, createdAt: 1, updatedAt: 1 }
      backing.set(newId, clone)
      return clone
    }),
    // Stubs touched when flow-store is instantiated by rename().
    getFileContent: vi.fn().mockResolvedValue(null),
    clearAll: vi.fn().mockResolvedValue(undefined),
    shouldUseIndexedDB: vi.fn().mockReturnValue(false),
    getAllCatalogDatasets: vi.fn().mockResolvedValue([])
  }
}))

import { useSavedFlowsStore } from '../../src/stores/saved-flows-store'

function seed(n: number) {
  for (let i = 0; i < n; i++) {
    backing.set(`f${i}`, {
      id: `f${i}`, name: `Flow ${i}`, description: '', createdAt: i, updatedAt: i, nodeCount: i, snapshot: {}
    })
  }
}

describe('Saved Flows Store', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    backing.clear()
    vi.clearAllMocks()
  })

  it('refresh maps entries newest-modified-first', async () => {
    seed(3)
    const s = useSavedFlowsStore()
    await s.refresh()
    expect(s.flows.map((f) => f.id)).toEqual(['f2', 'f1', 'f0'])
  })

  it('recent caps at 8 by updatedAt', async () => {
    seed(12)
    const s = useSavedFlowsStore()
    await s.refresh()
    expect(s.recent).toHaveLength(8)
    expect(s.recent[0].id).toBe('f11')
  })

  it('rename keeps the same id and bumps updatedAt (no duplicate)', async () => {
    seed(1) // f0, updatedAt 0
    const s = useSavedFlowsStore()
    await s.refresh()
    await s.rename('f0', 'Renamed')
    const e = backing.get('f0')
    expect(e.name).toBe('Renamed')
    expect(e.updatedAt).toBeGreaterThan(0)
    expect(backing.size).toBe(1)
  })

  it('updateDescription persists and keeps the id', async () => {
    seed(1)
    const s = useSavedFlowsStore()
    await s.updateDescription('f0', 'hello')
    expect(backing.get('f0').description).toBe('hello')
    expect(backing.size).toBe(1)
  })

  it('duplicate creates a new id', async () => {
    seed(1)
    const s = useSavedFlowsStore()
    await s.refresh()
    const newId = await s.duplicate('f0')
    expect(newId).toBeTruthy()
    expect(newId).not.toBe('f0')
    expect(backing.has(newId!)).toBe(true)
  })

  it('remove deletes the entry', async () => {
    seed(2)
    const s = useSavedFlowsStore()
    await s.refresh()
    await s.remove('f0')
    expect(backing.has('f0')).toBe(false)
    expect(s.flows.map((f) => f.id)).toEqual(['f1'])
  })
})
