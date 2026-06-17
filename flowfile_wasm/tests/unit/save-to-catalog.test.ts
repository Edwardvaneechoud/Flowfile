/**
 * End-to-end persistence: saving a flow must write it to the catalog store and
 * be readable by the saved-flows store. Uses the REAL file-storage layer
 * (fake-indexeddb) — only pyodide is mocked.
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

import { useFlowStore } from '../../src/stores/flow-store'
import { useSavedFlowsStore } from '../../src/stores/saved-flows-store'
import { fileStorage } from '../../src/stores/file-storage'

describe('Save to catalog (end-to-end persistence)', () => {
  beforeEach(async () => {
    setActivePinia(createPinia())
    sessionStorage.clear()
    for (const f of await fileStorage.getAllSavedFlows()) await fileStorage.deleteSavedFlow(f.id)
  })

  it('saveToLibrary persists a flow readable via getAllSavedFlows and the store', async () => {
    const flow = useFlowStore()
    flow.addNode('filter', 0, 0)

    const res = await flow.saveToLibrary('My Catalog Flow')
    expect(res.id).toBeTruthy()
    expect(flow.currentFlowId).toBe(res.id)

    const stored = await fileStorage.getAllSavedFlows()
    expect(stored.map((f) => f.name)).toContain('My Catalog Flow')
    const entry = stored.find((f) => f.id === res.id)
    expect(entry).toBeTruthy()
    expect(entry!.nodeCount).toBe(1)

    const saved = useSavedFlowsStore()
    await saved.refresh()
    expect(saved.flows.map((f) => f.name)).toContain('My Catalog Flow')
  })

  it('re-saving updates the same entry (no duplicate)', async () => {
    const flow = useFlowStore()
    flow.addNode('filter', 0, 0)
    const first = await flow.saveToLibrary('Flow A')
    await flow.saveToLibrary('Flow A renamed')

    const stored = await fileStorage.getAllSavedFlows()
    const mine = stored.filter((f) => f.id === first.id)
    expect(mine).toHaveLength(1)
    expect(mine[0].name).toBe('Flow A renamed')
  })
})
