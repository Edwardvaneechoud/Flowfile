/**
 * Share-link import tests (composable level).
 * A shared flow arrives as a URL hash, decodes, and opens in a new tab via the
 * tabs store — the current flow is kept, with a confirm gate when non-empty.
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

vi.mock('../../src/stores/file-storage', () => ({
  SIZE_THRESHOLD: 5 * 1024 * 1024,
  fileStorage: {
    setFileContent: vi.fn().mockResolvedValue(undefined),
    getFileContent: vi.fn().mockResolvedValue(null),
    deleteFileContent: vi.fn().mockResolvedValue(undefined),
    getDownloadContent: vi.fn().mockResolvedValue(null),
    setDownloadContent: vi.fn().mockResolvedValue(undefined),
    clearAll: vi.fn().mockResolvedValue(undefined),
    shouldUseIndexedDB: vi.fn().mockReturnValue(false),
    getAllCatalogDatasets: vi.fn().mockResolvedValue([]),
    putCatalogDataset: vi.fn().mockResolvedValue(undefined),
    deleteCatalogDataset: vi.fn().mockResolvedValue(undefined),
    getAllRecentFlows: vi.fn().mockResolvedValue([]),
    putRecentFlow: vi.fn().mockResolvedValue(undefined),
    pruneRecentFlows: vi.fn().mockResolvedValue(undefined),
    getRecentFlow: vi.fn().mockResolvedValue(null),
    getAllSavedFlows: vi.fn().mockResolvedValue([]),
    getSavedFlow: vi.fn().mockResolvedValue(null),
    putSavedFlow: vi.fn().mockResolvedValue(undefined),
    deleteSavedFlow: vi.fn().mockResolvedValue(undefined),
    duplicateSavedFlow: vi.fn().mockResolvedValue(null),
    putRun: vi.fn().mockResolvedValue(undefined),
    pruneRuns: vi.fn().mockResolvedValue(undefined)
  }
}))

import { useFlowStore } from '../../src/stores/flow-store'
import { useFlowTabsStore } from '../../src/stores/flow-tabs-store'
import { useShareLink } from '../../src/composables/useShareLink'
import { encodeShareHash } from '../../src/utils/share-link'
import type { FlowfileData } from '../../src/types'

function makeSharedFlow(): FlowfileData {
  return {
    flowfile_version: '1.0.0',
    flowfile_id: 42,
    flowfile_name: 'Shared Flow',
    flowfile_settings: {
      description: '',
      execution_mode: 'Development',
      execution_location: 'local',
      auto_save: true,
      show_detailed_progress: true
    },
    nodes: [
      {
        id: 1,
        type: 'read',
        is_start_node: true,
        description: '',
        x_position: 100,
        y_position: 100,
        input_ids: [],
        outputs: [2],
        setting_input: { file_name: 'data.csv', file_type: 'csv', has_headers: true, delimiter: ',' }
      },
      {
        id: 2,
        type: 'filter',
        is_start_node: false,
        description: '',
        x_position: 300,
        y_position: 100,
        input_ids: [1],
        outputs: [],
        setting_input: {}
      }
    ],
    connections: [{ from_node: 1, to_node: 2, from_handle: 'main', to_handle: 'main' }]
  }
}

describe('importShareHash', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    sessionStorage.clear()
    vi.clearAllMocks()
    vi.stubGlobal('confirm', vi.fn(() => true))
  })

  it('imports a shared flow into a new active tab with file contents restored', async () => {
    const tabs = useFlowTabsStore()
    const flow = useFlowStore()
    tabs.init()
    const initialTabCount = tabs.tabs.length

    const hash = await encodeShareHash(makeSharedFlow(), { 1: 'a,b\n1,2\n' })
    const result = await useShareLink().importShareHash(hash)

    expect(result.status).toBe('imported')
    expect(tabs.tabs.length).toBe(initialTabCount + 1)
    expect(flow.nodes.size).toBe(2)
    expect(flow.currentFlowName).toBe('Shared Flow')
    expect(flow.currentFlowId).toBe(null)
    expect(flow.hasFileContent(1)).toBe(true)
    if (result.status === 'imported') {
      expect(result.missingFiles).toEqual([])
    }
  })

  it('reports binary/large inputs the sender could not inline as missing', async () => {
    const tabs = useFlowTabsStore()
    tabs.init()

    const hash = await encodeShareHash(makeSharedFlow()) // no files travel
    const result = await useShareLink().importShareHash(hash)

    expect(result.status).toBe('imported')
    if (result.status === 'imported') {
      expect(result.missingFiles).toEqual([{ nodeId: 1, fileName: 'data.csv' }])
    }
  })

  it('skips the confirm when the live flow is empty', async () => {
    const tabs = useFlowTabsStore()
    tabs.init()
    const confirmOpen = vi.fn(() => true)

    const hash = await encodeShareHash(makeSharedFlow())
    const result = await useShareLink().importShareHash(hash, { confirmOpen })

    expect(result.status).toBe('imported')
    expect(confirmOpen).not.toHaveBeenCalled()
  })

  it('asks before opening over a non-empty flow and keeps it on cancel', async () => {
    const tabs = useFlowTabsStore()
    const flow = useFlowStore()
    tabs.init()
    flow.addNode('filter', 0, 0)
    const tabCount = tabs.tabs.length

    const hash = await encodeShareHash(makeSharedFlow())
    const result = await useShareLink().importShareHash(hash, { confirmOpen: () => false })

    expect(result.status).toBe('cancelled')
    expect(tabs.tabs.length).toBe(tabCount)
    expect(flow.nodes.size).toBe(1)
  })

  it('opens in a new tab on confirm, keeping the previous flow in its tab', async () => {
    const tabs = useFlowTabsStore()
    const flow = useFlowStore()
    tabs.init()
    flow.addNode('filter', 0, 0)
    const previousTabId = tabs.activeTabId

    const hash = await encodeShareHash(makeSharedFlow())
    const result = await useShareLink().importShareHash(hash, { confirmOpen: () => true })

    expect(result.status).toBe('imported')
    expect(tabs.activeTabId).not.toBe(previousTabId)
    expect(flow.nodes.size).toBe(2)

    tabs.switchTab(previousTabId)
    expect(flow.nodes.size).toBe(1)
  })

  it('returns none for an empty or unrelated hash', async () => {
    const share = useShareLink()
    expect((await share.importShareHash('')).status).toBe('none')
    expect((await share.importShareHash('#section-2')).status).toBe('none')
  })

  it('returns invalid for a garbage payload without touching the tabs', async () => {
    const tabs = useFlowTabsStore()
    tabs.init()
    const tabCount = tabs.tabs.length

    const result = await useShareLink().importShareHash('#flow=not-a-real-payload')

    expect(result.status).toBe('invalid')
    expect(tabs.tabs.length).toBe(tabCount)
  })
})
