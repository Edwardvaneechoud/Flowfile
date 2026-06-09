/**
 * Flow Tabs Store Unit Tests
 * Multi-flow tabs: only the active flow is live; others are snapshots. Switching
 * captures the active flow and restores the target — without re-executing.
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
    getRecentFlow: vi.fn().mockResolvedValue(null)
  }
}))

import { useFlowStore } from '../../src/stores/flow-store'
import { useFlowTabsStore } from '../../src/stores/flow-tabs-store'

describe('Flow Tabs Store', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    sessionStorage.clear()
    vi.clearAllMocks()
    // closeTab confirms before discarding a tab that has content.
    vi.stubGlobal('confirm', vi.fn(() => true))
  })

  it('seeds a single tab mirroring the live flow on init', () => {
    const tabs = useFlowTabsStore()
    tabs.init()
    expect(tabs.tabs.length).toBe(1)
    expect(tabs.activeTabId).toBe(tabs.tabs[0].id)
  })

  it('newTab opens an additional, blank tab and switches to it', () => {
    const flow = useFlowStore()
    const tabs = useFlowTabsStore()
    tabs.init()
    flow.addNode('filter', 10, 20)
    const firstId = tabs.activeTabId

    tabs.newTab()

    expect(tabs.tabs.length).toBe(2)
    expect(tabs.activeTabId).not.toBe(firstId)
    expect(flow.nodes.size).toBe(0) // new tab is blank
  })

  it('switchTab restores the graph + id counter without re-running', () => {
    const flow = useFlowStore()
    const tabs = useFlowTabsStore()
    tabs.init()
    const tabA = tabs.activeTabId

    flow.addNode('filter', 0, 0) // id 1
    flow.addNode('select', 0, 0) // id 2
    expect(flow.nodes.size).toBe(2)

    tabs.newTab() // tab B (blank)
    expect(flow.nodes.size).toBe(0)
    flow.addNode('sort', 0, 0) // id 1 in B

    tabs.switchTab(tabA) // back to A
    expect(flow.nodes.size).toBe(2)
    // No id collision: A's counter (2) is restored, next node is 3.
    expect(flow.addNode('group_by', 0, 0)).toBe(3)
  })

  it('captureSnapshot / loadFromSnapshot is lossless (graph + files + counter)', () => {
    const flow = useFlowStore()
    const id = flow.addNode('read', 5, 6)
    flow.setFileContent(id, 'a,b\n1,2')

    const snap = flow.captureSnapshot()
    expect(snap.nodeIdCounter).toBe(1)

    flow.clearFlow()
    expect(flow.nodes.size).toBe(0)
    expect(flow.hasFileContent(id)).toBe(false)

    expect(flow.loadFromSnapshot(snap)).toBe(true)
    expect(flow.nodes.size).toBe(1)
    expect(flow.getNode(id)).toBeDefined()
    expect(flow.hasFileContent(id)).toBe(true)
  })

  it('closeTab removes a tab and always keeps one open', () => {
    const tabs = useFlowTabsStore()
    tabs.init()
    tabs.newTab()
    expect(tabs.tabs.length).toBe(2)

    tabs.closeTab(tabs.activeTabId)
    expect(tabs.tabs.length).toBe(1)

    // Closing the last tab seeds a fresh blank tab rather than leaving none.
    tabs.closeTab(tabs.activeTabId)
    expect(tabs.tabs.length).toBe(1)
  })

  it('persists tabs to sessionStorage and restores them on a fresh store', () => {
    const tabs = useFlowTabsStore()
    tabs.init()
    tabs.newTab()
    expect(tabs.tabs.length).toBe(2)
    const savedActive = tabs.activeTabId

    // Simulate a reload: brand-new pinia, same sessionStorage.
    setActivePinia(createPinia())
    const tabs2 = useFlowTabsStore()
    tabs2.init()

    expect(tabs2.tabs.length).toBe(2)
    expect(tabs2.activeTabId).toBe(savedActive)
  })
})
