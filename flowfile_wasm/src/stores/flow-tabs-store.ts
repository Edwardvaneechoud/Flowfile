/**
 * Multi-flow tabs store (app shell only — never imported by Canvas/src/lib).
 *
 * The browser engine keys LazyFrames/schemas by node id *globally*, so only the
 * active flow can be "live" in Pyodide at a time. This store keeps the active
 * flow live (in flow-store) and stashes every other open flow as a snapshot.
 * Switching tabs captures the active flow, then restores the target snapshot —
 * it does NOT re-run (the graph + inputs are restored, results are cleared).
 *
 * The persistent catalog (uploaded tables / external datasets) lives in its own
 * IndexedDB store and is untouched by tab switching.
 */

import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { useFlowStore, type FlowStateSnapshot } from './flow-store'
import { SIZE_THRESHOLD } from './file-storage'
import { asFileContent, contentByteSize, type FileContent } from '../types/file-content'

const FLOW_TABS_KEY = 'flowfile_wasm_tabs'

export interface FlowTab {
  id: string
  name: string
  /** Stable library id of the tab's flow, or null if never saved. */
  flowId: string | null
  /** Graph snapshot (FlowfileData). Fresh for inactive tabs; for the active tab
   *  the live flow-store is the source of truth (re-captured on switch-away). */
  snapshot: FlowStateSnapshot['snapshot']
  /** In-memory file contents by node id. Persisted small-text-only to
   *  sessionStorage (as plain strings); binary survives tab switches in memory
   *  and reloads via IndexedDB/re-pick. */
  fileContents: Record<number, FileContent>
  nodeIdCounter: number
}

function genId(): string {
  return `tab-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`
}

export const useFlowTabsStore = defineStore('flowTabs', () => {
  const flowStore = useFlowStore()

  const tabs = ref<FlowTab[]>([])
  const activeTabId = ref<string>('')
  let initialized = false

  const activeTab = computed(() => tabs.value.find((t) => t.id === activeTabId.value) ?? null)

  /** A tab "has content" if its (snapshot) graph has nodes. For the active tab
   *  the live flow is authoritative. */
  function tabHasContent(tab: FlowTab): boolean {
    if (tab.id === activeTabId.value) return flowStore.nodes.size > 0
    return (tab.snapshot?.nodes?.length ?? 0) > 0
  }

  /** A fresh "Untitled Flow [N]" name that doesn't collide with open tabs. */
  function uniqueUntitledName(): string {
    const base = 'Untitled Flow'
    if (!tabs.value.some((t) => t.name === base)) return base
    let n = 2
    while (tabs.value.some((t) => t.name === `${base} ${n}`)) n++
    return `${base} ${n}`
  }

  function persist(): void {
    try {
      const slim = tabs.value.map((t) => {
        const small: Record<number, string> = {}
        for (const [nid, content] of Object.entries(t.fileContents)) {
          // Keep only small inline text files; large/binary files are re-flagged
          // as missing on reload (same trade-off as Recent Flows).
          if (content.kind === 'text' && contentByteSize(content) < SIZE_THRESHOLD) {
            small[Number(nid)] = content.data
          }
        }
        return {
          id: t.id,
          name: t.name,
          flowId: t.flowId,
          snapshot: t.snapshot,
          nodeIdCounter: t.nodeIdCounter,
          fileContents: small
        }
      })
      sessionStorage.setItem(FLOW_TABS_KEY, JSON.stringify({ tabs: slim, activeTabId: activeTabId.value }))
    } catch (e) {
      console.warn('[flow-tabs] failed to persist tabs:', e)
    }
  }

  /** Capture the live flow into the currently active tab (before switching). */
  function captureActive(): void {
    const tab = activeTab.value
    if (!tab) return
    const snap = flowStore.captureSnapshot()
    tab.name = snap.name
    tab.flowId = snap.flowId
    tab.snapshot = snap.snapshot
    tab.fileContents = snap.fileContents
    tab.nodeIdCounter = snap.nodeIdCounter
  }

  /** Seed the tab set on first use: restore from sessionStorage, or create one
   *  tab representing whatever the live flow-store already loaded. */
  function init(): void {
    if (initialized) return
    initialized = true

    const raw = sessionStorage.getItem(FLOW_TABS_KEY)
    if (raw) {
      try {
        const parsed = JSON.parse(raw) as { tabs?: FlowTab[]; activeTabId?: string }
        if (parsed.tabs && parsed.tabs.length > 0) {
          tabs.value = parsed.tabs.map((t) => ({
            id: t.id || genId(),
            name: t.name || 'Untitled Flow',
            flowId: t.flowId ?? null,
            snapshot: t.snapshot,
            // Persisted tabs hold plain strings — normalize to FileContent.
            fileContents: Object.fromEntries(
              Object.entries(t.fileContents || {}).map(([nid, c]) => [nid, asFileContent(c as string | FileContent)])
            ),
            nodeIdCounter: t.nodeIdCounter ?? 0
          }))
          activeTabId.value =
            parsed.activeTabId && tabs.value.some((t) => t.id === parsed.activeTabId)
              ? parsed.activeTabId
              : tabs.value[0].id
          // The active flow's live content was already restored by flow-store
          // from its own sessionStorage; the active tab simply represents it.
          return
        }
      } catch (e) {
        console.warn('[flow-tabs] failed to restore tabs:', e)
      }
    }

    // No saved tabs → one tab mirroring the current live flow.
    const snap = flowStore.captureSnapshot()
    const id = genId()
    tabs.value = [
      { id, name: snap.name, flowId: snap.flowId, snapshot: snap.snapshot, fileContents: snap.fileContents, nodeIdCounter: snap.nodeIdCounter }
    ]
    activeTabId.value = id
    persist()
  }

  /** Switch to an existing tab: stash the active flow, restore the target. */
  function switchTab(id: string): void {
    if (id === activeTabId.value) return
    const target = tabs.value.find((t) => t.id === id)
    if (!target) return
    captureActive()
    activeTabId.value = id
    flowStore.loadFromSnapshot({
      name: target.name,
      flowId: target.flowId,
      snapshot: target.snapshot,
      fileContents: target.fileContents,
      nodeIdCounter: target.nodeIdCounter
    })
    persist()
  }

  /** Open a brand-new blank flow in a new tab and make it active. */
  function newTab(): void {
    captureActive()
    const name = uniqueUntitledName()
    flowStore.clearFlow()
    flowStore.currentFlowName = name
    const snap = flowStore.captureSnapshot()
    const id = genId()
    tabs.value.push({ id, name, flowId: snap.flowId, snapshot: snap.snapshot, fileContents: {}, nodeIdCounter: snap.nodeIdCounter })
    activeTabId.value = id
    persist()
  }

  /**
   * Open in a new tab using an arbitrary loader that populates the live flow
   * (e.g. the demo, or a recent-flow snapshot). Stashes the active flow first,
   * runs the loader, then adopts the resulting live flow as a new active tab.
   * On loader failure the previous active flow is restored. Returns success.
   */
  async function openWith(loader: () => Promise<boolean> | boolean): Promise<boolean> {
    const previous = activeTab.value
    captureActive()
    let ok = true
    try {
      ok = (await loader()) !== false
    } catch (e) {
      console.warn('[flow-tabs] openWith loader failed:', e)
      ok = false
    }
    if (!ok) {
      // Restore the flow we stashed so the live editor isn't left half-loaded.
      if (previous) {
        flowStore.loadFromSnapshot({
          name: previous.name,
          flowId: previous.flowId,
          snapshot: previous.snapshot,
          fileContents: previous.fileContents,
          nodeIdCounter: previous.nodeIdCounter
        })
      }
      return false
    }
    const snap = flowStore.captureSnapshot()
    const id = genId()
    tabs.value.push({
      id,
      name: snap.name,
      flowId: snap.flowId,
      snapshot: snap.snapshot,
      fileContents: snap.fileContents,
      nodeIdCounter: snap.nodeIdCounter
    })
    activeTabId.value = id
    persist()
    return true
  }

  /**
   * Open a flow file in a new tab. Stashes the active flow, loads the file into
   * the live flow, then registers it as a new active tab. Returns loadFlowfile's
   * result (for missing-file handling).
   */
  async function openFile(file: File): Promise<{ success: boolean; missingFiles?: Array<{ nodeId: number; fileName: string }> }> {
    captureActive()
    const result = await flowStore.loadFlowfile(file)
    if (!result.success) return result
    const snap = flowStore.captureSnapshot()
    const id = genId()
    tabs.value.push({
      id,
      name: snap.name,
      flowId: snap.flowId,
      snapshot: snap.snapshot,
      fileContents: snap.fileContents,
      nodeIdCounter: snap.nodeIdCounter
    })
    activeTabId.value = id
    persist()
    return result
  }

  /** Close a tab. If it was active, switch to a neighbour (or seed a blank one). */
  function closeTab(id: string): void {
    const idx = tabs.value.findIndex((t) => t.id === id)
    if (idx === -1) return

    const tab = tabs.value[idx]
    if (tabHasContent(tab) && !window.confirm(`Close "${tab.name}"? Unsaved changes will be lost.`)) {
      return
    }

    const wasActive = id === activeTabId.value
    tabs.value.splice(idx, 1)

    if (tabs.value.length === 0) {
      // Always keep at least one open flow.
      flowStore.clearFlow()
      const name = 'Untitled Flow'
      flowStore.currentFlowName = name
      const snap = flowStore.captureSnapshot()
      const newId = genId()
      tabs.value.push({ id: newId, name, flowId: snap.flowId, snapshot: snap.snapshot, fileContents: {}, nodeIdCounter: snap.nodeIdCounter })
      activeTabId.value = newId
    } else if (wasActive) {
      const next = tabs.value[Math.min(idx, tabs.value.length - 1)]
      activeTabId.value = next.id
      flowStore.loadFromSnapshot({
        name: next.name,
        flowId: next.flowId,
        snapshot: next.snapshot,
        fileContents: next.fileContents,
        nodeIdCounter: next.nodeIdCounter
      })
    }
    persist()
  }

  /** Rename a tab (and the live flow name if it's the active tab). */
  function renameTab(id: string, name: string): void {
    const tab = tabs.value.find((t) => t.id === id)
    if (!tab) return
    const trimmed = name.trim()
    if (!trimmed) return
    tab.name = trimmed
    if (id === activeTabId.value) flowStore.currentFlowName = trimmed
    persist()
  }

  /** Sync the active tab's name to the live flow name (e.g. after Save). */
  function syncActiveName(): void {
    const tab = activeTab.value
    if (tab) {
      tab.name = flowStore.currentFlowName
      persist()
    }
  }

  /** Sync the active tab's library identity + name to the live flow (after a
   *  first save mints currentFlowId), so the tab tracks the same library entry. */
  function syncActiveIdentity(): void {
    const tab = activeTab.value
    if (tab) {
      tab.name = flowStore.currentFlowName
      tab.flowId = flowStore.currentFlowId
      persist()
    }
  }

  return {
    tabs,
    activeTabId,
    activeTab,
    tabHasContent,
    init,
    switchTab,
    newTab,
    openWith,
    openFile,
    closeTab,
    renameTab,
    syncActiveName,
    syncActiveIdentity
  }
})
