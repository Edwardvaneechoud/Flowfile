/**
 * Saved Flows — the persistent in-browser flow library (app-shell only).
 *
 * The WASM analogue of the full app's catalog flow registrations: every flow
 * the user saves is stored in IndexedDB with a stable uuid id + metadata, so it
 * can be browsed, reopened, renamed, duplicated, and deleted. Home "recent
 * flows" is the top slice of this same library. Never imported by Canvas/src/lib
 * (flow-store owns the writes via saveToLibrary).
 */

import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { fileStorage } from './file-storage'
import { useFlowStore } from './flow-store'
import { useFlowTabsStore } from './flow-tabs-store'
import type { FlowfileData } from '../types'

const RECENT_LIMIT = 8

export interface SavedFlow {
  id: string
  name: string
  description: string
  createdAt: number
  updatedAt: number
  nodeCount: number
}

export const useSavedFlowsStore = defineStore('savedFlows', () => {
  const flows = ref<SavedFlow[]>([])

  /** Top N most-recently-modified flows, for the Home page. */
  const recent = computed(() => flows.value.slice(0, RECENT_LIMIT))

  async function refresh() {
    try {
      const all = await fileStorage.getAllSavedFlows()
      flows.value = all.map(({ id, name, description, createdAt, updatedAt, nodeCount }) => ({
        id,
        name,
        description,
        createdAt,
        updatedAt,
        nodeCount
      }))
    } catch (e) {
      console.warn('[saved-flows] refresh failed:', e)
      flows.value = []
    }
  }

  /** Load a saved flow into the live editor, carrying its library identity. */
  async function loadInto(id: string): Promise<boolean> {
    try {
      const entry = await fileStorage.getSavedFlow(id)
      if (!entry) return false
      const flowStore = useFlowStore()
      const ok = flowStore.importFromFlowfile(entry.snapshot as FlowfileData)
      if (!ok) return false
      if (entry.fileContents) {
        for (const [nid, content] of Object.entries(entry.fileContents)) {
          flowStore.setFileContent(Number(nid), content)
        }
      }
      flowStore.currentFlowId = entry.id
      flowStore.currentFlowName = entry.name
      return true
    } catch (e) {
      console.warn('[saved-flows] loadInto failed:', e)
      return false
    }
  }

  /** Open a saved flow in a new designer tab. */
  async function open(id: string): Promise<boolean> {
    const tabs = useFlowTabsStore()
    return tabs.openWith(() => loadInto(id))
  }

  /** Rename a flow in place (same id — non-lossy). */
  async function rename(id: string, name: string) {
    const trimmed = name.trim()
    if (!trimmed) return
    const entry = await fileStorage.getSavedFlow(id)
    if (!entry) return
    await fileStorage.putSavedFlow({ ...entry, name: trimmed, updatedAt: Date.now() })
    const flowStore = useFlowStore()
    if (flowStore.currentFlowId === id) flowStore.currentFlowName = trimmed
    await refresh()
  }

  async function updateDescription(id: string, description: string) {
    const entry = await fileStorage.getSavedFlow(id)
    if (!entry) return
    await fileStorage.putSavedFlow({ ...entry, description, updatedAt: Date.now() })
    await refresh()
  }

  async function remove(id: string) {
    try {
      await fileStorage.deleteSavedFlow(id)
      flows.value = flows.value.filter((f) => f.id !== id)
    } catch (e) {
      console.warn('[saved-flows] remove failed:', e)
    }
  }

  /** Duplicate a flow under a new id (Save-As). Returns the new id. */
  async function duplicate(id: string): Promise<string | null> {
    const entry = await fileStorage.getSavedFlow(id)
    if (!entry) return null
    const newId =
      globalThis.crypto?.randomUUID?.() ?? `flow-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    await fileStorage.duplicateSavedFlow(id, newId, `${entry.name} (copy)`)
    await refresh()
    return newId
  }

  return { flows, recent, refresh, loadInto, open, rename, updateDescription, remove, duplicate }
})
