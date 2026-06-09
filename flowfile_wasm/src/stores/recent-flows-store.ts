/**
 * Recent Flows (Home page) — client-side, IndexedDB-backed.
 *
 * Unlike the desktop app (which reopens flows from filesystem paths), the
 * browser persists each saved/opened flow's JSON snapshot (+ small input CSVs)
 * to IndexedDB so it can be reopened entirely in-browser.
 */

import { defineStore } from 'pinia'
import { ref } from 'vue'
import { fileStorage } from './file-storage'
import { useFlowStore } from './flow-store'
import type { FlowfileData } from '../types'

export interface RecentFlow {
  id: string
  name: string
  savedAt: number
  nodeCount: number
}

export const useRecentFlowsStore = defineStore('recentFlows', () => {
  const recentFlows = ref<RecentFlow[]>([])

  async function refresh() {
    try {
      const all = await fileStorage.getAllRecentFlows()
      recentFlows.value = all.map((e) => ({
        id: e.id,
        name: e.name,
        savedAt: e.savedAt,
        nodeCount: e.nodeCount
      }))
    } catch (e) {
      console.warn('[recent-flows] refresh failed:', e)
      recentFlows.value = []
    }
  }

  /** Load a recent flow snapshot back into the editor. */
  async function openRecent(id: string): Promise<boolean> {
    try {
      const entry = await fileStorage.getRecentFlow(id)
      if (!entry) return false
      const flowStore = useFlowStore()
      const ok = flowStore.importFromFlowfile(entry.snapshot as FlowfileData)
      if (!ok) return false
      // Re-apply bundled small input CSVs so previews/execution have data.
      if (entry.fileContents) {
        for (const [nid, content] of Object.entries(entry.fileContents)) {
          flowStore.setFileContent(Number(nid), content)
        }
      }
      return true
    } catch (e) {
      console.warn('[recent-flows] openRecent failed:', e)
      return false
    }
  }

  async function remove(id: string) {
    try {
      await fileStorage.deleteRecentFlow(id)
      recentFlows.value = recentFlows.value.filter((f) => f.id !== id)
    } catch (e) {
      console.warn('[recent-flows] remove failed:', e)
    }
  }

  return { recentFlows, refresh, openRecent, remove }
})
