/**
 * Run History (Catalog) — reads per-run summaries that flow-store writes to
 * IndexedDB on each executeFlow. Read-only here; flow-store owns the writes so
 * the lib build never pulls this store into the Canvas graph.
 */

import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { fileStorage, type RunHistoryEntry } from './file-storage'

export type RunSummary = RunHistoryEntry

export const useRunHistoryStore = defineStore('runHistory', () => {
  const runs = ref<RunSummary[]>([])

  async function refresh() {
    try {
      runs.value = await fileStorage.getAllRuns()
    } catch (e) {
      console.warn('[run-history] refresh failed:', e)
      runs.value = []
    }
  }

  async function clear() {
    try {
      await fileStorage.clearRuns()
      runs.value = []
    } catch (e) {
      console.warn('[run-history] clear failed:', e)
    }
  }

  const total = computed(() => runs.value.length)
  const successCount = computed(() => runs.value.filter((r) => r.success).length)
  const failureCount = computed(() => runs.value.filter((r) => !r.success).length)

  return { runs, refresh, clear, total, successCount, failureCount }
})
