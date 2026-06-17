/**
 * Dashboards — a localStorage-backed library of tile layouts that combine saved
 * visuals into a grid. Browser-only analogue of the full app's
 * `/catalog/dashboards`. `current` is the working copy being edited; `save()`
 * commits it into `library`. Pure client-side; no backend.
 */

import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import type { Dashboard, DashboardLayout } from '../types/visuals'
import { EMPTY_DASHBOARD_LAYOUT } from '../types/visuals'

const KEY = 'flowfile_wasm_dashboards'

function genId(): string {
  return (
    globalThis.crypto?.randomUUID?.() ??
    `dash-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
  )
}

function clone<T>(v: T): T {
  return JSON.parse(JSON.stringify(v)) as T
}

function load(): Dashboard[] {
  try {
    const raw = localStorage.getItem(KEY)
    if (!raw) return []
    const parsed = JSON.parse(raw)
    return Array.isArray(parsed) ? (parsed as Dashboard[]) : []
  } catch {
    return []
  }
}

export const useDashboardsStore = defineStore('dashboards', () => {
  const library = ref<Dashboard[]>(load())
  /** The dashboard currently open in the editor (a detached working copy). */
  const current = ref<Dashboard | null>(null)

  function persist() {
    try {
      localStorage.setItem(KEY, JSON.stringify(library.value))
    } catch (e) {
      console.warn('[dashboards] persist failed:', e)
    }
  }

  function refresh() {
    library.value = load()
  }

  function get(id: string): Dashboard | undefined {
    return library.value.find((d) => d.id === id)
  }

  /** Start a fresh, unsaved dashboard (no id until save()). */
  function newBlankDashboard(): Dashboard {
    const now = Date.now()
    const d: Dashboard = {
      id: '',
      name: 'Untitled dashboard',
      layout: clone(EMPTY_DASHBOARD_LAYOUT),
      createdAt: now,
      updatedAt: now,
    }
    current.value = d
    return d
  }

  /** Open an existing dashboard for editing (detached clone). */
  function loadDashboard(id: string): boolean {
    const found = library.value.find((d) => d.id === id)
    if (!found) {
      current.value = null
      return false
    }
    current.value = clone(found)
    return true
  }

  function setLayout(next: DashboardLayout) {
    if (!current.value) return
    current.value = { ...current.value, layout: next }
  }

  function setName(name: string) {
    if (!current.value) return
    current.value = { ...current.value, name }
  }

  /** Commit `current` into the library (insert if new, replace otherwise). */
  function save(): Dashboard | null {
    if (!current.value) return null
    const now = Date.now()
    let saved: Dashboard
    if (!current.value.id) {
      saved = { ...clone(current.value), id: genId(), createdAt: now, updatedAt: now }
      library.value = [saved, ...library.value]
    } else {
      saved = { ...clone(current.value), updatedAt: now }
      const idx = library.value.findIndex((d) => d.id === saved.id)
      if (idx === -1) {
        library.value = [saved, ...library.value]
      } else {
        const copy = library.value.slice()
        copy[idx] = saved
        library.value = copy
      }
    }
    persist()
    current.value = clone(saved)
    return saved
  }

  function deleteDashboard(id: string) {
    library.value = library.value.filter((d) => d.id !== id)
    persist()
    if (current.value?.id === id) current.value = null
  }

  function reset() {
    current.value = null
  }

  const count = computed(() => library.value.length)

  return {
    library,
    current,
    count,
    refresh,
    get,
    newBlankDashboard,
    loadDashboard,
    setLayout,
    setName,
    save,
    deleteDashboard,
    reset,
  }
})
