/**
 * Saved visualizations — a localStorage-backed library of GraphicWalker charts,
 * each bound to a catalog dataset by name. The browser-only analogue of the full
 * app's `/catalog/visualizations` registry. Pure client-side; no backend.
 *
 * Mirrors the favorites-store pattern (KEY + load() + persist(), quota-safe).
 */

import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import type {
  SavedVisual,
  VisualizationCreatePayload,
  VisualizationUpdatePayload,
} from '../types/visuals'

const KEY = 'flowfile_wasm_visuals'

function genId(): string {
  return (
    globalThis.crypto?.randomUUID?.() ??
    `viz-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
  )
}

function load(): SavedVisual[] {
  try {
    const raw = localStorage.getItem(KEY)
    if (!raw) return []
    const parsed = JSON.parse(raw)
    return Array.isArray(parsed) ? (parsed as SavedVisual[]) : []
  } catch {
    return []
  }
}

export const useVisualsStore = defineStore('visuals', () => {
  const visuals = ref<SavedVisual[]>(load())

  function persist() {
    try {
      localStorage.setItem(KEY, JSON.stringify(visuals.value))
    } catch (e) {
      // localStorage quota (thumbnails are the usual culprit) / private mode.
      console.warn('[visuals] persist failed:', e)
    }
  }

  /** Re-read from localStorage (e.g. another tab wrote). */
  function refresh() {
    visuals.value = load()
  }

  function get(id: string): SavedVisual | undefined {
    return visuals.value.find((v) => v.id === id)
  }

  function create(payload: VisualizationCreatePayload): SavedVisual {
    const now = Date.now()
    const viz: SavedVisual = {
      id: genId(),
      name: payload.name,
      description: payload.description ?? null,
      chart_type: payload.chart_type ?? null,
      spec: payload.spec,
      spec_gw_version: payload.spec_gw_version ?? null,
      source_type: payload.source_type,
      dataset_name: payload.dataset_name,
      source_kind: payload.source_kind,
      thumbnail_data_url: payload.thumbnail_data_url ?? null,
      createdAt: now,
      updatedAt: now,
    }
    visuals.value = [viz, ...visuals.value]
    persist()
    return viz
  }

  function update(id: string, patch: VisualizationUpdatePayload): SavedVisual | undefined {
    const idx = visuals.value.findIndex((v) => v.id === id)
    if (idx === -1) return undefined
    const prev = visuals.value[idx]
    const next: SavedVisual = {
      ...prev,
      name: patch.name ?? prev.name,
      description: patch.description !== undefined ? patch.description : prev.description,
      chart_type: patch.chart_type !== undefined ? patch.chart_type : prev.chart_type,
      spec: patch.spec ?? prev.spec,
      spec_gw_version:
        patch.spec_gw_version !== undefined ? patch.spec_gw_version : prev.spec_gw_version,
      thumbnail_data_url:
        patch.thumbnail_data_url !== undefined
          ? patch.thumbnail_data_url
          : prev.thumbnail_data_url,
      updatedAt: Date.now(),
    }
    const copy = visuals.value.slice()
    copy[idx] = next
    visuals.value = copy
    persist()
    return next
  }

  function remove(id: string) {
    visuals.value = visuals.value.filter((v) => v.id !== id)
    persist()
  }

  const count = computed(() => visuals.value.length)

  return { visuals, count, refresh, get, create, update, remove }
})
