/**
 * Centralized z-index management for draggable panels
 * Uses Pinia for proper reactive state sharing across all panel instances
 */

import { defineStore } from 'pinia'
import { ref, computed } from 'vue'

export const usePanelZIndexStore = defineStore('panelZIndex', () => {
  const panels = ref<Record<string, number>>({})

  const BASE_Z_INDEX = 100
  // Z-index used by a panel while in fullscreen (sits above normal panels).
  const FULLSCREEN_Z_INDEX = 250
  // Once the top panel climbs past this, re-pack everyone into BASE..N to keep
  // values bounded (mirrors the main editor's normalization).
  const NORMALIZE_CEILING = BASE_Z_INDEX + 200

  const maxZIndex = computed(() => {
    const values = Object.values(panels.value)
    return values.length > 0 ? Math.max(...values) : BASE_Z_INDEX
  })

  function registerPanel(panelId: string, initialZIndex: number) {
    if (!panels.value[panelId]) {
      panels.value[panelId] = initialZIndex
    }
  }

  function unregisterPanel(panelId: string) {
    if (panels.value[panelId] !== undefined) {
      delete panels.value[panelId]
    }
  }

  function updateZIndex(panelId: string, newZIndex: number) {
    panels.value[panelId] = newZIndex
  }

  /** Re-pack z-indices into BASE..BASE+n preserving order, to bound growth. */
  function normalize() {
    const sorted = Object.entries(panels.value).sort((a, b) => a[1] - b[1])
    sorted.forEach(([id], index) => {
      panels.value[id] = BASE_Z_INDEX + index
    })
  }

  function bringToFront(panelId: string): number {
    const currentZIndex = panels.value[panelId] ?? BASE_Z_INDEX
    const currentMax = maxZIndex.value

    // Already on top — nothing to do.
    if (currentZIndex >= currentMax && Object.keys(panels.value).length > 0) {
      return currentZIndex
    }

    let newZIndex = currentMax + 1
    panels.value[panelId] = newZIndex

    if (newZIndex > NORMALIZE_CEILING) {
      normalize()
      newZIndex = panels.value[panelId]
    }
    return newZIndex
  }

  function getZIndex(panelId: string): number {
    return panels.value[panelId] ?? BASE_Z_INDEX
  }

  return {
    panels,
    maxZIndex,
    BASE_Z_INDEX,
    FULLSCREEN_Z_INDEX,
    registerPanel,
    unregisterPanel,
    updateZIndex,
    bringToFront,
    getZIndex
  }
})
