/**
 * Centralized z-index management for draggable panels
 * Uses Pinia for proper reactive state sharing across all panel instances
 */

import { defineStore } from 'pinia'
import { ref, computed } from 'vue'

export const usePanelZIndexStore = defineStore('panelZIndex', () => {
  const panels = ref<Record<string, number>>({})

  const BASE_Z_INDEX = 100

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

  function bringToFront(panelId: string): number {
    const currentZIndex = panels.value[panelId] ?? BASE_Z_INDEX
    const currentMax = maxZIndex.value

    // Only increment if this panel is not already at the top
    if (currentZIndex < currentMax) {
      const newZIndex = currentMax + 1
      panels.value[panelId] = newZIndex
      return newZIndex
    }
    return currentZIndex
  }

  function getZIndex(panelId: string): number {
    return panels.value[panelId] ?? BASE_Z_INDEX
  }

  return {
    panels,
    maxZIndex,
    registerPanel,
    unregisterPanel,
    updateZIndex,
    bringToFront,
    getZIndex
  }
})
