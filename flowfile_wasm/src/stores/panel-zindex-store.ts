/**
 * Centralized z-index management for draggable panels
 * Uses Pinia for proper reactive state sharing across all panel instances
 */

import { defineStore } from 'pinia'
import { ref, computed } from 'vue'

export const usePanelZIndexStore = defineStore('panelZIndex', () => {
  // Track all registered panels and their z-indices
  const panels = ref<Record<string, number>>({})

  // Base z-index for panels
  const BASE_Z_INDEX = 100

  // Get the current maximum z-index across all panels
  const maxZIndex = computed(() => {
    const values = Object.values(panels.value)
    return values.length > 0 ? Math.max(...values) : BASE_Z_INDEX
  })

  // Register a panel with its initial z-index
  function registerPanel(panelId: string, initialZIndex: number) {
    if (!panels.value[panelId]) {
      panels.value[panelId] = initialZIndex
      console.log(`[ZIndexStore] Registered ${panelId} with zIndex=${initialZIndex}, maxZIndex=${maxZIndex.value}`)
    }
  }

  // Unregister a panel when it's unmounted
  function unregisterPanel(panelId: string) {
    if (panels.value[panelId] !== undefined) {
      delete panels.value[panelId]
      console.log(`[ZIndexStore] Unregistered ${panelId}`)
    }
  }

  // Update a panel's z-index (e.g., when restoring from saved state)
  function updateZIndex(panelId: string, newZIndex: number) {
    panels.value[panelId] = newZIndex
  }

  // Bring a panel to the front - returns the new z-index
  function bringToFront(panelId: string): number {
    const currentZIndex = panels.value[panelId] ?? BASE_Z_INDEX
    const currentMax = maxZIndex.value

    console.log(`[ZIndexStore] bringToFront(${panelId}):`, {
      currentZIndex,
      maxZIndex: currentMax,
      allPanels: { ...panels.value }
    })

    // Only increment if this panel is not already at the top
    if (currentZIndex < currentMax) {
      const newZIndex = currentMax + 1
      panels.value[panelId] = newZIndex
      console.log(`[ZIndexStore] ${panelId} moved to front: ${newZIndex}`)
      return newZIndex
    } else {
      console.log(`[ZIndexStore] ${panelId} already at top, no change`)
      return currentZIndex
    }
  }

  // Get z-index for a specific panel
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
