/**
 * Panel docking-group store.
 *
 * Mirrors the main editor's draggable-panel grouping: panels that share a group
 * name and opt into `syncDimensions` keep their width/height in sync. Each
 * DraggablePanel manages its own geometry locally, so syncing happens via a
 * broadcast signal — when one grouped panel resizes, peers in the same group
 * adopt the new dimensions.
 */

import { defineStore } from 'pinia'
import { ref } from 'vue'

export const usePanelGroupStore = defineStore('panelGroup', () => {
  // panelId -> group name
  const members = ref<Record<string, string>>({})

  // Last resize broadcast; peers in the same group watch this and adopt dims.
  const syncSignal = ref<{
    group: string
    sourceId: string
    width: number
    height: number
    nonce: number
  } | null>(null)
  let nonce = 0

  function register(panelId: string, group: string) {
    members.value[panelId] = group
  }

  function unregister(panelId: string) {
    delete members.value[panelId]
  }

  function groupMembers(group: string): string[] {
    return Object.entries(members.value)
      .filter(([, g]) => g === group)
      .map(([id]) => id)
  }

  /** A grouped panel calls this on resize so same-group peers can follow. */
  function broadcastDims(group: string, sourceId: string, width: number, height: number) {
    nonce += 1
    syncSignal.value = { group, sourceId, width, height, nonce }
  }

  return { members, syncSignal, register, unregister, groupMembers, broadcastDims }
})
