/**
 * Learning mode — a user setting, not a build flag.
 *
 * Layers under the embeddable `teachingMode` prop: the prop decides whether the
 * capability exists at all (a host can switch it off entirely), this decides
 * whether the person sitting in front of it wants to be taught right now.
 * When on, the Code panel opens on the walkthrough instead of Polars and the
 * per-node explainer appears in node settings.
 */

import { defineStore } from 'pinia'
import { ref, watch } from 'vue'

const STORAGE_KEY = 'flowfile-learning-mode'

function saved(): boolean {
  if (typeof localStorage === 'undefined') return false
  return localStorage.getItem(STORAGE_KEY) === '1'
}

export const useLearningStore = defineStore('learning', () => {
  const enabled = ref(saved())

  // Sync flush: a setting should be on disk the moment it changes, not a tick
  // later, in case the click that changed it also navigates away.
  watch(
    enabled,
    value => {
      if (typeof localStorage !== 'undefined') localStorage.setItem(STORAGE_KEY, value ? '1' : '0')
    },
    { flush: 'sync' }
  )

  function toggle() {
    enabled.value = !enabled.value
  }

  return { enabled, toggle }
})
