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
import { ref, watch, type Ref } from 'vue'

const STORAGE_KEY = 'flowfile-learning-mode'
const BACKGROUND_KEY = 'flowfile-codegen-background'

function saved(key: string): boolean {
  if (typeof localStorage === 'undefined') return false
  return localStorage.getItem(key) === '1'
}

// Sync flush: a setting should be on disk the moment it changes, not a tick
// later, in case the click that changed it also navigates away.
function persist(source: Ref<boolean>, key: string) {
  watch(
    source,
    value => {
      if (typeof localStorage !== 'undefined') localStorage.setItem(key, value ? '1' : '0')
    },
    { flush: 'sync' }
  )
}

export const useLearningStore = defineStore('learning', () => {
  const enabled = ref(saved(STORAGE_KEY))
  // The concept prose is opt-in: plenty of people want the code and the rows
  // and nothing else. Off by default, and it stays however they left it.
  const showBackground = ref(saved(BACKGROUND_KEY))

  persist(enabled, STORAGE_KEY)
  persist(showBackground, BACKGROUND_KEY)

  function toggle() {
    enabled.value = !enabled.value
  }

  function toggleBackground() {
    showBackground.value = !showBackground.value
  }

  return { enabled, showBackground, toggle, toggleBackground }
})
