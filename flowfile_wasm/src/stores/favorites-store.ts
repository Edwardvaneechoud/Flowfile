/**
 * Catalog favorites — a localStorage-backed set of CatalogItem ids (e.g.
 * `file-3`, `out-7`, `ext-name`). Pure client-side; no backend.
 */

import { defineStore } from 'pinia'
import { ref, computed } from 'vue'

const KEY = 'flowfile_wasm_favorites'

function load(): Set<string> {
  try {
    const raw = localStorage.getItem(KEY)
    return new Set<string>(raw ? (JSON.parse(raw) as string[]) : [])
  } catch {
    return new Set<string>()
  }
}

export const useFavoritesStore = defineStore('favorites', () => {
  const favorites = ref<Set<string>>(load())

  function persist() {
    try {
      localStorage.setItem(KEY, JSON.stringify([...favorites.value]))
    } catch {
      /* ignore quota / private-mode failures */
    }
  }

  function isFavorite(id: string): boolean {
    return favorites.value.has(id)
  }

  function toggle(id: string) {
    const next = new Set(favorites.value)
    if (next.has(id)) next.delete(id)
    else next.add(id)
    favorites.value = next // reassign so dependents re-render
    persist()
  }

  /** Drop favorites whose item no longer exists. */
  function clearMissing(validIds: Set<string>) {
    const next = new Set([...favorites.value].filter((id) => validIds.has(id)))
    if (next.size !== favorites.value.size) {
      favorites.value = next
      persist()
    }
  }

  const count = computed(() => favorites.value.size)

  return { favorites, isFavorite, toggle, clearMissing, count }
})
