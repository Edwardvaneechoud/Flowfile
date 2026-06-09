/**
 * Designer UI store — a thin bridge so the app header (AppLayout) can trigger
 * the flow actions whose logic lives in Canvas (file input, modals, Pyodide
 * execution). Canvas registers its handlers on mount; the header invokes them.
 *
 * The embeddable library never uses this — it keeps Canvas's own toolbar.
 */

import { defineStore } from 'pinia'
import { ref } from 'vue'

export interface DesignerActions {
  run: () => void | Promise<void>
  save: () => void | Promise<void>
  open: () => void
  clear: () => void
}

export const useDesignerUiStore = defineStore('designerUi', () => {
  // Shared so the header button, Canvas toolbar (lib), and the modal agree.
  const showCodeGenerator = ref(false)
  // Action handlers registered by the mounted Canvas.
  const actions = ref<DesignerActions | null>(null)

  function registerActions(a: DesignerActions) {
    actions.value = a
  }
  function clearActions() {
    actions.value = null
  }

  return { showCodeGenerator, actions, registerActions, clearActions }
})
