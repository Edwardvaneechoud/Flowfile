/**
 * Panel state persistence store
 * Saves and restores draggable panel positions and sizes using localStorage
 */

const PANEL_STORAGE_KEY = 'flowfile-panel-state'

export interface PanelState {
  width: number
  height: number
  left: number
  top: number
  isMinimized: boolean
  zIndex?: number
  // Viewport dimensions at the time of saving (for resize detection on load)
  savedViewportWidth?: number
  savedViewportHeight?: number
}

interface StoredPanelStates {
  [panelId: string]: PanelState
}

/**
 * Load all stored panel states from localStorage
 */
function loadAllPanelStates(): StoredPanelStates {
  if (typeof localStorage === 'undefined') return {}

  try {
    const stored = localStorage.getItem(PANEL_STORAGE_KEY)
    if (stored) {
      return JSON.parse(stored)
    }
  } catch (e) {
    console.warn('[panel-store] Failed to parse stored panel states:', e)
  }
  return {}
}

/**
 * Save all panel states to localStorage
 */
function saveAllPanelStates(states: StoredPanelStates): void {
  if (typeof localStorage === 'undefined') return

  try {
    localStorage.setItem(PANEL_STORAGE_KEY, JSON.stringify(states))
  } catch (e) {
    console.warn('[panel-store] Failed to save panel states:', e)
  }
}

/**
 * Get the stored state for a specific panel
 */
export function getPanelState(panelId: string): PanelState | null {
  const states = loadAllPanelStates()
  return states[panelId] || null
}

/**
 * Save the state for a specific panel
 */
export function savePanelState(panelId: string, state: PanelState): void {
  const states = loadAllPanelStates()
  states[panelId] = state
  saveAllPanelStates(states)
}

/**
 * Clear the stored state for a specific panel
 */
export function clearPanelState(panelId: string): void {
  const states = loadAllPanelStates()
  delete states[panelId]
  saveAllPanelStates(states)
}

/**
 * Clear all stored panel states
 */
export function clearAllPanelStates(): void {
  if (typeof localStorage === 'undefined') return
  localStorage.removeItem(PANEL_STORAGE_KEY)
}
