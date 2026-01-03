/**
 * Composable for session storage persistence
 */
import { watch, onMounted } from 'vue';
import { STORAGE_KEY } from '../constants';
import type { NodeMetadata, DesignerSection } from '../types';

interface StorageState {
  nodeMetadata: NodeMetadata;
  sections: DesignerSection[];
  processCode: string;
}

export function useSessionStorage(
  getState: () => StorageState,
  setState: (state: Partial<StorageState>) => void,
  resetState: () => void
) {
  function saveToSessionStorage() {
    const state = getState();
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  }

  function loadFromSessionStorage() {
    const saved = sessionStorage.getItem(STORAGE_KEY);
    if (saved) {
      try {
        const state = JSON.parse(saved);
        setState(state);
      } catch (e) {
        console.error('Failed to load from session storage:', e);
      }
    }
  }

  function clearSessionStorage() {
    sessionStorage.removeItem(STORAGE_KEY);
    resetState();
  }

  // Setup auto-save watcher
  function setupAutoSave(watchSources: () => any[]) {
    watch(watchSources, () => {
      saveToSessionStorage();
    }, { deep: true });
  }

  // Load on mount
  function setupLoadOnMount() {
    onMounted(() => {
      loadFromSessionStorage();
    });
  }

  return {
    saveToSessionStorage,
    loadFromSessionStorage,
    clearSessionStorage,
    setupAutoSave,
    setupLoadOnMount,
  };
}
