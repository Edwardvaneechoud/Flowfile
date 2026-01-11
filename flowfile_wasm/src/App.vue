<template>
  <div class="app-container">
    <header class="app-header">
      <div class="header-left">
        <img src="/flowfile.png" alt="Flowfile" class="app-logo" />
        <h1 class="app-title">Flowfile</h1>
        <span class="app-subtitle">Browser-Based Data Designer</span>
      </div>
      <div class="header-right">
        <div v-if="!pyodideReady" class="loading-indicator">
          <span class="spinner"></span>
          <span>Loading Pyodide & Polars...</span>
        </div>
        <div v-else class="ready-indicator">
          <span class="ready-dot"></span>
          <span>Ready</span>
        </div>
        <button class="theme-toggle" @click="toggleTheme" :title="isDark ? 'Switch to light mode' : 'Switch to dark mode'">
          <!-- Sun icon for dark mode (click to go light) -->
          <svg v-if="isDark" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <circle cx="12" cy="12" r="5"/>
            <line x1="12" y1="1" x2="12" y2="3"/>
            <line x1="12" y1="21" x2="12" y2="23"/>
            <line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/>
            <line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/>
            <line x1="1" y1="12" x2="3" y2="12"/>
            <line x1="21" y1="12" x2="23" y2="12"/>
            <line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/>
            <line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/>
          </svg>
          <!-- Moon icon for light mode (click to go dark) -->
          <svg v-else xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>
          </svg>
        </button>
        <button class="run-button" :disabled="!pyodideReady || isRunning" @click="runFlow">
          <span v-if="isRunning" class="spinner small"></span>
          <span v-else>{{ isRunning ? 'Running...' : 'Run Flow' }}</span>
        </button>
      </div>
    </header>
    <main class="app-main">
      <Canvas />
    </main>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import Canvas from './components/Canvas.vue'
import { usePyodideStore } from './stores/pyodide-store'
import { useFlowStore } from './stores/flow-store'
import { useThemeStore } from './stores/theme-store'
import { useTheme } from './composables/useTheme'
import { storeToRefs } from 'pinia'

const pyodideStore = usePyodideStore()
const flowStore = useFlowStore()
const themeStore = useThemeStore()
const { isReady: pyodideReady } = storeToRefs(pyodideStore)
const { isDark, toggleTheme } = useTheme()
const isRunning = ref(false)

onMounted(async () => {
  // Initialize theme
  themeStore.initialize()

  // Initialize Pyodide
  await pyodideStore.initialize()
})

const runFlow = async () => {
  if (!pyodideReady.value) return
  isRunning.value = true
  try {
    await flowStore.executeFlow()
  } finally {
    isRunning.value = false
  }
}
</script>

<style scoped>
.app-container {
  display: flex;
  flex-direction: column;
  height: 100vh;
  background: var(--color-background-secondary);
}

.app-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px 16px;
  background: var(--color-background-primary);
  border-bottom: 1px solid var(--color-border-primary);
  height: 50px;
}

.header-left {
  display: flex;
  align-items: center;
  gap: 10px;
}

.app-logo {
  height: 32px;
  width: auto;
}

.app-title {
  font-size: 18px;
  font-weight: 600;
  margin: 0;
  color: var(--color-text-primary);
}

.app-subtitle {
  font-size: 12px;
  color: var(--color-text-secondary);
}

.header-right {
  display: flex;
  align-items: center;
  gap: 12px;
}

.loading-indicator, .ready-indicator {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 13px;
  color: var(--color-text-secondary);
}

.ready-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--color-success);
}

.spinner {
  width: 16px;
  height: 16px;
  border: 2px solid var(--color-border-primary);
  border-top-color: var(--color-accent);
  border-radius: 50%;
  animation: spin 1s linear infinite;
}

.spinner.small {
  width: 12px;
  height: 12px;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.theme-toggle {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-sm);
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  cursor: pointer;
  transition: all var(--transition-base);
}

.theme-toggle:hover {
  background: var(--color-background-hover);
  border-color: var(--color-accent);
}

.theme-toggle svg {
  width: 18px;
  height: 18px;
}

.run-button {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 8px 16px;
  background: var(--color-accent);
  color: white;
  border: none;
  border-radius: var(--border-radius-sm);
  font-size: 13px;
  font-weight: 500;
  cursor: pointer;
  transition: background var(--transition-base);
}

.run-button:hover:not(:disabled) {
  background: var(--color-accent-hover);
}

.run-button:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.app-main {
  flex: 1;
  overflow: hidden;
}
</style>
