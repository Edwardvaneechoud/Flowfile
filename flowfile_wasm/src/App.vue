<template>
  <div class="app-container">
    <header class="app-header">
      <div class="header-left">
        <div class="action-buttons">
          <button class="action-btn" :disabled="!pyodideReady || isRunning" @click="runFlow" title="Run Flow">
            <span v-if="isRunning" class="spinner small"></span>
            <svg v-else xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor" class="btn-icon">
              <path d="M8 5v14l11-7z"/>
            </svg>
            <span class="btn-text">{{ isRunning ? 'Running...' : 'Run' }}</span>
          </button>
        </div>
      </div>
      <div class="header-center">
        <img src="/flowfile.png" alt="Flowfile" class="app-logo" />
        <h1 class="app-title">Flowfile</h1>
        <span class="app-subtitle">Browser-Based Data Designer</span>
      </div>
      <div class="header-right">
        <div v-if="!pyodideReady" class="loading-indicator">
          <span class="spinner"></span>
          <span>Loading Pyodide...</span>
        </div>
        <div v-else class="ready-indicator">
          <span class="ready-dot"></span>
          <span>Ready</span>
        </div>
      </div>
    </header>
    <main class="app-main">
      <Canvas />
    </main>

    <!-- Theme Toggle - Bottom Left Corner -->
    <button class="theme-toggle-floating" @click="toggleTheme" :title="isDark ? 'Switch to light mode' : 'Switch to dark mode'">
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
  position: relative;
}

.app-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0 16px;
  background: var(--color-background-primary);
  border-bottom: 1px solid var(--color-border-primary);
  height: 50px;
}

.header-left {
  display: flex;
  align-items: center;
  min-width: 200px;
}

.header-center {
  display: flex;
  align-items: center;
  gap: 10px;
}

.header-right {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  min-width: 200px;
}

/* Action buttons - matching flowfile_frontend HeaderButtons.vue */
.action-buttons {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  height: 50px;
  font-family: var(--font-family-base);
}

.action-btn {
  display: flex;
  align-items: center;
  gap: var(--spacing-1-5);
  padding: var(--spacing-2) var(--spacing-3);
  height: 34px;
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-lg);
  cursor: pointer;
  transition: all var(--transition-fast);
  color: var(--color-text-primary);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  box-shadow: var(--shadow-xs);
}

.action-btn:hover:not(:disabled) {
  background-color: var(--color-background-tertiary);
  border-color: var(--color-border-secondary);
}

.action-btn:active:not(:disabled) {
  transform: translateY(1px);
  box-shadow: none;
}

.action-btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.action-btn .btn-icon {
  width: 18px;
  height: 18px;
  color: var(--color-text-secondary);
}

.action-btn:hover:not(:disabled) .btn-icon {
  color: var(--color-text-primary);
}

.btn-text {
  white-space: nowrap;
}

.app-logo {
  height: 28px;
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
  margin-left: 8px;
  padding-left: 8px;
  border-left: 1px solid var(--color-border-light);
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
  width: 14px;
  height: 14px;
  border-width: 2px;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.app-main {
  flex: 1;
  overflow: hidden;
}

/* Floating Theme Toggle - Bottom Left Corner */
.theme-toggle-floating {
  position: fixed;
  bottom: 20px;
  left: 20px;
  z-index: 1000;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 40px;
  height: 40px;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-full);
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  cursor: pointer;
  transition: all var(--transition-base);
  box-shadow: var(--shadow-md);
}

.theme-toggle-floating:hover {
  background: var(--color-background-hover);
  border-color: var(--color-accent);
  transform: scale(1.05);
}

.theme-toggle-floating svg {
  width: 20px;
  height: 20px;
}
</style>
