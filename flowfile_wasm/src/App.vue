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
import { storeToRefs } from 'pinia'

const pyodideStore = usePyodideStore()
const flowStore = useFlowStore()
const { isReady: pyodideReady } = storeToRefs(pyodideStore)
const isRunning = ref(false)

onMounted(async () => {
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
  background: var(--bg-primary);
}

.app-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px 16px;
  background: var(--bg-secondary);
  border-bottom: 1px solid var(--border-color);
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
  color: var(--text-primary);
}

.app-subtitle {
  font-size: 12px;
  color: var(--text-secondary);
}

.header-right {
  display: flex;
  align-items: center;
  gap: 16px;
}

.loading-indicator, .ready-indicator {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 13px;
  color: var(--text-secondary);
}

.ready-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: #4caf50;
}

.spinner {
  width: 16px;
  height: 16px;
  border: 2px solid var(--border-color);
  border-top-color: var(--accent-color);
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

.run-button {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 8px 16px;
  background: var(--accent-color);
  color: white;
  border: none;
  border-radius: 4px;
  font-size: 13px;
  font-weight: 500;
  cursor: pointer;
  transition: background 0.2s;
}

.run-button:hover:not(:disabled) {
  background: var(--accent-hover);
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
