<template>
  <div class="app-container">
    <header class="app-header">
      <div class="header-left">
        <div class="header-brand">
          <img src="/flowfile.png" alt="Flowfile" class="app-logo" />
          <h1 class="app-title">Flowfile</h1>
        </div>
        <span class="app-subtitle">Browser-Based Data Designer</span>
        <div class="header-divider"></div>
        <div v-if="!pyodideReady" class="loading-indicator">
          <span class="spinner"></span>
          <span>Loading Pyodide...</span>
        </div>
        <div v-else class="ready-indicator">
          <span class="ready-dot"></span>
          <span>Ready</span>
        </div>
      </div>

      <nav class="header-nav">
        <RouterLink to="/" class="header-nav-link" :class="{ active: route.name === 'designer' }">
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <rect x="3" y="3" width="7" height="7" rx="1"/>
            <rect x="14" y="3" width="7" height="7" rx="1"/>
            <rect x="14" y="14" width="7" height="7" rx="1"/>
            <path d="M10 6.5h4M17.5 10v4M10 17.5H6a2 2 0 0 1-2-2V11"/>
          </svg>
          <span>Designer</span>
        </RouterLink>
        <RouterLink to="/catalog" class="header-nav-link" :class="{ active: route.name === 'catalog' }">
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <ellipse cx="12" cy="5" rx="9" ry="3"/>
            <path d="M3 5v14a9 3 0 0 0 18 0V5"/>
            <path d="M3 12a9 3 0 0 0 18 0"/>
          </svg>
          <span>Catalog</span>
        </RouterLink>
      </nav>

      <!-- Designer flow actions (header-driven; the canvas toolbar is hidden in app mode) -->
      <div v-if="route.name === 'designer'" class="header-actions">
        <button class="header-btn header-btn--run" :disabled="isExecuting" title="Run flow (Ctrl+E)" @click="uiStore.actions?.run()">
          <svg viewBox="0 0 24 24" fill="currentColor"><polygon points="5 3 19 12 5 21 5 3"/></svg>
          <span>{{ isExecuting ? 'Running…' : 'Run' }}</span>
        </button>
        <button class="header-btn header-btn-icon" title="Save flow" @click="uiStore.actions?.save()">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg>
        </button>
        <button class="header-btn header-btn-icon" title="Open flow" @click="uiStore.actions?.open()">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/></svg>
        </button>
        <button class="header-btn" :class="{ active: uiStore.showCodeGenerator }" title="Generate Python code" @click="uiStore.showCodeGenerator = !uiStore.showCodeGenerator">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg>
          <span>Code</span>
        </button>
        <button class="header-btn header-btn-icon header-btn--danger" title="Clear flow" @click="uiStore.actions?.clear()">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
        </button>
      </div>

      <div class="header-right">
        <button class="header-btn" @click="isDocsOpen = true">
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
            <polyline points="14 2 14 8 20 8"/>
            <line x1="16" y1="13" x2="8" y2="13"/>
            <line x1="16" y1="17" x2="8" y2="17"/>
          </svg>
          <span>About</span>
        </button>
        <a href="https://edwardvaneechoud.github.io/Flowfile/" target="_blank" rel="noopener" class="header-btn">
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z"/>
            <path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z"/>
          </svg>
          <span>Docs</span>
        </a>
        <a href="https://github.com/Edwardvaneechoud/Flowfile" target="_blank" rel="noopener" class="header-btn">
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
            <path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"/>
          </svg>
          <span>GitHub</span>
        </a>
        <div class="header-divider"></div>
        <button class="header-btn header-btn-icon" @click="toggleTheme" :title="isDark ? 'Switch to light mode' : 'Switch to dark mode'">
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
          <svg v-else xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>
          </svg>
        </button>
      </div>
    </header>

    <main class="app-main">
      <router-view />
    </main>

    <DocsModal :is-open="isDocsOpen" @close="isDocsOpen = false" />

    <!-- Prominent demo button for first-time visitors on the designer canvas
         (hidden when auto-loading via URL or once the demo has been seen). -->
    <DemoButton
      v-if="route.name === 'designer' && !shouldAutoLoadDemo && !hasSeenDemo && !hasDismissedDemo && pyodideReady"
      prominent
    />
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, watch } from 'vue'
import { useRoute, RouterLink } from 'vue-router'
import DocsModal from '../components/DocsModal.vue'
import DemoButton from '../components/DemoButton.vue'
import { usePyodideStore } from '../stores/pyodide-store'
import { useThemeStore } from '../stores/theme-store'
import { useFlowStore } from '../stores/flow-store'
import { useDesignerUiStore } from '../stores/designer-ui-store'
import { useTheme } from '../composables/useTheme'
import { useDemo } from '../composables/useDemo'
import { storeToRefs } from 'pinia'

const route = useRoute()
const pyodideStore = usePyodideStore()
const themeStore = useThemeStore()
const flowStore = useFlowStore()
const uiStore = useDesignerUiStore()
const { isExecuting } = storeToRefs(flowStore)
const { isReady: pyodideReady } = storeToRefs(pyodideStore)
const { isDark, toggleTheme } = useTheme()
const { hasSeenDemo, hasDismissedDemo, loadDemo } = useDemo()

const urlParams = new URLSearchParams(window.location.search)
const shouldAutoLoadDemo = urlParams.get('demo') === 'true'

const isDocsOpen = ref(false)

// Pyodide + theme init are idempotent and run here once; because the layout
// stays mounted across Designer <-> Catalog navigation, nothing re-initializes.
onMounted(async () => {
  themeStore.initialize()
  await pyodideStore.initialize()
})

if (shouldAutoLoadDemo) {
  watch(pyodideReady, async (ready) => {
    if (ready) {
      // Load demo without confirmation since user explicitly requested it via URL
      await loadDemo(false)
    }
  }, { immediate: true })
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
  gap: 10px;
}

.header-right {
  display: flex;
  align-items: center;
  gap: 8px;
}

.header-brand {
  display: flex;
  align-items: center;
  gap: 10px;
}

.header-divider {
  width: 1px;
  height: 24px;
  background: var(--color-border-light);
  margin: 0 6px;
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

/* Center nav tabs: Designer / Catalog */
.header-nav {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
}

.header-nav-link {
  display: flex;
  align-items: center;
  gap: var(--spacing-1-5);
  height: 32px;
  padding: 0 var(--spacing-3);
  border-radius: var(--border-radius-lg);
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  text-decoration: none;
  cursor: pointer;
  transition: all var(--transition-fast);
}

.header-nav-link svg {
  width: 16px;
  height: 16px;
}

.header-nav-link:hover {
  background: var(--color-background-tertiary);
  color: var(--color-text-primary);
}

.header-nav-link.active {
  background: var(--color-accent-subtle);
  color: var(--color-accent);
}

/* Header buttons share the same visual system as the in-canvas toolbar
   (.action-btn) so the app chrome reads as one coherent bar. */
.header-btn {
  display: flex;
  align-items: center;
  gap: var(--spacing-1-5);
  height: 34px;
  padding: 0 var(--spacing-3);
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-lg);
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  text-decoration: none;
  cursor: pointer;
  box-shadow: var(--shadow-xs);
  transition: all var(--transition-fast);
}

.header-btn:hover {
  background: var(--color-background-tertiary);
  color: var(--color-text-primary);
  border-color: var(--color-border-secondary);
}

.header-btn:active {
  transform: translateY(1px);
  box-shadow: none;
}

.header-btn svg {
  width: 16px;
  height: 16px;
}

.header-btn-icon {
  padding: 0;
  width: 34px;
  justify-content: center;
}

.header-btn-icon svg {
  width: 18px;
  height: 18px;
}

/* Designer action cluster */
.header-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  padding: 0 var(--spacing-2);
  margin: 0 var(--spacing-1);
  border-left: 1px solid var(--color-border-light);
  border-right: 1px solid var(--color-border-light);
}

.header-btn--run {
  background: var(--color-success);
  border-color: var(--color-success);
  color: #fff;
}

.header-btn--run svg { color: #fff; }

.header-btn--run:hover:not(:disabled) {
  background: var(--color-success-hover);
  border-color: var(--color-success-hover);
  color: #fff;
}

.header-btn--run:disabled {
  opacity: 0.7;
  cursor: not-allowed;
}

.header-btn.active {
  background: var(--color-accent-subtle);
  border-color: var(--color-accent);
  color: var(--color-accent);
}

.header-btn--danger:hover {
  background: var(--color-danger-light);
  border-color: var(--color-danger);
  color: var(--color-danger);
}

.loading-indicator, .ready-indicator {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  color: var(--color-text-secondary);
}

.ready-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--color-success);
}

.spinner {
  width: 14px;
  height: 14px;
  border: 2px solid var(--color-border-primary);
  border-top-color: var(--color-accent);
  border-radius: 50%;
  animation: spin 1s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.app-main {
  flex: 1;
  overflow: hidden;
  min-height: 0;
}

@media (max-width: 900px) {
  .app-subtitle {
    display: none;
  }
  .header-nav-link span {
    display: none;
  }
}

@media (max-width: 768px) {
  .header-btn span {
    display: none;
  }

  .header-btn {
    padding: 6px;
  }
}
</style>
