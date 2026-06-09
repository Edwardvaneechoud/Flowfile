<template>
  <nav class="icon-rail">
    <button class="rail-logo" title="Home" @click="router.push({ name: 'home' })">
      <img src="/flowfile.png" alt="Flowfile" />
    </button>

    <div class="rail-nav">
      <RouterLink to="/" class="rail-item" :class="{ active: route.name === 'home' }" data-tooltip="Home">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 9.5 12 3l9 6.5"/><path d="M5 10v10a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1V10"/><path d="M9 21v-6h6v6"/></svg>
      </RouterLink>
      <RouterLink to="/designer" class="rail-item" :class="{ active: route.name === 'designer' }" data-tooltip="Designer">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/><path d="M10 6.5h4M17.5 10v4M10 17.5H6a2 2 0 0 1-2-2V11"/></svg>
      </RouterLink>
      <RouterLink to="/catalog" class="rail-item" :class="{ active: route.name === 'catalog' }" data-tooltip="Catalog">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14a9 3 0 0 0 18 0V5"/><path d="M3 12a9 3 0 0 0 18 0"/></svg>
      </RouterLink>
    </div>

    <div class="rail-spacer"></div>

    <div class="rail-footer">
      <!-- Help / more popover -->
      <div class="rail-popover-wrap">
        <button class="rail-item" :class="{ active: helpOpen }" data-tooltip="More & help" @click.stop="helpOpen = !helpOpen">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
        </button>
        <div v-if="helpOpen" class="rail-popover">
          <button class="rail-popover-item" @click="loadTemplate">
            <span class="material-icons">layers</span><span>Templates</span>
          </button>
          <button class="rail-popover-item" @click="openDocs">
            <span class="material-icons">menu_book</span><span>Documentation</span>
          </button>
        </div>
      </div>

      <button class="rail-item" :title="isDark ? 'Switch to light mode' : 'Switch to dark mode'" @click="toggleTheme">
        <svg v-if="isDark" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg>
        <svg v-else viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>
      </button>

      <button class="rail-item" data-tooltip="About" @click="uiStore.showAbout = true">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>
      </button>

      <a class="rail-item" href="https://github.com/edwardvaneechoud/Flowfile" target="_blank" rel="noopener" data-tooltip="GitHub">
        <svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"/></svg>
      </a>

      <span class="rail-ready" :class="{ ready: pyodideReady }" :title="pyodideReady ? 'Engine ready' : 'Loading Pyodide…'"></span>
    </div>
  </nav>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'
import { useRoute, useRouter, RouterLink } from 'vue-router'
import { storeToRefs } from 'pinia'
import { useTheme } from '../../composables/useTheme'
import { useDemo } from '../../composables/useDemo'
import { useDesignerUiStore } from '../../stores/designer-ui-store'
import { usePyodideStore } from '../../stores/pyodide-store'

const route = useRoute()
const router = useRouter()
const { isDark, toggleTheme } = useTheme()
const { loadDemo } = useDemo()
const uiStore = useDesignerUiStore()
const { isReady: pyodideReady } = storeToRefs(usePyodideStore())

const helpOpen = ref(false)

function closeHelp() {
  helpOpen.value = false
}
function onWindowClick() {
  if (helpOpen.value) helpOpen.value = false
}
onMounted(() => window.addEventListener('click', onWindowClick))
onUnmounted(() => window.removeEventListener('click', onWindowClick))

async function loadTemplate() {
  closeHelp()
  await loadDemo(false)
  router.push({ name: 'designer' })
}
function openDocs() {
  closeHelp()
  uiStore.showDocs = true
}
</script>

<style scoped>
.icon-rail {
  width: 64px;
  flex-shrink: 0;
  height: 100%;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: var(--spacing-1);
  padding: var(--spacing-3) 0;
  background: var(--color-background-primary);
  border-right: 1px solid var(--color-border-primary);
}

.rail-logo {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 44px;
  height: 44px;
  margin-bottom: var(--spacing-2);
  border: none;
  background: transparent;
  cursor: pointer;
}
.rail-logo img { width: 34px; height: auto; }

.rail-nav {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: var(--spacing-1);
}

.rail-spacer { flex: 1; }

.rail-footer {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: var(--spacing-1);
  padding-top: var(--spacing-2);
  border-top: 1px solid var(--color-border-light);
  width: 44px;
}

.rail-item {
  position: relative;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 44px;
  height: 44px;
  border: none;
  background: transparent;
  border-radius: var(--border-radius-lg);
  color: var(--color-text-secondary);
  cursor: pointer;
  text-decoration: none;
  transition: all var(--transition-fast);
}
.rail-item:hover {
  background: var(--color-background-tertiary);
  color: var(--color-text-primary);
}
.rail-item.active {
  background: var(--color-accent-subtle);
  color: var(--color-accent);
}
.rail-item svg { width: 20px; height: 20px; }

/* Right-side tooltip (no FontAwesome / Element Plus) */
.rail-item[data-tooltip]::after {
  content: attr(data-tooltip);
  position: absolute;
  left: calc(100% + 10px);
  top: 50%;
  transform: translateY(-50%);
  padding: 4px 8px;
  background: var(--color-gray-800);
  color: var(--color-text-inverse);
  font-size: var(--font-size-xs);
  border-radius: var(--border-radius-sm);
  white-space: nowrap;
  opacity: 0;
  pointer-events: none;
  transition: opacity var(--transition-fast);
  z-index: var(--z-index-tooltip, 1100);
}
.rail-item[data-tooltip]:hover::after { opacity: 1; }

.rail-popover-wrap { position: relative; }
.rail-popover {
  position: absolute;
  left: calc(100% + 10px);
  bottom: 0;
  min-width: 180px;
  padding: var(--spacing-1);
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  box-shadow: var(--shadow-lg);
  z-index: var(--z-index-popover, 1075);
}
.rail-popover-item {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  width: 100%;
  padding: var(--spacing-2) var(--spacing-3);
  border: none;
  background: transparent;
  border-radius: var(--border-radius-sm);
  color: var(--color-text-primary);
  font-size: var(--font-size-sm);
  cursor: pointer;
  text-align: left;
}
.rail-popover-item:hover { background: var(--color-background-tertiary); }
.rail-popover-item .material-icons { font-size: 18px; color: var(--color-text-secondary); }

.rail-ready {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--color-warning);
  margin-top: var(--spacing-1);
}
.rail-ready.ready { background: var(--color-success); }
</style>
