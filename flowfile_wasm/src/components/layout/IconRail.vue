<template>
  <nav class="icon-rail">
    <button class="rail-logo" title="Home" @click="router.push({ name: 'home' })">
      <img src="/flowfile.png" alt="Flowfile" />
    </button>

    <div class="rail-nav">
      <RouterLink to="/" class="rail-item" :class="{ active: route.name === 'home' }" data-tooltip="Home">
        <i class="fa-solid fa-house"></i>
      </RouterLink>
      <RouterLink to="/designer" class="rail-item" :class="{ active: route.name === 'designer' }" data-tooltip="Designer">
        <i class="fa-solid fa-diagram-project"></i>
      </RouterLink>
      <RouterLink to="/catalog" class="rail-item" :class="{ active: route.name === 'catalog' }" data-tooltip="Catalog">
        <i class="fa-solid fa-folder-tree"></i>
      </RouterLink>
    </div>

    <div class="rail-spacer"></div>

    <div class="rail-footer">
      <!-- Help / more popover -->
      <div class="rail-popover-wrap">
        <button class="rail-item" :class="{ active: helpOpen }" data-tooltip="More & help" @click.stop="helpOpen = !helpOpen">
          <i class="fa-solid fa-circle-question"></i>
        </button>
        <div v-if="helpOpen" class="rail-popover">
          <button class="rail-popover-item" @click="loadTemplate">
            <i class="fa-solid fa-layer-group"></i><span>Templates</span>
          </button>
          <button class="rail-popover-item" @click="openDocs">
            <i class="fa-solid fa-book"></i><span>Documentation</span>
          </button>
        </div>
      </div>

      <button class="rail-item" :title="isDark ? 'Switch to light mode' : 'Switch to dark mode'" @click="toggleTheme">
        <i v-if="isDark" class="fa-solid fa-sun"></i>
        <i v-else class="fa-solid fa-moon"></i>
      </button>

      <button class="rail-item" data-tooltip="About" @click="uiStore.showAbout = true">
        <i class="fa-solid fa-circle-info"></i>
      </button>

      <a class="rail-item" href="https://github.com/edwardvaneechoud/Flowfile" target="_blank" rel="noopener" data-tooltip="GitHub">
        <i class="fa-brands fa-github"></i>
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
.rail-item i { font-size: 18px; line-height: 1; }

/* Right-side tooltip (CSS-only, no icon library) */
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
.rail-popover-item i { font-size: 16px; width: 18px; text-align: center; color: var(--color-text-secondary); }

.rail-ready {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--color-warning);
  margin-top: var(--spacing-1);
}
.rail-ready.ready { background: var(--color-success); }
</style>
