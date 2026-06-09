<template>
  <div class="app-shell">
    <IconRail />

    <div class="app-content">
      <!-- Designer top bar (only on the designer route). Reuses the action
           registry that Canvas populates on mount. -->
      <header v-if="route.name === 'designer'" class="designer-header">
        <div class="designer-header__left">
          <button class="action-btn" title="New flow" @click="uiStore.actions?.clear()">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 5v14M5 12h14"/></svg>
            <span>New</span>
          </button>
          <button class="action-btn" title="Open flow" @click="uiStore.actions?.open()">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/></svg>
            <span>Open</span>
          </button>
          <button class="action-btn" title="Save flow" @click="uiStore.actions?.save()">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg>
            <span>Save</span>
          </button>
          <span class="designer-flow-name">{{ flowName }}</span>
        </div>

        <div class="designer-header__right">
          <button class="action-btn" :class="{ active: uiStore.showCodeGenerator }" title="Generate Python code" @click="uiStore.showCodeGenerator = !uiStore.showCodeGenerator">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg>
            <span>Code</span>
          </button>
          <button class="action-btn action-btn--run" :disabled="isExecuting" title="Run flow (Ctrl+E)" @click="uiStore.actions?.run()">
            <svg viewBox="0 0 24 24" fill="currentColor"><polygon points="5 3 19 12 5 21 5 3"/></svg>
            <span>{{ isExecuting ? 'Running…' : 'Run' }}</span>
          </button>
        </div>
      </header>

      <main class="app-page"><router-view /></main>
    </div>

    <DocsModal :is-open="uiStore.showDocs" @close="uiStore.showDocs = false" />
    <AboutDialog v-model:visible="uiStore.showAbout" :version="version" />

    <!-- Prominent demo button for first-time visitors on Home and the designer
         canvas (mirrors the original app's prominent demo). Loads then opens the
         designer. -->
    <DemoButton
      v-if="(route.name === 'home' || route.name === 'designer') && !shouldAutoLoadDemo && !hasSeenDemo && !hasDismissedDemo && pyodideReady"
      prominent
      @loaded="router.push({ name: 'designer' })"
    />
  </div>
</template>

<script setup lang="ts">
import { onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { storeToRefs } from 'pinia'
import IconRail from '../components/layout/IconRail.vue'
import DocsModal from '../components/DocsModal.vue'
import DemoButton from '../components/DemoButton.vue'
import AboutDialog from './HomeView/AboutDialog.vue'
import { usePyodideStore } from '../stores/pyodide-store'
import { useThemeStore } from '../stores/theme-store'
import { useFlowStore } from '../stores/flow-store'
import { useDesignerUiStore } from '../stores/designer-ui-store'
import { useDemo } from '../composables/useDemo'

const route = useRoute()
const router = useRouter()
const pyodideStore = usePyodideStore()
const themeStore = useThemeStore()
const flowStore = useFlowStore()
const uiStore = useDesignerUiStore()
const { isReady: pyodideReady } = storeToRefs(pyodideStore)
const { isExecuting, currentFlowName: flowName } = storeToRefs(flowStore)
const { hasSeenDemo, hasDismissedDemo, loadDemo } = useDemo()

const version = __APP_VERSION__
const urlParams = new URLSearchParams(window.location.search)
const shouldAutoLoadDemo = urlParams.get('demo') === 'true'

// Pyodide + theme init are idempotent and run here once; the layout stays
// mounted across Home / Designer / Catalog navigation, so nothing re-initializes
// and flow state persists.
onMounted(async () => {
  themeStore.initialize()
  await pyodideStore.initialize()
})

if (shouldAutoLoadDemo) {
  watch(pyodideReady, async (ready) => {
    if (ready) {
      await loadDemo(false)
      router.push({ name: 'designer' })
    }
  }, { immediate: true })
}
</script>

<style scoped>
.app-shell {
  display: flex;
  height: 100vh;
  background: var(--color-background-secondary);
  position: relative;
}

.app-content {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
}

.app-page {
  flex: 1;
  overflow: auto;
  min-height: 0;
}

/* Designer top bar */
.designer-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  height: 50px;
  padding: 0 var(--spacing-4);
  background: var(--color-background-primary);
  border-bottom: 1px solid var(--color-border-primary);
  flex-shrink: 0;
}

.designer-header__left,
.designer-header__right {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.designer-flow-name {
  margin-left: var(--spacing-3);
  padding-left: var(--spacing-3);
  border-left: 1px solid var(--color-border-light);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
  max-width: 280px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.action-btn {
  display: flex;
  align-items: center;
  gap: var(--spacing-1-5);
  height: 34px;
  padding: 0 var(--spacing-3);
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-lg);
  color: var(--color-text-primary);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  cursor: pointer;
  box-shadow: var(--shadow-xs);
  transition: all var(--transition-fast);
}
.action-btn svg { width: 16px; height: 16px; color: var(--color-text-secondary); }
.action-btn:hover { background: var(--color-background-tertiary); border-color: var(--color-border-secondary); }
.action-btn:hover svg { color: var(--color-text-primary); }
.action-btn:active { transform: translateY(1px); box-shadow: none; }
.action-btn.active {
  background: var(--color-accent-subtle);
  border-color: var(--color-accent);
  color: var(--color-accent);
}
.action-btn.active svg { color: var(--color-accent); }

/* Purple Run button (mirrors the full app) */
.action-btn--run {
  background: var(--color-accent-purple);
  border-color: var(--color-accent-purple);
  color: #fff;
}
.action-btn--run svg { color: #fff; }
.action-btn--run:hover:not(:disabled) {
  background: var(--color-accent-purple-hover);
  border-color: var(--color-accent-purple-hover);
  color: #fff;
}
.action-btn--run:disabled { opacity: 0.6; cursor: not-allowed; }

@media (max-width: 768px) {
  .designer-flow-name { display: none; }
  .action-btn span { display: none; }
  .action-btn { padding: 0 var(--spacing-2); }
}
</style>
