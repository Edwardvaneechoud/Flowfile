<template>
  <div class="app-shell">
    <IconRail />

    <div class="app-content">
      <!-- Designer top bar (only on the designer route). Reuses the action
           registry that Canvas populates on mount. -->
      <header v-if="route.name === 'designer'" class="designer-header">
        <div class="designer-header__left">
          <button class="action-btn" title="New flow (opens a new tab)" @click="flowTabsStore.newTab()">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 5v14M5 12h14"/></svg>
            <span>New</span>
          </button>
          <button class="action-btn" title="Open flow in a new tab" @click="triggerOpen">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/></svg>
            <span>Open</span>
          </button>
          <button class="action-btn" title="Save flow to the catalog" @click="handleSave">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg>
            <span>Save</span>
          </button>
          <button class="action-btn" title="Export flow to file" @click="uiStore.actions?.exportFile()">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
            <span>Export</span>
          </button>
          <transition name="fade">
            <span v-if="savedFlash" class="save-flash">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>
              Saved
            </span>
          </transition>
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

      <!-- Open-flow file input (Open → new tab) -->
      <input
        ref="openInput"
        type="file"
        accept=".yaml,.yml,.json,.flowfile"
        style="display: none"
        @change="handleOpenChange"
      />

      <FlowTabs v-if="route.name === 'designer'" />

      <main class="app-page"><router-view /></main>
    </div>

    <DocsModal :is-open="uiStore.showDocs" @close="uiStore.showDocs = false" />
    <AboutDialog v-model:visible="uiStore.showAbout" :version="version" />
    <SaveFlowModal
      :is-open="showSaveModal"
      :initial-name="saveModalName"
      @save="onSaveModal"
      @close="showSaveModal = false"
    />

    <!-- Prominent demo button for first-time visitors on Home and the designer
         canvas (mirrors the original app's prominent demo). Loads then opens the
         designer. -->
    <DemoButton
      v-if="(route.name === 'home' || route.name === 'designer') && !shouldAutoLoadDemo && !hasSeenDemo && !hasDismissedDemo && pyodideReady"
      prominent
      @loaded="onDemoLoaded"
    />
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { storeToRefs } from 'pinia'
import IconRail from '../components/layout/IconRail.vue'
import FlowTabs from '../components/FlowTabs.vue'
import DocsModal from '../components/DocsModal.vue'
import SaveFlowModal from '../components/SaveFlowModal.vue'
import DemoButton from '../components/DemoButton.vue'
import AboutDialog from './HomeView/AboutDialog.vue'
import { usePyodideStore } from '../stores/pyodide-store'
import { useThemeStore } from '../stores/theme-store'
import { useFlowStore } from '../stores/flow-store'
import { useFlowTabsStore } from '../stores/flow-tabs-store'
import { useSavedFlowsStore } from '../stores/saved-flows-store'
import { useDesignerUiStore } from '../stores/designer-ui-store'
import { useDemo } from '../composables/useDemo'

const route = useRoute()
const router = useRouter()
const pyodideStore = usePyodideStore()
const themeStore = useThemeStore()
const flowStore = useFlowStore()
const flowTabsStore = useFlowTabsStore()
const savedFlowsStore = useSavedFlowsStore()
const uiStore = useDesignerUiStore()
const { isReady: pyodideReady } = storeToRefs(pyodideStore)
const { isExecuting } = storeToRefs(flowStore)
const { hasSeenDemo, hasDismissedDemo, loadDemo } = useDemo()

const version = __APP_VERSION__
const urlParams = new URLSearchParams(window.location.search)
const shouldAutoLoadDemo = urlParams.get('demo') === 'true'

const openInput = ref<HTMLInputElement | null>(null)

function triggerOpen() {
  openInput.value?.click()
}

async function handleOpenChange(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]
  input.value = ''
  if (!file) return
  const result = await flowTabsStore.openFile(file)
  if (!result.success) {
    alert('Failed to load flow file. Please check the file format.')
  } else if (result.missingFiles?.length) {
    const names = result.missingFiles.map((m) => m.fileName).join(', ')
    alert(`Flow opened. Some input files need to be re-loaded: ${names}`)
  }
}

// Save the active flow into the catalog. New/untitled flows open the
// "Save to catalog" dialog to pick a name; flows already in the catalog re-save
// silently. After a save, sync the tab's identity/name and flash a confirmation.
const savedFlash = ref(false)
const showSaveModal = ref(false)
const saveModalName = ref('')

function afterSave() {
  flowTabsStore.syncActiveIdentity()
  void savedFlowsStore.refresh() // keep the catalog list in sync immediately
  savedFlash.value = true
  setTimeout(() => (savedFlash.value = false), 1600)
}

async function handleSave() {
  const named = flowStore.currentFlowName && flowStore.currentFlowName !== 'Untitled Flow'
  if (flowStore.currentFlowId && named) {
    await flowStore.saveToLibrary()
    afterSave()
    return
  }
  saveModalName.value = named ? flowStore.currentFlowName : ''
  showSaveModal.value = true
}

async function onSaveModal(name: string) {
  showSaveModal.value = false
  await flowStore.saveToLibrary(name)
  afterSave()
}

// The prominent demo button loads into the live (active) flow; reflect its name
// on the active tab and open the designer.
function onDemoLoaded() {
  flowTabsStore.syncActiveName()
  router.push({ name: 'designer' })
}

// Pyodide + theme init are idempotent and run here once; the layout stays
// mounted across Home / Designer / Catalog navigation, so nothing re-initializes
// and flow state persists.
onMounted(async () => {
  themeStore.initialize()
  flowTabsStore.init()
  await pyodideStore.initialize()
})

if (shouldAutoLoadDemo) {
  watch(pyodideReady, async (ready) => {
    if (ready) {
      await loadDemo(false)
      flowTabsStore.syncActiveName()
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

/* Transient "Saved" confirmation next to the Save button */
.save-flash {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-success);
}
.save-flash svg { width: 15px; height: 15px; }
.fade-enter-active, .fade-leave-active { transition: opacity var(--transition-fast); }
.fade-enter-from, .fade-leave-to { opacity: 0; }

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
  .action-btn span { display: none; }
  .action-btn { padding: 0 var(--spacing-2); }
}
</style>
