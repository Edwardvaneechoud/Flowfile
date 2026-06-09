<template>
  <WelcomeScreen
    :recent-flows="savedFlowsStore.recent"
    @create="handleCreate"
    @open="handleOpen"
    @browse-templates="handleBrowseTemplates"
    @open-recent="handleOpenRecent"
    @remove-recent="savedFlowsStore.remove"
  />
  <input
    ref="fileInput"
    type="file"
    accept=".json,.yaml,.yml,.flowfile"
    style="display: none"
    @change="onFileChange"
  />
  <TemplatesGallery
    :visible="showTemplates"
    :loading="isLoading"
    :error="loadError"
    @select="onSelectTemplate"
    @close="showTemplates = false"
  />
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import WelcomeScreen from './HomeView/WelcomeScreen.vue'
import TemplatesGallery from '../components/TemplatesGallery.vue'
import { useFlowTabsStore } from '../stores/flow-tabs-store'
import { useSavedFlowsStore } from '../stores/saved-flows-store'
import { useTemplates } from '../composables/useTemplates'
import type { FlowTemplate } from '../config/templates'

const router = useRouter()
const flowTabsStore = useFlowTabsStore()
const savedFlowsStore = useSavedFlowsStore()
const { isLoading, loadError, loadTemplate } = useTemplates()

const fileInput = ref<HTMLInputElement | null>(null)
const showTemplates = ref(false)

onMounted(() => {
  savedFlowsStore.refresh()
})

const goDesigner = () => router.push({ name: 'designer' })

// All Home actions open in a (new) tab so existing open flows are preserved.
function handleCreate() {
  flowTabsStore.newTab()
  goDesigner()
}

function handleOpen() {
  fileInput.value?.click()
}

async function onFileChange(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]
  input.value = '' // allow re-selecting the same file
  if (!file) return
  const result = await flowTabsStore.openFile(file)
  if (result.success) {
    goDesigner()
  } else {
    alert('Failed to load flow file. Please check the file format.')
  }
}

function handleBrowseTemplates() {
  showTemplates.value = true
}

async function onSelectTemplate(template: FlowTemplate) {
  const ok = await flowTabsStore.openWith(() => loadTemplate(template))
  if (ok) {
    showTemplates.value = false
    goDesigner()
  }
}

async function handleOpenRecent(id: string) {
  const ok = await savedFlowsStore.open(id)
  if (ok) goDesigner()
}
</script>
