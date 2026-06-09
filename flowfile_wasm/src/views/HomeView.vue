<template>
  <WelcomeScreen
    :recent-flows="recentFlowsStore.recentFlows"
    @create="handleCreate"
    @open="handleOpen"
    @browse-templates="handleBrowseTemplates"
    @open-recent="handleOpenRecent"
    @remove-recent="recentFlowsStore.remove"
  />
  <input
    ref="fileInput"
    type="file"
    accept=".json,.yaml,.yml,.flowfile"
    style="display: none"
    @change="onFileChange"
  />
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import WelcomeScreen from './HomeView/WelcomeScreen.vue'
import { useFlowTabsStore } from '../stores/flow-tabs-store'
import { useRecentFlowsStore } from '../stores/recent-flows-store'
import { useDemo } from '../composables/useDemo'

const router = useRouter()
const flowTabsStore = useFlowTabsStore()
const recentFlowsStore = useRecentFlowsStore()
const { loadDemo } = useDemo()

const fileInput = ref<HTMLInputElement | null>(null)

onMounted(() => {
  recentFlowsStore.refresh()
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
    await recentFlowsStore.refresh()
    goDesigner()
  } else {
    alert('Failed to load flow file. Please check the file format.')
  }
}

async function handleBrowseTemplates() {
  const ok = await flowTabsStore.openWith(async () => {
    await loadDemo(false)
    return true
  })
  if (ok) goDesigner()
}

async function handleOpenRecent(id: string) {
  const ok = await flowTabsStore.openWith(() => recentFlowsStore.openRecent(id))
  if (ok) goDesigner()
}
</script>
