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
import { useFlowStore } from '../stores/flow-store'
import { useRecentFlowsStore } from '../stores/recent-flows-store'
import { useDemo } from '../composables/useDemo'

const router = useRouter()
const flowStore = useFlowStore()
const recentFlowsStore = useRecentFlowsStore()
const { loadDemo } = useDemo()

const fileInput = ref<HTMLInputElement | null>(null)

onMounted(() => {
  recentFlowsStore.refresh()
})

const goDesigner = () => router.push({ name: 'designer' })

function handleCreate() {
  flowStore.clearFlow()
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
  const result = await flowStore.loadFlowfile(file)
  if (result.success) {
    await recentFlowsStore.refresh()
    goDesigner()
  } else {
    alert('Failed to load flow file. Please check the file format.')
  }
}

async function handleBrowseTemplates() {
  await loadDemo(false)
  goDesigner()
}

async function handleOpenRecent(id: string) {
  const ok = await recentFlowsStore.openRecent(id)
  if (ok) goDesigner()
}
</script>
