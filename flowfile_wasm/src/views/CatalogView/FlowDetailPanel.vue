<template>
  <div v-if="flow" class="flow-detail">
    <header class="detail-header">
      <div class="header-main">
        <input
          v-model="nameDraft"
          class="detail-name"
          title="Rename flow"
          @change="commitName"
          @keyup.enter="commitName"
        />
        <div class="detail-actions">
          <button class="ghost-btn primary" @click="openFlow">
            <i class="fa-solid fa-up-right-from-square"></i> Open in Designer
          </button>
          <button
            class="ghost-btn"
            :class="{ active: isFavorite }"
            :title="isFavorite ? 'Unfavorite' : 'Favorite'"
            @click="favorites.toggle(itemId)"
          >
            <i :class="isFavorite ? 'fa-solid fa-star' : 'fa-regular fa-star'"></i>
          </button>
          <button class="ghost-btn" @click="duplicateFlow">
            <i class="fa-solid fa-copy"></i> Duplicate
          </button>
          <button class="ghost-btn danger" @click="deleteFlow">
            <i class="fa-solid fa-trash"></i> Delete
          </button>
        </div>
      </div>
    </header>

    <textarea
      v-model="descDraft"
      class="detail-desc"
      rows="2"
      placeholder="Add a description…"
      @change="commitDesc"
    ></textarea>

    <div class="meta-grid">
      <div class="meta-card">
        <span class="meta-label">Nodes</span>
        <span class="meta-value">{{ flow.nodeCount }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Created</span>
        <span class="meta-value">{{ formatDate(flow.createdAt) }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Modified</span>
        <span class="meta-value">{{ formatDate(flow.updatedAt) }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Last run</span>
        <span class="meta-value">
          <span v-if="lastRun" class="status-badge" :class="lastRun.success ? 'is-success' : 'is-failure'">
            <i :class="lastRun.success ? 'fa-solid fa-circle-check' : 'fa-solid fa-circle-xmark'"></i>
            {{ formatDate(lastRun.startedAt) }}
          </span>
          <span v-else class="muted">Never run</span>
        </span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useRouter } from 'vue-router'
import { useSavedFlowsStore } from '../../stores/saved-flows-store'
import { useRunHistoryStore } from '../../stores/run-history-store'
import { useFavoritesStore } from '../../stores/favorites-store'

const props = defineProps<{ flowId: string }>()
const emit = defineEmits<{ (e: 'deleted'): void; (e: 'select', itemId: string): void }>()

const router = useRouter()
const savedFlows = useSavedFlowsStore()
const runHistory = useRunHistoryStore()
const favorites = useFavoritesStore()

const flow = computed(() => savedFlows.flows.find((f) => f.id === props.flowId) ?? null)
const itemId = computed(() => `flow-${props.flowId}`)
const isFavorite = computed(() => favorites.isFavorite(itemId.value))

// Newest run for this flow — join by stable id, fallback to name for legacy runs.
const lastRun = computed(() => {
  const f = flow.value
  if (!f) return null
  return (
    runHistory.runs.find((r) => r.flowId === f.id) ??
    runHistory.runs.find((r) => !r.flowId && r.flowName === f.name) ??
    null
  )
})

const nameDraft = ref('')
const descDraft = ref('')
watch(
  flow,
  (f) => {
    nameDraft.value = f?.name ?? ''
    descDraft.value = f?.description ?? ''
  },
  { immediate: true }
)

async function commitName() {
  const f = flow.value
  if (!f) return
  const v = nameDraft.value.trim()
  if (!v || v === f.name) {
    nameDraft.value = f.name
    return
  }
  await savedFlows.rename(f.id, v)
}

async function commitDesc() {
  const f = flow.value
  if (!f || descDraft.value === f.description) return
  await savedFlows.updateDescription(f.id, descDraft.value)
}

async function openFlow() {
  const ok = await savedFlows.open(props.flowId)
  if (ok) router.push({ name: 'designer' })
}

async function duplicateFlow() {
  const newId = await savedFlows.duplicate(props.flowId)
  if (newId) emit('select', `flow-${newId}`)
}

async function deleteFlow() {
  const f = flow.value
  if (!f) return
  if (!confirm(`Delete "${f.name}"? This cannot be undone.`)) return
  await savedFlows.remove(f.id)
  if (favorites.isFavorite(itemId.value)) favorites.toggle(itemId.value)
  emit('deleted')
}

function formatDate(ts: number): string {
  return new Date(ts).toLocaleString(undefined, {
    month: 'short', day: 'numeric', year: 'numeric', hour: '2-digit', minute: '2-digit'
  })
}
</script>

<style scoped>
.flow-detail { display: flex; flex-direction: column; gap: var(--spacing-4); max-width: 1000px; }

.detail-header { display: flex; flex-direction: column; gap: var(--spacing-2); }
.header-main { display: flex; align-items: center; justify-content: space-between; gap: var(--spacing-3); flex-wrap: wrap; }

.detail-name {
  flex: 1;
  min-width: 160px;
  padding: var(--spacing-1) var(--spacing-2);
  font-size: var(--font-size-2xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
  background: transparent;
  border: 1px solid transparent;
  border-radius: var(--border-radius-md);
}
.detail-name:hover { border-color: var(--color-border-light); }
.detail-name:focus { outline: none; border-color: var(--color-accent); background: var(--color-background-primary); }

.detail-actions { display: flex; gap: var(--spacing-2); flex-wrap: wrap; }

.ghost-btn {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  padding: var(--spacing-1-5) var(--spacing-3);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  border: 1px solid var(--color-border-primary);
  background: var(--color-background-primary);
  color: var(--color-text-secondary);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  transition: all var(--transition-fast);
}
.ghost-btn i { font-size: 13px; }
.ghost-btn:hover { background: var(--color-background-hover); color: var(--color-text-primary); }
.ghost-btn.primary { border-color: var(--color-accent); background: var(--color-accent-subtle); color: var(--color-accent); }
.ghost-btn.primary:hover { background: var(--color-accent); color: #fff; }
.ghost-btn.active { color: var(--color-warning); border-color: var(--color-warning); }
.ghost-btn.danger:hover { border-color: var(--color-danger); color: var(--color-danger); }

.detail-desc {
  width: 100%;
  resize: vertical;
  padding: var(--spacing-2) var(--spacing-3);
  font-family: inherit;
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
}
.detail-desc:focus { outline: none; border-color: var(--color-accent); box-shadow: 0 0 0 2px var(--color-focus-ring-accent); }

.meta-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: var(--spacing-3); }
.meta-card {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
  padding: var(--spacing-3);
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
}
.meta-label { font-size: var(--font-size-xs); text-transform: uppercase; letter-spacing: 0.04em; color: var(--color-text-muted); }
.meta-value { font-size: var(--font-size-sm); font-weight: var(--font-weight-medium); color: var(--color-text-primary); }
.muted { color: var(--color-text-muted); font-weight: var(--font-weight-normal); }

.status-badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 1px 8px;
  border-radius: var(--border-radius-full);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
}
.status-badge i { font-size: 11px; }
.status-badge.is-success { background: color-mix(in srgb, var(--color-success) 14%, transparent); color: var(--color-success); }
.status-badge.is-failure { background: color-mix(in srgb, var(--color-danger) 14%, transparent); color: var(--color-danger); }
</style>
