<template>
  <div class="flows-view">
    <aside class="flows-sidebar">
      <div class="flows-filters">
        <button class="new-btn" @click="createNew">
          <i class="fa-solid fa-plus"></i>
          <span>New flow</span>
        </button>
        <input v-model="search" type="text" class="search-input" placeholder="Search flows..." />
      </div>
      <div class="flows-scroll">
        <ul v-if="filtered.length" class="listbox">
          <li
            v-for="flow in filtered"
            :key="flow.id"
            :class="{ 'is-selected': flow.id === selectedId }"
            @click="selectedId = flow.id"
          >
            <i class="fa-solid fa-diagram-project flow-icon"></i>
            <span class="item-name">{{ flow.name }}</span>
            <span class="item-meta">{{ flow.nodeCount }} node{{ flow.nodeCount === 1 ? '' : 's' }}</span>
          </li>
        </ul>
        <p v-else class="group-empty">
          {{ search ? 'No flows match your search.' : 'No saved flows yet. Build a flow in the Designer and click Save.' }}
        </p>
      </div>
    </aside>

    <section class="flows-detail">
      <div v-if="selected" class="detail">
        <header class="detail-header">
          <input v-model="nameDraft" class="detail-name" title="Rename flow" @change="commitName" @keyup.enter="commitName" />
          <div class="detail-actions">
            <button class="ghost-btn primary" @click="openFlow(selected.id)">
              <i class="fa-solid fa-up-right-from-square"></i> Open
            </button>
            <button class="ghost-btn" @click="duplicateFlow(selected.id)">
              <i class="fa-solid fa-copy"></i> Duplicate
            </button>
            <button class="ghost-btn danger" @click="deleteFlow(selected)">
              <i class="fa-solid fa-trash"></i> Delete
            </button>
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
            <span class="meta-value">{{ selected.nodeCount }}</span>
          </div>
          <div class="meta-card">
            <span class="meta-label">Created</span>
            <span class="meta-value">{{ formatDate(selected.createdAt) }}</span>
          </div>
          <div class="meta-card">
            <span class="meta-label">Modified</span>
            <span class="meta-value">{{ formatDate(selected.updatedAt) }}</span>
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

      <div v-else class="flows-placeholder">
        <i class="fa-solid fa-diagram-project"></i>
        <p>Select a flow to manage it, or build one in the Designer and click Save.</p>
      </div>
    </section>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { useSavedFlowsStore, type SavedFlow } from '../../stores/saved-flows-store'
import { useRunHistoryStore } from '../../stores/run-history-store'
import { useFlowTabsStore } from '../../stores/flow-tabs-store'

const router = useRouter()
const savedFlows = useSavedFlowsStore()
const runHistory = useRunHistoryStore()
const flowTabs = useFlowTabsStore()

const search = ref('')
const selectedId = ref<string | null>(null)

const filtered = computed(() => {
  const q = search.value.trim().toLowerCase()
  if (!q) return savedFlows.flows
  return savedFlows.flows.filter(
    (f) => f.name.toLowerCase().includes(q) || f.description.toLowerCase().includes(q)
  )
})

const selected = computed(() => savedFlows.flows.find((f) => f.id === selectedId.value) ?? null)

// Newest run for the selected flow — join by stable id, falling back to name
// for legacy runs recorded before flow ids existed. runHistory.runs is sorted
// newest-first by the store.
const lastRun = computed(() => {
  const f = selected.value
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
  selected,
  (f) => {
    nameDraft.value = f?.name ?? ''
    descDraft.value = f?.description ?? ''
  },
  { immediate: true }
)

async function commitName() {
  const f = selected.value
  if (!f) return
  const v = nameDraft.value.trim()
  if (!v || v === f.name) {
    nameDraft.value = f.name
    return
  }
  await savedFlows.rename(f.id, v)
}

async function commitDesc() {
  const f = selected.value
  if (!f || descDraft.value === f.description) return
  await savedFlows.updateDescription(f.id, descDraft.value)
}

async function openFlow(id: string) {
  const ok = await savedFlows.open(id)
  if (ok) router.push({ name: 'designer' })
}

async function duplicateFlow(id: string) {
  const newId = await savedFlows.duplicate(id)
  if (newId) selectedId.value = newId
}

async function deleteFlow(flow: SavedFlow) {
  if (!confirm(`Delete "${flow.name}"? This cannot be undone.`)) return
  await savedFlows.remove(flow.id)
  if (selectedId.value === flow.id) selectedId.value = null
}

function createNew() {
  flowTabs.newTab()
  router.push({ name: 'designer' })
}

function formatDate(ts: number): string {
  return new Date(ts).toLocaleString(undefined, {
    month: 'short', day: 'numeric', year: 'numeric', hour: '2-digit', minute: '2-digit'
  })
}

onMounted(() => {
  savedFlows.refresh()
  runHistory.refresh()
})
</script>

<style scoped>
.flows-view { display: flex; flex: 1; overflow: hidden; height: 100%; }

/* Sidebar */
.flows-sidebar {
  width: 320px;
  min-width: 280px;
  display: flex;
  flex-direction: column;
  border-right: 1px solid var(--color-border-primary);
  background: var(--color-background-secondary);
}

.flows-filters {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
  padding: var(--spacing-3) var(--spacing-4);
  border-bottom: 1px solid var(--color-border-light);
}

.new-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-2);
  width: 100%;
  height: 32px;
  border: 1px solid var(--color-accent);
  background: var(--color-accent-subtle);
  color: var(--color-accent);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  cursor: pointer;
  transition: all var(--transition-fast);
}
.new-btn:hover { background: var(--color-accent); color: #fff; }
.new-btn i { font-size: 14px; }

.search-input {
  width: 100%;
  padding: var(--spacing-2) var(--spacing-3);
  font-size: var(--font-size-sm);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  color: var(--color-text-primary);
}
.search-input:focus { outline: none; border-color: var(--color-accent); box-shadow: 0 0 0 2px var(--color-focus-ring-accent); }

.flows-scroll { flex: 1; overflow-y: auto; padding: var(--spacing-3); }

.listbox li { display: flex; align-items: center; gap: var(--spacing-2); }
.flow-icon { font-size: 16px; color: var(--color-text-secondary); flex-shrink: 0; }
.item-name { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--color-text-primary); }
.item-meta { font-size: var(--font-size-xs); color: var(--color-text-muted); flex-shrink: 0; }

.group-empty { margin: 0; padding: var(--spacing-3); font-size: var(--font-size-xs); color: var(--color-text-muted); font-style: italic; }

/* Detail */
.flows-detail { flex: 1; overflow-y: auto; padding: var(--spacing-5) var(--spacing-6); }
.detail { display: flex; flex-direction: column; gap: var(--spacing-4); }

.detail-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-3);
  flex-wrap: wrap;
}

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

.detail-actions { display: flex; gap: var(--spacing-2); }

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

.flows-placeholder {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-2);
  height: 100%;
  color: var(--color-text-muted);
  text-align: center;
}
.flows-placeholder i { font-size: 28px; opacity: 0.6; }
.flows-placeholder p { margin: 0; max-width: 320px; font-size: var(--font-size-sm); }

@media (max-width: 768px) {
  .flows-sidebar { width: 240px; min-width: 220px; }
}
</style>
