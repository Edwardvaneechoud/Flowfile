<template>
  <Teleport to="body">
    <Transition name="tg-fade">
      <div v-if="visible" class="tg-overlay" @click.self="close">
        <div class="tg-card" role="dialog" aria-modal="true" aria-label="Flow templates">
          <header class="tg-header">
            <div>
              <h2 class="tg-title">Templates</h2>
              <p class="tg-sub">Start from a working example — opens in a new tab</p>
            </div>
            <button class="tg-close" title="Close" @click="close">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
            </button>
          </header>

          <div class="tg-grid">
            <button
              v-for="t in templates"
              :key="t.id"
              class="tg-tile"
              :disabled="loading"
              @click="select(t)"
            >
              <span class="tg-tile-top">
                <span class="tg-icon"><span class="material-icons">{{ t.icon }}</span></span>
                <span class="tg-badge" :class="`tg-badge--${t.category.toLowerCase()}`">{{ t.category }}</span>
              </span>
              <span class="tg-name">{{ t.name }}</span>
              <span class="tg-desc">{{ t.description }}</span>
              <span class="tg-meta">
                <span class="tg-chips">
                  <span v-for="h in t.highlights" :key="h" class="tg-chip">{{ h }}</span>
                </span>
                <span class="tg-nodes">{{ t.nodeCount }} nodes</span>
              </span>
            </button>
          </div>

          <p v-if="error" class="tg-error">{{ error }}</p>
        </div>
      </div>
    </Transition>
  </Teleport>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted } from 'vue'
import { FLOW_TEMPLATES, type FlowTemplate } from '../config/templates'

defineProps<{ visible: boolean; loading?: boolean; error?: string | null }>()
const emit = defineEmits<{
  (e: 'select', template: FlowTemplate): void
  (e: 'close'): void
}>()

const templates = FLOW_TEMPLATES

function close() {
  emit('close')
}

function select(template: FlowTemplate) {
  emit('select', template)
}

function onKey(e: KeyboardEvent) {
  if (e.key === 'Escape') close()
}
onMounted(() => window.addEventListener('keydown', onKey))
onUnmounted(() => window.removeEventListener('keydown', onKey))
</script>

<style scoped>
.tg-overlay {
  position: fixed;
  inset: 0;
  z-index: var(--z-index-modal, 1050);
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--color-overlay, rgba(0, 0, 0, 0.5));
  padding: var(--spacing-4);
}

.tg-card {
  position: relative;
  width: 100%;
  max-width: 880px;
  max-height: 88vh;
  display: flex;
  flex-direction: column;
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-xl);
  box-shadow: var(--shadow-lg);
  overflow: hidden;
}

.tg-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  padding: var(--spacing-5) var(--spacing-5) var(--spacing-3);
  border-bottom: 1px solid var(--color-border-light);
}

.tg-title {
  margin: 0;
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.tg-sub {
  margin: var(--spacing-1) 0 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-tertiary);
}

.tg-close {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 30px;
  height: 30px;
  border: none;
  background: transparent;
  border-radius: var(--border-radius-md);
  color: var(--color-text-muted);
  cursor: pointer;
  flex-shrink: 0;
}
.tg-close:hover { background: var(--color-background-tertiary); color: var(--color-text-primary); }
.tg-close svg { width: 16px; height: 16px; }

.tg-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: var(--spacing-3);
  padding: var(--spacing-4) var(--spacing-5) var(--spacing-5);
  overflow-y: auto;
}

@media (max-width: 640px) {
  .tg-grid { grid-template-columns: 1fr; }
}

.tg-tile {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: var(--spacing-2);
  padding: var(--spacing-4);
  text-align: left;
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-lg);
  cursor: pointer;
  font-family: inherit;
  transition: all var(--transition-fast);
}
.tg-tile:hover:not(:disabled) {
  border-color: var(--color-accent);
  box-shadow: var(--shadow-md);
  transform: translateY(-1px);
}
.tg-tile:disabled { opacity: 0.6; cursor: progress; }
.tg-tile:focus-visible { outline: 2px solid var(--color-accent); outline-offset: 2px; }

.tg-tile-top {
  display: flex;
  align-items: center;
  justify-content: space-between;
  width: 100%;
}

.tg-icon {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 38px;
  height: 38px;
  border-radius: var(--border-radius-lg);
  background: var(--color-accent-subtle);
  color: var(--color-accent);
}
.tg-icon .material-icons { font-size: 20px; }

.tg-badge {
  font-size: var(--font-size-2xs);
  font-weight: var(--font-weight-semibold);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  padding: 2px var(--spacing-2);
  border-radius: var(--border-radius-full);
}
.tg-badge--beginner { color: var(--color-success); background: var(--color-success-light, rgba(72, 187, 120, 0.12)); }
.tg-badge--intermediate { color: var(--color-accent); background: var(--color-accent-subtle); }

.tg-name {
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.tg-desc {
  font-size: var(--font-size-sm);
  line-height: var(--line-height-normal);
  color: var(--color-text-tertiary);
  flex: 1;
}

.tg-meta {
  display: flex;
  align-items: center;
  justify-content: space-between;
  width: 100%;
  margin-top: var(--spacing-1);
}

.tg-chips { display: flex; flex-wrap: wrap; gap: var(--spacing-1); }

.tg-chip {
  font-size: var(--font-size-2xs);
  color: var(--color-text-secondary);
  background: var(--color-background-tertiary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-sm);
  padding: 1px var(--spacing-1-5);
}

.tg-nodes {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  flex-shrink: 0;
  margin-left: var(--spacing-2);
}

.tg-error {
  margin: 0;
  padding: 0 var(--spacing-5) var(--spacing-4);
  font-size: var(--font-size-sm);
  color: var(--color-danger);
}

.tg-fade-enter-active, .tg-fade-leave-active { transition: opacity var(--transition-base); }
.tg-fade-enter-from, .tg-fade-leave-to { opacity: 0; }
</style>
