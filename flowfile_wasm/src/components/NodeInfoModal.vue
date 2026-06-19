<template>
  <Teleport to="body">
    <div
      ref="cardRef"
      class="node-info-card"
      :style="{ top: `${coords.y}px`, left: `${coords.x}px` }"
      role="dialog"
      :aria-label="`About ${name}`"
    >
      <button class="node-info-close" title="Close" @click="emit('close')">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
      </button>
      <div class="node-info-body">
        <div class="node-info-header">
          <h2 class="node-info-title">{{ name }}</h2>
          <span v-if="!available" class="node-info-badge">Full app only</span>
        </div>

        <p v-if="intro" class="node-info-intro">{{ intro }}</p>

        <!-- Full-app-only nodes: explain the limitation and link to the installer. -->
        <div v-if="!available" class="node-info-upgrade">
          <p class="node-info-upgrade-title">This node runs in the full Flowfile installation only.</p>
          <p class="node-info-upgrade-text">
            The in-browser editor is a lite build. Install the full version to use this node —
            along with scheduling, database &amp; cloud connections, and more.
          </p>
          <a
            class="node-info-upgrade-btn"
            href="https://flowfile.io/install/"
            target="_blank"
            rel="noopener noreferrer"
          >
            <span>Install the full version</span>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg>
          </a>
        </div>

        <!-- Learn more: the node's documentation section (opens in a new tab). -->
        <a
          v-if="docsUrl"
          class="node-info-docs"
          :href="docsUrl"
          target="_blank"
          rel="noopener noreferrer"
        >
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"/><path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"/></svg>
          <span>Learn more about this node</span>
        </a>
      </div>
    </div>
  </Teleport>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted, nextTick, ref } from 'vue'

const props = defineProps<{
  name: string
  intro: string
  docsUrl: string
  available: boolean
  // Viewport coordinates of the click that opened the popup; the card anchors here.
  position: { x: number; y: number }
}>()

const emit = defineEmits<{ (e: 'close'): void }>()

const cardRef = ref<HTMLElement | null>(null)
const coords = ref({ x: props.position.x, y: props.position.y })

// Keep the card on-screen: shift it left/up if anchoring at the cursor would overflow.
function clampToViewport() {
  const el = cardRef.value
  if (!el) return
  const margin = 8
  const rect = el.getBoundingClientRect()
  let left = props.position.x
  let top = props.position.y
  if (left + rect.width > window.innerWidth - margin) left = window.innerWidth - rect.width - margin
  if (top + rect.height > window.innerHeight - margin) top = window.innerHeight - rect.height - margin
  coords.value = { x: Math.max(margin, left), y: Math.max(margin, top) }
}

function onKeydown(event: KeyboardEvent) {
  if (event.key === 'Escape') emit('close')
}

function onPointerDown(event: PointerEvent) {
  if (cardRef.value && !cardRef.value.contains(event.target as Node)) {
    emit('close')
  }
}

onMounted(() => {
  nextTick(clampToViewport)
  document.addEventListener('keydown', onKeydown)
  // Capture phase so the VueFlow canvas can't swallow the event before we see it.
  document.addEventListener('pointerdown', onPointerDown, true)
})

onUnmounted(() => {
  document.removeEventListener('keydown', onKeydown)
  document.removeEventListener('pointerdown', onPointerDown, true)
})
</script>

<style scoped>
.node-info-card {
  position: fixed;
  z-index: var(--z-index-canvas-context-menu, 100002);
  width: 320px;
  max-width: calc(100vw - 16px);
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-lg);
  box-shadow: var(--shadow-lg);
}

.node-info-close {
  position: absolute;
  top: var(--spacing-2);
  right: var(--spacing-2);
  display: flex;
  align-items: center;
  justify-content: center;
  width: 28px;
  height: 28px;
  border: none;
  background: transparent;
  border-radius: var(--border-radius-md);
  color: var(--color-text-muted);
  cursor: pointer;
}
.node-info-close:hover { background: var(--color-background-tertiary); color: var(--color-text-primary); }
.node-info-close svg { width: 16px; height: 16px; }

.node-info-body {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
  padding: var(--spacing-4);
}

.node-info-header {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding-right: var(--spacing-6);
}

.node-info-title {
  margin: 0;
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.node-info-badge {
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
  background: var(--color-background-tertiary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-full);
  padding: 2px var(--spacing-2);
  white-space: nowrap;
}

.node-info-intro {
  margin: 0;
  font-size: var(--font-size-sm);
  line-height: var(--line-height-relaxed);
  color: var(--color-text-secondary);
}

.node-info-upgrade {
  padding: var(--spacing-3);
  background: rgba(8, 145, 178, 0.1);
  border: 1px solid rgba(8, 145, 178, 0.35);
  border-radius: var(--border-radius-md);
}

.node-info-upgrade-title {
  margin: 0;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.node-info-upgrade-text {
  margin: var(--spacing-2) 0 0;
  font-size: var(--font-size-xs);
  line-height: var(--line-height-relaxed);
  color: var(--color-text-secondary);
}

.node-info-upgrade-btn {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-2);
  margin-top: var(--spacing-3);
  padding: var(--spacing-2) var(--spacing-4);
  background: var(--color-accent);
  border: none;
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-inverse);
  text-decoration: none;
  transition: all var(--transition-fast);
}
.node-info-upgrade-btn:hover { background: var(--color-accent-hover, var(--color-accent)); transform: translateY(-1px); }
.node-info-upgrade-btn svg { width: 15px; height: 15px; }

.node-info-docs {
  display: inline-flex;
  align-items: center;
  align-self: flex-start;
  gap: var(--spacing-2);
  padding: var(--spacing-1-5) var(--spacing-3);
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  text-decoration: none;
  transition: all var(--transition-fast);
}
.node-info-docs:hover { background: var(--color-background-tertiary); border-color: var(--color-accent); color: var(--color-accent); }
.node-info-docs svg { width: 15px; height: 15px; }
</style>
