<template>
  <div class="welcome">
    <div class="welcome-inner">
      <header class="welcome-hero">
        <img src="/flowfile.png" alt="Flowfile logo" class="welcome-logo" />
        <h1 class="welcome-title">Flowfile</h1>
        <p class="welcome-tagline">Browser-based visual data designer</p>
      </header>

      <section aria-label="Start building">
        <h2 class="welcome-section-title welcome-section-title--tight">Start building</h2>
        <p class="welcome-section-sub">Three ways to start a visual data flow</p>
        <div class="welcome-primary">
          <article class="welcome-tile welcome-tile--primary">
            <button class="tile-main" @click="emit('create')">
              <span class="tile-icon"><span class="material-icons">add</span></span>
              <span class="tile-title">New flow</span>
              <span class="tile-sub">Start building on a blank canvas</span>
            </button>
            <div class="tile-footer"></div>
          </article>

          <article class="welcome-tile">
            <button class="tile-main" @click="emit('open')">
              <span class="tile-icon"><span class="material-icons">folder_open</span></span>
              <span class="tile-title">Open flow</span>
              <span class="tile-sub">From a .flowfile / .yaml on your device</span>
            </button>
            <div class="tile-footer">
              <span class="tile-kbd"><kbd>{{ MODIFIER }}</kbd><kbd>O</kbd></span>
            </div>
          </article>

          <article class="welcome-tile">
            <button class="tile-main" @click="emit('browse-templates')">
              <span class="tile-icon"><span class="material-icons">layers</span></span>
              <span class="tile-title">Browse templates</span>
              <span class="tile-sub">Start from a working example</span>
            </button>
            <div class="tile-footer"></div>
          </article>
        </div>
      </section>

      <section aria-label="Recent flows">
        <h2 class="welcome-section-title">Recent flows</h2>
        <ul v-if="recentFlows.length" class="recent-list">
          <li v-for="flow in recentFlows" :key="flow.id">
            <button class="recent-row" :title="flow.name" @click="emit('open-recent', flow.id)">
              <span class="material-icons recent-icon">description</span>
              <span class="recent-name">{{ flow.name }}</span>
              <span class="recent-loc">
                <span class="recent-meta">{{ flow.nodeCount }} node{{ flow.nodeCount === 1 ? '' : 's' }}</span>
              </span>
              <span class="recent-time">{{ relativeTime(flow.savedAt) }}</span>
              <span
                class="recent-delete"
                title="Remove from recents"
                @click.stop="emit('remove-recent', flow.id)"
              >
                <span class="material-icons">close</span>
              </span>
            </button>
          </li>
        </ul>
        <p v-else class="recent-empty">Flows you create or open will show up here.</p>
      </section>

      <section aria-label="Explore">
        <h2 class="welcome-section-title">Explore</h2>
        <div class="explore-grid">
          <button class="explore-tile" @click="goCatalog">
            <span class="explore-icon"><span class="material-icons">account_tree</span></span>
            <span class="explore-text">
              <span class="explore-label">Data Catalog</span>
              <span class="explore-desc">Tables, favorites &amp; run history</span>
            </span>
          </button>
          <button class="explore-tile" @click="goRuns">
            <span class="explore-icon"><span class="material-icons">history</span></span>
            <span class="explore-text">
              <span class="explore-label">Run History</span>
              <span class="explore-desc">Past flow executions</span>
            </span>
          </button>
          <button class="explore-tile" @click="emit('browse-templates')">
            <span class="explore-icon"><span class="material-icons">school</span></span>
            <span class="explore-text">
              <span class="explore-label">Interactive demo</span>
              <span class="explore-desc">Load a worked example flow</span>
            </span>
          </button>
          <button class="explore-tile" @click="uiStore.showDocs = true">
            <span class="explore-icon"><span class="material-icons">menu_book</span></span>
            <span class="explore-text">
              <span class="explore-label">Documentation</span>
              <span class="explore-desc">Guides and node reference</span>
            </span>
          </button>
          <a class="explore-tile" href="https://github.com/edwardvaneechoud/Flowfile" target="_blank" rel="noopener">
            <span class="explore-icon"><span class="material-icons">code</span></span>
            <span class="explore-text">
              <span class="explore-label">GitHub</span>
              <span class="explore-desc">Source, issues &amp; releases</span>
            </span>
          </a>
          <a class="explore-tile" href="https://github.com/edwardvaneechoud/Flowfile/discussions" target="_blank" rel="noopener">
            <span class="explore-icon"><span class="material-icons">forum</span></span>
            <span class="explore-text">
              <span class="explore-label">Discussions</span>
              <span class="explore-desc">Questions &amp; show-and-tell</span>
            </span>
          </a>
        </div>
      </section>

      <footer class="welcome-footer">
        <button class="welcome-link" @click="uiStore.showAbout = true">About</button>
        <span class="footer-sep" aria-hidden="true">·</span>
        <a class="welcome-link" href="https://github.com/edwardvaneechoud/Flowfile" target="_blank" rel="noopener">GitHub</a>
        <span class="footer-sep" aria-hidden="true">·</span>
        <a class="welcome-link" href="https://github.com/edwardvaneechoud/Flowfile/discussions" target="_blank" rel="noopener">Discussions</a>
        <template v-if="version">
          <span class="footer-sep" aria-hidden="true">·</span>
          <span class="welcome-version">v{{ version }}</span>
        </template>
      </footer>
    </div>
  </div>
</template>

<script setup lang="ts">
import { useRouter } from 'vue-router'
import { useDesignerUiStore } from '../../stores/designer-ui-store'
import type { RecentFlow } from '../../stores/recent-flows-store'

defineProps<{ recentFlows: RecentFlow[] }>()

const emit = defineEmits<{
  (e: 'create'): void
  (e: 'open'): void
  (e: 'browse-templates'): void
  (e: 'open-recent', id: string): void
  (e: 'remove-recent', id: string): void
}>()

const router = useRouter()
const uiStore = useDesignerUiStore()
const version = __APP_VERSION__
const MODIFIER = typeof navigator !== 'undefined' && navigator.platform.toLowerCase().includes('mac') ? '⌘' : 'Ctrl'

const goCatalog = () => router.push({ name: 'catalog' })
const goRuns = () => router.push({ name: 'catalog', query: { tab: 'runs' } })

const relativeFormat = new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' })
const RELATIVE_STEPS: [Intl.RelativeTimeFormatUnit, number][] = [
  ['year', 31536000000],
  ['month', 2592000000],
  ['week', 604800000],
  ['day', 86400000],
  ['hour', 3600000],
  ['minute', 60000]
]
function relativeTime(timestamp: number): string {
  const delta = timestamp - Date.now()
  for (const [unit, ms] of RELATIVE_STEPS) {
    if (Math.abs(delta) >= ms) return relativeFormat.format(Math.round(delta / ms), unit)
  }
  return 'just now'
}
</script>

<style scoped>
/* The parent app-layout page bounds the height and owns scrolling. */
.welcome {
  min-height: 100%;
  display: flex;
  background-color: var(--color-background-secondary);
}

.welcome-inner {
  margin: auto;
  width: 100%;
  max-width: 920px;
  padding: var(--spacing-10) var(--spacing-6);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-8);
}

/* Hero */
.welcome-hero {
  display: flex;
  flex-direction: column;
  align-items: center;
  text-align: center;
  gap: var(--spacing-1);
}

.welcome-logo {
  width: 72px;
  height: auto;
  margin-bottom: var(--spacing-2);
}

.welcome-title {
  margin: 0;
  font-size: var(--font-size-4xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
  letter-spacing: -0.01em;
}

.welcome-tagline {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-tertiary);
}

/* Section labels */
.welcome-section-title {
  margin: 0 0 var(--spacing-3);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-semibold);
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--color-text-tertiary);
}

.welcome-section-title--tight {
  margin-bottom: var(--spacing-1);
}

.welcome-section-sub {
  margin: 0 0 var(--spacing-4);
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

/* Primary action tiles */
.welcome-primary {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: var(--spacing-4);
}

@media (max-width: 1024px) {
  .welcome-primary {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
  .welcome-tile--primary {
    grid-column: 1 / -1;
  }
}

@media (max-width: 560px) {
  .welcome-primary {
    grid-template-columns: 1fr;
  }
}

.welcome-tile {
  display: flex;
  flex-direction: column;
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-xl);
  box-shadow: var(--shadow-xs);
  overflow: hidden;
  transition: all var(--transition-normal) var(--transition-timing);
}

.welcome-tile:hover,
.welcome-tile:focus-within {
  border-color: var(--color-accent);
  box-shadow: var(--shadow-md);
  transform: translateY(-1px);
}

.tile-main {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: var(--spacing-2);
  padding: var(--spacing-5);
  background: transparent;
  border: none;
  cursor: pointer;
  text-align: left;
  font-family: inherit;
}

.tile-main:focus-visible {
  outline: 2px solid var(--color-accent);
  outline-offset: -2px;
  border-radius: var(--border-radius-xl);
}

.tile-icon {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 40px;
  height: 40px;
  border-radius: var(--border-radius-lg);
  background-color: var(--color-accent-subtle);
  color: var(--color-accent);
  margin-bottom: var(--spacing-1);
}

.welcome-tile--primary .tile-icon {
  background-color: var(--color-accent);
  color: var(--color-text-inverse);
}

.tile-icon .material-icons {
  font-size: 22px;
}

.tile-title {
  font-size: var(--font-size-lg);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.tile-sub {
  font-size: var(--font-size-sm);
  color: var(--color-text-tertiary);
  line-height: var(--line-height-normal);
}

.tile-footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-2);
  min-height: 30px;
  padding: 0 var(--spacing-5) var(--spacing-3);
}

.tile-kbd {
  display: inline-flex;
  gap: var(--spacing-0-5);
}

.welcome kbd {
  padding: 1px 5px;
  border: 1px solid var(--color-border-secondary);
  border-bottom-width: 2px;
  border-radius: var(--border-radius-sm);
  background-color: var(--color-background-tertiary);
  font-family: var(--font-family-mono);
  font-size: var(--font-size-2xs);
  color: var(--color-text-secondary);
}

/* Recent flows */
.recent-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.recent-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  width: 100%;
  padding: var(--spacing-2) var(--spacing-3);
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  text-align: left;
  font-family: inherit;
  transition: all var(--transition-fast);
}

.recent-row:hover {
  background-color: var(--color-background-tertiary);
  border-color: var(--color-border-secondary);
}

.recent-row:focus-visible {
  outline: 2px solid var(--color-accent);
  outline-offset: 1px;
}

.recent-icon {
  font-size: 16px;
  color: var(--color-text-secondary);
  flex-shrink: 0;
}

.recent-name {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
  flex-shrink: 0;
  max-width: 45%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.recent-loc {
  flex: 1;
  min-width: 0;
  display: flex;
  align-items: center;
}

.recent-meta {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.recent-time {
  flex-shrink: 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
}

.recent-delete {
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  width: 20px;
  height: 20px;
  border-radius: var(--border-radius-sm);
  color: var(--color-text-muted);
  opacity: 0;
  transition: all var(--transition-fast);
}

.recent-row:hover .recent-delete {
  opacity: 1;
}

.recent-delete:hover {
  background: var(--color-danger-light);
  color: var(--color-danger);
}

.recent-delete .material-icons {
  font-size: 14px;
}

.recent-empty {
  margin: 0;
  padding: var(--spacing-4);
  text-align: center;
  font-size: var(--font-size-sm);
  color: var(--color-text-muted);
  border: 1px dashed var(--color-border-light);
  border-radius: var(--border-radius-md);
}

/* Explore: 6 tiles */
.explore-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: var(--spacing-3);
}

@media (max-width: 1024px) {
  .explore-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 560px) {
  .explore-grid {
    grid-template-columns: 1fr;
  }
}

.explore-tile {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  padding: var(--spacing-3);
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-lg);
  cursor: pointer;
  text-align: left;
  text-decoration: none;
  font-family: inherit;
  transition: all var(--transition-fast);
}

.explore-tile:hover {
  background-color: var(--color-background-tertiary);
  border-color: var(--color-border-secondary);
}

.explore-tile:focus-visible {
  outline: 2px solid var(--color-accent);
  outline-offset: 1px;
}

.explore-icon {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  border-radius: var(--border-radius-md);
  background-color: var(--color-accent-subtle);
  color: var(--color-accent);
  flex-shrink: 0;
}

.explore-icon .material-icons {
  font-size: 18px;
}

.explore-text {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-0-5);
  min-width: 0;
}

.explore-label {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.explore-desc {
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
}

/* Footer */
.welcome-footer {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-2);
  padding-top: var(--spacing-2);
}

.welcome-link {
  padding: 0;
  background: transparent;
  border: none;
  cursor: pointer;
  font-family: inherit;
  font-size: var(--font-size-sm);
  color: var(--color-text-tertiary);
  text-decoration: none;
  transition: color var(--transition-fast);
}

.welcome-link:hover {
  color: var(--color-accent);
}

.footer-sep {
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
}

.welcome-version {
  font-size: var(--font-size-sm);
  color: var(--color-text-muted);
}
</style>
