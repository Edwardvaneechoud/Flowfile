<template>
  <div class="welcome">
    <div class="welcome-inner">
      <header class="welcome-hero">
        <img src="/images/flowfile.png" alt="Flowfile logo" class="welcome-logo" />
        <h1 class="welcome-title">Flowfile</h1>
      </header>

      <section aria-label="Start building">
        <h2 class="welcome-section-title welcome-section-title--tight">Start building</h2>
        <p class="welcome-section-sub">Design data transformations and visually</p>
        <div class="welcome-primary">
          <article class="welcome-tile welcome-tile--primary">
            <button class="tile-main" @click="emit('create')">
              <span class="tile-icon"><span class="material-icons">add</span></span>
              <span class="tile-title">New flow</span>
              <span class="tile-sub">Start building on a blank canvas</span>
            </button>
            <div class="tile-footer">
              <!-- Cmd/Ctrl+N is reserved by the browser and can't be intercepted
                   in web mode; only the desktop shell honors it. -->
              <span v-if="isDesktop" class="tile-kbd">
                <kbd>{{ MODIFIER_LABEL }}</kbd>
                <kbd>N</kbd>
              </span>
              <span v-else></span>
              <button class="tile-link" @click="emit('create-at-location')">
                Choose location…
              </button>
            </div>
          </article>

          <article class="welcome-tile">
            <button class="tile-main" @click="emit('open')">
              <span class="tile-icon"><span class="material-icons">folder_open</span></span>
              <span class="tile-title">Open flow</span>
              <span class="tile-sub">From the file system or catalog</span>
            </button>
            <div class="tile-footer">
              <span class="tile-kbd">
                <kbd>{{ MODIFIER_LABEL }}</kbd>
                <kbd>O</kbd>
              </span>
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
          <li v-for="flow in recentFlows" :key="flow.path">
            <el-tooltip placement="top" :show-after="400" :hide-after="0">
              <template #content>
                <div v-if="flow.catalogRef">{{ flow.catalogRef }}</div>
                <div>{{ flow.path }}</div>
              </template>
              <el-dropdown class="recent-dropdown" trigger="contextmenu" placement="bottom-start">
                <div
                  class="recent-row"
                  role="button"
                  tabindex="0"
                  @click="emit('open-recent', flow.path)"
                  @keydown.enter="emit('open-recent', flow.path)"
                  @keydown.space.prevent="emit('open-recent', flow.path)"
                >
                  <i
                    v-if="flow.catalogRef"
                    class="fa-solid fa-folder-tree recent-icon recent-icon--catalog"
                  ></i>
                  <span v-else class="material-icons recent-icon">description</span>
                  <span class="recent-name">{{ displayName(flow) }}</span>
                  <span v-if="flow.catalogRef" class="recent-catalog-ref">
                    {{ flow.catalogRef }}
                  </span>
                  <span v-else-if="parentFolder(flow.path)" class="recent-folder">
                    {{ parentFolder(flow.path) }}
                  </span>
                  <span class="recent-time">{{ relativeTime(flow.lastOpened) }}</span>
                  <button
                    class="recent-remove"
                    type="button"
                    :aria-label="`Remove ${displayName(flow)} from recents`"
                    title="Remove from recents"
                    @click.stop="emit('remove-recent', flow.path)"
                    @keydown.enter.stop="emit('remove-recent', flow.path)"
                    @keydown.space.stop.prevent="emit('remove-recent', flow.path)"
                  >
                    <span class="material-icons">close</span>
                  </button>
                </div>
                <template #dropdown>
                  <el-dropdown-menu>
                    <el-dropdown-item @click="emit('open-recent', flow.path)">
                      <span class="material-icons recent-menu-icon">open_in_new</span>
                      Open flow
                    </el-dropdown-item>
                    <el-dropdown-item
                      :disabled="flow.catalogId === undefined"
                      @click="viewInCatalog(flow)"
                    >
                      <span class="material-icons recent-menu-icon">folder_open</span>
                      View in catalog
                    </el-dropdown-item>
                    <el-dropdown-item divided @click="emit('remove-recent', flow.path)">
                      <span class="material-icons recent-menu-icon">close</span>
                      Remove from recents
                    </el-dropdown-item>
                  </el-dropdown-menu>
                </template>
              </el-dropdown>
            </el-tooltip>
          </li>
        </ul>
        <p v-else class="recent-empty">Flows you create or open will show up here.</p>
      </section>

      <section aria-label="Explore">
        <h2 class="welcome-section-title">Explore</h2>
        <div class="explore-grid">
          <button class="explore-tile" @click="goCatalog">
            <span class="explore-icon"><i class="fa-solid fa-folder-tree"></i></span>
            <span class="explore-text">
              <span class="explore-label">Data Catalog</span>
              <span class="explore-desc">Tables, favorites &amp; run history</span>
            </span>
          </button>
          <button class="explore-tile" @click="goVisuals">
            <span class="explore-icon"><i class="fa-solid fa-chart-pie"></i></span>
            <span class="explore-text">
              <span class="explore-label">Visuals</span>
              <span class="explore-desc">Dashboards built on your data</span>
            </span>
          </button>
          <button class="explore-tile" @click="goSchedules">
            <span class="explore-icon"><i class="fa-solid fa-calendar-days"></i></span>
            <span class="explore-text">
              <span class="explore-label">Schedules</span>
              <span class="explore-desc">Run flows on a schedule</span>
            </span>
          </button>
          <button class="explore-tile" @click="goSecrets">
            <span class="explore-icon"><i class="fa-solid fa-key"></i></span>
            <span class="explore-text">
              <span class="explore-label">Connections &amp; Secrets</span>
              <span class="explore-desc">Databases, cloud storage &amp; API keys</span>
            </span>
          </button>
          <button class="explore-tile" @click="goDocs">
            <span class="explore-icon"><i class="fa-solid fa-book"></i></span>
            <span class="explore-text">
              <span class="explore-label">Documentation</span>
              <span class="explore-desc">Guides and node reference</span>
            </span>
          </button>
          <button class="explore-tile" @click="emit('start-tutorial')">
            <span class="explore-icon"><span class="material-icons">school</span></span>
            <span class="explore-text">
              <span class="explore-label">Interactive tutorial</span>
              <span class="explore-desc">Build your first flow step by step</span>
            </span>
          </button>
        </div>
      </section>

      <footer class="welcome-footer">
        <button class="welcome-link" @click="aboutVisible = true">About</button>
        <span class="footer-sep" aria-hidden="true">·</span>
        <a
          class="welcome-link"
          href="https://github.com/edwardvaneechoud/Flowfile"
          target="_blank"
          rel="noopener"
        >
          GitHub
        </a>
        <span class="footer-sep" aria-hidden="true">·</span>
        <a
          class="welcome-link"
          href="https://github.com/edwardvaneechoud/Flowfile/discussions"
          target="_blank"
          rel="noopener"
        >
          Discussions
        </a>
        <template v-if="version">
          <span class="footer-sep" aria-hidden="true">·</span>
          <span class="welcome-version">v{{ version }}</span>
        </template>
      </footer>
    </div>

    <about-dialog v-model:visible="aboutVisible" :version="version" />
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from "vue";
import { useRouter } from "vue-router";
import { MODIFIER_LABEL } from "../../utils/shortcuts";
import { desktop, isDesktop } from "../../../lib/desktop";
import type { RecentFlow } from "../../composables/useRecentFlows";
import AboutDialog from "./AboutDialog.vue";

defineProps<{ recentFlows: RecentFlow[] }>();

const emit = defineEmits<{
  (e: "create"): void;
  (e: "create-at-location"): void;
  (e: "open"): void;
  (e: "browse-templates"): void;
  (e: "open-recent", path: string): void;
  (e: "remove-recent", path: string): void;
  (e: "start-tutorial"): void;
}>();

const router = useRouter();
const aboutVisible = ref(false);
const version = ref("");

onMounted(async () => {
  try {
    version.value = (isDesktop ? await desktop.getAppVersion() : "") || __APP_VERSION__;
  } catch {
    version.value = __APP_VERSION__;
  }
});

const goCatalog = () => router.push({ name: "catalog" });
const goVisuals = () => router.push({ name: "catalog", query: { tab: "visuals" } });
const goSchedules = () => router.push({ name: "catalog", query: { tab: "schedules" } });
const goSecrets = () => router.push({ name: "connections", query: { tab: "secrets" } });
const goDocs = () => router.push({ name: "documentation" });

const viewInCatalog = (flow: RecentFlow) => {
  if (flow.catalogId === undefined) return;
  router.push({ name: "catalog", query: { tab: "catalog", flowId: String(flow.catalogId) } });
};

const KNOWN_FLOW_EXTS = /\.(ya?ml|flowfile)$/i;

// Display-only; never mutates the stored entry. Catalog names have no extension
// and pass through unchanged.
function displayName(flow: RecentFlow): string {
  return flow.name.replace(KNOWN_FLOW_EXTS, "");
}

// Parent directory only (e.g. "unnamed_flows") — the orientation the chip shows
// in place of the full path, which now lives in the row tooltip.
function parentFolder(path: string): string {
  const parts = path.split(/[\\/]/).filter(Boolean);
  return parts.length >= 2 ? parts[parts.length - 2] : "";
}

const relativeFormat = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });
const RELATIVE_STEPS: [Intl.RelativeTimeFormatUnit, number][] = [
  ["year", 31536000000],
  ["month", 2592000000],
  ["week", 604800000],
  ["day", 86400000],
  ["hour", 3600000],
  ["minute", 60000],
];

function relativeTime(timestamp: number): string {
  const delta = timestamp - Date.now();
  for (const [unit, ms] of RELATIVE_STEPS) {
    if (Math.abs(delta) >= ms) return relativeFormat.format(Math.round(delta / ms), unit);
  }
  return "just now";
}
</script>

<style scoped>
/* The parent app-layout__page bounds the height and owns scrolling. */
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

/* Primary action tiles. Explicit column counts (not auto-fit) so no row is
   ever left with a lone orphan tile. */
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

.tile-link {
  padding: 0;
  background: transparent;
  border: none;
  cursor: pointer;
  font-family: inherit;
  font-size: var(--font-size-sm);
  color: var(--color-accent);
}

.tile-link:hover {
  text-decoration: underline;
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

/* el-dropdown defaults to inline-block; keep the row full-width. */
.recent-dropdown {
  display: block;
  width: 100%;
}

.recent-menu-icon {
  font-size: 16px;
  margin-right: var(--spacing-2);
  vertical-align: middle;
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

.recent-icon--catalog {
  font-size: 13px;
  width: 16px;
  text-align: center;
  color: var(--color-accent);
}

.recent-name {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
  flex: 1;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

/* Muted parent-folder chip — quieter than the accent catalog ref badge. */
.recent-folder {
  flex-shrink: 0;
  max-width: 30%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  padding: 1px var(--spacing-2);
  background-color: var(--color-background-tertiary);
  border-radius: var(--border-radius-full);
  font-size: var(--font-size-2xs);
  color: var(--color-text-tertiary);
}

.recent-catalog-ref {
  flex-shrink: 0;
  max-width: 45%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  padding: 1px var(--spacing-2);
  background-color: var(--color-accent-subtle);
  border-radius: var(--border-radius-full);
  font-family: var(--font-family-mono);
  font-size: var(--font-size-xs);
  color: var(--color-accent);
}

.recent-time {
  flex-shrink: 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
}

/* Remove-from-recents X — hidden until the row is hovered/focused. */
.recent-remove {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  width: 22px;
  height: 22px;
  padding: 0;
  border: none;
  background: transparent;
  border-radius: var(--border-radius-full);
  color: var(--color-text-muted);
  cursor: pointer;
  opacity: 0;
  transition:
    opacity var(--transition-fast),
    color var(--transition-fast),
    background-color var(--transition-fast);
}

.recent-row:hover .recent-remove,
.recent-remove:focus-visible {
  opacity: 1;
}

.recent-remove:hover {
  color: var(--color-danger);
  background-color: var(--color-danger-bg, rgba(239, 68, 68, 0.1));
}

.recent-remove:focus-visible {
  outline: 2px solid var(--color-accent);
  outline-offset: 1px;
}

.recent-remove .material-icons {
  font-size: 15px;
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

/* Explore: 6 tiles — 2 rows of 3 on desktop, 3 rows of 2 on medium. */
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
  font-size: 14px;
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
