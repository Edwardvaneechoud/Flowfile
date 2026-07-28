<template>
  <div class="designer-empty-state">
    <div class="empty-content">
      <span class="material-icons empty-icon">account_tree</span>
      <h3 class="empty-title">No flow open</h3>
      <p class="empty-description">Create a new flow or open an existing one.</p>
      <div class="empty-actions">
        <button type="button" class="empty-btn empty-btn--primary" @click="emit('new-flow')">
          <span class="material-icons btn-icon">add</span>
          <span>New flow</span>
        </button>
        <button type="button" class="empty-btn" @click="emit('open-flow')">
          <span class="material-icons btn-icon">folder_open</span>
          <span>Open flow</span>
        </button>
      </div>
      <p class="empty-kbd-hint">
        <span class="kbd-group"
          ><kbd>{{ MODIFIER_LABEL }}</kbd
          ><kbd>N</kbd></span
        >
        new
        <span class="kbd-sep">·</span>
        <span class="kbd-group"
          ><kbd>{{ MODIFIER_LABEL }}</kbd
          ><kbd>O</kbd></span
        >
        open
      </p>
      <div v-if="recentsToShow.length" class="empty-recents">
        <h4 class="recents-title">Recent flows</h4>
        <ul class="recents-list">
          <li v-for="flow in recentsToShow" :key="flow.path">
            <div
              class="recent-row"
              role="button"
              tabindex="0"
              :title="flow.catalogRef || flow.path"
              @click="emit('open-recent', flow)"
              @keydown.enter="emit('open-recent', flow)"
              @keydown.space.prevent="emit('open-recent', flow)"
            >
              <i
                v-if="flow.catalogRef"
                class="fa-solid fa-folder-tree recent-icon recent-icon--catalog"
              ></i>
              <span v-else class="material-icons recent-icon">description</span>
              <span class="recent-name">{{ recentDisplayName(flow) }}</span>
              <span v-if="flow.catalogRef" class="recent-sub recent-sub--catalog">
                {{ flow.catalogRef }}
              </span>
              <span v-else-if="parentFolder(flow.path)" class="recent-sub">
                {{ parentFolder(flow.path) }}
              </span>
            </div>
          </li>
        </ul>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted } from "vue";
import {
  recentDisplayName,
  useRecentFlows,
  type RecentFlow,
} from "../../composables/useRecentFlows";
import { MODIFIER_LABEL } from "../../utils/shortcuts";

const emit = defineEmits<{
  (e: "new-flow"): void;
  (e: "open-flow"): void;
  (e: "open-recent", flow: RecentFlow): void;
}>();

const { recentFlows, refreshCatalogRefs } = useRecentFlows();

// Miniature of the Home screen's recents — full management (remove, context menu) stays there.
const recentsToShow = computed(() => recentFlows.value.slice(0, 5));

const parentFolder = (path: string): string => {
  const parts = path.split(/[/\\]/).filter(Boolean);
  return parts.length > 1 ? parts[parts.length - 2] : "";
};

onMounted(() => {
  refreshCatalogRefs();
});
</script>

<style scoped>
/* Opaque overlay: covers the (blank) canvas and node palette so nothing
   underneath is interactive while no flow is open. Stays below the
   .switch-indicator (z-index 10) so the "Loading flow…" chip shows on top. */
.designer-empty-state {
  position: absolute;
  inset: 0;
  z-index: 5;
  display: flex;
  align-items: center;
  justify-content: center;
  background-color: var(--color-background-primary);
  overflow-y: auto;
}

.empty-content {
  display: flex;
  flex-direction: column;
  align-items: center;
  max-width: 420px;
  width: 100%;
  padding: var(--spacing-6);
}

.empty-icon {
  font-size: 56px;
  color: var(--color-text-muted);
  margin-bottom: var(--spacing-3);
}

.empty-title {
  margin: 0 0 var(--spacing-1);
  font-size: var(--font-size-lg);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.empty-description {
  margin: 0 0 var(--spacing-5);
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.empty-actions {
  display: flex;
  gap: var(--spacing-3);
}

.empty-btn {
  display: flex;
  align-items: center;
  gap: var(--spacing-1-5);
  padding: var(--spacing-2) var(--spacing-4);
  height: 36px;
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-lg);
  cursor: pointer;
  transition: all var(--transition-fast);
  color: var(--color-text-primary);
  font-family: inherit;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  box-shadow: var(--shadow-xs);
}

.empty-btn:hover {
  background-color: var(--color-background-tertiary);
  border-color: var(--color-border-secondary);
}

.empty-btn--primary {
  background-color: var(--color-accent);
  border-color: var(--color-accent);
  color: var(--color-text-inverse);
}

.empty-btn--primary:hover {
  background-color: var(--color-accent);
  border-color: var(--color-accent);
  opacity: 0.9;
}

.btn-icon {
  font-size: 18px;
}

.empty-kbd-hint {
  margin: var(--spacing-4) 0 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  display: flex;
  align-items: center;
  gap: var(--spacing-1-5);
}

.kbd-group {
  display: inline-flex;
  gap: var(--spacing-0-5);
}

.empty-kbd-hint kbd {
  padding: 1px 5px;
  border: 1px solid var(--color-border-secondary);
  border-bottom-width: 2px;
  border-radius: var(--border-radius-sm);
  background-color: var(--color-background-tertiary);
  font-family: var(--font-family-mono);
  font-size: var(--font-size-2xs);
  color: var(--color-text-secondary);
}

.kbd-sep {
  color: var(--color-border-secondary);
}

.empty-recents {
  width: 100%;
  margin-top: var(--spacing-6);
}

.recents-title {
  margin: 0 0 var(--spacing-2);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-semibold);
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: var(--color-text-muted);
}

.recents-list {
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

.recent-sub {
  flex-shrink: 0;
  max-width: 40%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.recent-sub--catalog {
  color: var(--color-accent);
}
</style>
