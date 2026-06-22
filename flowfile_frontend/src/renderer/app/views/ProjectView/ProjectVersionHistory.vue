<template>
  <div class="card mb-3">
    <div class="card-header">
      <h3 class="card-title">
        <i class="fa-solid fa-clock-rotate-left card-title__icon"></i>
        History
        <span class="count-badge">{{ store.versions.length }}</span>
        <el-tooltip
          effect="dark"
          placement="top"
          content="Every saved version, newest first. Expand Details to see what changed, or Restore to roll the whole project back to that point."
        >
          <button type="button" class="help-hint" aria-label="About version history">
            <i class="fa-solid fa-circle-info"></i>
          </button>
        </el-tooltip>
      </h3>
      <button
        type="button"
        class="overview-link"
        :disabled="store.loadingVersions"
        @click="refresh"
      >
        <i class="fa-solid fa-rotate" :class="{ spin: store.loadingVersions }"></i>
        Refresh
      </button>
    </div>
    <div class="card-content">
      <div v-if="store.loadingVersions && !store.versions.length" class="history-empty">
        Loading history…
      </div>
      <div v-else-if="!store.versions.length" class="history-empty">
        No versions yet. Use “Save a version” above to create your first snapshot.
      </div>
      <div v-else class="history-scroll">
        <ul class="timeline" :class="{ 'has-line': pagedVersions.length > 1 }">
          <li
            v-for="(v, i) in pagedVersions"
            :key="v.sha"
            class="tl-item"
            :class="{ latest: pageStart + i === 0 }"
          >
            <div class="tl-row">
              <div class="tl-info">
                <p class="tl-message">{{ v.message }}</p>
                <p class="tl-meta">
                  {{ timeAgo(v.committed_at) }}
                  <span v-if="pageStart + i === 0" class="tl-current">· current</span>
                </p>
              </div>
              <div class="tl-actions">
                <button
                  type="button"
                  class="history-details-btn"
                  :aria-expanded="!!expanded[v.sha]"
                  @click="toggle(v.sha)"
                >
                  <i
                    class="fa-solid"
                    :class="expanded[v.sha] ? 'fa-chevron-up' : 'fa-chevron-down'"
                  ></i>
                  Details
                </button>
                <el-button v-if="pageStart + i !== 0" size="small" text @click="openRestore(v)">
                  <i class="fa-solid fa-rotate-left restore-icon"></i>
                  Restore
                </el-button>
              </div>
            </div>

            <div v-if="expanded[v.sha]" class="history-detail">
              <div v-if="loadingDetails[v.sha]" class="history-detail__empty">Loading changes…</div>
              <ChangeList v-else-if="(details[v.sha] || []).length" :changes="details[v.sha]" />
              <p v-else class="history-detail__empty">No file changes in this version.</p>
            </div>
          </li>
        </ul>
      </div>
    </div>

    <div v-if="totalPages > 1" class="card-footer history-pager">
      <span class="history-pager__info">
        Page {{ page }} of {{ totalPages }} · {{ store.versions.length }} versions
      </span>
      <div class="history-pager__controls">
        <button type="button" class="pager-btn" :disabled="page <= 1" @click="page--">
          <i class="fa-solid fa-chevron-left"></i>
          Newer
        </button>
        <button type="button" class="pager-btn" :disabled="page >= totalPages" @click="page++">
          Older
          <i class="fa-solid fa-chevron-right"></i>
        </button>
      </div>
    </div>

    <RestoreDialog v-model="dialogVisible" :version="selectedVersion" />
  </div>
</template>

<script setup lang="ts">
import { computed, reactive, ref, watch } from "vue";
import ChangeList from "./ChangeList.vue";
import RestoreDialog from "./RestoreDialog.vue";
import { useProjectStore } from "../../stores/project-store";
import type { ProjectVersion, ProjectVersionChange } from "../../types";

const PAGE_SIZE = 8;

const store = useProjectStore();
const dialogVisible = ref(false);
const selectedVersion = ref<ProjectVersion | null>(null);

const expanded = reactive<Record<string, boolean>>({});
const loadingDetails = reactive<Record<string, boolean>>({});
const details = reactive<Record<string, ProjectVersionChange[]>>({});

const page = ref(1);
const totalPages = computed(() => Math.max(1, Math.ceil(store.versions.length / PAGE_SIZE)));
const pageStart = computed(() => (page.value - 1) * PAGE_SIZE);
const pagedVersions = computed(() =>
  store.versions.slice(pageStart.value, pageStart.value + PAGE_SIZE),
);

// History can shrink (restore) or grow (save) under us — keep the page in range.
watch(totalPages, (max) => {
  if (page.value > max) page.value = max;
});

const refresh = () => store.loadVersions();

const toggle = async (sha: string) => {
  expanded[sha] = !expanded[sha];
  if (!expanded[sha] || details[sha]) return;
  loadingDetails[sha] = true;
  try {
    details[sha] = await store.loadVersionDiff(sha);
  } catch {
    details[sha] = [];
  } finally {
    loadingDetails[sha] = false;
  }
};

const openRestore = (v: ProjectVersion) => {
  selectedVersion.value = v;
  dialogVisible.value = true;
};

const timeAgo = (iso: string): string => {
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return iso;
  const secs = Math.round((Date.now() - then) / 1000);
  const units: [number, string][] = [
    [60, "second"],
    [60, "minute"],
    [24, "hour"],
    [7, "day"],
    [4.34524, "week"],
    [12, "month"],
    [Number.POSITIVE_INFINITY, "year"],
  ];
  let value = Math.max(secs, 0);
  let unit = "second";
  for (const [size, name] of units) {
    if (value < size) {
      unit = name;
      break;
    }
    value = Math.floor(value / size);
  }
  if (value <= 0 && unit === "second") return "just now";
  return `${value} ${unit}${value === 1 ? "" : "s"} ago`;
};
</script>

<style scoped>
.card-title {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-2);
}

.card-title__icon {
  color: var(--color-accent);
  font-size: var(--font-size-md);
}

.count-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 18px;
  height: 18px;
  padding: 0 6px;
  border-radius: var(--border-radius-full);
  background: var(--color-background-tertiary);
  color: var(--color-text-secondary);
  font-size: var(--font-size-2xs);
  font-weight: var(--font-weight-semibold);
}

.help-hint {
  display: inline-flex;
  align-items: center;
  padding: 0;
  border: none;
  background: transparent;
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
  line-height: 1;
  cursor: help;
  border-radius: var(--border-radius-full);
  transition: color var(--transition-base) var(--transition-timing);
}

.help-hint:hover {
  color: var(--color-accent);
}

.help-hint:focus-visible {
  outline: none;
  color: var(--color-accent);
  box-shadow: 0 0 0 2px var(--color-accent-subtle);
}

.overview-link {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  background: transparent;
  border: none;
  color: var(--color-text-secondary);
  font-size: var(--font-size-base);
  cursor: pointer;
}

.overview-link:hover:not(:disabled) {
  color: var(--color-accent);
}

.overview-link:focus-visible,
.history-details-btn:focus-visible,
.pager-btn:focus-visible {
  outline: none;
  color: var(--color-accent);
  box-shadow: 0 0 0 2px var(--color-accent-subtle);
  border-radius: var(--border-radius-sm);
}

.overview-link .spin {
  animation: spin 1s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

.history-empty {
  padding: var(--spacing-3) 0;
  font-size: var(--font-size-base);
  color: var(--color-text-tertiary);
}

.history-scroll {
  max-height: 380px;
  overflow-y: auto;
  margin-right: calc(-1 * var(--spacing-2));
  padding-right: var(--spacing-2);
}

/* ===== Timeline ===== */
.timeline {
  position: relative;
  list-style: none;
  margin: 0;
  /* small left inset so the dots + focus rings clear the scroll container's clip edge */
  padding: 0 0 0 var(--spacing-1);
}

/* Continuous rail behind the dots, trimmed to the first/last dot. */
.timeline.has-line::before {
  content: "";
  position: absolute;
  left: 9px;
  top: 18px;
  bottom: 18px;
  width: 2px;
  background: var(--color-border-light);
}

.tl-item {
  position: relative;
  padding: var(--spacing-2) var(--spacing-2) var(--spacing-2) var(--spacing-6);
  border-radius: var(--border-radius-md);
  transition: background var(--transition-fast) var(--transition-timing);
}

.tl-item:hover {
  background: var(--color-background-muted);
}

.tl-item::before {
  content: "";
  position: absolute;
  left: 0;
  top: 14px;
  width: 12px;
  height: 12px;
  border-radius: 50%;
  background: var(--color-background-primary);
  border: 2px solid var(--color-border-secondary);
  box-sizing: border-box;
}

.tl-item:hover::before {
  background: var(--color-background-muted);
}

.tl-item.latest::before {
  background: var(--color-success);
  border-color: var(--color-success);
  box-shadow: 0 0 0 3px var(--color-success-light);
}

.tl-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
}

.tl-info {
  flex: 1;
  min-width: 0;
}

.tl-message {
  margin: 0;
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
  word-break: break-word;
}

.tl-meta {
  margin: 2px 0 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-tertiary);
}

.tl-current {
  color: var(--color-success-dark);
  font-weight: var(--font-weight-medium);
}

.tl-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  flex-shrink: 0;
}

.restore-icon {
  margin-right: 4px;
  font-size: var(--font-size-xs);
}

.history-details-btn {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  flex-shrink: 0;
  background: transparent;
  border: none;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
  cursor: pointer;
}

.history-details-btn:hover {
  color: var(--color-accent);
}

.history-detail {
  padding: var(--spacing-1) 0 var(--spacing-2);
}

.history-detail__empty {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-tertiary);
}

/* ===== Pager ===== */
.history-pager {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-3);
}

.history-pager__info {
  font-size: var(--font-size-sm);
  color: var(--color-text-tertiary);
}

.history-pager__controls {
  display: flex;
  gap: var(--spacing-2);
}

.pager-btn {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  background: transparent;
  border: none;
  color: var(--color-text-secondary);
  font-size: var(--font-size-base);
  cursor: pointer;
}

.pager-btn:hover:not(:disabled) {
  color: var(--color-accent);
}

.pager-btn:disabled {
  color: var(--color-border-secondary);
  cursor: default;
}
</style>
