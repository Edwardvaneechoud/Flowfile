<template>
  <div class="card mb-3">
    <div class="card-header">
      <h3 class="card-title">History ({{ store.versions.length }})</h3>
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
        <ul class="history-list">
          <li v-for="(v, i) in pagedVersions" :key="v.sha" class="history-item">
            <div class="history-row">
              <div class="history-dot" :class="{ latest: pageStart + i === 0 }"></div>
              <div class="history-body">
                <p class="history-message">{{ v.message }}</p>
                <p class="history-meta">
                  {{ timeAgo(v.committed_at) }}
                  <span v-if="pageStart + i === 0" class="history-current">· current</span>
                </p>
              </div>
              <button type="button" class="history-details-btn" @click="toggle(v.sha)">
                <i
                  class="fa-solid"
                  :class="expanded[v.sha] ? 'fa-chevron-up' : 'fa-chevron-down'"
                ></i>
                Details
              </button>
              <el-button v-if="pageStart + i !== 0" size="small" text @click="openRestore(v)">
                Restore
              </el-button>
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
.overview-link {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  background: transparent;
  border: none;
  color: var(--color-text-secondary, #475569);
  font-size: var(--font-size-sm, 14px);
  cursor: pointer;
}

.overview-link:hover:not(:disabled) {
  color: var(--color-accent, #2563eb);
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
  padding: var(--spacing-3, 12px) 0;
  font-size: var(--font-size-sm, 14px);
  color: var(--color-text-tertiary, #94a3b8);
}

.history-scroll {
  max-height: 360px;
  overflow-y: auto;
  margin-right: calc(-1 * var(--spacing-2, 8px));
  padding-right: var(--spacing-2, 8px);
}

.history-list {
  list-style: none;
  margin: 0;
  padding: 0;
}

.history-item {
  border-bottom: 1px solid var(--color-border-light, #eef2f7);
}

.history-item:last-child {
  border-bottom: none;
}

.history-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-3, 12px);
  padding: var(--spacing-2, 8px) 0;
}

.history-dot {
  width: 9px;
  height: 9px;
  flex-shrink: 0;
  border-radius: 50%;
  background: var(--color-border-secondary, #cbd5e1);
}

.history-dot.latest {
  background: var(--color-success, #16a34a);
}

.history-body {
  flex: 1;
  min-width: 0;
}

.history-message {
  margin: 0;
  font-size: var(--font-size-sm, 14px);
  font-weight: var(--font-weight-medium, 500);
  color: var(--color-text-primary, #0f172a);
  word-break: break-word;
}

.history-meta {
  margin: 2px 0 0;
  font-size: 12px;
  color: var(--color-text-tertiary, #94a3b8);
}

.history-current {
  color: var(--color-success, #16a34a);
}

.history-details-btn {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  flex-shrink: 0;
  background: transparent;
  border: none;
  color: var(--color-text-secondary, #475569);
  font-size: 12px;
  cursor: pointer;
}

.history-details-btn:hover {
  color: var(--color-accent, #2563eb);
}

.history-detail {
  padding: 4px 0 12px 21px;
}

.history-detail__empty {
  margin: 0;
  font-size: 12px;
  color: var(--color-text-tertiary, #94a3b8);
}

.history-pager {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-3, 12px);
}

.history-pager__info {
  font-size: 12px;
  color: var(--color-text-tertiary, #94a3b8);
}

.history-pager__controls {
  display: flex;
  gap: var(--spacing-2, 8px);
}

.pager-btn {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  background: transparent;
  border: none;
  color: var(--color-text-secondary, #475569);
  font-size: var(--font-size-sm, 14px);
  cursor: pointer;
}

.pager-btn:hover:not(:disabled) {
  color: var(--color-accent, #2563eb);
}

.pager-btn:disabled {
  color: var(--color-border-secondary, #cbd5e1);
  cursor: default;
}
</style>
