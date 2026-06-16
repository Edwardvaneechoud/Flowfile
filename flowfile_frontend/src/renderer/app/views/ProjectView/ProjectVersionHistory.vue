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
      <ul v-else class="history-list">
        <li v-for="(v, i) in store.versions" :key="v.sha" class="history-item">
          <div class="history-row">
            <div class="history-dot" :class="{ latest: i === 0 }"></div>
            <div class="history-body">
              <p class="history-message">{{ v.message }}</p>
              <p class="history-meta">
                {{ timeAgo(v.committed_at) }}
                <span v-if="i === 0" class="history-current">· current</span>
              </p>
            </div>
            <button type="button" class="history-details-btn" @click="toggle(v.sha)">
              <i
                class="fa-solid"
                :class="expanded[v.sha] ? 'fa-chevron-up' : 'fa-chevron-down'"
              ></i>
              Details
            </button>
            <el-button v-if="i !== 0" size="small" text @click="openRestore(v)">Restore</el-button>
          </div>

          <div v-if="expanded[v.sha]" class="history-detail">
            <div v-if="loadingDetails[v.sha]" class="history-detail__empty">Loading changes…</div>
            <ChangeList v-else-if="(details[v.sha] || []).length" :changes="details[v.sha]" />
            <p v-else class="history-detail__empty">No file changes in this version.</p>
          </div>
        </li>
      </ul>
    </div>

    <RestoreDialog v-model="dialogVisible" :version="selectedVersion" />
  </div>
</template>

<script setup lang="ts">
import { reactive, ref } from "vue";
import ChangeList from "./ChangeList.vue";
import RestoreDialog from "./RestoreDialog.vue";
import { useProjectStore } from "../../stores/project-store";
import type { ProjectVersion, ProjectVersionChange } from "../../types";

const store = useProjectStore();
const dialogVisible = ref(false);
const selectedVersion = ref<ProjectVersion | null>(null);

const expanded = reactive<Record<string, boolean>>({});
const loadingDetails = reactive<Record<string, boolean>>({});
const details = reactive<Record<string, ProjectVersionChange[]>>({});

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
</style>
