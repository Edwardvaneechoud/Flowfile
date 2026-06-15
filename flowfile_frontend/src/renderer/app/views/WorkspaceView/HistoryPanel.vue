<template>
  <section class="ws-card">
    <header class="ws-card-head">
      <h3 class="ws-card-title"><i class="fa-solid fa-clock-rotate-left" /> History</h3>
      <button
        v-if="history?.git_available"
        class="ws-btn ws-btn-primary ws-btn-sm"
        :disabled="committing"
        @click="emit('commit')"
      >
        <span v-if="committing" class="ws-spin" />
        <i v-else class="fa-solid fa-code-commit" /> Commit
      </button>
    </header>

    <p v-if="!history || !history.git_available" class="ws-muted">
      Git isn't available on the server, so in-app history is disabled. You can still version the
      exported folder with git manually.
    </p>

    <template v-else>
      <div v-if="history.dirty" class="ws-alert ws-alert-warn">
        <i class="fa-solid fa-pen" />
        <div>
          {{ history.uncommitted.length }} uncommitted change(s).
          <a class="ws-link" @click="emit('view-diff', null)">View</a> — commit to snapshot this
          version.
        </div>
      </div>

      <p v-if="!history.commits.length" class="ws-muted">
        No snapshots yet — commit to start tracking history.
      </p>

      <ul v-else class="ws-commits">
        <li v-for="commit in history.commits" :key="commit.sha" class="ws-commit">
          <div class="ws-commit-info">
            <div class="ws-commit-line">
              <code class="ws-commit-sha">{{ commit.short_sha }}</code>
              <span class="ws-commit-subject">{{ commit.subject }}</span>
            </div>
            <div class="ws-commit-meta">{{ commit.author }} · {{ relativeTime(commit.date) }}</div>
          </div>
          <div class="ws-commit-actions">
            <button class="ws-icon-btn" title="View changes" @click="emit('view-diff', commit.sha)">
              <i class="fa-solid fa-eye" />
            </button>
            <button
              class="ws-icon-btn"
              title="Restore this version"
              @click="emit('restore', commit)"
            >
              <i class="fa-solid fa-clock-rotate-left" />
            </button>
          </div>
        </li>
      </ul>
    </template>
  </section>
</template>

<script setup lang="ts">
import type { GitCommit, WorkspaceGitHistory } from "../../types";

defineProps<{ history: WorkspaceGitHistory | null; committing: boolean }>();
const emit = defineEmits<{
  (e: "commit"): void;
  (e: "view-diff", sha: string | null): void;
  (e: "restore", commit: GitCommit): void;
}>();

function relativeTime(iso: string): string {
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return iso;
  const seconds = Math.round((Date.now() - then) / 1000);
  if (seconds < 60) return "just now";
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.round(hours / 24);
  if (days < 30) return `${days}d ago`;
  return new Date(iso).toLocaleDateString();
}
</script>
