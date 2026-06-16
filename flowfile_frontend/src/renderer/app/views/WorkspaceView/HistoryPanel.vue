<template>
  <section class="ws-card">
    <header class="ws-card-head">
      <h3 class="ws-card-title"><i class="fa-solid fa-clock-rotate-left" /> Checkpoints</h3>
    </header>

    <p v-if="!history || !history.git_available" class="ws-muted">
      Git isn't available on the server, so checkpoints are disabled. You can still version the
      exported folder with git manually.
    </p>

    <template v-else>
      <p v-if="!history.commits.length" class="ws-muted">
        No checkpoints yet — click “Create checkpoint” to save the current state.
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
              title="Restore this checkpoint"
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

defineProps<{ history: WorkspaceGitHistory | null }>();
const emit = defineEmits<{
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
