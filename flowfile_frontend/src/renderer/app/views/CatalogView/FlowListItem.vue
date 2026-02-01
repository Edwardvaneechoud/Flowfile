<template>
  <div class="flow-list-item" :class="{ selected }" @click="$emit('select')">
    <div class="flow-main">
      <i class="fa-solid fa-diagram-project flow-icon"></i>
      <div class="flow-info">
        <span class="flow-name">{{ flow.name }}</span>
        <span class="flow-meta">{{ flow.run_count }} runs</span>
      </div>
    </div>
    <div class="flow-actions">
      <button
        class="action-btn star-btn"
        :class="{ active: flow.is_favorite }"
        :title="flow.is_favorite ? 'Unfavorite' : 'Favorite'"
        @click.stop="$emit('toggleFavorite')"
      >
        <i :class="flow.is_favorite ? 'fa-solid fa-star' : 'fa-regular fa-star'"></i>
      </button>
      <button
        class="action-btn follow-btn"
        :class="{ active: flow.is_following }"
        :title="flow.is_following ? 'Unfollow' : 'Follow'"
        @click.stop="$emit('toggleFollow')"
      >
        <i :class="flow.is_following ? 'fa-solid fa-bell' : 'fa-regular fa-bell'"></i>
      </button>
      <span
        v-if="flow.last_run_success !== null"
        class="run-dot"
        :class="flow.last_run_success ? 'success' : 'failure'"
      ></span>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { FlowRegistration } from "../../types";

defineProps<{
  flow: FlowRegistration;
  selected: boolean;
}>();

defineEmits<{
  select: [];
  toggleFavorite: [];
  toggleFollow: [];
}>();
</script>

<style scoped>
.flow-list-item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--spacing-2) var(--spacing-3);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  transition: background var(--transition-fast);
}

.flow-list-item:hover { background: var(--color-background-hover); }
.flow-list-item.selected { background: rgba(59, 130, 246, 0.1); }

.flow-main {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  min-width: 0;
  flex: 1;
}

.flow-icon {
  color: var(--color-accent);
  font-size: var(--font-size-md);
  flex-shrink: 0;
}

.flow-info {
  display: flex;
  flex-direction: column;
  min-width: 0;
}

.flow-name {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.flow-meta {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.flow-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
}

.action-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 24px;
  height: 24px;
  border: none;
  background: transparent;
  color: var(--color-text-muted);
  cursor: pointer;
  border-radius: var(--border-radius-sm);
  font-size: 12px;
  transition: all var(--transition-fast);
}

.action-btn:hover { color: var(--color-primary); }
.star-btn.active { color: #f59e0b; }
.follow-btn.active { color: var(--color-primary); }

.run-dot {
  width: 8px;
  height: 8px;
  border-radius: var(--border-radius-full);
}

.run-dot.success { background: #22c55e; }
.run-dot.failure { background: #ef4444; }
</style>
